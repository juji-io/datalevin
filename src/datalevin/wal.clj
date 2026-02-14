;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.wal
  "Write Ahead Log (WAL)"
  (:require
   [clojure.java.io :as io]
   [datalevin.bits :as b]
   [datalevin.buffer :as bf]
   [datalevin.constants :as c]
   [datalevin.util :as u])
  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream
    DataOutputStream EOFException File]
   [java.util.zip CRC32C]
   [java.util Arrays]
   [java.nio ByteBuffer]
   [java.nio.channels FileChannel]
   [java.nio.file StandardOpenOption]
   [java.nio.charset StandardCharsets]))

(def ^:const wal-format-major 1)
(def ^:const wal-format-minor 0)
(def ^:private wal-record-magic (byte-array [(byte 0x44) (byte 0x4C)
                                             (byte 0x57) (byte 0x4C)]))
(def ^:private wal-meta-magic (byte-array [(byte 0x44) (byte 0x4C)
                                           (byte 0x57) (byte 0x4D)]))

(def ^:const wal-meta-version (byte 1))

(def ^:const op-kv-put (byte 0x10))
(def ^:const op-kv-del (byte 0x11))
(def ^:const op-kv-put-list (byte 0x16))
(def ^:const op-kv-del-list (byte 0x17))

(def ^:const wal-type-tuple-tag (byte 0x00))

(def ^:private wal-flag-keywords
  [:nooverwrite :nodupdata :current :reserve :append :appenddup :multiple])

(def ^:private wal-flag->id
  (zipmap wal-flag-keywords
          (mapv byte (range 1 (inc (count wal-flag-keywords))))))

(def ^:private wal-id->flag
  (into {} (map (fn [[k v]] [(int (bit-and (int v) 0xFF)) k])) wal-flag->id))

(def ^:private wal-type-keywords
  [:string :bigint :bigdec :long :float :double :bytes :keyword :symbol
   :boolean :instant :uuid :instant-pre-06 :byte :short :int :id :int-int
   :ints :bitmap :term-info :doc-info :pos-info :attr :avg :raw :datom :data])

(def ^:private wal-type->id
  (zipmap wal-type-keywords
          (mapv byte (range 1 (inc (count wal-type-keywords))))))

(def ^:private wal-id->type
  (into {} (map (fn [[k v]] [(int (bit-and (int v) 0xFF)) k])) wal-type->id))

(defn- type->byte
  [t]
  (let [t (or t :data)]
    (or (wal-type->id t)
        (u/raise "Unsupported WAL value type" {:error :wal/invalid-type
                                               :type  t}))))

(defn- byte->type
  [b]
  (or (wal-id->type (int (bit-and (int b) 0xFF)))
      (u/raise "Unknown WAL value type tag"
               {:error :wal/invalid-type-tag :tag b})))

(declare write-u16 read-u16)

(defn- flag->byte
  [f]
  (or (wal-flag->id f)
      (u/raise "Unsupported WAL flag" {:error :wal/invalid-flag
                                       :flag  f})))

(defn- byte->flag
  [b]
  (or (wal-id->flag (int (bit-and (int b) 0xFF)))
      (u/raise "Unknown WAL flag tag"
               {:error :wal/invalid-flag-tag :tag b})))

(defn- normalize-flags
  [flags]
  (cond
    (nil? flags) []
    (set? flags) (->> flags (sort-by name) vec)
    (sequential? flags) (vec flags)
    :else (u/raise "Invalid WAL flags" {:error :wal/invalid-flags
                                        :flags flags})))

(defn- write-type!
  [^DataOutputStream out t]
  (if (vector? t)
    (let [cnt (count t)]
      (when (zero? cnt)
        (u/raise "Tuple type must not be empty"
                 {:error :wal/invalid-type :type t}))
      (.writeByte out (int wal-type-tuple-tag))
      (write-u16 out cnt)
      (doseq [et t]
        (when (vector? et)
          (u/raise "Nested tuple types are not supported in WAL"
                   {:error :wal/invalid-type :type t}))
        (.writeByte out (int (type->byte et)))))
    (.writeByte out (int (type->byte t)))))

(defn- read-type
  [^DataInputStream in]
  (let [tag (.readUnsignedByte in)]
    (if (zero? ^int tag)
      (let [cnt (read-u16 in)]
        (when (zero? ^short cnt)
          (u/raise "Tuple type must not be empty"
                   {:error :wal/invalid-type-count :count cnt}))
        (vec (repeatedly cnt #(byte->type (byte (.readUnsignedByte in))))))
      (byte->type (byte tag)))))

(defn- write-flags!
  [^DataOutputStream out flags]
  (let [fs  (normalize-flags flags)
        cnt (count fs)]
    (when (> cnt 255)
      (u/raise "Too many WAL flags"
               {:error :wal/flags-overflow :count cnt}))
    (.writeByte out (int cnt))
    (doseq [f fs]
      (.writeByte out (int (flag->byte f))))))

(defn- read-flags
  [^DataInputStream in]
  (let [cnt (.readUnsignedByte in)]
    (when (pos? ^int cnt)
      (vec (repeatedly cnt #(byte->flag (byte (.readUnsignedByte in))))))))

(defn- crc32c
  ^long [^bytes bs off len]
  (let [crc (CRC32C.)]
    (.update crc bs off len)
    (.getValue crc)))

(defn- write-bytes!
  [^DataOutputStream out ^bytes bs]
  (.write out bs 0 (alength bs)))

(defn- read-bytes
  [^DataInputStream in n]
  (let [bs (byte-array n)]
    (.readFully in bs)
    bs))

(defn- encode-bits
  ^bytes [x t]
  (let [t (or t :data)]
    (loop [^ByteBuffer buf (bf/get-tl-buffer)]
      (let [result (try
                     (b/put-bf buf x t)
                     (b/get-bytes (.duplicate buf))
                     (catch Exception e
                       (if (instance? java.nio.BufferOverflowException e)
                         ::overflow
                         (throw e))))]
        (if (not= result ::overflow)
          result
          (let [new-buf (ByteBuffer/allocate (* 2 (.capacity buf)))]
            (bf/set-tl-buffer new-buf)
            (recur new-buf)))))))

(defn- dbi-name-bytes
  ^bytes [dbi-name]
  (.getBytes ^String dbi-name StandardCharsets/UTF_8))

(defn- write-u16
  [^DataOutputStream out ^long v]
  (.writeShort out (int v)))

(defn- write-u32
  [^DataOutputStream out ^long v]
  (.writeInt out (unchecked-int v)))

(defn- write-u64
  [^DataOutputStream out ^long v]
  (.writeLong out v))

(defn- read-u16
  [^DataInputStream in]
  (bit-and (.readUnsignedShort in) 0xFFFF))

(defn- read-u32
  [^DataInputStream in]
  (bit-and (.readInt in) 0xFFFFFFFF))

(defn- read-u64
  [^DataInputStream in]
  (.readLong in))

(defn- normalize-type
  [t]
  (or t :data))

(defn- write-kv-op!
  "Encode and write a single KV op directly to the output stream."
  [^DataOutputStream out op-map]
  (let [{:keys [op dbi k v kt vt flags k-bytes dbi-bytes]} op-map
        ^bytes dbi-bs (or dbi-bytes (dbi-name-bytes dbi))
        k-type        (normalize-type kt)
        ^bytes k-bs   (or k-bytes (encode-bits k k-type))
        dbi-len       (alength dbi-bs)
        k-len         (alength k-bs)]
    (case op
      :put
      (let [v-type  (normalize-type vt)
            ^bytes v-bs (encode-bits v v-type)
            v-len   (alength v-bs)]
        (.writeByte out (int op-kv-put))
        (write-u16 out dbi-len) (write-bytes! out dbi-bs)
        (write-type! out k-type) (write-u16 out k-len) (write-bytes! out k-bs)
        (write-type! out v-type) (write-u32 out v-len) (write-bytes! out v-bs)
        (write-flags! out flags))

      :del
      (do
        (.writeByte out (int op-kv-del))
        (write-u16 out dbi-len) (write-bytes! out dbi-bs)
        (write-type! out k-type) (write-u16 out k-len) (write-bytes! out k-bs)
        (write-flags! out flags))

      :put-list
      (let [v-type  (normalize-type vt)
            ^bytes v-bs (encode-bits v :data)
            v-len   (alength v-bs)]
        (.writeByte out (int op-kv-put-list))
        (write-u16 out dbi-len) (write-bytes! out dbi-bs)
        (write-type! out k-type) (write-u16 out k-len) (write-bytes! out k-bs)
        (write-type! out v-type) (write-u32 out v-len) (write-bytes! out v-bs)
        (write-flags! out flags))

      :del-list
      (let [v-type  (normalize-type vt)
            ^bytes v-bs (encode-bits v :data)
            v-len   (alength v-bs)]
        (.writeByte out (int op-kv-del-list))
        (write-u16 out dbi-len) (write-bytes! out dbi-bs)
        (write-type! out k-type) (write-u16 out k-len) (write-bytes! out k-bs)
        (write-type! out v-type) (write-u32 out v-len) (write-bytes! out v-bs)
        (write-flags! out flags))

      (u/raise "Unsupported KV WAL op" {:error :wal/invalid-op :op op}))))

(defn- decode-kv-op
  [^DataInputStream in]
  (let [opcode  (byte (.readUnsignedByte in))
        dbi-len (read-u16 in)
        dbi-bs  (read-bytes in dbi-len)
        dbi     (String. ^bytes dbi-bs StandardCharsets/UTF_8)
        k-type  (read-type in)
        k-len   (read-u16 in)
        k-bs    (read-bytes in k-len)]
    (case opcode
      0x10 (let [v-type (read-type in)
                 v-len  (read-u32 in)
                 v-bs   (read-bytes in v-len)
                 flags  (read-flags in)]
             (cond-> {:op :put :dbi dbi :k k-bs :kt k-type
                      :v v-bs :vt v-type :raw? true}
               (seq flags) (assoc :flags flags)))
      0x11 (let [flags (read-flags in)]
             (cond-> {:op :del :dbi dbi :k k-bs :kt k-type :raw? true}
               (seq flags) (assoc :flags flags)))
      0x16 (let [v-type (read-type in)
                 v-len  (read-u32 in)
                 v-bs   (read-bytes in v-len)
                 flags  (read-flags in)]
             (cond-> {:op :put-list :dbi dbi :k k-bs :kt k-type
                      :v v-bs :vt v-type :raw? true}
               (seq flags) (assoc :flags flags)))
      0x17 (let [v-type (read-type in)
                 v-len  (read-u32 in)
                 v-bs   (read-bytes in v-len)
                 flags  (read-flags in)]
             (cond-> {:op :del-list :dbi dbi :k k-bs :kt k-type
                      :v v-bs :vt v-type :raw? true}
               (seq flags) (assoc :flags flags)))
      (u/raise "Unknown WAL opcode" {:error  :wal/invalid-opcode
                                     :opcode opcode}))))

(def ^:const ^:private record-header-size 14)

(def ^:private ^ThreadLocal tl-record-bos
  (ThreadLocal/withInitial
    (reify java.util.function.Supplier
      (get [_] (ByteArrayOutputStream. 512)))))

(defn record-bytes
  [wal-id user-tx-id tx-time ops]
  (let [op-count (count ops)
        ^ByteArrayOutputStream bos (.get tl-record-bos)
        _        (.reset bos)
        out      (DataOutputStream. bos)]
    ;; header with placeholders for body-len and checksum
    (write-bytes! out wal-record-magic)
    (.writeByte out (int wal-format-major))
    (.writeByte out 0)
    (write-u32 out 0)                          ;; body-len placeholder
    (write-u32 out 0)                          ;; checksum placeholder
    ;; body
    (write-u64 out wal-id)
    (write-u64 out user-tx-id)
    (write-u64 out user-tx-id)
    (write-u32 out 1)
    ;; user-tx entry
    (write-u64 out user-tx-id)
    (write-u64 out tx-time)
    (write-u32 out 0)                          ;; tx-meta-len
    (write-u32 out 0)                          ;; op-start
    (write-u32 out op-count)                   ;; op-end (exclusive)
    (write-u32 out op-count)
    (doseq [op ops]
      (write-kv-op! out op))
    (.close out)
    (let [buf      (.toByteArray bos)
          body-len (- (alength buf) record-header-size)
          checksum (crc32c buf record-header-size body-len)
          bb       (ByteBuffer/wrap buf)]
      ;; patch body-len at offset 6 and checksum at offset 10
      (.putInt bb 6 (unchecked-int body-len))
      (.putInt bb 10 (unchecked-int checksum))
      buf)))

(defn wal-dir-path
  [dir]
  (str dir u/+separator+ c/wal-dir))

(defn wal-meta-path
  [dir]
  (str (wal-dir-path dir) u/+separator+ c/wal-meta-file))

(defn segment-path
  [dir segment-id]
  (format "%s%ssegment-%016d.wal" (wal-dir-path dir) u/+separator+ segment-id))

(defn segment-files
  "Return a sorted seq of WAL segment files in the given directory."
  [dir]
  (let [wal-dir (io/file (wal-dir-path dir))]
    (when (.exists wal-dir)
      (->> (.listFiles wal-dir)
           (filter #(and (.isFile ^File %)
                         (.endsWith (.getName ^File %) ".wal")))
           (sort-by #(.getName ^File %))))))

(defn parse-segment-id
  [^File f]
  (let [name (.getName f)
        n    (count name)]
    (try
      (Long/parseLong (subs name 8 (- n 4)))
      (catch Exception _ nil))))

(defn- open-channel
  [path]
  (FileChannel/open (.toPath (io/file path))
                    (into-array StandardOpenOption
                                [StandardOpenOption/CREATE
                                 StandardOpenOption/WRITE
                                 StandardOpenOption/APPEND])))

(defn sync-channel!
  [^FileChannel ch mode]
  (case mode
    :none nil
    :fdatasync (.force ch false)
    :fsync (.force ch true)
    (.force ch true)))

(defn- read-record
  [^DataInputStream in]
  (let [^bytes magic (read-bytes in 4)]
    (when (not (Arrays/equals magic ^bytes wal-record-magic))
      (u/raise "Bad WAL record magic" {:error :wal/bad-magic}))
    (let [version  (.readUnsignedByte in)
          _flags   (.readUnsignedByte in)
          body-len (read-u32 in)
          checksum (read-u32 in)
          body     (read-bytes in body-len)
          actual   (crc32c body 0 body-len)]
      (when (not= checksum actual)
        (u/raise "WAL record checksum mismatch"
                 {:error    :wal/bad-checksum
                  :expected checksum
                  :actual   actual}))
      (when (not= version wal-format-major)
        (u/raise "WAL format version mismatch"
                 {:error    :wal/bad-version
                  :expected wal-format-major
                  :actual   version}))
      (with-open [bin (ByteArrayInputStream. body)
                  din (DataInputStream. bin)]
        (let [wal-id      (read-u64 din)
              _user-start (read-u64 din)
              _user-end   (read-u64 din)
              user-count  (read-u32 din)
              _entries    (dotimes [_ user-count]
                            (read-u64 din)
                            (read-u64 din)
                            (let [^int meta-len (read-u32 din)]
                              (when (pos? meta-len)
                                (read-bytes din meta-len)))
                            (read-u32 din)
                            (read-u32 din))
              op-count    ^int (read-u32 din)
              ops         (loop [i 0 acc []]
                            (if (< i op-count)
                              (recur (inc i) (conj acc (decode-kv-op din)))
                              acc))]
          {:wal/tx-id wal-id :wal/ops ops})))))

(defn read-wal-records
  [dir from-id upto-id]
  (let [from-id (long (or from-id 0))
        upto-id (long (or upto-id Long/MAX_VALUE))]
    (lazy-seq
      (mapcat
        (fn [^File f]
          (let [path (.getAbsolutePath f)]
            (with-open [in (DataInputStream. (io/input-stream path))]
              (loop [acc (transient [])]
                (let [rec (try
                            (read-record in)
                            (catch EOFException _ ::eof))]
                  (if (= rec ::eof)
                    (persistent! acc)
                    (let [wal-id (long (:wal/tx-id rec))]
                      (cond
                        (<= wal-id from-id) (recur acc)
                        (<= wal-id upto-id) (recur (conj! acc rec))
                        :else               (persistent! acc)))))))))
        (segment-files dir)))))

(defn scan-last-wal
  [dir]
  (let [files (segment-files dir)]
    (loop [fs        files
           last-id   0
           last-seg  0
           last-time 0]
      (if-let [^File f (first fs)]
        (let [seg-id (long (or (parse-segment-id f) 0))
              path   (.getAbsolutePath f)
              cur-id (with-open [in (DataInputStream. (io/input-stream path))]
                       (loop [cid last-id]
                         (let [rec (try
                                     (read-record in)
                                     (catch EOFException _ ::eof))]
                           (if (= rec ::eof)
                             cid
                             (recur (long (:wal/tx-id rec)))))))]
          (recur (next fs)
                 (long cur-id)
                 (max last-seg seg-id)
                 (max last-time (.lastModified f))))
        {:last-wal-id     last-id
         :last-segment-id last-seg
         :last-segment-ms last-time}))))

(defn segment-max-wal-id
  "Return the highest wal-id in a segment file, or 0 if empty."
  [^File f]
  (let [path (.getAbsolutePath f)]
    (with-open [in (DataInputStream. (io/input-stream path))]
      (loop [max-id 0]
        (let [rec (try
                    (read-record in)
                    (catch EOFException _ ::eof))]
          (if (= rec ::eof)
            max-id
            (recur (long (:wal/tx-id rec)))))))))

(def ^:private wal-meta-slot-size 64)
(def ^:private wal-meta-payload-off 9)
(def ^:private wal-meta-off-committed-wal 9)
(def ^:private wal-meta-off-committed-user 17)
(def ^:private wal-meta-off-committed-last-ms 25)
(def ^:private wal-meta-off-indexed-wal 33)
(def ^:private wal-meta-off-revision 41)
(def ^:private wal-meta-off-format-major 49)
(def ^:private wal-meta-off-format-minor 50)
(def ^:private wal-meta-off-enabled 51)
(def ^:private wal-meta-off-last-segment 52)

(defn- meta-slot-bytes
  [snapshot]
  (let [buf (byte-array wal-meta-slot-size)
        bb  (ByteBuffer/wrap buf)]
    (b/put-bytes bb wal-meta-magic)
    (b/put-byte bb wal-meta-version)
    (.position bb ^int wal-meta-payload-off)
    (b/put-long bb (get snapshot c/last-committed-wal-tx-id 0))
    (b/put-long bb (get snapshot c/last-committed-user-tx-id 0))
    (b/put-long bb (get snapshot c/committed-last-modified-ms 0))
    (b/put-long bb (get snapshot c/last-indexed-wal-tx-id 0))
    (b/put-long bb (get snapshot c/wal-meta-revision 0))
    (b/put-byte bb wal-format-major)
    (b/put-byte bb wal-format-minor)
    (b/put-byte bb (if (get snapshot :wal/enabled? true) 1 0))
    (b/put-long bb (get snapshot :wal/last-segment-id 0))
    (.position bb ^int wal-meta-slot-size)
    (let [checksum (crc32c buf wal-meta-payload-off
                           (- ^int wal-meta-slot-size
                              ^int wal-meta-payload-off))]
      (aset-byte buf 5 (unchecked-byte (bit-shift-right checksum 24)))
      (aset-byte buf 6 (unchecked-byte (bit-shift-right checksum 16)))
      (aset-byte buf 7 (unchecked-byte (bit-shift-right checksum 8)))
      (aset-byte buf 8 (unchecked-byte checksum)))
    buf))

(defn- read-slot
  [^bytes slot]
  (when (and slot (= (alength slot) wal-meta-slot-size))
    (let [magic (byte-array 4)]
      (System/arraycopy slot 0 magic 0 4)
      (when (Arrays/equals ^bytes magic ^bytes wal-meta-magic)
        (let [version  (aget slot 4)
              c1       (bit-and (int (aget slot 5)) 0xFF)
              c2       (bit-and (int (aget slot 6)) 0xFF)
              c3       (bit-and (int (aget slot 7)) 0xFF)
              c4       (bit-and (int (aget slot 8)) 0xFF)
              checksum (bit-or (bit-shift-left c1 24)
                               (bit-shift-left c2 16)
                               (bit-shift-left c3 8)
                               c4)
              actual   (crc32c slot wal-meta-payload-off
                               (- ^int wal-meta-slot-size
                                  ^int wal-meta-payload-off))]
          (when (and (= (byte wal-meta-version) version)
                     (= checksum actual))
            (let [bb (ByteBuffer/wrap slot)]
              {c/last-committed-wal-tx-id
               (.getLong bb wal-meta-off-committed-wal)
               c/last-committed-user-tx-id
               (.getLong bb wal-meta-off-committed-user)
               c/committed-last-modified-ms
               (.getLong bb wal-meta-off-committed-last-ms)
               c/last-indexed-wal-tx-id
               (.getLong bb wal-meta-off-indexed-wal)
               c/wal-meta-revision
               (.getLong bb wal-meta-off-revision)
               :wal-format-major
               (bit-and (int (.get bb ^int wal-meta-off-format-major)) 0xFF)
               :wal-format-minor
               (bit-and (int (.get bb ^int wal-meta-off-format-minor)) 0xFF)
               :wal/enabled?
               (= 1 (.get bb ^int wal-meta-off-enabled))
               :wal/last-segment-id
               (.getLong bb wal-meta-off-last-segment)})))))))

(defn read-wal-meta
  [dir]
  (let [f (io/file (wal-meta-path dir))]
    (when (.exists f)
      (with-open [in (DataInputStream. (io/input-stream f))]
        (let [slot-a (read-bytes in 64)
              slot-b (read-bytes in 64)
              a      (read-slot slot-a)
              b      (read-slot slot-b)]
          (cond
            (and a b)
            (let [ra ^long (get a c/wal-meta-revision)
                  rb ^long (get b c/wal-meta-revision)]
              (cond
                (> ra rb)                                    a
                (< ra rb)                                    b
                (> ^long (get a c/last-committed-wal-tx-id)
                   ^long (get b c/last-committed-wal-tx-id)) a
                :else                                        b))
            a     a
            b     b
            :else nil))))))

(defn publish-wal-meta!
  [dir snapshot]
  (u/file (wal-dir-path dir))
  (let [f          (io/file (wal-meta-path dir))
        existed?   (.exists f)
        existing   (read-wal-meta dir)
        rev        (u/long-inc (or (get existing c/wal-meta-revision) 0))
        merged     (merge existing snapshot
                          {c/wal-meta-revision rev
                           :wal/enabled?       true
                           :wal-format-major   wal-format-major
                           :wal-format-minor   wal-format-minor})
        slot-bytes (meta-slot-bytes merged)
        empty-slot (byte-array 64)
        use-a?     (odd? rev)
        slot-a     (if use-a?
                     slot-bytes
                     (if existing (meta-slot-bytes existing) empty-slot))
        slot-b     (if use-a?
                     (if existing (meta-slot-bytes existing) empty-slot)
                     slot-bytes)
        payload    (byte-array 128)
        metadata?  (not= c/*wal-sync-mode* :none)]
    (System/arraycopy slot-a 0 payload 0 64)
    (System/arraycopy slot-b 0 payload 64 64)
    (with-open [ch (FileChannel/open
                     (.toPath f)
                     (into-array StandardOpenOption
                                 [StandardOpenOption/CREATE
                                  StandardOpenOption/WRITE
                                  StandardOpenOption/TRUNCATE_EXISTING]))]
      (.write ch (ByteBuffer/wrap payload))
      (when metadata? (sync-channel! ch c/*wal-sync-mode*)))
    (when (and (not existed?) metadata?)
      (try
        (with-open [dch (FileChannel/open
                          (.toPath (io/file (wal-dir-path dir)))
                          (into-array StandardOpenOption
                                      [StandardOpenOption/READ]))]
          (sync-channel! dch c/*wal-sync-mode*))
        (catch Exception _ nil)))
    merged))

(defn open-segment-channel
  "Open a FileChannel for a WAL segment. Caller is responsible for closing."
  ^FileChannel [dir segment-id]
  (u/file (wal-dir-path dir))
  (open-channel (segment-path dir segment-id)))

(defn close-segment-channel!
  "Close a cached WAL segment channel if open."
  [^FileChannel ch]
  (when (and ch (.isOpen ch))
    (.close ch)))

(defn append-record-bytes!
  "Append a WAL record. When ch is provided, uses it directly (caller manages
  lifecycle). When ch is nil, opens and closes a channel per call."
  ([dir segment-id ^bytes record sync-mode]
   (u/file (wal-dir-path dir))
   (with-open [^FileChannel ch (open-channel (segment-path dir segment-id))]
     (.write ch (ByteBuffer/wrap record))
     (sync-channel! ch sync-mode))
   (alength ^bytes record))
  ([^FileChannel ch ^bytes record sync-mode]
   (.write ch (ByteBuffer/wrap record))
   (sync-channel! ch sync-mode)
   (alength ^bytes record)))

(defn append-kv-record!
  [dir segment-id wal-id user-tx-id tx-time ops sync-mode]
  (append-record-bytes!
    dir segment-id (record-bytes wal-id user-tx-id tx-time ops) sync-mode))
