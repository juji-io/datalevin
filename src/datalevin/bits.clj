(ns datalevin.bits
  "low level bits"
  (:require [clojure.java.io :as io]
            [datalevin.datom :as d]
            [datalevin.constants :as c]
            [taoensso.nippy :as nippy])
  (:import [java.io File DataInput DataOutput]
           [java.nio.charset StandardCharsets]
           [java.util Arrays UUID]
           [java.nio ByteBuffer]
           [datalevin.datom Datom]))

;; files

(defn delete-files
  "Recursively delete "
  [& fs]
  (when-let [f (first fs)]
    (if-let [cs (seq (.listFiles (io/file f)))]
      (recur (concat cs fs))
      (do (io/delete-file f)
          (recur (rest fs))))))

(defn file
  "Return path as File, create it if missing"
  [path]
  (let [^File f (io/file path)]
    (if (.exists f)
      f
      (do (.mkdir f)
          f))))

;; byte buffer

(defn- get-long
  "Get a long from a ByteBuffer"
  [^ByteBuffer bb]
  (.getLong bb))

(defn- get-int
  "Get an int from a ByteBuffer"
  [^ByteBuffer bb]
  (.getInt bb))

(defn- get-byte
  "Get a byte from a ByteBuffer"
  [^ByteBuffer bb]
  (.get bb))

(defn- get-bytes
  "Copy content from a ByteBuffer to a byte array, useful for
   e.g. read txn result, as buffer content is gone when txn is done"
  [^ByteBuffer bb]
  (let [n   (.remaining bb)
        arr (byte-array n)]
    (.get bb arr)
    arr))

(defn- get-data
  "Read data from a ByteBuffer"
  [^ByteBuffer bb]
  (when-let [bs (get-bytes bb)]
    (nippy/fast-thaw bs)))

(defn- check-buffer-overflow
  [^long length ^long remaining]
  (assert (<= length remaining)
          (str c/buffer-overflow
               " trying to put "
               length
               " bytes while "
               remaining
               "remaining in the ByteBuffer.")))

(defn- put-long
  ([^ByteBuffer bb n]
   (check-buffer-overflow Long/BYTES (.remaining bb))
   (.putLong bb ^long n)))

(defn- encode-float
  [x]
  (assert (not (Float/isNaN x)) "Cannot index NaN")
  (if neg?
    (bit-not (Float/floatToRawIntBits (float x)))
    (bit-flip (Float/floatToRawIntBits (float x)) 31)))

(defn- put-float
  [^ByteBuffer bb n]
  (check-buffer-overflow Float/BYTES (.remaining bb))
  (.putFloat bb (encode-float n)))

(defn- encode-double
  [x]
  (assert (not (Double/isNaN x)) "Cannot index NaN")
  (if neg?
    (bit-not (Double/doubleToRawLongBits (double x)))
    (bit-flip (Double/doubleToRawLongBits (double x)) 63)))

(defn- put-double
  [^ByteBuffer bb n]
  (check-buffer-overflow Double/BYTES (.remaining bb))
  (.putDouble bb (encode-double n)))

(defn- put-int
  [^ByteBuffer bb n]
  (check-buffer-overflow Integer/BYTES (.remaining bb))
  (.putInt bb ^int (int n)))

(defn- put-bytes
  [^ByteBuffer bb ^bytes bs]
  (check-buffer-overflow (alength bs) (.remaining bb))
  (.put bb bs))

(defn- put-byte
  [^ByteBuffer bb b]
  (check-buffer-overflow 1 (.remaining bb))
  (.put bb ^byte (byte b)))

(defn- put-data
  [^ByteBuffer bb x]
  (put-bytes bb (nippy/fast-freeze x)))

;; bytes

(defn measure-size
  "measure size of x in number of bytes"
  [x]
  (cond
    (bytes? x)         (alength ^bytes x)
    (int? x)           8
    (instance? Byte x) 1
    :else              (alength ^bytes (nippy/fast-freeze x))))

(defn hexify
  "Convert bytes to hex string"
  [bs]
  (let [hex [\0 \1 \2 \3 \4 \5 \6 \7 \8 \9 \A \B \C \D \E \F]]
    (letfn [(hexify-byte [b]
              (let [v (bit-and ^byte b 0xFF)]
                [(hex (bit-shift-right v 4)) (hex (bit-and v 0x0F))]))]
      (apply str (mapcat hexify-byte bs)))))

(defn unhexify
  "Convert hex string to byte sequence"
  [s]
  (letfn [(unhexify-2 [c1 c2]
            (unchecked-byte
             (+ (bit-shift-left (Character/digit ^char c1 16) 4)
                (Character/digit ^char c2 16))))]
    (map #(apply unhexify-2 %) (partition 2 s))))

;; datom

(nippy/extend-freeze Datom :datalevin/datom
 [^Datom x ^DataOutput out]
 (.writeLong out (.-e x))
 (nippy/freeze-to-out! out (.-a x))
 (nippy/freeze-to-out! out (.-v x))
 (.writeLong out (.-tx x)))

(nippy/extend-thaw :datalevin/datom
 [^DataInput in]
 (d/datom (.readLong in)
          (nippy/thaw-from-in! in)
          (nippy/thaw-from-in! in)
          (.readLong in)))

(defn- put-datom
  [bf ^Datom x]
  (put-bytes bf (nippy/freeze x)))

(defn- get-datom
  [bb]
  (nippy/thaw (get-bytes bb)))

;; index

(defn- string-bytes
  [^String v]
  (.getBytes v StandardCharsets/UTF_8))

(defn- keyword-bytes
  [x]
  (-> x str (subs 1) string-bytes))

(defn- put-attr
  [bf x]
  (let [^bytes a (keyword-bytes x)
        al       (alength a)]
    (assert (<= al c/+max-key-size+)
            "Attribute cannot be longer than 511 bytes")
    (put-bytes bf a)))

(defn- get-attr
  [bf]
  (let [^bytes bs (get-bytes bf)]
    (-> bs String. keyword)))

(defn- symbol-bytes
  [v]
  (-> v str string-bytes))

(defn- data-bytes
  [v]
  (nippy/fast-freeze v))

(defn- val-bytes
  "Turn value into bytes according to :db/valueType"
  [v t]
  (case t
    :db.type/keyword (keyword-bytes v)
    :db.type/symbol  (symbol-bytes v)
    :db.type/string  (string-bytes v)
    :db.type/boolean nil
    :db.type/long    nil
    :db.type/double  nil
    :db.type/float   nil
    :db.type/ref     nil
    :db.type/instant nil
    :db.type/uuid    nil
    :db.type/bytes   v
    (data-bytes v)))

(defn- val-header
  [v t]
  (case t
    :db.type/keyword c/type-keyword
    :db.type/symbol  c/type-symbol
    :db.type/string  c/type-string
    :db.type/boolean c/type-boolean
    :db.type/long    (if (neg? ^long v) c/type-long-neg c/type-long-pos)
    :db.type/float   c/type-float
    :db.type/double  c/type-double
    :db.type/ref     c/type-ref
    :db.type/instant c/type-instant
    :db.type/uuid    c/type-uuid
    :db.type/bytes   c/type-bytes
    nil))

(deftype Indexable [e a v f b h t])

(defn indexable
  "Turn datom parts into a form that is suitable for putting in indices,
  where aid is the integer id of an attribute, vt is its :db/valueType"
  [eid aid val tx vt]
  (let [hdr (val-header val vt)]
    (if-let [vb (val-bytes val vt)]
     (let [bl   (alength ^bytes vb)
           cut? (> bl c/+val-bytes-wo-hdr+)
           bas  (if cut?
                  (Arrays/copyOf ^bytes vb c/+val-bytes-trunc+)
                  vb)
           hsh  (when cut? (hash val))]
       (->Indexable eid aid nil hdr bas hsh tx))
     (->Indexable eid aid val hdr nil nil tx))))

(defn- put-uuid
  [bf ^UUID val]
  (put-long bf (.getMostSignificantBits val))
  (put-long bf (.getLeastSignificantBits val)))

(defn- get-uuid
  [bf]
  (UUID. (get-long bf) (get-long bf)))

(defn- put-native
  [bf val hdr]
  (condp = hdr
    c/type-long-pos (put-long bf val)
    c/type-long-neg (put-long bf val)
    c/type-ref      (put-long bf val)
    c/type-uuid     (put-uuid bf val)
    c/type-boolean  (put-byte bf (if val c/true-value c/false-value))
    c/type-instant  (put-long bf val)
    c/type-double   (put-double bf val)
    c/type-float    (put-float bf val)))

(defn- put-eavt
  [bf ^Indexable x]
  (put-long bf (.-e x))
  (put-int bf (.-a x))
  (when-let [hdr (.-f x)] (put-byte bf hdr))
  (if-let [bs (.-b x)] (put-bytes bf bs) (put-native bf (.-v x) (.-f x)))
  (put-byte bf c/separator)
  (put-long bf (.-t x))
  (when-let [h (.-h x)] (put-int bf h)))

(defn- put-aevt
  [bf ^Indexable x]
  (put-int bf (.-a x))
  (put-long bf (.-e x))
  (when-let [hdr (.-f x)] (put-byte bf hdr))
  (if-let [bs (.-b x)] (put-bytes bf bs) (put-native bf (.-v x) (.-f x)))
  (put-byte bf c/separator)
  (put-long bf (.-t x))
  (when-let [h (.-h x)] (put-int bf h)))

(defn- put-avet
  [bf ^Indexable x]
  (put-int bf (.-a x))
  (when-let [hdr (.-f x)] (put-byte bf hdr))
  (if-let [bs (.-b x)] (put-bytes bf bs) (put-native bf (.-v x) (.-f x)))
  (put-byte bf c/separator)
  (put-long bf (.-e x))
  (put-long bf (.-t x))
  (when-let [h (.-h x)] (put-int bf h)))

(defn- put-vaet
  [bf ^Indexable x]
  (when-let [hdr (.-f x)] (put-byte bf hdr))
  (if-let [bs (.-b x)] (put-bytes bf bs) (put-native bf (.-v x) (.-f x)))
  (put-byte bf c/separator)
  (put-int bf (.-a x))
  (put-long bf (.-e x))
  (put-long bf (.-t x))
  (when-let [h (.-h x)] (put-int bf h)))

(defn put-buffer
  "Put the given type of data x in buffer bf, x-type can be one of :long,
  :byte, :bytes, :data, :datom, :attr or index type :eavt, :aevt, :avet,
  or :vaet"
  [bf x x-type]
  (case x-type
    :long  (put-long bf x)
    :byte  (put-byte bf x)
    :bytes (put-bytes bf x)
    :attr  (put-attr bf x)
    :datom (put-datom bf x)
    :eavt  (put-eavt bf x)
    :aevt  (put-aevt bf x)
    :avet  (put-avet bf x)
    :vaet  (put-vaet bf x)
    (put-data bf x)))

(defn read-buffer
  "Get the given type of data from buffer bf, v-type can be one of
  :long, :byte, :bytes, :datom, :attr or :data"
  [^ByteBuffer bb v-type]
  (case v-type
    :long  (get-long bb)
    :byte  (get-byte bb)
    :bytes (get-bytes bb)
    :attr  (get-attr bb)
    :datom (get-datom bb)
    (get-data bb)))
