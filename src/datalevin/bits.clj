(ns datalevin.bits
  "low level bits"
  (:require [clojure.java.io :as io]
            [datalevin.datom :as d]
            [datalevin.constants :as c]
            [taoensso.nippy :as nippy])
  (:import [java.io File DataInput DataOutput]
           [java.nio.charset StandardCharsets]
           [java.util Arrays]
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
  [^ByteBuffer bb n]
  (check-buffer-overflow Long/BYTES (.remaining bb))
  (.putLong bb ^long (long n)))

(defn- put-int
  [^ByteBuffer bb n]
  (check-buffer-overflow Integer/BYTES (.remaining bb))
  (.putInt bb ^int (int n)))

(defn- put-bytes
  [^ByteBuffer bb ^bytes bs]
  (let [len (alength bs)]
    (assert (< 0 len) "Cannot put empty byte array into ByteBuffer")
    (check-buffer-overflow len (.remaining bb))
    (.put bb bs)))

(defn- put-byte
  [^ByteBuffer bb b]
  (check-buffer-overflow 1 (.remaining bb))
  (.put bb ^byte (byte b)))

(defn- put-data
  [^ByteBuffer bb x]
  (put-bytes bb (nippy/fast-freeze x)))

(defn measure-size
  "measure size of x in number of bytes"
  [x]
  (cond
    (bytes? x)         (alength ^bytes x)
    (integer? x)       8
    (instance? Byte x) 1
    :else              (alength ^bytes (nippy/fast-freeze x))))

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

(deftype DatomIndexable [e a p h t])

(defn datom-indexable
  "Turn datom to a form that is suitable for putting in indices"
  [^Datom d]
  (let [^bytes a (-> (.-a d) str (subs 1) (.getBytes StandardCharsets/UTF_8))
        al       (alength a)
        _        (assert (<= al c/+max-attr-size+)
                         "Attribute name cannot be longer than 400 bytes")
        ^bytes v (nippy/freeze (.-v d))
        vl       (alength v)
        p        (if (<= (+ al vl) c/+idx-attr+prefix-size+)
                   v
                   (Arrays/copyOf
                    v (- c/+idx-attr+prefix-size+ al Integer/BYTES)))
        h        (when-not (identical? p v) (hash (.-v d)))]
    (->DatomIndexable (.-e d) a p h (.-tx d))))

(defn- put-eavt
  [bf ^DatomIndexable x]
  (put-long bf (.-e x))
  (put-bytes bf (.-a x))
  (put-byte bf c/separator)
  (put-bytes bf (.-p x))
  (put-byte bf c/separator)
  (put-long bf (.-t x))
  (when-let [h (.-h x)] (put-int bf h)))

(defn- put-aevt
  [bf ^DatomIndexable x]
  (put-bytes bf (.-a x))
  (put-byte bf c/separator)
  (put-long bf (.-e x))
  (put-bytes bf (.-p x))
  (put-byte bf c/separator)
  (put-long bf (.-t x))
  (when-let [h (.-h x)] (put-int bf h)))

(defn- put-avet
  [bf ^DatomIndexable x]
  (put-bytes bf (.-a x))
  (put-byte bf c/separator)
  (put-bytes bf (.-p x))
  (put-byte bf c/separator)
  (put-long bf (.-e x))
  (put-long bf (.-t x))
  (when-let [h (.-h x)] (put-int bf h)))

(defn- put-vaet
  [bf ^DatomIndexable x]
  (put-bytes bf (.-p x))
  (put-byte bf c/separator)
  (put-bytes bf (.-a x))
  (put-byte bf c/separator)
  (put-long bf (.-e x))
  (put-long bf (.-t x))
  (when-let [h (.-h x)] (put-int bf h)))

(defn put-buffer
  "Put the given type of data x in buffer bf, x-type can be one of :long,
  :byte, :bytes, :data, :datom or index type :eavt, :aevt, :avet, :vaet"
  [bf x x-type]
  (case x-type
    :long  (put-long bf x)
    :byte  (put-byte bf x)
    :bytes (put-bytes bf x)
    :datom (put-datom bf x)
    :eavt  (put-eavt bf x)
    :aevt  (put-aevt bf x)
    :avet  (put-avet bf x)
    :vaet  (put-vaet bf x)
    (put-data bf x)))

(defn read-buffer
  "Get the given type of data from buffer bf, v-type can be one of
  :long, :byte, :bytes, :datom or :data"
  [^ByteBuffer bb v-type]
  (case v-type
    :long  (get-long bb)
    :byte  (get-byte bb)
    :bytes (get-bytes bb)
    :datom (get-datom bb)
    (get-data bb)))
