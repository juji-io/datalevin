(ns datalevin.bits
  "Handle binary encoding, byte buffers, etc."
  (:require [clojure.java.io :as io]
            [datalevin.datom :as d]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [taoensso.nippy :as nippy])
  (:import [java.io File DataInput DataOutput]
           [java.nio.charset StandardCharsets]
           [java.util Arrays UUID]
           [java.nio ByteBuffer]
           [datalevin.datom Datom]))

;; files

(defn ^:no-doc delete-files
  "Recursively delete "
  [& fs]
  (when-let [f (first fs)]
    (if-let [cs (seq (.listFiles (io/file f)))]
      (recur (concat cs fs))
      (do (io/delete-file f)
          (recur (rest fs))))))

(defn ^:no-doc file
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
  ([^ByteBuffer bb]
   (let [n  (.remaining bb)
         ba (byte-array n)]
     (.get bb ba)
     ba))
  ([^ByteBuffer bb n]
   (let [ba (byte-array n)]
     (.get bb ba 0 n)
     ba)))

(defn- get-bytes-val
  [^ByteBuffer bf ^long post-v]
  (get-bytes bf (- (.remaining bf) post-v)))

(defn- get-data
  "Read data from a ByteBuffer"
  ([^ByteBuffer bb]
   (when-let [bs (get-bytes bb)]
     (nippy/fast-thaw bs)))
  ([^ByteBuffer bb n]
   (when-let [bs (get-bytes-val bb n)]
     (nippy/fast-thaw bs))))

(def ^:no-doc ^:const float-sign-idx 31)
(def ^:no-doc ^:const double-sign-idx 63)

(defn- decode-float
  [x]
  (if (bit-test x float-sign-idx)
    (Float/intBitsToFloat (bit-flip x float-sign-idx))
    (Float/intBitsToFloat (bit-not ^int x))))

(defn- get-float
  [^ByteBuffer bb]
  (decode-float (.getInt bb)))

(defn- decode-double
  [x]
  (if (bit-test x double-sign-idx)
    (Double/longBitsToDouble (bit-flip x double-sign-idx))
    (Double/longBitsToDouble (bit-not ^long x))))

(defn- get-double
  [^ByteBuffer bb]
  (decode-double (.getLong bb)))

(defn- get-boolean
  [^ByteBuffer bb]
  (case (short (get-byte bb))
    2 true
    1 false
    (u/raise "Illegal value in buffer, expecting a boolean" {})))

(defn- check-buffer-overflow
  [^long length ^long remaining]
  (when (< remaining length)
    (u/raise (str c/buffer-overflow
                  " trying to put "
                  length
                  " bytes while "
                  remaining
                  " remaining in the ByteBuffer.")
             {})))

(defn- put-long
  ([^ByteBuffer bb n]
   (check-buffer-overflow Long/BYTES (.remaining bb))
   (.putLong bb ^long n)))

(defn- encode-float
  [x]
  (assert (not (Float/isNaN x)) "Cannot index NaN")
  (let [x (if (= x -0.0) 0.0 x)]
    (if (neg? ^float x)
      (bit-not (Float/floatToRawIntBits x))
      (bit-flip (Float/floatToRawIntBits x) float-sign-idx))))

(defn- put-float
  [^ByteBuffer bb n]
  (check-buffer-overflow Float/BYTES (.remaining bb))
  (.putInt bb (encode-float n)))

(defn- encode-double
  [^double x]
  (assert (not (Double/isNaN x)) "Cannot index NaN")
  (let [x (if (= x -0.0) 0.0 x)]
    (if (neg? x)
      (bit-not (Double/doubleToRawLongBits x))
      (bit-flip (Double/doubleToRawLongBits x) double-sign-idx))))

(defn- put-double
  [^ByteBuffer bb n]
  (check-buffer-overflow Double/BYTES (.remaining bb))
  (.putLong bb (encode-double n)))

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
  (.put bb ^byte (unchecked-byte b)))

(defn- put-data
  [^ByteBuffer bb x]
  (put-bytes bb (nippy/fast-freeze x)))

;; bytes

(defn ^:no-doc measure-size
  "measure size of x in number of bytes"
  [x]
  (cond
    (bytes? x)         (alength ^bytes x)
    (int? x)           8
    (instance? Byte x) 1
    :else              (alength ^bytes (nippy/fast-freeze x))))

(defn ^:no-doc hexify
  "Convert bytes to hex string"
  [bs]
  (let [hex [\0 \1 \2 \3 \4 \5 \6 \7 \8 \9 \A \B \C \D \E \F]]
    (letfn [(hexify-byte [b]
              (let [v (bit-and ^byte b 0xFF)]
                [(hex (bit-shift-right v 4)) (hex (bit-and v 0x0F))]))]
      (apply str (mapcat hexify-byte bs)))))

(defn ^:no-doc unhexify
  "Convert hex string to byte sequence"
  [s]
  (letfn [(unhexify-2 [c1 c2]
            (unchecked-byte
             (+ (bit-shift-left (Character/digit ^char c1 16) 4)
                (Character/digit ^char c2 16))))]
    (map #(apply unhexify-2 %) (partition 2 s))))

;; nippy

(nippy/extend-freeze Datom :datalevin/datom
 [^Datom x ^DataOutput out]
 (.writeLong out (.-e x))
 (nippy/freeze-to-out! out (.-a x))
 (nippy/freeze-to-out! out (.-v x)))

(nippy/extend-thaw :datalevin/datom
 [^DataInput in]
 (d/datom (.readLong in)
          (nippy/thaw-from-in! in)
          (nippy/thaw-from-in! in)
          c/tx0))

;; datom

(defn- put-datom
  [bf ^Datom x]
  (put-bytes bf (nippy/freeze x)))

(defn- get-datom
  [bb]
  (nippy/thaw (get-bytes bb)))

;; index

(defmacro ^:no-doc wrap-extrema
  [v vmin vmax b]
  `(if (keyword? ~v)
     (condp = ~v
       c/v0   ~vmin
       c/vmax ~vmax
       (u/raise "Illegal keyword value " ~v {}))
     ~b))

(defn- string-bytes
  [^String v]
  (wrap-extrema v c/min-bytes c/max-bytes (.getBytes v StandardCharsets/UTF_8)))

(defn- key-sym-bytes
  [x]
  (let [^bytes nmb (string-bytes (name x))
        nml        (alength nmb)]
    (if-let [ns (namespace x)]
      (let [^bytes nsb (string-bytes ns)
            nsl        (alength nsb)
            res        (byte-array (+ nsl nml 1))]
        (System/arraycopy nsb 0 res 0 nsl)
        (System/arraycopy c/separator-ba 0 res nsl 1)
        (System/arraycopy nmb 0 res (inc nsl) nml)
        res)
      (let [res (byte-array (inc nml))]
        (System/arraycopy c/separator-ba 0 res 0 1)
        (System/arraycopy nmb 0 res 1 nml)
        res))))

(defn- keyword-bytes
  [x]
  (condp = x
    c/v0   c/min-bytes
    c/vmax c/max-bytes
    (key-sym-bytes x)))

(defn- symbol-bytes
  [x]
  (wrap-extrema x c/min-bytes c/max-bytes (key-sym-bytes x)))

(defn- data-bytes
  [v]
  (condp = v
    c/v0   c/min-bytes
    c/vmax c/max-bytes
    (nippy/fast-freeze v)))

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
    :db.type/bytes   (wrap-extrema v c/min-bytes c/max-bytes v)
    (data-bytes v)))

(defn- long-header
  [v]
  (if (keyword? v)
    (condp = v
      c/v0   c/type-long-neg
      c/vmax c/type-long-pos
      (u/raise "Illegal keyword value " v {}))
    (if (neg? ^long v)
      c/type-long-neg
      c/type-long-pos)))

(defn- val-header
  [v t]
  (case t
    :db.type/sysMin  c/separator
    :db.type/sysMax  c/truncator
    :db.type/keyword c/type-keyword
    :db.type/symbol  c/type-symbol
    :db.type/string  c/type-string
    :db.type/boolean c/type-boolean
    :db.type/float   c/type-float
    :db.type/double  c/type-double
    :db.type/ref     c/type-ref
    :db.type/instant c/type-instant
    :db.type/uuid    c/type-uuid
    :db.type/bytes   c/type-bytes
    :db.type/long    (long-header v)
    nil))

(deftype ^:no-doc Indexable [e a v f b h])

(defn ^:no-doc indexable
  "Turn datom parts into a form that is suitable for putting in indices,
  where aid is the integer id of an attribute, vt is its :db/valueType"
  [eid aid val vt]
  (let [hdr (val-header val vt)]
    (if-let [vb (val-bytes val vt)]
      (let [bl   (alength ^bytes vb)
            cut? (> bl c/+val-bytes-wo-hdr+)
            bas  (if cut?
                   (Arrays/copyOf ^bytes vb c/+val-bytes-trunc+)
                   vb)
            hsh  (when cut? (hash val))]
        (->Indexable eid aid val hdr bas hsh))
      (->Indexable eid aid val hdr nil nil))))

(defn ^:no-doc giant?
  [^Indexable i]
  (.-h i))

(defn- put-uuid
  [bf ^UUID val]
  (put-long bf (.getMostSignificantBits val))
  (put-long bf (.getLeastSignificantBits val)))

(defn- get-uuid
  [bf]
  (UUID. (get-long bf) (get-long bf)))

(defn- put-native
  [bf val hdr]
  (case (short hdr)
    -64 (put-long bf (wrap-extrema val Long/MIN_VALUE -1 val))
    -63 (put-long bf (wrap-extrema val 0 Long/MAX_VALUE val))
    -11 (put-float bf (wrap-extrema val Float/NEGATIVE_INFINITY
                                    Float/POSITIVE_INFINITY val))
    -10 (put-double bf (wrap-extrema val Double/NEGATIVE_INFINITY
                                     Double/POSITIVE_INFINITY val))
    -9  (put-long bf (wrap-extrema val 0 Long/MAX_VALUE val))
    -8  (put-long bf (wrap-extrema val c/e0 Long/MAX_VALUE val))
    -7  (put-uuid bf (wrap-extrema val c/min-uuid c/max-uuid val))
    -3  (put-byte bf (wrap-extrema val c/false-value c/true-value
                                   (if val c/true-value c/false-value)))))

(defn- put-eav
  [bf ^Indexable x]
  (put-long bf (.-e x))
  (put-int bf (.-a x))
  (when-let [hdr (.-f x)] (put-byte bf hdr))
  (if-let [bs (.-b x)]
    (do (put-bytes bf bs)
        (when (.-h x) (put-byte bf c/truncator)))
    (put-native bf (.-v x) (.-f x)))
  (put-byte bf c/separator)
  (when-let [h (.-h x)] (put-int bf h)))

(defn- put-aev
  [bf ^Indexable x]
  (put-int bf (.-a x))
  (put-long bf (.-e x))
  (when-let [hdr (.-f x)] (put-byte bf hdr))
  (if-let [bs (.-b x)]
    (do (put-bytes bf bs)
        (when (.-h x) (put-byte bf c/truncator)))
     (put-native bf (.-v x) (.-f x)))
  (put-byte bf c/separator)
  (when-let [h (.-h x)] (put-int bf h)))

(defn- put-ave
  [bf ^Indexable x]
  (put-int bf (.-a x))
  (when-let [hdr (.-f x)] (put-byte bf hdr))
  (if-let [bs (.-b x)]
    (do (put-bytes bf bs)
        (when (.-h x) (put-byte bf c/truncator)))
    (put-native bf (.-v x) (.-f x)))
  (put-byte bf c/separator)
  (put-long bf (.-e x))
  (when-let [h (.-h x)] (put-int bf h)))

(defn- put-vae
  [bf ^Indexable x]
  (put-native bf (.-v x) (.-f x))
  (put-int bf (.-a x))
  (put-long bf (.-e x)))

(defn- sep->slash
  [^bytes bs]
  (let [n-1 (dec (alength bs))]
    (loop [i 0]
      (cond
        (= (aget bs i) c/separator) (aset-byte bs i c/slash)
        (< i n-1)                   (recur (inc i)))))
  bs)

(defn- get-string
  [^ByteBuffer bf ^long post-v]
  (String. ^bytes (get-bytes-val bf post-v)))

(defn- get-key-sym-str
  [^ByteBuffer bf ^long post-v]
  (let [^bytes ba (sep->slash (get-bytes-val bf post-v))
        s         (String. ^bytes ba)]
    (if (= (aget ba 0) c/slash) (subs s 1) s)))

(defn- get-keyword
  [^ByteBuffer bf ^long post-v]
  (keyword (get-key-sym-str bf post-v)))

(defn- get-symbol
  [^ByteBuffer bf ^long post-v]
  (symbol (get-key-sym-str bf post-v)))

(defn- get-value
  [^ByteBuffer bf post-v]
  (.mark bf)
  (case (short (get-byte bf))
    -64 (get-long bf)
    -63 (get-long bf)
    -11 (get-float bf)
    -10 (get-double bf)
    -9  (get-long bf)
    -8  (get-long bf)
    -7  (get-uuid bf)
    -6  (get-string bf post-v)
    -5  (get-keyword bf post-v)
    -4  (get-symbol bf post-v)
    -3  (get-boolean bf)
    -2  (get-bytes-val bf post-v)
    (do (.reset bf)
        (get-data bf post-v))))

(deftype ^:no-doc Retrieved [e a v])

(defn- indexable->retrieved
  [^Indexable i]
  (->Retrieved (.-e i) (.-a i) (.-v i)))

(defn ^:no-doc expected-return
  "Given what's put in, return the expected output from storage"
  [x x-type]
  (case x-type
    :eav   (indexable->retrieved x)
    :eavt  (indexable->retrieved x)
    :aev   (indexable->retrieved x)
    :aevt  (indexable->retrieved x)
    :ave   (indexable->retrieved x)
    :avet  (indexable->retrieved x)
    :vae   (indexable->retrieved x)
    :vaet  (indexable->retrieved x)
    x))

(defn- get-eav
  [bf]
  (let [e (get-long bf)
        a (get-int bf)
        v (get-value bf 1)]
    (->Retrieved e a v)))

(defn- get-aev
  [bf]
  (let [a (get-int bf)
        e (get-long bf)
        v (get-value bf 1)]
    (->Retrieved e a v)))

(defn- get-ave
  [bf]
  (let [a (get-int bf)
        v (get-value bf 9)
        _ (get-byte bf)
        e (get-long bf)]
    (->Retrieved e a v)))

(defn- get-vae
  [^ByteBuffer bf]
  (let [v (get-long bf)
        a (get-int bf)
        e (get-long bf)]
    (->Retrieved e a v)))

(defn- put-attr
  "NB. not going to do range query on attr names"
  [bf x]
  (let [^bytes a (-> x str (subs 1) string-bytes)
        al       (alength a)]
    (assert (<= al c/+max-key-size+)
            "Attribute cannot be longer than 511 bytes")
    (put-bytes bf a)))

(defn- get-attr
  [bf]
  (let [^bytes bs (get-bytes bf)]
    (-> bs String. keyword)))

(defn put-buffer
  "Put the given type of data `x` in buffer `bf`, `x-type` can be one of
  `:data` (default), `:long`, `:byte`, `:bytes`, `:datom`, `:attr` or
  index type `:eav`, `:aev`, `:ave`, or `:vae`"
  ([bf x]
   (put-buffer bf x :data))
  ([bf x x-type]
   (case x-type
     :long  (put-long bf x)
     :byte  (put-byte bf x)
     :bytes (put-bytes bf x)
     :attr  (put-attr bf x)
     :datom (put-datom bf x)
     :eav   (put-eav bf x)
     :eavt  (put-eav bf x)
     :aev   (put-aev bf x)
     :aevt  (put-aev bf x)
     :ave   (put-ave bf x)
     :avet  (put-ave bf x)
     :vae   (put-vae bf x)
     :vaet  (put-vae bf x)
     (put-data bf x))))

(defn read-buffer
  "Get the given type of data from buffer `bf`, `v-type` can be one of
  `:data` (default), `:long`, `:byte`, `:bytes`, `:datom`, `:attr`, or
  index type `:eav`, `:aev`, `:ave`, or `:vae`"
  ([bf]
   (read-buffer bf :data))
  ([^ByteBuffer bf v-type]
   (case v-type
     :long  (get-long bf)
     :byte  (get-byte bf)
     :bytes (get-bytes bf)
     :attr  (get-attr bf)
     :datom (get-datom bf)
     :eav   (get-eav bf)
     :eavt  (get-eav bf)
     :aev   (get-aev bf)
     :aevt  (get-aev bf)
     :ave   (get-ave bf)
     :avet  (get-ave bf)
     :vae   (get-vae bf)
     :vaet  (get-vae bf)
     (get-data bf))))
