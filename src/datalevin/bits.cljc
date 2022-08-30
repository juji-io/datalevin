(ns ^:no-doc datalevin.bits
  "Handle binary encoding, byte buffers, etc."
  (:require [datalevin.datom :as d]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [datalevin.sparselist :as sl]
            [clojure.string :as s]
            [taoensso.nippy :as nippy])
  (:import [java.util ArrayList Arrays UUID Date Base64]
           [java.util.regex Pattern]
           [java.io Writer DataInput DataOutput ObjectInput ObjectOutput]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.lang String Character]
           [org.roaringbitmap RoaringBitmap RoaringBitmapWriter]
           [datalevin.datom Datom]
           [datalevin.utl BitOps]))

;; bytes <-> text

(def ^:no-doc hex [\0 \1 \2 \3 \4 \5 \6 \7 \8 \9 \A \B \C \D \E \F])

(defn ^:no-doc hexify-byte
  "Convert a byte to a hex pair"
  [b]
  (let [v (bit-and ^byte b 0xFF)]
    [(hex (bit-shift-right v 4)) (hex (bit-and v 0x0F))]))

(defn ^:no-doc hexify
  "Convert bytes to hex string"
  [bs]
  (apply str (mapcat hexify-byte bs)))

(defn ^:no-doc unhexify-2c
  "Convert two hex characters to a byte"
  [c1 c2]
  (unchecked-byte
    (+ (bit-shift-left (Character/digit ^char c1 16) 4)
       (Character/digit ^char c2 16))))

(defn ^:no-doc unhexify
  "Convert hex string to byte sequence"
  [s]
  (map #(apply unhexify-2c %) (partition 2 s)))

(defn ^:no-doc hexify-string [^String s] (hexify (.getBytes s)))

(defn ^:no-doc unhexify-string [s] (String. (byte-array (unhexify s))))

(defn text-ba->str
  "Convert a byte array to string, the array is known to contain text data"
  [^bytes ba]
  (String. ba StandardCharsets/UTF_8))

(defmethod print-method (Class/forName "[B")
  [^bytes bs, ^Writer w]
  (.write w "#datalevin/bytes ")
  (.write w "\"")
  (.write w ^String (u/encode-base64 bs))
  (.write w "\""))

(defn ^bytes bytes-from-reader
  [s]
  (u/decode-base64 s))

(defmethod print-method Pattern
  [^Pattern p, ^Writer w]
  (.write w "#datalevin/regex ")
  (.write w "\"")
  (.write w ^String (s/escape (.toString p) {\\ "\\\\"}))
  (.write w "\""))

(defn ^Pattern regex-from-reader
  [s]
  (Pattern/compile s))

;; nippy

(defn serialize
  "Serialize data to bytes"
  [x]
  (nippy/fast-freeze x))

(defn deserialize
  "Deserialize from bytes. "
  [bs]
  (binding [nippy/*thaw-serializable-allowlist*
            (into nippy/*thaw-serializable-allowlist*
                  c/*data-serializable-classes*)]
    (nippy/fast-thaw bs)))

;; bitmap

(defn bitmap
  "Create a roaringbitmap. Expect a sorted integer collection"
  ([]
   (RoaringBitmap.))
  ([ints]
   (let [^RoaringBitmapWriter writer (-> (RoaringBitmapWriter/writer)
                                         (.initialCapacity (count ints))
                                         (.get))]
     (doseq [^int i ints] (.add writer i))
     (.get writer))))

(defn bitmap-del
  "Delete an int from the bitmap"
  [^RoaringBitmap bm i]
  (.remove bm ^int i)
  bm)

(defn bitmap-add
  "Add an int from the bitmap"
  [^RoaringBitmap bm i]
  (.add bm ^int i)
  bm)

(defn- get-bitmap
  [^ByteBuffer bf]
  (let [bm (RoaringBitmap.)] (.deserialize bm bf) bm))

(defn- put-bitmap [^ByteBuffer bf ^RoaringBitmap x] (.serialize x bf))

;; byte buffer

(defn ^ByteBuffer allocate-buffer
  "Allocate JVM off-heap ByteBuffer in the Datalevin expected endian order"
  [size]
  (ByteBuffer/allocateDirect size))

(defn buffer-transfer
  "Transfer content from one bytebuffer to another"
  ([^ByteBuffer src ^ByteBuffer dst]
   (.put dst src))
  ([^ByteBuffer src ^ByteBuffer dst n]
   (dotimes [_ n] (.put dst (.get src)))))

(defn- get-long
  "Get a long from a ByteBuffer"
  [^ByteBuffer bb]
  (.getLong bb))

(defn get-int
  "Get an int from a ByteBuffer"
  [^ByteBuffer bb]
  (.getInt bb))

(defn get-short
  "Get a short from a ByteBuffer"
  [^ByteBuffer bb]
  (.getShort bb))

(defn get-short-array
  "Get a short array from a ByteBuffer"
  [^ByteBuffer bb]
  (let [len (.getInt bb)
        sa  (short-array len)]
    (dotimes [i len]
      (aset sa i (.getShort bb)))
    sa))

(defn- get-byte
  "Get a byte from a ByteBuffer"
  [^ByteBuffer bb]
  (.get bb))

(defn get-bytes
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
     (deserialize bs)))
  ([^ByteBuffer bb n]
   (when-let [bs (get-bytes-val bb n)] (deserialize bs))))

(def ^:no-doc ^:const float-sign-idx 31)
(def ^:no-doc ^:const double-sign-idx 63)
(def ^:no-doc ^:const instant-sign-idx 63)

(defn- code-instant [^long x] (bit-flip x instant-sign-idx))

(defn- get-instant
  [^ByteBuffer bf]
  (Date. ^long (code-instant (.getLong bf))))

(defn- put-instant
  [^ByteBuffer bf x]
  (.putLong bf (code-instant
                 (if (inst? x)
                   (inst-ms x)
                   (u/raise "Expect an inst? value" {:value x})))))

(defn- decode-float
  [x]
  (if (bit-test x float-sign-idx)
    (Float/intBitsToFloat (BitOps/intFlip x float-sign-idx))
    (Float/intBitsToFloat (BitOps/intNot ^int x))))

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

(defn boolean-value
  "Datalevin's boolean value in byte"
  [b]
  (case (short b)
    2 true
    1 false
    (u/raise "Illegal value, expecting a Datalevin boolean value" {})))

(defn- get-boolean
  [^ByteBuffer bb]
  (boolean-value (get-byte bb)))

(defn- put-long
  [^ByteBuffer bb n]
  (.putLong bb ^long n))

(defn- encode-float
  [x]
  (assert (not (Float/isNaN x)) "Cannot index NaN")
  (let [x (if (= x -0.0) 0.0 x)]
    (if (neg? ^float x)
      (BitOps/intNot (Float/floatToIntBits x))
      (BitOps/intFlip (Float/floatToIntBits x) ^long float-sign-idx))))

(defn- put-float
  [^ByteBuffer bb x]
  (.putInt bb (encode-float (float x))))

(defn- encode-double
  [^double x]
  (assert (not (Double/isNaN x)) "Cannot index NaN")
  (let [x (if (= x -0.0) 0.0 x)]
    (if (neg? x)
      (bit-not (Double/doubleToLongBits x))
      (bit-flip (Double/doubleToLongBits x) double-sign-idx))))

(defn- put-double
  [^ByteBuffer bb x]
  (.putLong bb (encode-double (double x))))

(defn put-int
  [^ByteBuffer bb n]
  (.putInt bb ^int (int n)))

(defn put-short
  [^ByteBuffer bb n]
  (.putShort bb ^short (short n)))

(defn put-bytes
  "Put bytes into a bytebuffer"
  [^ByteBuffer bb ^bytes bs]
  (.put bb bs))

(defn- put-byte
  [^ByteBuffer bb b]
  (.put bb ^byte (unchecked-byte b)))
(type (byte 1))

(defn- put-data
  [^ByteBuffer bb x]
  (put-bytes bb (serialize x)))

;; bytes

(defn type-size
  "Return the storage size (bytes) of the fixed length data type (a keyword).
  Useful when calling `open-dbi`. E.g. return 9 for `:long` (i.e. 8 bytes plus
  an 1 byte header). Return nil if the given data type has no fixed size."
  [type]
  (case type
    :int     4
    :long    9
    :id      8
    :float   5
    :double  9
    :byte    1
    :boolean 2
    :instant 9
    :uuid    17
    nil))

(defn ^:no-doc measure-size
  "measure size of x in number of bytes"
  [x]
  (cond
    (bytes? x)         (inc (alength ^bytes x))
    (int? x)           8
    (instance? Byte x) 1
    :else              (alength ^bytes (serialize x))))

;; index

(defmacro ^:no-doc wrap-extrema
  [v vmin vmax b]
  `(if (keyword? ~v)
     (condp = ~v
       c/v0   ~vmin
       c/vmax ~vmax
       (u/raise "Expect other data types, got keyword instead" ~v {}))
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
    (serialize v)))

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

(defn pr-indexable [^Indexable i]
  [(.-e i) (.-a i) (.-v i) (.-f i) (hexify (.-b i)) (.-h i)])

(defmethod print-method Indexable
  [^Indexable i, ^Writer w]
  (.write w "#datalevin/Indexable ")
  (.write w (pr-str (pr-indexable i))))

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
    -9  (put-long bf (code-instant
                       (wrap-extrema val Long/MIN_VALUE Long/MAX_VALUE
                                     (.getTime ^Date val))))
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

(defn- put-vea
  [bf ^Indexable x]
  (put-native bf (.-v x) (.-f x))
  (put-long bf (.-e x))
  (put-int bf (.-a x)))

(defn- sep->slash
  [^bytes bs]
  (let [n-1 (dec (alength bs))]
    (loop [i 0]
      (cond
        (= (aget bs i) c/separator) (aset-byte bs i c/slash)
        (< i n-1)                   (recur (inc i)))))
  bs)

(defn- get-string
  ([^ByteBuffer bf]
   (String. ^bytes (get-bytes bf) StandardCharsets/UTF_8))
  ([^ByteBuffer bf ^long post-v]
   (String. ^bytes (get-bytes-val bf post-v) StandardCharsets/UTF_8)))

(defn- get-key-sym-str
  [^ByteBuffer bf ^long post-v]
  (let [^bytes ba (sep->slash (get-bytes-val bf post-v))
        s         (String. ^bytes ba StandardCharsets/UTF_8)]
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
    -9  (get-instant bf)
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

(def ^:no-doc ^:const overflown-key (->Retrieved c/e0 c/overflown c/overflown))

(defn- indexable->retrieved
  [^Indexable i]
  (->Retrieved (.-e i) (.-a i) (.-v i)))

(defn ^:no-doc expected-return
  "Given what's put in, return the expected output from storage"
  [x x-type]
  (case x-type
    :eav  (indexable->retrieved x)
    :eavt (indexable->retrieved x)
    :ave  (indexable->retrieved x)
    :avet (indexable->retrieved x)
    :vea  (indexable->retrieved x)
    :veat (indexable->retrieved x)
    x))

(defn- get-eav
  [bf]
  (let [e (get-long bf)
        a (get-int bf)
        v (get-value bf 1)]
    (->Retrieved e a v)))

(defn- get-ave
  [bf]
  (let [a (get-int bf)
        v (get-value bf 9)
        _ (get-byte bf)
        e (get-long bf)]
    (->Retrieved e a v)))

(defn- get-vea
  [bf]
  (let [v (get-long bf)
        e (get-long bf)
        a (get-int bf)]
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
    (keyword (String. bs StandardCharsets/UTF_8))))

(defn- raw-header
  [v t]
  (case t
    :keyword c/type-keyword
    :symbol  c/type-symbol
    :string  c/type-string
    :boolean c/type-boolean
    :float   c/type-float
    :double  c/type-double
    :instant c/type-instant
    :uuid    c/type-uuid
    :bytes   c/type-bytes
    :long    (long-header v)
    nil))

(defn- put-sparse-list
  [bf x]
  (sl/serialize x bf))

(defn put-buffer
  ([bf x]
   (put-buffer bf x :data))
  ([^ByteBuffer bf x x-type]
   (case x-type
     :string         (do (put-byte bf (raw-header x :string))
                         (put-bytes
                           bf (.getBytes ^String x StandardCharsets/UTF_8)))
     :int            (put-int bf x)
     :short          (put-short bf x)
     :int-int        (let [[i1 i2] x]
                       (put-int bf i1)
                       (put-int bf i2))
     :sial           (put-sparse-list bf x)
     :bitmap         (put-bitmap bf x)
     :term-info      (let [[i1 i2 i3] x]
                       (put-int bf i1)
                       (.putFloat bf (float i2))
                       (put-sparse-list bf i3))
     :doc-info       (let [[i1 i2] x]
                       (put-short bf i1)
                       (put-data bf i2))
     :long           (do (put-byte bf (raw-header x :long))
                         (put-long bf x))
     :id             (put-long bf x)
     :float          (do (put-byte bf (raw-header x :float))
                         (put-float bf x))
     :double         (do (put-byte bf (raw-header x :double))
                         (put-double bf x))
     :byte           (put-byte bf x)
     :bytes          (do (put-byte bf (raw-header x :bytes))
                         (put-bytes bf x))
     :keyword        (do (put-byte bf (raw-header x :keyword))
                         (put-bytes bf (key-sym-bytes x)))
     :symbol         (do (put-byte bf (raw-header x :symbol))
                         (put-bytes bf (key-sym-bytes x)))
     :boolean        (do (put-byte bf (raw-header x :boolean))
                         (put-byte bf (if x c/true-value c/false-value)))
     :instant        (do (put-byte bf (raw-header x :instant))
                         (put-instant bf x))
     :instant-pre-06 (do (put-byte bf (raw-header x :instant))
                         (.putLong bf (.getTime ^Date x)))
     :uuid           (do (put-byte bf (raw-header x :uuid))
                         (put-uuid bf x))
     :attr           (put-attr bf x)
     :eav            (put-eav bf x)
     :eavt           (put-eav bf x)
     :ave            (put-ave bf x)
     :avet           (put-ave bf x)
     :vea            (put-vea bf x)
     :veat           (put-vea bf x)
     :raw            (put-bytes bf x)
     (put-data bf x))))

(defn- get-sparse-list
  [bf]
  (let [sl (sl/sparse-arraylist)]
    (sl/deserialize sl bf)
    sl))

(defn read-buffer
  ([bf]
   (read-buffer bf :data))
  ([^ByteBuffer bf v-type]
   (case v-type
     :string         (do (get-byte bf) (get-string bf))
     :short          (get-short bf)
     :int            (get-int bf)
     :int-int        [(get-int bf) (get-int bf)]
     :bitmap         (get-bitmap bf)
     :sial           (get-sparse-list bf)
     :term-info      [(get-int bf) (.getFloat bf) (get-sparse-list bf)]
     :doc-info       [(get-short bf) (get-data bf)]
     :long           (do (get-byte bf) (get-long bf))
     :id             (get-long bf)
     :float          (do (get-byte bf) (get-float bf))
     :double         (do (get-byte bf) (get-double bf))
     :byte           (get-byte bf)
     :bytes          (do (get-byte bf) (get-bytes bf))
     :keyword        (do (get-byte bf) (get-keyword bf 0))
     :symbol         (do (get-byte bf) (get-symbol bf 0))
     :boolean        (do (get-byte bf) (get-boolean bf))
     :instant        (do (get-byte bf) (get-instant bf))
     :instant-pre-06 (do (get-byte bf) (Date. (.getLong bf)))
     :uuid           (do (get-byte bf) (get-uuid bf))
     :attr           (get-attr bf)
     :eav            (get-eav bf)
     :eavt           (get-eav bf)
     :ave            (get-ave bf)
     :avet           (get-ave bf)
     :vea            (get-vea bf)
     :veat           (get-vea bf)
     :raw            (get-bytes bf)
     (get-data bf))))

(defn valid-data?
  "validate data type"
  [x t]
  (case t
    (:db.type/string :string) (string? x)
    :int                      (and (int? x)
                                   (<= Integer/MIN_VALUE x Integer/MAX_VALUE))

    (:db.type/long :db.type/ref :long) (int? x)

    (:db.type/float :float)     (and (float? x)
                                     (<= Float/MIN_VALUE x Float/MAX_VALUE))
    (:db.type/double :double)   (float? x)
    :byte                       (or (instance? java.lang.Byte x)
                                    (and (int? x) (<= -128 x 127)))
    (:db.type/bytes :bytes)     (bytes? x)
    (:db.type/keyword :keyword) (keyword? x)
    (:db.type/symbol :symbol)   (symbol? x)
    (:db.type/boolean :boolean) (boolean? x)
    (:db.type/instant :instant) (inst? x)
    (:db.type/uuid :uuid)       (uuid? x)
    true))
