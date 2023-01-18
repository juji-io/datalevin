(ns ^:no-doc datalevin.sparselist
  "Sparse array list of integers"
  (:refer-clojure :exclude [get set remove])
  (:require
   [taoensso.nippy :as nippy])
  (:import
   [java.io Writer DataInput DataOutput]
   [java.nio ByteBuffer]
   [datalevin.utl GrowingIntArray]
   [me.lemire.integercompression IntCompressor]
   [me.lemire.integercompression.differential IntegratedIntCompressor]
   [org.roaringbitmap RoaringBitmap]))

(defonce compressor (IntCompressor.))
(defonce sorted-compressor (IntegratedIntCompressor.))

(defn get-ints
  "get compressed int array from a ByteBuffer"
  [^ByteBuffer bf]
  (let [size (.getInt bf)
        car  (int-array size)]
    (dotimes [i size] (aset car i (.getInt bf)))
    (.uncompress ^IntCompressor compressor car)))

(defn put-ints
  "put an int array in compressed form in a ByteBuffer"
  [^ByteBuffer bf ar]
  (let [car  (.compress ^IntCompressor compressor ar)
        size (alength car)]
    (.putInt bf size)
    (dotimes [i size] (.putInt bf (aget car i)))))

(defn get-sorted-ints
  "get compressed int array from a ByteBuffer"
  [^ByteBuffer bf]
  (let [size (.getInt bf)
        car  (int-array size)]
    (dotimes [i size] (aset car i (.getInt bf)))
    (.uncompress ^IntegratedIntCompressor sorted-compressor car)))

(defn put-sorted-ints
  "put an int array in compressed form in a ByteBuffer"
  [^ByteBuffer bf ar]
  (let [car  (.compress ^IntegratedIntCompressor sorted-compressor ar)
        size (alength car)]
    (.putInt bf size)
    (dotimes [i size] (.putInt bf (aget car i)))))

(defprotocol ISparseIntArrayList
  (contains-index? [this index] "return true if containing index")
  (get [this index] "get item by index")
  (set [this index item] "set an item by index")
  (remove [this index] "remove an item by index")
  (size [this] "return the size")
  (select [this nth] "return the nth item")
  (serialize [this bf] "serialize to a bytebuffer")
  (deserialize [this bf] "serialize from a bytebuffer"))

(deftype SparseIntArrayList [^RoaringBitmap indices
                             ^GrowingIntArray items]
  ISparseIntArrayList
  (contains-index? [_ index]
    (.contains indices (int index)))

  (get [_ index]
    (when (.contains indices (int index))
      (.get items (dec (.rank indices index)))))

  (set [this index item]
    (let [index (int index)]
      (if (.contains indices index)
        (.set items (dec (.rank indices index)) item)
        (do (.add indices index)
            (.insert items (dec (.rank indices index)) item))))
    this)

  (remove [this index]
    (.remove items (dec (.rank indices index)))
    (.remove indices index)
    this)

  (size [_] (.getCardinality indices))

  (select [_ nth]
    (.get items nth))

  (serialize [_ bf]
    (put-ints bf (.toArray items))
    (.serialize indices ^ByteBuffer bf))

  (deserialize [_ bf]
    (.addAll items (get-ints bf))
    (.deserialize indices ^ByteBuffer bf))

  Object
  (equals [_ other]
    (and (instance? SparseIntArrayList other)
         (.equals indices (.-indices ^SparseIntArrayList other))
         (.equals items (.-items ^SparseIntArrayList other)))))

(nippy/extend-freeze
  RoaringBitmap :dtlv/bm
  [^RoaringBitmap x ^DataOutput out]
  (.serialize x out))

(nippy/extend-freeze
  GrowingIntArray :dtlv/gia
  [^GrowingIntArray x ^DataOutput out]
  (let [ar   (.toArray  x)
        car  (.compress ^IntCompressor compressor ar)
        size (alength car)]
    (.writeInt out size)
    (dotimes [i size] (.writeInt out (aget car i)))))

(nippy/extend-freeze
  SparseIntArrayList :dtlv/sial
  [^SparseIntArrayList x ^DataOutput out]
  (nippy/freeze-to-out! out (.-items x))
  (nippy/freeze-to-out! out (.-indices x)))

(nippy/extend-thaw
  :dtlv/bm [^DataInput in]
  (doto (RoaringBitmap.) (.deserialize in)))

(nippy/extend-thaw
  :dtlv/gia [^DataInput in]
  (let [size  (.readInt in)
        car   (int-array size)
        items (GrowingIntArray.)]
    (dotimes [i size] (aset car i (.readInt in)))
    (.addAll items (.uncompress ^IntCompressor compressor car))
    items))

(nippy/extend-thaw
  :dtlv/sial [^DataInput in]
  (let [items (nippy/thaw-from-in! in)]
    (->SparseIntArrayList (nippy/thaw-from-in! in) items)))

(defn sparse-arraylist
  ([]
   (->SparseIntArrayList (RoaringBitmap.) (GrowingIntArray.)))
  ([m]
   (let [ssl (sparse-arraylist)]
     (doseq [[k v] m] (set ssl k v))
     ssl))
  ([ks vs]
   (let [ssl (sparse-arraylist)]
     (dorun (map #(set ssl %1 %2) ks vs))
     ssl)) )

(defmethod print-method SparseIntArrayList
  [^SparseIntArrayList s ^Writer w]
  (.write w (str "#datalevin/SparseList "))
  (binding [*out* w]
    (pr (for [i (.-indices s)] [i (get s i)]))))
