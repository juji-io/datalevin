(ns ^:no-doc datalevin.sparselist
  "Sparse array list of integers"
  (:refer-clojure :exclude [get set remove])
  (:require
   [taoensso.nippy :as nippy])
  (:import
   [java.io Writer DataInput DataOutput]
   [java.nio ByteBuffer]
   [java.io Writer]
   [datalevin.utl GrowingIntArray]
   [me.lemire.integercompression IntCompressor]
   [org.roaringbitmap RoaringBitmap]))

(defonce compressor (IntCompressor.))

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

  (size [_]
    (.getCardinality indices))

  (select [_ nth]
    (.get items nth))

  (serialize [_ bf]
    (let [ar   (.toArray items)
          car  (.compress ^IntCompressor compressor ar)
          size (alength car)]
      (.putInt ^ByteBuffer bf size)
      (dotimes [i size] (.putInt ^ByteBuffer bf (aget car i))))
    (.serialize indices ^ByteBuffer bf))

  (deserialize [_ bf]
    (let [size (.getInt ^ByteBuffer bf)
          car  (int-array size)]
      (dotimes [i size] (aset car i (.getInt ^ByteBuffer bf)))
      (.addAll items (.uncompress ^IntCompressor compressor car)))
    (.deserialize indices ^ByteBuffer bf))

  Object
  (equals [_ other]
    (and (instance? SparseIntArrayList other)
         (.equals indices (.-indices ^SparseIntArrayList other))
         (.equals items (.-items ^SparseIntArrayList other)))))

(nippy/extend-freeze
  SparseIntArrayList :dtlv/sial
  [^SparseIntArrayList x ^DataOutput out]
  (let [ar   (.toArray ^GrowingIntArray (.-items x))
        car  (.compress ^IntCompressor compressor ar)
        size (alength car)]
    (.writeInt out size)
    (dotimes [i size] (.writeInt out (aget car i))))
  (let [^RoaringBitmap bm (.-indices x)]
    (.runOptimize bm)
    (nippy/freeze-to-out! out bm)))

(nippy/extend-thaw
  :dtlv/sial
  [^DataInput in]
  (let [size (.readInt in)
        car  (int-array size)]
    (dotimes [i size] (aset car i (.readInt in)))
    (let [items (GrowingIntArray.)]
      (->SparseIntArrayList
        (nippy/thaw-from-in! in)
        (.addAll items (.uncompress ^IntCompressor compressor car))))))

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
