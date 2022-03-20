(ns datalevin.sparselist
  "Sparse array list of integers"
  (:refer-clojure :exclude [get set remove])
  (:import
   [java.nio ByteBuffer]
   [java.io Writer]
   [me.lemire.integercompression IntCompressor]
   [org.roaringbitmap RoaringBitmap]
   [org.eclipse.collections.impl.list.mutable.primitive IntArrayList]))

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
                             ^IntArrayList items]
  ISparseIntArrayList
  (contains-index? [_ index]
    (.contains indices (int index)))

  (get [_ index]
    (when (.contains indices (int index))
      (.get items (dec (.rank indices index)))))

  (set [this index item]
    (.add indices (int index))
    (let [i (dec (.rank indices index))]
      (if (< i (.size items))
        (.set items i item)
        (.addAtIndex items i item)))
    this)

  (remove [this index]
    (.removeAtIndex items (dec (.rank indices index)))
    (.remove indices index)
    this)

  (size [_]
    (.getCardinality indices))

  (select [this nth]
    (.get items nth))

  (serialize [this bf]
    (let [ar   (.toArray items)
          car  (.compress ^IntCompressor compressor ar)
          size (alength car)]
      (.putInt ^ByteBuffer bf size)
      (dotimes [i size] (.putInt ^ByteBuffer bf (aget car i))))
    (.serialize indices ^ByteBuffer bf))

  (deserialize [this bf]
    (let [size (.getInt ^ByteBuffer bf)
          car  (int-array size)]
      (dotimes [i size] (aset car i (.getInt ^ByteBuffer bf)))
      (.addAll items (.uncompress ^IntCompressor compressor car)))
    (.deserialize indices ^ByteBuffer bf))

  Object
  (equals [this other]
    (and (instance? SparseIntArrayList other)
         (.equals indices (.-indices ^SparseIntArrayList other))
         (.equals items (.-items ^SparseIntArrayList other)))))

(defn sparse-arraylist
  ([]
   (->SparseIntArrayList (RoaringBitmap.) (IntArrayList.)))
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
