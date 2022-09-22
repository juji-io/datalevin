(ns ^:no-doc datalevin.sparselist
  "Sparse array list of integers"
  (:refer-clojure :exclude [get set remove])
  (:import [java.nio ByteBuffer]
           [java.io Writer]
           [datalevin.utl GrowingIntArray]
           [me.lemire.integercompression IntCompressor]
           [org.roaringbitmap RoaringBitmap]))

(set! *unchecked-math* true)

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

(comment


  (-> (sparse-arraylist)
      (set 2 2)
      (set 1 1)
      (set 3 3)
      (set 1 5)
      )

  (= (-> (sparse-arraylist)
         (set 2 2)
         (set 1 1)
         (set 3 3))
     (-> (sparse-arraylist)
         (set 1 1)
         (set 2 2)
         (set 3 3)))


  (.size (.-items (-> (sparse-arraylist)
                      (set 2 2)
                      (set 1 1)
                      (set 3 3)
                      (set 1 5))))


  )
