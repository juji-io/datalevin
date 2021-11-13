(ns datalevin.sparselist
  "Sparse array list of integers"
  (:refer-clojure :exclude [get set remove])
  (:require [taoensso.nippy :as nippy])
  (:import
   [java.io Writer DataInput DataOutput]
   [me.lemire.integercompression IntCompressor]
   [org.roaringbitmap FastRankRoaringBitmap]
   [org.eclipse.collections.impl.list.mutable.primitive IntArrayList]))

(defprotocol ISparseIntArrayList
  (contains-index? [this index] "return true if containing index")
  (get [this index] "get the item by index")
  (set [this index item] "set an item by index")
  (remove [this index] "remove an item by index")
  (size [this] "return the size")
  (select [this nth] "return the nth item"))

(deftype SparseIntArrayList [^FastRankRoaringBitmap indices
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
    (.get this (.select indices nth)))

  Object
  (equals [this other]
    (and (instance? SparseIntArrayList other)
         (.equals indices (.-indices ^SparseIntArrayList other))
         (.equals items (.-items ^SparseIntArrayList other)))))

(defn sparse-arraylist
  ([]
   (->SparseIntArrayList (FastRankRoaringBitmap.)
                         (IntArrayList.)))
  ([m]
   (let [ssl (sparse-arraylist)]
     (doseq [[k v] m] (set ssl k v))
     ssl))
  ([ks vs]
   (let [ssl (sparse-arraylist)]
     (dorun (map #(set ssl %1 %2) ks vs))
     ssl)) )

(defmethod print-method SparseIntArrayList
  [^SparseIntArrayList s, ^Writer w]
  (.write w (str "#datalevin/SparseList "))
  (binding [*out* w]
    (pr (for [i (.-indices s)] [i (get s i)]))))

(defonce compressor (IntCompressor.))

(nippy/extend-freeze SparseIntArrayList :dtlv/sial
                     [^SparseIntArrayList x ^DataOutput out]
                     (nippy/freeze-to-out! out (.-indices x))
                     (let [ar   (.toArray ^IntArrayList (.-items x))
                           car  (.compress ^IntCompressor compressor ar)
                           size (alength car)]
                       (.writeInt out size)
                       (dotimes [i size]
                         (.writeInt out (aget car i)))))

(nippy/extend-thaw :dtlv/sial
                   [^DataInput in]
                   (let [indices (nippy/thaw-from-in! in)
                         size    (.readInt in)
                         car     (int-array size)]
                     (dotimes [i size]
                       (aset car i (.readInt in)))
                     (let [ar    (.uncompress ^IntCompressor compressor car)
                           items (IntArrayList. ar)]
                       (->SparseIntArrayList indices items))))
