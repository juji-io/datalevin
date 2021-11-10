(ns datalevin.sslist
  "Sparse array list of shorts"
  (:refer-clojure :exclude [get set remove])
  (:require [taoensso.nippy :as nippy])
  (:import
   [java.io Writer DataInput DataOutput]
   [org.roaringbitmap FastRankRoaringBitmap]
   [org.eclipse.collections.impl.list.mutable.primitive ShortArrayList]))

(defprotocol ISparseShortArrayList
  (get [this index] "get the short by index")
  (set [this index item] "set a short by index")
  (remove [this index] "remove a short by index")
  (size [this] "return the size")
  (select [this nth] "return the nth short"))

(deftype SparseShortArrayList [^FastRankRoaringBitmap indices
                               ^ShortArrayList items]
  ISparseShortArrayList
  (get [_ index]
    (when (.contains indices (int index))
      (.get items (dec (.rank indices index)))))

  (set [this index item]
    (.add indices (int index))
    (let [i    (dec (.rank indices index))
          item (short item)]
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
    (and (instance? SparseShortArrayList other)
         (.equals indices (.-indices ^SparseShortArrayList other))
         (.equals items (.-items ^SparseShortArrayList other)))))

(defn sparse-short-arraylist
  ([]
   (->SparseShortArrayList (FastRankRoaringBitmap.)
                           (ShortArrayList.)))
  ([m]
   (let [ssl (sparse-short-arraylist)]
     (doseq [[k v] m] (set ssl k v))
     ssl))
  ([ks vs]
   (let [ssl (sparse-short-arraylist)]
     (dorun (map #(set ssl %1 %2) ks vs))
     ssl)) )

(defmethod print-method SparseShortArrayList
  [^SparseShortArrayList s, ^Writer w]
  (.write w (str "#datalevin/SparseShortArrayList "))
  (binding [*out* w]
    (pr (for [i (.-indices s)] [i (get s i)]))))

(nippy/extend-freeze SparseShortArrayList :datalevin/sslist
                     [^Datom x ^DataOutput out]
                     (nippy/freeze-to-out! out (.-indices x))
                     (nippy/freeze-to-out! out (.-items x)))

(nippy/extend-thaw :datalevin/sslist
                   [^DataInput in]
                   (let [indices (nippy/thaw-from-in! in)
                         items   (nippy/thaw-from-in! in)]
                     (->SparseShortArrayList indices items)))
