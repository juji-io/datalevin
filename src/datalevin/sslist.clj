(ns datalevin.sslist
  "Sparse array list of shorts"
  (:refer-clojure :exclude [get set])
  (:require [taoensso.nippy :as nippy])
  (:import
   [java.io DataInput DataOutput]
   [org.roaringbitmap FastRankRoaringBitmap]
   [org.eclipse.collections.impl.list.mutable.primitive ShortArrayList]))

(defprotocol ISparseShortArrayList
  (get [this index])
  (set [this index item]))

(deftype SparseShortArrayList [^FastRankRoaringBitmap indices
                               ^ShortArrayList items]
  ISparseShortArrayList
  (get [_ index]
    (when (.contains indices (int index))
      (.get items (dec (.rank indices index)))))

  (set [_ index item]
    (.add indices (int index))
    (.addAtIndex items (dec (.rank indices index)) item)))

(defn sparse-short-arraylist
  []
  (->SparseShortArrayList (FastRankRoaringBitmap.)
                          (ShortArrayList.)))

(nippy/extend-freeze SparseShortArrayList :datalevin/sslist
                     [^Datom x ^DataOutput out]
                     (nippy/freeze-to-out! out (.-indices x))
                     (nippy/freeze-to-out! out (.-items x)))

(nippy/extend-thaw :datalevin/sslist
                   [^DataInput in]
                   (let [indices (nippy/thaw-from-in! in)
                         items   (nippy/thaw-from-in! in)]
                     (->SparseShortArrayList indices items)))
