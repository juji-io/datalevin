;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.join
  "Join algorithms"
  (:require
   [datalevin.relation :as r]
   [datalevin.db :as db]
   [datalevin.query-util :as qu]
   [datalevin.util :as u :refer [concatv]])
  (:import
   [java.util List]
   [clojure.lang ILookup]
   [org.eclipse.collections.impl.map.mutable UnifiedMap]
   [org.eclipse.collections.impl.list.mutable FastList]))

;; hash join

(defn- int-tuple-get
  [tuple idx]
  (if (u/array? tuple)
    (aget ^objects tuple idx)
    (nth tuple idx)))

(defn- resolve-eid
  [eid]
  (cond
    (number? eid)     eid ;; quick path to avoid fn call
    (sequential? eid) (db/entid qu/*implicit-source* eid)
    (u/array? eid)    (db/entid qu/*implicit-source* eid)
    :else             eid))

(defn getter-fn
  [attrs attr]
  (let [idx (attrs attr)]
    (if (contains? qu/*lookup-attrs* attr)
      (if (integer? idx)
        (fn contained-int-getter-fn [tuple]
          (resolve-eid (int-tuple-get tuple idx)))
        ;; If the index is not an int?, the target can never be an array
        (fn contained-getter-fn [tuple]
          (resolve-eid (.valAt ^ILookup tuple idx))))
      (if (integer? idx)
        (fn int-getter [tuple] (int-tuple-get tuple idx))
        (fn getter [tuple] (.valAt ^ILookup tuple idx))))))

(defn tuple-key-fn
  [attrs common-attrs]
  (let [n (count common-attrs)]
    (if (== n 1)
      (getter-fn attrs (first common-attrs))
      (let [^objects getters-arr (into-array Object common-attrs)]
        (loop [i 0]
          (if (< i n)
            (do
              (aset getters-arr i (getter-fn attrs (aget getters-arr i)))
              (recur (unchecked-inc i)))
            (fn [tuple]
              (let [^objects arr (make-array Object n)]
                (loop [i 0]
                  (if (< i n)
                    (do
                      (aset arr i ((aget getters-arr i) tuple))
                      (recur (unchecked-inc i)))
                    (r/wrap-array arr)))))))))))

(defn -group-by
  [f init coll]
  (let [^UnifiedMap ret (UnifiedMap.)]
    (doseq [x    coll
            :let [k (f x)]]
      (.put ret k (conj (.getIfAbsentPut ret k init) x)))
    ret))

(defn hash-tuples
  [key-fn tuples]
  ;; (let [^UnifiedMap ret (UnifiedMap.)]
  ;;   (doseq [x    tuples
  ;;           :let [k (key-fn x)
  ;;                 l ^FastList (.getIfAbsentPut ret k (FastList.))]]
  ;;     (.add l x))
  ;;   ret)
  (-group-by key-fn '() tuples))

(defn- attr-keys
  "attrs are map, preserve order by val"
  [attrs]
  (->> attrs (sort-by val) (mapv key)))

(defn- diff-keys
  "return (- vec2 vec1) elements"
  [vec1 vec2]
  (persistent!
    (reduce
      (fn [d e2]
        (if (some (fn [e1] (= e1 e2)) vec1)
          d
          (conj! d e2)))
      (transient []) vec2)))

(defn hash-join
  [rel1 rel2]
  (let [tuples1      ^List (:tuples rel1)
        tuples2      ^List (:tuples rel2)
        attrs1       (:attrs rel1)
        attrs2       (:attrs rel2)
        common-attrs (vec (qu/intersect-keys attrs1 attrs2))
        keep-attrs1  (attr-keys attrs1)
        keep-attrs2  (diff-keys keep-attrs1 (attr-keys attrs2))
        keep-idxs1   (to-array (sort (vals attrs1)))
        keep-idxs2   (to-array (->Eduction (map attrs2) keep-attrs2))
        key-fn1      (tuple-key-fn attrs1 common-attrs)
        key-fn2      (tuple-key-fn attrs2 common-attrs)
        attrs        (zipmap (concatv keep-attrs1 keep-attrs2) (range))]
    (if (< (count tuples1) (count tuples2))
      (let [^UnifiedMap hash (hash-tuples key-fn1 tuples1)]
        (r/relation!
          attrs
          #_(let [acc (FastList.)]
              (dotimes [i (.size tuples2)]
                (let [tuple2 (.get tuples2 i)]
                  (when-some [^List tuples1 (.get hash (key-fn2 tuple2))]
                    (dotimes [j (.size tuples1)]
                      (.add acc (r/join-tuples (.get tuples1 j) keep-idxs1
                                               tuple2 keep-idxs2))))))
              acc)
          (reduce
            (fn outer [acc tuple2]
              (if-some [tuples1 (.get hash (key-fn2 tuple2))]
                (reduce
                  (fn inner [^List acc tuple1]
                    (.add acc
                          (r/join-tuples tuple1 keep-idxs1 tuple2 keep-idxs2))
                    acc)
                  acc tuples1)
                acc))
            (FastList.) tuples2)))
      (let [^UnifiedMap hash (hash-tuples key-fn2 tuples2)]
        (r/relation!
          attrs
          #_(let [acc (FastList.)]
              (dotimes [i (.size tuples1)]
                (let [tuple1 (.get tuples1 i)]
                  (when-some [^List tuples2 (.get hash (key-fn1 tuple1))]
                    (dotimes [j (.size tuples2)]
                      (.add acc (r/join-tuples tuple1 keep-idxs1
                                               (.get tuples2 j) keep-idxs2))))))
              acc)
          (reduce
            (fn outer [acc tuple1]
              (if-some [tuples2 (.get hash (key-fn1 tuple1))]
                (reduce
                  (fn inner [^List acc tuple2]
                    (.add acc
                          (r/join-tuples tuple1 keep-idxs1 tuple2 keep-idxs2))
                    acc)
                  acc tuples2)
                acc))
            (FastList.) tuples1))))))

(defn subtract-rel
  [a b]
  (let [{attrs-a :attrs, tuples-a :tuples} a
        {attrs-b :attrs, tuples-b :tuples} b

        attrs    (vec (qu/intersect-keys attrs-a attrs-b))
        key-fn-b (tuple-key-fn attrs-b attrs)
        hash     ^UnifiedMap (hash-tuples key-fn-b tuples-b)
        key-fn-a (tuple-key-fn attrs-a attrs)]
    (assoc a :tuples (let [res (FastList.)]
                       (dotimes [i (.size ^List tuples-a)]
                         (let [t (.get ^List tuples-a i)]
                           (when (nil? (.get hash (key-fn-a t)))
                             (.add res t))))
                       res))))
