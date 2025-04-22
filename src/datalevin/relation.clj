;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.relation
  "Functions for relational algebra"
  (:require
   [clojure.pprint :as pp]
   [datalevin.parser :as dp]
   [datalevin.util :as u :refer [raise tuple-get tuple-add]]
   [datalevin.timeout :as timeout])
  (:import
   [java.util List]
   [java.io Writer]
   [org.eclipse.collections.impl.list.mutable FastList]))

;; attrs:
;;    {?e 0, ?v 1} or {?e2 "a", ?age "v"}
;; tuples is a list of objects:
;;    [ objects ... ]
;; or [ (Datom. 2 "Oleg" 1 55) ... ]
(defrecord Relation [attrs tuples])

(defmethod print-method Relation [^Relation r, ^Writer w]
  (binding [*out* w]
    (let [{:keys [attrs tuples]} r]
      (pp/pprint {:attrs attrs :tuples (mapv vec tuples)}))))

(defn relation! [attrs tuples]
  (timeout/assert-time-left)
  (Relation. attrs tuples))

;; Relation algebra

(defn empty-rel
  ^Relation [binding]
  (let [vars (->> (dp/collect-vars-distinct binding)
                  (map :symbol))]
    (relation! (zipmap vars (range)) (FastList.))))

(defn join-tuples
  ([^objects t1 ^objects t2]
   (let [l1  (alength t1)
         l2  (alength t2)
         res (object-array (+ l1 l2))]
     (System/arraycopy t1 0 res 0 l1)
     (System/arraycopy t2 0 res l1 l2)
     res))
  ([t1 ^objects idxs1 t2 ^objects idxs2]
   (let [l1    (alength idxs1)
         l2    (alength idxs2)
         res   (object-array (+ l1 l2))
         get-1 (tuple-get t1)
         get-2 (tuple-get t2)]
     (dotimes [i l1] (aset res i (get-1 t1 (aget idxs1 i))))
     (dotimes [i l2] (aset res (+ l1 i) (get-2 t2 (aget idxs2 i))))
     res)))

(defn conj-tuple
  [^objects tuple item]
  (let [len (alength tuple)
        res (object-array (inc len))]
    (System/arraycopy tuple 0 res 0 len)
    (aset res len item)
    res))

(defn same-keys?
  [a b]
  (and (= (count a) (count b))
       (every? #(contains? b %) (keys a))
       (every? #(contains? a %) (keys b))))

(defn- sum-rel*
  [attrs-a tuples-a attrs-b tuples-b]
  (let [idxb->idxa (vec (for [[sym idx-b] attrs-b]
                          [idx-b (attrs-a sym)]))
        tlen       (->> (vals attrs-a) ^long (reduce max) u/long-inc)]
    (if (seq tuples-b)
      (let [tg (tuple-get (first tuples-b))
            ta (tuple-add tuples-a)]
        (relation! attrs-a
                   (reduce
                     (fn [acc tuple-b]
                       (let [tuple' (make-array Object tlen)]
                         (doseq [[idx-b idx-a] idxb->idxa]
                           (aset ^objects tuple' idx-a (tg tuple-b idx-b)))
                         (ta acc tuple')))
                     tuples-a tuples-b)))
      (relation! attrs-a tuples-a))))

(defn sum-rel
  ([] (relation! {} (FastList.)))
  ([a] a)
  ([a b]
   (let [{attrs-a :attrs, tuples-a :tuples} a
         {attrs-b :attrs, tuples-b :tuples} b]

     (cond
       (= attrs-a attrs-b)
       (relation! attrs-a (if (sequential? tuples-a)
                            (into tuples-a tuples-b)
                            (do (.addAll ^List tuples-a tuples-b)
                                tuples-a)))

       (empty? tuples-a) b
       (empty? tuples-b) a

       (not (same-keys? attrs-a attrs-b))
       (raise
         "Can’t sum relations with different attrs: " attrs-a " and " attrs-b
         {:error :query/where})

       (every? number? (vals attrs-a))
       (sum-rel* attrs-a tuples-a attrs-b tuples-b)

       :else
       (let [number-attrs (zipmap (keys attrs-a) (range))
             size-a       (.size ^List tuples-a)
             size-b       (.size ^List tuples-b)]
         (-> (sum-rel* number-attrs (FastList. (+ size-a size-b))
                       attrs-a tuples-a)
             (sum-rel b)))))))

(defn prod-tuples
  ([] (doto (FastList.) (.add (object-array []))))
  ([tuples] tuples)
  ([tuples1 tuples2]
   (let [l1  (.size ^List tuples1)
         l2  (.size ^List tuples2)
         acc (FastList. (* l1 l2))]
     (dotimes [i l1]
       (dotimes [j l2]
         (.add acc (join-tuples (.get ^List tuples1 i) (.get ^List tuples2 j)))))
     acc)))

(defn prod-rel
  ([] (relation! {} (doto (FastList.) (.add (make-array Object 0)))))
  ([rel1] rel1)
  ([rel1 rel2]
   (let [attrs1  (keys (:attrs rel1))
         attrs2  (keys (:attrs rel2))
         tuples1 ^List (:tuples rel1)
         tuples2 ^List (:tuples rel2)
         idxs1   (to-array (->Eduction (map (:attrs rel1)) attrs1))
         idxs2   (to-array (->Eduction (map (:attrs rel2)) attrs2))]
     (relation!
       (zipmap (u/concatv attrs1 attrs2) (range))
       (reduce
         (fn [acc t1]
           (reduce (fn [^FastList acc t2]
                     (.add acc (join-tuples t1 idxs1 t2 idxs2))
                     acc)
                   acc tuples2))
         (FastList. (* (.size tuples1) (.size tuples2)))
         tuples1)))))

(defn vertical-tuples
  [coll]
  (doto (FastList. (count coll))
    (.addAll (mapv #(object-array [%]) coll))))

(defn horizontal-tuples
  [coll]
  (doto (FastList.) (.add (object-array coll))))

(defn single-tuples [tuple] (doto (FastList.) (.add tuple)))

(defn many-tuples [values] (transduce (map vertical-tuples) prod-tuples values))

(defn filter-rel
  [{:keys [attrs ^List tuples]} v pred]
  (let [new-tuples (FastList.)
        idx        (attrs v)]
    (dotimes [i (.size tuples)]
      (let [tuple ^objects (.get tuples i)]
        (when (pred (aget tuple idx))
          (.add new-tuples tuple))))
    (relation! attrs new-tuples)))

(defn projection
  [tuples index]
  (persistent!
    (reduce
      (fn [s ^objects t] (conj! s (aget t index)))
      (transient #{}) tuples)))
