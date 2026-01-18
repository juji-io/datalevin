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
   [datalevin.util :as u :refer [raise]]
   [datalevin.timeout :as timeout])
  (:import
   [java.util List Arrays HashSet]
   [java.io Writer]
   [org.eclipse.collections.impl.list.mutable FastList]))

(deftype ArrayWrapper [^objects a ^int h]
  Object
  (hashCode [_] h)
  (equals [_ that]
    (and (instance? ArrayWrapper that)
         (Arrays/equals a ^objects (.-a ^ArrayWrapper that)))))

(defn wrap-array [^objects a] (ArrayWrapper. a (Arrays/hashCode a)))

;; attrs: {?e 0, ?v 1}
;; tuples is a list of objects: [ objects ... ]
(defrecord Relation [attrs tuples])

(defmethod print-method Relation [^Relation r, ^Writer w]
  (binding [*out* w]
    (let [{:keys [attrs tuples]} r]
      (pp/pprint {:attrs attrs :tuples (mapv vec tuples)}))))

(defn relation!
  [attrs tuples]
  (timeout/assert-time-left)
  (Relation. attrs tuples))

;; Relation algebra

(defn join-tuples
  ([^objects t1 ^objects t2]
   (let [l1  (alength t1)
         l2  (alength t2)
         res (object-array (+ l1 l2))]
     (System/arraycopy t1 0 res 0 l1)
     (System/arraycopy t2 0 res l1 l2)
     res))
  ([^objects t1 ^objects idxs1 ^objects t2 ^objects idxs2]
   (let [l1  (alength idxs1)
         l2  (alength idxs2)
         res (object-array (+ l1 l2))]
     (dotimes [i l1] (aset res i (aget t1 (aget idxs1 i))))
     (dotimes [i l2] (aset res (+ l1 i) (aget t2 (aget idxs2 i))))
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
  [attrs-a ^List tuples-a attrs-b ^List tuples-b]
  (let [idxb->idxa (vec (for [[sym idx-b] attrs-b]
                          [idx-b (attrs-a sym)]))
        tlen       (->> (vals attrs-a) ^long (apply max) u/long-inc)
        size-a     (.size tuples-a)
        size-b     (.size tuples-b)]
    (if (< 0 (.size tuples-b))
      (relation!
        attrs-a
        (let [res (FastList. (+ size-a size-b))]
          (.addAll res tuples-a)
          (dotimes [i (.size tuples-b)]
            (let [^objects tuple-b (.get tuples-b i)
                  ^objects tuple   (object-array tlen)]
              (doseq [[idx-b idx-a] idxb->idxa]
                (aset tuple idx-a (aget tuple-b idx-b)))
              (.add res tuple)))
          res))
      (relation! attrs-a tuples-a))))

(defn sum-rel
  ([] (relation! {} (FastList.)))
  ([a] a)
  ([a b]
   (let [{attrs-a :attrs, tuples-a :tuples} a
         {attrs-b :attrs, tuples-b :tuples} b]

     (cond
       (= attrs-a attrs-b)
       (relation! attrs-a (let [size-a (.size ^List tuples-a)
                                size-b (.size ^List tuples-b)]
                            (doto (FastList. (+ size-a size-b))
                              (.addAll tuples-a)
                              (.addAll tuples-b))))

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

(defn dedupe-rel
  [rel]
  (let [tuples ^List (:tuples rel)]
    (if (or (nil? tuples) (zero? (.size tuples)))
      rel
      (assoc rel :tuples
             (let [tset (HashSet.)
                   new  (FastList.)]
               (dotimes [i (.size tuples)]
                 (let [t  (.get tuples i)
                       tw (wrap-array t)]
                   (when-not (.contains tset tw)
                     (.add new t)
                     (.add tset tw))))
               new)))))

(defn sum-rel-dedupe
  ([] (relation! {} (FastList.)))
  ([a] a)
  ([a b]
   (let [attrs-a  (:attrs a)
         attrs-b  (:attrs b)
         tuples-a (:tuples a)
         tuples-b (:tuples b)]
     (cond
       (or (nil? tuples-a) (zero? (.size ^List tuples-a))) b
       (or (nil? tuples-b) (zero? (.size ^List tuples-b))) a

       (= attrs-a attrs-b)
       (relation!
         attrs-a
         (let [^List tuples-a tuples-a
               ^List tuples-b tuples-b
               la            (.size tuples-a)
               lb            (.size tuples-b)
               res           (FastList. (+ la lb))
               seen          (HashSet.)]
           (dotimes [i la]
             (let [t  (.get tuples-a i)
                   tw (wrap-array t)]
               (when-not (.contains seen tw)
                 (.add seen tw)
                 (.add res t))))
           (dotimes [i lb]
             (let [t  (.get tuples-b i)
                   tw (wrap-array t)]
               (when-not (.contains seen tw)
                 (.add seen tw)
                 (.add res t))))
           res))

       (not (same-keys? attrs-a attrs-b))
       (raise
         "Can’t sum relations with different attrs: " attrs-a " and " attrs-b
         {:error :query/where})

       (every? number? (vals attrs-a))
       (let [idxb->idxa (vec (for [[sym idx-b] attrs-b]
                               [idx-b (attrs-a sym)]))
             tlen       (->> (vals attrs-a) ^long (apply max) u/long-inc)
             ^List tuples-a tuples-a
             ^List tuples-b tuples-b
             la         (.size tuples-a)
             lb         (.size tuples-b)
             res        (FastList. (+ la lb))
             seen       (HashSet.)]
         (dotimes [i la]
           (let [t  (.get tuples-a i)
                 tw (wrap-array t)]
             (when-not (.contains seen tw)
               (.add seen tw)
               (.add res t))))
         (dotimes [i lb]
           (let [^objects tuple-b (.get tuples-b i)
                 ^objects tuple   (object-array tlen)]
             (doseq [[idx-b idx-a] idxb->idxa]
               (aset tuple idx-a (aget tuple-b idx-b)))
             (let [tw (wrap-array tuple)]
               (when-not (.contains seen tw)
                 (.add seen tw)
                 (.add res tuple)))))
         (relation! attrs-a res))

       :else
       (dedupe-rel (sum-rel a b))))))

(defn prod-tuples
  ([] (doto (FastList.) (.add (object-array []))))
  ([tuples] tuples)
  ([^List tuples1 ^List tuples2]
   (let [l1  (.size tuples1)
         l2  (.size tuples2)
         acc (FastList. (* l1 l2))]
     (dotimes [i l1]
       (dotimes [j l2]
         (.add acc (join-tuples (.get tuples1 i) (.get tuples2 j)))))
     acc)))

(defn prod-rel
  ([] (relation! {} (doto (FastList.) (.add (make-array Object 0)))))
  ([rel1] rel1)
  ([rel1 rel2]
   (let [attrs1 (keys (:attrs rel1))
         attrs2 (keys (:attrs rel2))
         idxs1  (to-array (->Eduction (map (:attrs rel1)) attrs1))
         idxs2  (to-array (->Eduction (map (:attrs rel2)) attrs2))]
     (relation!
       (zipmap (u/concatv attrs1 attrs2) (range))
       (let [tuples1 ^List (:tuples rel1)
             tuples2 ^List (:tuples rel2)
             l1      (.size tuples1)
             l2      (.size tuples2)
             acc     (FastList. (* l1 l2))]
         (dotimes [i l1]
           (dotimes [j l2]
             (.add acc (join-tuples (.get tuples1 i) idxs1
                                    (.get tuples2 j) idxs2))))
         acc)))))

(defn vertical-tuples [coll] (u/map-fl #(object-array [%]) coll))

(defn single-tuples [tuple] (doto (FastList.) (.add tuple)))

(defn many-tuples [values] (transduce (map vertical-tuples) prod-tuples values))

(defn difference
  "Returns r1 - r2. Assumes r1 and r2 have same attrs."
  [r1 r2]
  (let [^List t1 (:tuples r1)
        ^List t2 (:tuples r2)]
    (if (.isEmpty t2)
      r1
      (assoc r1 :tuples (let [s2         (HashSet.)
                              new-tuples (FastList.)]
                          (dotimes [i (.size t2)]
                            (.add s2 (wrap-array (.get t2 i))))
                          (dotimes [i (.size t1)]
                            (let [tuple (.get t1 i)]
                              (when-not (.contains s2 (wrap-array tuple))
                                (.add new-tuples tuple))))
                          new-tuples)))))

(defn select-tuples
  [pred ^List tuples]
  (let [res (FastList.)]
    (dotimes [i (.size tuples)]
      (let [t (.get tuples i)]
        (when (pred t)
          (.add res t))))
    res))

(defn rel-not-empty
  [rel]
  (let [tuples (:tuples rel)]
    (and tuples (< 0 (.size ^List tuples)))))

(defn rel-empty
  [rel]
  (let [tuples (:tuples rel)]
    (or (nil? tuples) (zero? (.size ^List tuples)))))
