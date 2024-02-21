(ns ^:no-doc datalevin.relation
  (:require
   [datalevin.util :as u :refer [raise]]
   [datalevin.timeout :as timeout])
  (:import
   [java.util List]
   [org.eclipse.collections.impl.list.mutable FastList]))

;; attrs:
;;    {?e 0, ?v 1} or {?e2 "a", ?age "v"}
;; tuples is a list of objects:
;;    [ objects ... ]
;; or [ (Datom. 2 "Oleg" 1 55) ... ]
(defrecord Relation [attrs tuples])

(defn relation! [attrs tuples]
  (timeout/assert-time-left)
  (Relation. attrs tuples))

;; Relation algebra

(defn typed-aget [a i] (aget ^objects a ^Long i))

(defn join-tuples
  ([t1 t2]
   (let [idxs1 (object-array (range (alength ^objects t1)))
         idxs2 (object-array (range (alength ^objects t2)))]
     (join-tuples t1 idxs1 t2 idxs2)))
  ([t1 ^{:tag "[[Ljava.lang.Object;"} idxs1
    t2 ^{:tag "[[Ljava.lang.Object;"} idxs2]
   (let [l1 (alength idxs1)
         l2 (alength idxs2)

         ^{:tag "[[Ljava.lang.Object;"} res
         (make-array Object (+ l1 l2))]
     (if (.isArray (.getClass ^Object t1))
       (dotimes [i l1] (aset res i (typed-aget t1 (aget idxs1 i))))
       (dotimes [i l1] (aset res i (get t1 (aget idxs1 i)))))
     (if (.isArray (.getClass ^Object t2))
       (dotimes [i l2] (aset res (+ l1 i) (typed-aget t2 (aget idxs2 i))))
       (dotimes [i l2] (aset res (+ l1 i) (get t2 (aget idxs2 i)))))
     res)))

(defn same-keys?
  [a b]
  (and (= (count a) (count b))
       (every? #(contains? b %) (keys a))
       (every? #(contains? b %) (keys a))))

(defn sum-rel
  [a b]
  (let [{attrs-a :attrs, tuples-a :tuples} a
        {attrs-b :attrs, tuples-b :tuples} b]
    (cond
      (= attrs-a attrs-b)
      (relation! attrs-a
                 (if (vector? tuples-a)
                   (into tuples-a tuples-b)
                   (do (.addAll ^List tuples-a ^List tuples-b)
                       tuples-a)))

      (not (same-keys? attrs-a attrs-b))
      (raise
        "Can’t sum relations with different attrs: " attrs-a " and " attrs-b
        {:error :query/where})

      (every? number? (vals attrs-a)) ;; can’t conj into BTSetIter
      (let [idxb->idxa (vec (for [[sym idx-b] attrs-b]
                              [idx-b (attrs-a sym)]))
            tlen       (->> (vals attrs-a) ^long (reduce max) (inc))]
        (relation! attrs-a
                   (reduce
                     (fn [^List acc tuple-b]
                       (let [tuple' (make-array Object tlen)
                             tg     (if (u/array? tuple-b) typed-aget get)]
                         (doseq [[idx-b idx-a] idxb->idxa]
                           (aset ^objects tuple' idx-a (tg tuple-b idx-b)))
                         (.add acc tuple')
                         acc))
                     tuples-a
                     tuples-b)))

      :else
      (let [all-attrs (zipmap (keys (merge attrs-a attrs-b)) (range))]
        (-> (relation! all-attrs (FastList.))
            (sum-rel a)
            (sum-rel b))))))

(defn prod-tuples
  ([] (doto (FastList.) (.add (object-array []))))
  ([tuples] tuples)
  ([tuples1 tuples2]
   (reduce
     (fn [acc t1]
       (reduce (fn [^FastList acc t2]
                 (.add acc (join-tuples t1 t2))
                 acc)
               acc tuples2))
     (FastList.)
     tuples1)))

(defn prod-rel
  ([] (relation! {} (doto (FastList.) (.add (make-array Object 0)))))
  ([rel1 rel2]
   (let [attrs1 (keys (:attrs rel1))
         attrs2 (keys (:attrs rel2))
         idxs1  (to-array (map (:attrs rel1) attrs1))
         idxs2  (to-array (map (:attrs rel2) attrs2))]
     (relation!
       (zipmap (u/concatv attrs1 attrs2) (range))
       (reduce
         (fn [acc t1]
           (reduce (fn [^FastList acc t2]
                     (.add acc (join-tuples t1 idxs1 t2 idxs2))
                     acc)
                   acc (:tuples rel2)))
         (FastList.)
         (:tuples rel1))))))

(defn vertical-tuples
  [coll]
  (doto (FastList.) (.addAll (mapv #(object-array [%]) coll))))

(defn horizontal-tuples
  [coll]
  (doto (FastList.) (.add (object-array coll))))

(defn single-tuples [tuple] (doto (FastList.) (.add tuple)))

(defn many-tuples
  [values]
  (transduce
    (comp
      (partition-by #(instance? FastList %))
      (map #(if (instance? FastList (first %))
              (reduce prod-tuples (mapv vertical-tuples %))
              (horizontal-tuples %))))
    prod-tuples
    values))

(defn filter-rel
  [{:keys [attrs ^List tuples]} v pred]
  (let [new-tuples (FastList.)
        idx        (attrs v)]
    (dotimes [i (.size tuples)]
      (let [tuple ^objects (.get tuples i)]
        (when (pred (aget tuple idx))
          (.add new-tuples tuple))))
    (relation! attrs new-tuples)))
