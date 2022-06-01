(ns datalevin.relation
  (:require
   [clojure.set :as set]
   [me.tonsky.persistent-sorted-set.arrays :as da]
   [datalevin.parser :as dp
    #?@(:cljs [:refer [BindColl BindIgnore BindScalar BindTuple Constant
                       FindColl FindRel FindScalar FindTuple PlainSymbol
                       RulesVar SrcVar Variable]])]
   [datalevin.util :as u #?(:cljs :refer-macros :clj :refer) [raise]]))

(defn same-keys?
  [a b]
  (and (= (count a) (count b))
       (every? #(contains? b %) (keys a))
       (every? #(contains? b %) (keys a))))

(defn intersect-keys
  [attrs1 attrs2]
  (set/intersection (set (keys attrs1)) (set (keys attrs2))))

;; attrs:
;;    {?e 0, ?v 1} or {?e2 "a", ?age "v"}
;; tuples:
;;    [ #js [1 "Ivan" 5 14] ... ]
;; or [ (Datom. 2 "Oleg" 1 55) ... ]
(deftype Relation [attrs tuples])

;; Relation algebra

(def typed-aget
  #?(:cljs aget
     :clj  (fn [a i]
             (aget ^{:tag "[[Ljava.lang.Object;"} a ^Long i))))

(defn join-tuples
  [t1 #?(:cljs idxs1
         :clj  ^{:tag "[[Ljava.lang.Object;"} idxs1)
   t2 #?(:cljs idxs2
         :clj  ^{:tag "[[Ljava.lang.Object;"} idxs2)]
  (let [l1  (alength idxs1)
        l2  (alength idxs2)
        tg1 (if (da/array? t1) typed-aget get)
        tg2 (if (da/array? t2) typed-aget get)
        res (da/make-array (+ l1 l2))]
    (dotimes [i l1]
      (aset res i (tg1 t1 (aget idxs1 i))))
    (dotimes [i l2]
      (aset res (+ l1 i) (tg2 t2 (aget idxs2 i))))
    res))

(defn sum-rel
  [a b]
  (let [{attrs-a :attrs, tuples-a :tuples} a
        {attrs-b :attrs, tuples-b :tuples} b]
    (cond
      (= attrs-a attrs-b)
      (Relation. attrs-a (into (vec tuples-a) tuples-b))

      (not (same-keys? attrs-a attrs-b))
      (raise "Can’t sum relations with different attrs: " attrs-a " and " attrs-b
             {:error :query/where})

      (every? number? (vals attrs-a)) ;; can’t conj into BTSetIter
      (let [idxb->idxa (vec (for [[sym idx-b] attrs-b]
                              [idx-b (attrs-a sym)]))
            tlen       (->> (vals attrs-a) ^long (reduce max) (inc))
            tuples'    (persistent!
                         (reduce
                           (fn [acc tuple-b]
                             (let [tuple' (da/make-array tlen)
                                   tg     (if (da/array? tuple-b) typed-aget get)]
                               (doseq [[idx-b idx-a] idxb->idxa]
                                 (aset tuple' idx-a (tg tuple-b idx-b)))
                               (conj! acc tuple')))
                           (transient (vec tuples-a))
                           tuples-b))]
        (Relation. attrs-a tuples'))

      :else
      (let [all-attrs (zipmap (keys (merge attrs-a attrs-b)) (range))]
        (-> (Relation. all-attrs [])
            (sum-rel a)
            (sum-rel b))))))

(defn ^Relation prod-rel
  ([] (Relation. {} [(da/make-array 0)]))
  ([rel1 rel2]
   (let [attrs1 (keys (:attrs rel1))
         attrs2 (keys (:attrs rel2))
         idxs1  (to-array (map (:attrs rel1) attrs1))
         idxs2  (to-array (map (:attrs rel2) attrs2))]
     (Relation.
       (zipmap (concat attrs1 attrs2) (range))
       (persistent!
         (reduce
           (fn [acc t1]
             (reduce (fn [acc t2]
                       (conj! acc (join-tuples t1 idxs1 t2 idxs2)))
                     acc (:tuples rel2)))
           (transient []) (:tuples rel1)))))))

(defn ^Relation empty-rel
  [binding]
  (let [vars (->> (dp/collect-vars-distinct binding)
                  (map :symbol))]
    (Relation. (zipmap vars (range)) [])))

(defn equal-rel?
  [a b]
  (and (= (:attrs a) (:attrs b))
       (= (map seq (:tuples a))
          (map seq (:tuples b)))))
