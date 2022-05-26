(ns datalevin.relation
  (:require
   [clojure.set :as set]
   [me.tonsky.persistent-sorted-set.arrays :as da]
   [datalevin.parser :as dp
    #?@(:cljs [:refer [BindColl BindIgnore BindScalar BindTuple Constant
                       FindColl FindRel FindScalar FindTuple PlainSymbol
                       RulesVar SrcVar Variable]])]
   [datalevin.db :as db]
   [datalevin.util :as u #?(:cljs :refer-macros :clj :refer) [raise]]))

(defn same-keys? [a b]
  (and (= (count a) (count b))
       (every? #(contains? b %) (keys a))
       (every? #(contains? b %) (keys a))))

(defn intersect-keys [attrs1 attrs2]
  (set/intersection (set (keys attrs1))
                    (set (keys attrs2))))

;; attrs:
;;    {?e 0, ?v 1} or {?e2 "a", ?age "v"}
;; tuples:
;;    [ #js [1 "Ivan" 5 14] ... ]
;; or [ (Datom. 2 "Oleg" 1 55) ... ]
(defrecord Relation [attrs tuples])

;; Relation algebra

(def typed-aget
  #?(:cljs aget
     :clj  (fn [a i]
             (aget ^{:tag "[[Ljava.lang.Object;"} a ^Long i))))

(defn join-tuples [t1 #?(:cljs idxs1
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

(defn sum-rel [a b]
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

(def ^{:dynamic true
       :doc     "List of symbols in current pattern that might potentiall be
resolved to refs"}
  *lookup-attrs* nil)

(def ^{:dynamic true
       :doc     "Default pattern source. Lookup refs, patterns, rules will be
resolved with it"}
  *implicit-source* nil)

(defn getter-fn [attrs attr]
  (let [idx (attrs attr)]
    (if (contains? *lookup-attrs* attr)
      (fn [tuple]
        (let [tg  (if (da/array? tuple) typed-aget get)
              eid (tg tuple idx)]
          (cond
            (number? eid)     eid ;; quick path to avoid fn call
            (sequential? eid) (db/entid *implicit-source* eid)
            (da/array? eid)   (db/entid *implicit-source* eid)
            :else             eid)))
      (fn [tuple]
        (let [tg (if (da/array? tuple) typed-aget get)]
          (tg tuple idx)
          (#?(:cljs da/aget :clj get) tuple idx))))))

(defn tuple-key-fn [getters]
  (if (== (count getters) 1)
    (first getters)
    (let [getters (to-array getters)]
      (fn [tuple]
        (list* #?(:cljs (.map getters #(% tuple))
                  :clj  (to-array (map #(% tuple) getters))))))))

(defn hash-attrs [key-fn tuples]
  (loop [tuples     tuples
         hash-table (transient {})]
    (if-some [tuple (first tuples)]
      (let [key (key-fn tuple)]
        (recur (next tuples)
               (assoc! hash-table key (conj (get hash-table key '()) tuple))))
      (persistent! hash-table))))

(defn hash-join [rel1 rel2]
  (let [tuples1      (:tuples rel1)
        tuples2      (:tuples rel2)
        attrs1       (:attrs rel1)
        attrs2       (:attrs rel2)
        common-attrs (vec (intersect-keys (:attrs rel1) (:attrs rel2)))
        common-gtrs1 (map #(getter-fn attrs1 %) common-attrs)
        common-gtrs2 (map #(getter-fn attrs2 %) common-attrs)
        keep-attrs1  (keys attrs1)
        keep-attrs2  (vec
                       (set/difference (set (keys attrs2)) (set (keys attrs1))))
        keep-idxs1   (to-array (map attrs1 keep-attrs1))
        keep-idxs2   (to-array (map attrs2 keep-attrs2))
        key-fn1      (tuple-key-fn common-gtrs1)
        key-fn2      (tuple-key-fn common-gtrs2)]
    (if (< (count tuples1) (count tuples2))
      (let [hash       (hash-attrs key-fn1 tuples1)
            new-tuples (persistent!
                         (reduce
                           (fn [acc tuple2]
                             (let [key (key-fn2 tuple2)]
                               (if-some [tuples1 (get hash key)]
                                 (reduce
                                   (fn [acc tuple1]
                                     (conj! acc
                                            (join-tuples tuple1 keep-idxs1
                                                         tuple2 keep-idxs2)))
                                   acc tuples1)
                                 acc)))
                           (transient []) tuples2))]
        (Relation. (zipmap (concat keep-attrs1 keep-attrs2) (range))
                   new-tuples))
      (let [hash       (hash-attrs key-fn2 tuples2)
            new-tuples (persistent!
                         (reduce
                           (fn [acc tuple1]
                             (let [key (key-fn1 tuple1)]
                               (if-some [tuples2 (get hash key)]
                                 (reduce
                                   (fn [acc tuple2]
                                     (conj! acc (join-tuples tuple1 keep-idxs1
                                                             tuple2 keep-idxs2)))
                                   acc tuples2)
                                 acc)))
                           (transient []) tuples1))]
        (Relation. (zipmap (concat keep-attrs1 keep-attrs2) (range))
                   new-tuples)))))

(defn subtract-rel [a b]
  (let [{attrs-a :attrs, tuples-a :tuples} a
        {attrs-b :attrs, tuples-b :tuples} b
        attrs                              (intersect-keys attrs-a attrs-b)
        getters-b                          (map #(getter-fn attrs-b %) attrs)
        key-fn-b                           (tuple-key-fn getters-b)
        hash                               (hash-attrs key-fn-b tuples-b)
        getters-a                          (map #(getter-fn attrs-a %) attrs)
        key-fn-a                           (tuple-key-fn getters-a)]
    (assoc a
           :tuples (filterv #(nil? (hash (key-fn-a %))) tuples-a))))

(defn ^Relation empty-rel [binding]
  (let [vars (->> (dp/collect-vars-distinct binding)
                  (map :symbol))]
    (Relation. (zipmap vars (range)) [])))

(defn collapse-rels [rels new-rel]
  (loop [rels    rels
         new-rel new-rel
         acc     []]
    (if-some [rel (first rels)]
      (if (not-empty (intersect-keys (:attrs new-rel) (:attrs rel)))
        (recur (next rels) (hash-join rel new-rel) acc)
        (recur (next rels) new-rel (conj acc rel)))
      (conj acc new-rel))))
