(ns datascript-bench.core
  (:require
   [math-bench.core :as core]
   [datascript.core :as d]
   [jsonista.core :as json])
  (:import
   [java.util UUID]))

(def schema
  {:dissertation/cid  {:db/valueType :db.type/ref}
   :dissertation/univ {:db/index true}
   :dissertation/area {:db/index true}
   :person/name       {:db/index true}
   :person/advised    {:db/valueType   :db.type/ref
                       :db/cardinality :db.cardinality/many}})

(def datoms
  (let [nodes    (:nodes (json/read-value (slurp "data.json")
                                          json/keyword-keys-object-mapper))
        dcounter (volatile! 1000000)
        cids     (into #{} (map :id) nodes)
        clean    #(remove (complement cids) %)]
    (sequence
      (comp (remove #(nil? (:name %)))
         (map #(update % :advisors clean))
         (mapcat
           (fn [{:keys [id name thesis school country year subject advisors]}]
             (let [did (vswap! dcounter inc)]
               (into
                 (cond-> [(d/datom did :dissertation/cid id)]
                   name    (conj (d/datom id :person/name name))
                   thesis  (conj (d/datom did :dissertation/title thesis))
                   school  (conj (d/datom did :dissertation/univ school))
                   country (conj (d/datom did :dissertation/country country))
                   year    (conj (d/datom did :dissertation/year year))
                   subject (conj (d/datom did :dissertation/area subject)))
                 (map #(d/datom % :person/advised did) advisors))))))
      nodes)))

(defn load-data [] (d/conn-from-datoms datoms schema ))

(def conn (load-data))

(def rule-author '[[(author ?d ?c)
                    [?d :dissertation/cid ?c]]])

(def rule-adv '[[(adv ?x ?y)
                 [?x :person/advised ?d]
                 (author ?d ?y)]])

(def rule-area '[[(area ?c ?a)
                  [?d :dissertation/cid ?c]
                  [?d :dissertation/area ?a]]])

(def rule-univ '[[(univ ?c ?u)
                  [?d :dissertation/cid ?c]
                  [?d :dissertation/univ ?u]]])

(def rule-anc '[[(anc ?x ?y)
                 (adv ?x ?y)]
                [(anc ?x ?y)
                 (adv ?x ?z)
                 (adv ?z ?y)]])

(def rule-q1 (into rule-author rule-adv))
(def rule-q2 (into rule-q1 rule-univ))
(def rule-q3 (into rule-q1 rule-area))
(def rule-q4 (into rule-q1 rule-anc))

(defn q1 []
  (core/bench
    (d/q '[:find [?n ...]
           :in $ %
           :where
           [?d :person/name "David Scott Warren"]
           (adv ?x ?d)
           (adv ?y ?x)
           [?y :person/name ?n]]
         (d/db conn) rule-q1)))

(defn q2 []
  (core/bench
    (d/q '[:find [?n ...]
           :in $ %
           :where
           (adv ?x ?y)
           (univ ?x ?u)
           (univ ?y ?u)
           [?y :person/name ?n]]
         (d/db conn) rule-q2)))

(defn q3 []
  (core/bench
    (d/q '[:find [?n ...]
           :in $ %
           :where
           (adv ?x ?y)
           (area ?x ?a1)
           (area ?y ?a2)
           [(!= ?a1 ?a2)]
           [?y :person/name ?n]]
         (d/db conn) rule-q3)))

(defn q4 []
  (core/bench
    (d/q '[:find [?n ...]
           :in $ %
           :where
           [?x :person/name "David Scott Warren"]
           (anc ?y ?x)
           [?y :person/name ?n]]
         (d/db conn) rule-q4)))

(defn ^:export -main [& names]
  (doseq [n names]
    (if-some [benchmark (ns-resolve 'datascript-bench.core (symbol n))]
      (let [perf (benchmark)]
        (print (core/round perf) "\t")
        (flush))
      (do
        (print "---" "\t")
        (flush))))
  (println))
