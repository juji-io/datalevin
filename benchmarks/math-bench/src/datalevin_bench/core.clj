(ns datalevin-bench.core
  (:require
   [math-bench.core :as core]
   [datalevin.core :as d]
   [jsonista.core :as json])
  (:import
   [java.util UUID]))

(def schema
  {:dissertation/cid     {:db/valueType :db.type/ref}
   :dissertation/title   {:db/valueType :db.type/string}
   :dissertation/univ    {:db/valueType :db.type/string}
   :dissertation/country {:db/valueType :db.type/string}
   :dissertation/year    {:db/valueType :db.type/long}
   :dissertation/area    {:db/valueType :db.type/string}
   :person/name          {:db/valueType :db.type/string}
   :person/advised       {:db/valueType   :db.type/ref
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

(defn load-data
  [dir]
  (d/conn-from-datoms datoms dir schema {:kv-opts {:mapsize 300}}))

(def q1-conn (load-data (str "/tmp/math-q1-" (UUID/randomUUID))))
(def q2-conn (load-data (str "/tmp/math-q2-" (UUID/randomUUID))))
(def q3-conn (load-data (str "/tmp/math-q3-" (UUID/randomUUID))))
(def q4-conn (load-data (str "/tmp/math-q4-" (UUID/randomUUID))))

(defn q1 []
  (core/bench
    (d/q '[:find [?n ...]
           :in $ %
           :where
           [?d :person/name "David Scott Warren"]
           (adv ?x ?d)
           (adv ?y ?x)
           [?y :person/name ?n]]
         (d/db q1-conn) core/rule-q1)))

(defn q2 []
  (core/bench
    (d/q '[:find [?n ...]
           :in $ %
           :where
           (adv ?x ?y)
           (univ ?x ?u)
           (univ ?y ?u)
           [?y :person/name ?n]]
         (d/db q2-conn) core/rule-q2)))

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
         (d/db q3-conn) core/rule-q3)))

(defn q4 []
  (core/bench-once
    (d/q '[:find [?n ...]
           :in $ %
           :where
           [?x :person/name "David Scott Warren"]
           (anc ?y ?x)
           [?y :person/name ?n]]
         (d/db q4-conn) core/rule-q4)))

(defn ^:export -main [& names]
  (doseq [n names]
    (if-some [benchmark (ns-resolve 'datalevin-bench.core (symbol n))]
      (let [perf (benchmark)]
        (print (core/round perf) "\t")
        (flush))
      (do
        (print "---" "\t")
        (flush))))
  (println))
