(ns datomic-bench.core
  (:require
   [math-bench.core :as core]
   [datomic.api :as d]
   [jsonista.core :as json]))

(defn- schema-attr [name type & {:as args}]
  (merge
    {:db/id                 (d/tempid :db.part/db)
     :db/ident              name
     :db/valueType          type
     :db/cardinality        :db.cardinality/one
     :db.install/_attribute :db.part/db}
    args))

(defn new-conn
  ([] (new-conn "bench"))
  ([name]
   (let [url (str "datomic:mem://" name)]
     (d/delete-database url)
     (d/create-database url)
     (let [conn (d/connect url)]
       @(d/transact conn
                    [ (schema-attr :dissertation/cid :db.type/ref)
                     (schema-attr :dissertation/title :db.type/string)
                     (schema-attr :dissertation/univ :db.type/string)
                     (schema-attr :dissertation/country :db.type/string)
                     (schema-attr :dissertation/year :db.type/long)
                     (schema-attr :dissertation/area :db.type/string)
                     (schema-attr :person/name :db.type/string)
                     (schema-attr :person/advised :db.type/ref,
                                  :db/cardinality :db.cardinality/many)])
       conn))))

(def tx-data
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
             (let [did  (vswap! dcounter inc)
                   tdid (str did)
                   tid  (str id)]
               (into [(cond-> {:db/id tdid :dissertation/cid tid}
                        thesis  (assoc :dissertation/title thesis)
                        school  (assoc :dissertation/univ school)
                        country (assoc :dissertation/country country)
                        year    (assoc :dissertation/year year)
                        subject (assoc :dissertation/area subject))
                      {:db/id tid :person/name name}]
                     (map (fn [x]
                            {:db/id (str x) :person/advised tdid})
                          advisors))))))
      nodes)))

(defn db-with [conn tx-data]
  (-> conn
      (d/transact tx-data)
      deref
      :db-after))

(def db (db-with (new-conn "math") tx-data))

(defn q1 []
  (core/bench
    (d/q '[:find [?n ...]
           :in $ %
           :where
           [?d :person/name "David Scott Warren"]
           (adv ?x ?d)
           (adv ?y ?x)
           [?y :person/name ?n]]
         db core/rule-q1)))

(defn q2 []
  (core/bench
    (d/q '[:find [?n ...]
           :in $ %
           :where
           (adv ?x ?y)
           (univ ?x ?u)
           (univ ?y ?u)
           [?y :person/name ?n]]
         db core/rule-q2)))

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
         db core/rule-q3)))

(defn q4 []
  (core/bench-once
    (d/q '[:find [?n ...]
           :in $ %
           :where
           [?x :person/name "David Scott Warren"]
           (anc ?y ?x)
           [?y :person/name ?n]]
         db core/rule-q4)))

(defn ^:export -main [& names]
  (doseq [n names]
    (if-some [benchmark (ns-resolve 'datomic-bench.core (symbol n))]
      (let [perf (benchmark)]
        (print (core/round perf) "\t")
        (flush))
      (do
        (print "---" "\t")
        (flush))))
  (println))
