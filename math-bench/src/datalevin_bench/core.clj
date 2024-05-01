(ns datalevin-bench.core
  (:require
   [datalevin.core :as d]
   [jsonista.core :as json])
  (:import
   [java.util UUID]))

(def schema
  {:name     {:db/valueType :db.type/string}
   :thesis   {:db/valueType :db.type/string}
   :school   {:db/valueType :db.type/string}
   :country  {:db/valueType :db.type/string}
   :year     {:db/valueType :db.type/long}
   :subject  {:db/valueType :db.type/string}
   :advisors {:db/valueType   :db.type/ref
              :db/cardinality :db.cardinality/many}
   :students {:db/valueType   :db.type/ref
              :db/cardinality :db.cardinality/many}})

(def data-txs
  (let [nodes     (:nodes (json/read-value (slurp "data.json")
                                           json/keyword-keys-object-mapper))
        ids       (into #{} (map :id) nodes)
        temporize (fn [coll]
                    (sequence (comp (remove (complement ids))
                                 (map #(- %)))
                              coll))]
    (sequence
      (comp (map (fn [{:keys [id] :as node}]
                (-> node
                    (dissoc :id)
                    (assoc :db/id (- id))
                    (update :advisors temporize)
                    (update :students temporize))))
         (map #(into {} (remove (comp nil? val)) %)))
      nodes)))

(defn transact-data
  [dir]
  (let [conn (d/create-conn dir schema {:kv-opts {:mapsize 300}})]
    (d/transact! conn data-txs)
    conn))

(def q1-conn (transact-data (str "/tmp/math-q1-" (UUID/randomUUID))))
(def q2-conn (transact-data (str "/tmp/math-q2-" (UUID/randomUUID))))
(def q3-conn (transact-data (str "/tmp/math-q3-" (UUID/randomUUID))))
(def q4-conn (transact-data (str "/tmp/math-q4-" (UUID/randomUUID))))
(def q5-conn (transact-data (str "/tmp/math-q5-" (UUID/randomUUID))))
