(ns test-jar.core
  (:require [datalevin.core :as d])
  (:gen-class))

(defn run [opts]
  (let [schema {:aka    {:db/cardinality :db.cardinality/many}
                :name   {:db/valueType :db.type/string
                         :db/unique    :db.unique/identity}
                :height {:db/valueType :db.type/float}
                :weight {:db/valueType :db.type/bigdec}}
        conn   (d/get-conn "/tmp/datalevin/mydb" schema)]
    (d/transact! conn
                 [{:name   "Frege", :db/id -1, :nation "France", :aka ["foo" "fred"]
                   :height 1.73
                   :weight 12M}
                  {:name   "Peirce", :db/id -2, :nation "france"
                   :height 1.82
                   :weight 140M}
                  {:name   "De Morgan", :db/id -3, :nation "English"
                   :height 1.76
                   :weight 130M}])

    (d/q '[:find ?nation
           :in $ ?alias
           :where
           [?e :aka ?alias]
           [?e :nation ?nation]]
         (d/db conn)
         "fred")

    (d/transact! conn [[:db/retract 1 :name "Frege"]])

    (d/q '[:find (pull ?e [*])
           :in $ ?alias
           :where
           [?e :aka ?alias]]
         (d/db conn)
         "fred")

    (d/close conn)

    ))

(defn -main [& args]
  (run {})
  (println "Native Test Succeeded!"))
