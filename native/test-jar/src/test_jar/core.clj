(ns test-jar.core
  (:require [datalevin.core :as d])
  (:gen-class))

(defn run [_opts]
  (let [schema {:aka  {:db/cardinality :db.cardinality/many}
                ;; :db/valueType is optional, if unspecified, the attribute will be
                ;; treated as EDN blobs, and may not be optimal for range queries
                :name {:db/valueType :db.type/string
                       :db/unique    :db.unique/identity}}
        conn   (d/get-conn "/tmp/datalevin/mydb" schema)]
    (println
      (d/transact! conn
                   [{:name "Frege", :db/id -1, :nation "France", :aka ["foo" "fred"]}
                    {:name "Peirce", :db/id -2, :nation "france"}
                    {:name "De Morgan", :db/id -3, :nation "English"}]))

    ;; Query the data
    (println (d/q '[:find ?nation
                    :in $ ?alias
                    :where
                    [?e :aka ?alias]
                    [?e :nation ?nation]]
                  (d/db conn)
                  "fred"))
    ;; => #{["France"]}

    ;; Retract the name attribute of an entity
    (println (d/transact! conn [[:db/retract 1 :name "Frege"]]))

    ;; Pull the entity, now the name is gone
    (println (d/q '[:find (pull ?e [*])
                    :in $ ?alias
                    :where
                    [?e :aka ?alias]]
                  (d/db conn)
                  "fred"))
    ;; => ([{:db/id 1, :aka ["foo" "fred"], :nation "France"}])

    ;; Close DB connection
    (d/close conn)))

(defn -main [& _args]
  (run {}))
