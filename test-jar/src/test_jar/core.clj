(ns test-jar.core
  (:require [datalevin.core :as d]))

;; Define an optional schema.
;; Note that pre-defined schema is optional, as Datalevin does schema-on-write.
;; However, attributes requiring special handling need to be defined in schema,
;; e.g. many cardinality, uniqueness constraint, reference type, and so on.
(def schema {:aka    {:db/cardinality :db.cardinality/many}
             ;; :db/valueType is optional, if unspecified, the attribute will be
             ;; treated as EDN blobs, and may not be optimal for range queries
             :name   {:db/valueType :db.type/string
                      :db/unique    :db.unique/identity}
             :height {:db/valueType :db.type/float}
             :weight {:db/valueType :db.type/bigdec}
             })

;; Create DB on disk and connect to it, assume write permission to create given dir
(def conn (d/get-conn "/tmp/datalevin/mydb1" schema))
;; or if you have a Datalevin server running on myhost with default port 8898
;; (def conn (d/get-conn "dtlv://myname:mypasswd@myhost/mydb" schema))


(defn run [opts]
  (d/transact! conn
               [{:name   "Frege", :db/id -1, :nation "France",
                 :aka    ["foo" "fred"]
                 :height 1.73
                 :weight 12M}
                {:name   "Peirce", :db/id -2, :nation "france"
                 :height 1.82
                 :weight 140M}
                {:name   "De Morgan", :db/id -3, :nation "English"
                 :height 1.76
                 :weight 130M}])

  ;; Query the data
  (d/q '[:find ?nation
         :in $ ?alias
         :where
         [?e :aka ?alias]
         [?e :nation ?nation]]
       (d/db conn)
       "fred")
  ;; => #{["France"]}

  ;; Retract the name attribute of an entity
  (d/transact! conn [[:db/retract 1 :name "Frege"]])

  ;; Pull the entity, now the name is gone
  (d/q '[:find (pull ?e [*])
         :in $ ?alias
         :where
         [?e :aka ?alias]]
       (d/db conn)
       "fred")
  ;; => ([{:db/id 1, :aka ["foo" "fred"], :nation "France"}])

  ;; Close DB connection
  (d/close conn)

  (println "Success!"))

(defn -main [& args]
  (run {}))
