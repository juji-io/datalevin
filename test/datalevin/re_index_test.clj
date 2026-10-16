(ns datalevin.re-index-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.conn :as conn]
   [datalevin.core :as d]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(deftest re-index-clears-stale-secondary-references-test
  ;; The fuzzer minimized this failure to one insert followed by re-index.
  ;; Rebuilt attribute IDs must not reinterpret old fulltext references.
  (let [dir    (u/tmp-dir (str "re-index-secondary-" (random-uuid)))
        schema {:doc/md {:db/valueType :db.type/idoc
                         :db/idocFormat :markdown
                         :db/fulltext true
                         :db.fulltext/autoDomain true}
                :doc/parts {:db/valueType :db.type/idoc
                            :db/idocFormat :markdown
                            :db/cardinality :db.cardinality/many
                            :db/fulltext true
                            :db.fulltext/autoDomain true}
                :doc/edn {:db/valueType :db.type/idoc
                          :db/idocFormat :edn
                          :db/fulltext true
                          :db.fulltext/autoDomain true}}
        conn*  (volatile! (d/create-conn dir schema))]
    (try
      (d/transact! @conn* [{:db/id 1 :doc/md "# something\nOfficia."}])
      (vreset! conn* (d/re-index @conn* {}))
      (doseq [phase [:re-indexed :reopened]]
        (when (= phase :reopened)
          (d/close @conn*)
          (vreset! conn* (d/create-conn dir)))
        (testing (name phase)
          (is (= #{[1 :doc/md {:something "Officia."}]}
                 (set (d/q '[:find ?e ?a ?v :where [?e ?a ?v]] @@conn*))))
          (doseq [query ["something" "Officia"]]
            (is (= #{[1 :doc/md]}
                   (set (d/q '[:find ?e ?a
                               :in $ ?q
                               :where [(fulltext $ ?q) [[?e ?a _]]]]
                             @@conn* query))))
            (doseq [attr [:doc/md :doc/parts :doc/edn]]
              (is (= (if (= attr :doc/md) #{1} #{})
                     (set (d/q '[:find [?e ...]
                                 :in $ ?attr ?q
                                 :where [(fulltext $ ?attr ?q) [[?e _ _]]]]
                               @@conn* attr query))))))))
      (finally
        (when-not (d/closed? @conn*) (d/close @conn*))
        (u/delete-files dir)))))

(deftest re-index-preserves-user-kv-test
  (let [dir   (u/tmp-dir (str "re-index-user-kv-" (random-uuid)))
        conn* (volatile! (d/create-conn
                          dir {:doc/md {:db/valueType :db.type/idoc
                                        :db/idocFormat :markdown
                                        :db/fulltext true}}))]
    (try
      (let [kv (conn/datalog-kv @conn*)]
        (d/open-dbi kv "user-data")
        (d/transact-kv kv [[:put "user-data" :keep "value"]]))
      (d/transact! @conn* [{:db/id 1 :doc/md "# something\nOfficia."}])
      (vreset! conn* (d/re-index @conn* {}))
      (is (= "value" (d/get-value (conn/datalog-kv @conn*) "user-data" :keep)))
      (finally
        (when-not (d/closed? @conn*) (d/close @conn*))
        (u/delete-files dir)))))
