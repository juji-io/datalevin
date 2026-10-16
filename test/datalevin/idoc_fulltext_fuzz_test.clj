(ns datalevin.idoc-fulltext-fuzz-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.idoc-fulltext-fuzz :as fuzz]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(deftest markdown-fulltext-history-smoke-test
  (is (fuzz/run-history!
        [{:op :put :eid 1 :attr :doc/md
          :input "# something\nOfficia."
          :doc {:something "Officia."}}
         {:op :update-schema :attr :doc/md :patch {:db/fulltext true}}
         {:op :re-index}
         {:op :reopen}
         {:op :put :eid 1 :attr :doc/md
          :input {:preface "Quartz."} :doc {:preface "Quartz."}}
         {:op :put :eid 1 :attr :doc/parts
          :input "# something\nLaboris." :doc {:something "Laboris."}}
         {:op :put :eid 1 :attr :doc/parts
          :input "# heading\nZephyr." :doc {:heading "Zephyr."}}
         {:op :retract :eid 1 :attr :doc/parts}
         {:op :re-index}
         {:op :retract-attribute :eid 1 :attr :doc/md}
         {:op :retract-entity :eid 1}
         {:op :reopen}])))

(deftest generated-fulltext-history-smoke-test
  (let [result (fuzz/check! {:seed 353 :trials 3 :steps 10})]
    (is (:pass? result)
        (pr-str {:seed (:seed result)
                 :history (get-in result [:shrunk :smallest])}))))

(deftest re-index-preserves-user-kv-test
  (let [dir   (u/tmp-dir (str "re-index-user-kv-" (random-uuid)))
        conn* (volatile! (d/create-conn
                          dir {:doc/md {:db/valueType :db.type/idoc
                                        :db/idocFormat :markdown
                                        :db/fulltext true}}))]
    (try
      (let [kv (d/open-kv dir)]
        (try
          (d/open-dbi kv "user-data")
          (d/transact-kv kv [[:put "user-data" :keep "value"]])
          (finally (d/close-kv kv))))
      (d/transact! @conn* [{:db/id 1 :doc/md "# something\nOfficia."}])
      (vreset! conn* (d/re-index @conn* {}))
      (let [kv (d/open-kv dir)]
        (try
          (is (= "value" (d/get-value kv "user-data" :keep)))
          (finally (d/close-kv kv))))
      (finally
        (when-not (d/closed? @conn*) (d/close @conn*))
        (u/delete-files dir)))))
