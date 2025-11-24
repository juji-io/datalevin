(ns datalevin.migrate-test
  (:require
   [datalevin.migrate :as sut]
   [datalevin.core :as d]
   [datalevin.util :as u]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest is use-fixtures]]
   [clojure.string :as s])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(def old-jar (str (u/tmp-dir "datalevin-migrate")
                  u/+separator+
                  "jars"
                  u/+separator+
                  "0.9.27"
                  u/+separator+
                  "datalevin-0.9.27-standalone.jar"))

(deftest datalog-migrate-test
  (let [dir   (u/tmp-dir (str "migrate-test-" (UUID/randomUUID)))
        code  (str "(def conn (get-conn \"" dir "\"))"
                   "(transact! conn [{:name \"Datalevin\"}])"
                   "(close conn)")
        query '[:find ?e ?n :where [?e :name ?n]]
        jar   (sut/ensure-jar 0 9 27)
        cmd   (-> ["java"]
                  (into sut/java-opts)
                  (conj "-jar" jar "exec" code))]
    (is (= jar old-jar))
    (sut/run-cmd cmd)
    (let [conn (d/get-conn dir)
          res  (d/q query @conn)]
      (is (s/includes? res "#{[1 \"Datalevin\"]}"))
      (d/close conn))
    (u/delete-files dir)))

(deftest kv-migrate-test
  (let [dir  (u/tmp-dir (str "migrate-test-" (UUID/randomUUID)))
        code (str "(def db (open-kv \"" dir "\"))"
                  "(open-dbi db \"a\")"
                  "(transact-kv db [[:put \"a\" \"Hello\" \"Datalevin\"]])"
                  "(close-kv db)")
        jar  (sut/ensure-jar 0 9 27)
        cmd  (-> ["java"]
                 (into sut/java-opts)
                 (conj "-jar" jar "exec" code))]
    (is (= jar old-jar))
    (sut/run-cmd cmd)
    (let [db (d/open-kv dir)]
      (d/open-dbi db "a")
      (is (= "Datalevin" (d/get-value db "a" "Hello")))
      (d/close-kv db))
    (u/delete-files dir)))
