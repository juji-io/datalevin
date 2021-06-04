(ns datalevin.main-test
  (:require [datalevin.main :as sut]
            [datalevin.util :as u]
            [datalevin.core :as d]
            [clojure.string :as s]
            #?(:clj [clojure.test :refer [is deftest]]
               :cljs [cljs.test :as t :include-macros true]))
  (:import [java.util UUID]))

(deftest command-line-args-test
  (let [r (sut/validate-args ["-V"])]
    (is (:ok? r))
    (is (:exit-message r)))
  (let [r (sut/validate-args ["-h"])]
    (is (:ok? r))
    (is (:exit-message r)))
  (let [r (sut/validate-args ["-v"])]
    (is (:exit-message r))
    (is (s/includes? (:exit-message r) "Unknown option")))
  (let [r (sut/validate-args ["hello"])]
    (is (:exit-message r))
    (is (s/includes? (:exit-message r) "Usage")))
  (let [r (sut/validate-args [])]
    (is (= (:command r) "repl")))
  (let [r (sut/validate-args ["help"])]
    (is (= (:command r) "help"))
    (is (:summary r)))
  (let [r (sut/validate-args ["help" "repl"])]
    (is (= (:command r) "help"))
    (is (:summary r))
    (is (= (:arguments r) ["repl"]))))

(deftest exec-test
  (let [dir  (u/tmp-dir (str "datalevin-exec-test-" (UUID/randomUUID)))
        code (str "(def conn (get-conn \"" dir "\"))"
                  "(transact! conn [{:name \"Datalevin\"}])"
                  "(q (quote [:find ?e ?n :where [?e :name ?n]]) @conn)"
                  "(close conn)")
        res  (with-out-str (sut/exec code))]
    (is (s/includes? res "#{[1 \"Datalevin\"]}"))
    (u/delete-files dir)))

(deftest copy-test
  (let [src (u/tmp-dir (str "datalevin-copy-test-" (UUID/randomUUID)))
        dst (u/tmp-dir (str "datalevin-copy-test-" (UUID/randomUUID)))
        db  (d/open-kv src)
        dbi "a"]
    (d/open-dbi db dbi)
    (d/transact-kv db [[:put dbi "Hello" "Datalevin"]])
    (sut/copy src dst true)
    (let [db-copied (d/open-kv dst)]
      (d/open-dbi db-copied dbi)
      (is (= (d/get-value db-copied dbi "Hello") "Datalevin"))
      (d/close-kv db-copied))
    (d/close-kv db)
    (u/delete-files src)
    (u/delete-files dst)))

(deftest drop-test
  (let [dir (u/tmp-dir (str "datalevin-drop-test-" (UUID/randomUUID)))
        db  (d/open-kv dir)
        dbi "a"]
    (d/open-dbi db dbi)
    (d/transact-kv db [[:put dbi "Hello" "Datalevin"]])
    (d/close-kv db)
    (sut/drop dir [dbi] false)
    (let [db-droped (d/open-kv dir)]
      (d/open-dbi db-droped dbi)
      (is (nil? (d/get-value db-droped dbi "Hello")))
      (d/close-kv db-droped))
    (sut/drop dir [dbi] true)
    (let [db-droped (d/open-kv dir)]
      (is (empty? (d/list-dbis db-droped)))
      (d/close-kv db-droped))
    (u/delete-files dir)))

(deftest dump-load-raw-test
  (let [src-dir  (u/tmp-dir (str "datalevin-dump-raw-" (UUID/randomUUID)))
        src-db   (d/open-kv src-dir)
        dest-dir (u/tmp-dir (str "datalevin-load-raw-" (UUID/randomUUID)))
        raw-file (str (u/tmp-dir) "ok")
        dbi      "a"]
    (d/open-dbi src-db dbi)
    (d/transact-kv src-db [[:put dbi "Hello" "Datalevin"]])
    (d/close-kv src-db)
    (sut/dump src-dir raw-file nil false false true)
    (sut/load dest-dir raw-file "b" false)
    (let [db-load (d/open-kv dest-dir)]
      (d/open-dbi db-load "b")
      (is (= "Datalevin" (d/get-value db-load "b" "Hello")))
      (d/close-kv db-load))
    (u/delete-files src-dir)
    (u/delete-files dest-dir)))

(deftest dump-load-datalog-test
  (let [src-dir  (u/tmp-dir (str "datalevin-dump-dl-" (UUID/randomUUID)))
        src-conn (d/get-conn src-dir)
        dest-dir (u/tmp-dir (str "datalevin-load-dl-" (UUID/randomUUID)))
        dl-file  (str (u/tmp-dir) (UUID/randomUUID))]
    (d/transact! src-conn [{:db/id -1 :hello "Datalevin"}])
    (d/close src-conn)
    (sut/dump src-dir dl-file nil false true false)
    (sut/load dest-dir dl-file nil true)
    (let [conn-load (d/get-conn dest-dir)]
      (is (= "Datalevin" (d/q '[:find ?v .
                                :where [_ :hello ?v]]
                              @conn-load)))
      (d/close conn-load))
    (u/delete-files src-dir)
    (u/delete-files dest-dir)))

(deftest query-clause-test
  (let [dir  (u/tmp-dir (str "datalevin-query-clause-" (UUID/randomUUID)))
        code (str "(def conn (get-conn \"" dir "\"))"
                  "(transact! conn [{:name \"Datalevin\"}
                                    {:some \"value\"} {:another \"val\"}])"
                  "(q (quote [:find ?e :where (or [?e :name \"Datalevin\"]
                                                  [?e :some \"value\"])]) @conn)"
                  "(close conn)")
        res  (with-out-str (sut/exec code))]
    (is (or (s/includes? res "#{[1] [2]}")
            (s/includes? res "#{[2] [1]}")))
    (u/delete-files dir)))
