(ns datalevin.main-test
  (:require [datalevin.main :as sut]
            [datalevin.util :as u]
            [datalevin.core :as d]
            [clojure.test.check.generators :as gen]
            [clojure.string :as s]
            #?(:clj [clojure.test :refer [is deftest]]
               :cljs [cljs.test :as t :include-macros true])
            [clojure.java.io :as io])
  (:import [java.util UUID Date Arrays]
           [java.io ByteArrayInputStream]))

(deftest command-line-args-test
  (let [r (sut/validate-args ["-V"])]
    (is (:ok? r))
    (is (:exit-message r)))
  (let [r (sut/validate-args ["-h"])]
    (is (:ok? r))
    (is (:exit-message r)))
  (let [r (sut/validate-args ["-k"])]
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

(deftest exec-stdin-test
  (let [dir  (u/tmp-dir (str "datalevin-exec-stdin-test-" (UUID/randomUUID)))
        code (str "(def conn (get-conn \"" dir "\"))"
                  "(transact! conn [{:name \"Datalevin\"}])"
                  "(q (quote [:find ?e ?n :where [?e :name ?n]]) @conn)"
                  "(close conn)")
        res  (with-out-str (with-in-str code (sut/exec nil)))]
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
      (d/close-kv db-load))))

(deftest dump-load-datalog-test
  (let [src-dir  (u/tmp-dir (str "datalevin-dump-dl-" (UUID/randomUUID)))
        schema   {:a/keyword {:db/valueType :db.type/keyword}
                  :a/symbol  {:db/valueType :db.type/symbol}
                  :a/string  {:db/valueType :db.type/string}
                  :a/boolean {:db/valueType :db.type/boolean}
                  :a/long    {:db/valueType :db.type/long}
                  :a/double  {:db/valueType :db.type/double}
                  :a/float   {:db/valueType :db.type/float}
                  :a/ref     {:db/valueType :db.type/ref}
                  :a/instant {:db/valueType :db.type/instant}
                  :a/uuid    {:db/valueType :db.type/uuid}
                  :a/bytes   {:db/valueType :db.type/bytes}}
        src-conn (d/get-conn src-dir schema)
        now      (Date.)
        uuid     (UUID/randomUUID)
        s        (apply str (range 1000))
        bs       (.getBytes ^String s)
        vs       (repeatedly 10 #(gen/generate gen/any-printable-equatable 1000))
        txs      (into
                   [{:db/id -1
                     :hello "Datalevin"}
                    {:a/keyword :something/nice
                     :a/symbol  'wonderful/life
                     :a/string  s}
                    {:a/boolean true
                     :a/long    42}
                    {:a/double (double 3.141592)
                     :a/float  (float 2.71828)
                     :a/ref    -1}
                    {:a/instant now
                     :a/uuid    uuid
                     :a/bytes   bs}]
                   (mapv (fn [a v] {a v})
                         (repeat :large/random)
                         vs))
        dest-dir (u/tmp-dir (str "datalevin-load-dl-" (UUID/randomUUID)))
        dl-file  (str (u/tmp-dir) "dl" #_(UUID/randomUUID))]
    (d/transact! src-conn txs)
    (d/close src-conn)
    (sut/dump src-dir dl-file nil false true false)
    (sut/load dest-dir dl-file nil true)
    (let [conn-load (d/get-conn dest-dir)]
      (is (= (d/q '[:find ?v . :where [_ :hello ?v]] @conn-load) "Datalevin" ))
      (is (= (d/q '[:find ?v . :where [_ :a/keyword ?v]] @conn-load)
             :something/nice))
      (is (= (d/q '[:find ?v . :where [_ :a/symbol ?v]] @conn-load)
             'wonderful/life))
      (is (= (d/q '[:find ?v . :where [_ :a/string ?v]] @conn-load) s))
      (is (= (d/q '[:find ?v . :where [_ :a/boolean ?v]] @conn-load) true))
      (is (= (d/q '[:find ?v . :where [_ :a/long ?v]] @conn-load) 42))
      (is (= (d/q '[:find ?v . :where [_ :a/double ?v]] @conn-load)
             (double 3.141592)))
      (is (= (d/q '[:find ?v . :where [_ :a/float ?v]] @conn-load)
             (float 2.71828)))
      (is (= (d/q '[:find ?v . :where [_ :a/ref ?v]] @conn-load)
             (d/q '[:find ?e . :where [?e :hello]] @conn-load)))
      (is (= (d/q '[:find ?v . :where [_ :a/instant ?v]] @conn-load) now))
      (is (= (d/q '[:find ?v . :where [_ :a/uuid ?v]] @conn-load) uuid))
      (is (Arrays/equals
            ^bytes (d/q '[:find ?v . :where [_ :a/bytes ?v]] @conn-load)
            ^bytes bs))
      (is (= (set
               (d/q '[:find [?v ...] :where [_ :large/random ?v]] @conn-load))
             (set vs)))
      (d/close conn-load))))
