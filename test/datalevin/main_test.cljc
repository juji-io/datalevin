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
    (is (s/includes? res "#{[1 \"Datalevin\"]}"))))

(deftest copy-test
  (let [src (u/tmp-dir (str "datalevin-copy-test-" (UUID/randomUUID)))
        dst (u/tmp-dir (str "datalevin-copy-test-" (UUID/randomUUID)))
        db  (d/open-kv src)
        dbi "a"]
    (d/open-dbi db dbi)
    (d/transact-kv db [[:put dbi "Hello" "Datalevin"]])
    (d/close-kv db)
    (sut/copy src [dst] true)
    (let [db-copied (d/open-kv dst)]
      (d/open-dbi db-copied dbi)
      (is (= (d/get-value db-copied dbi "Hello") "Datalevin"))
      (d/close-kv db-copied))))

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
      (d/close-kv db-droped))))
