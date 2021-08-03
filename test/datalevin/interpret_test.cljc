(ns datalevin.interpret-test
  (:require [datalevin.interpret :as sut]
            [datalevin.util :as u]
            [clojure.string :as s]
            #?(:clj [clojure.test :refer [is deftest]]
               :cljs [cljs.test :as t :include-macros true]))
  (:import [java.util UUID]))

(deftest exec-test
  (let [dir  (u/tmp-dir (str "datalevin-exec-test-" (UUID/randomUUID)))
        code (str "(def conn (get-conn \"" dir "\"))"
                  "(transact! conn [{:name \"Datalevin\"}])"
                  "(q (quote [:find ?e ?n :where [?e :name ?n]]) @conn)"
                  "(close conn)")
        res  (with-out-str (sut/exec-code code))]
    (is (s/includes? res "#{[1 \"Datalevin\"]}"))
    (u/delete-files dir)))

(deftest query-clause-test
  (let [dir  (u/tmp-dir (str "datalevin-query-clause-" (UUID/randomUUID)))
        code (str "(def conn (get-conn \"" dir "\"))"
                  "(transact! conn [{:name \"Datalevin\"}
                                    {:some \"value\"} {:another \"val\"}])"
                  "(q (quote [:find ?e :where (or [?e :name \"Datalevin\"]
                                                  [?e :some \"value\"])]) @conn)"
                  "(close conn)")
        res  (with-out-str (sut/exec-code code))]
    (is (or (s/includes? res "#{[1] [2]}")
            (s/includes? res "#{[2] [1]}")))
    (u/delete-files dir)))
