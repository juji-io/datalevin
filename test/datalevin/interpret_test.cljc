(ns datalevin.interpret-test
  (:require [datalevin.interpret :as sut]
            [datalevin.util :as u]
            [datalevin.datom :as d]
            [datalevin.core :as dc]
            [taoensso.nippy :as nippy]
            [clojure.string :as s]
            #?(:clj [clojure.test :refer [is deftest]]
               :cljs [cljs.test :as t :include-macros true]))
  (:import [java.util UUID]
           [datalevin.datom Datom]))

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

(deftest inter-fn-test
  (let [equal-v   (sut/inter-fn [datom v] (= (dc/datom-v datom) v))
        equal-v'  (nippy/thaw (nippy/freeze equal-v))
        start     10
        end       20
        in-range  (sut/inter-fn [k] (< start ^long k end))
        in-range' (nippy/thaw (nippy/freeze in-range))]
    (is (sut/inter-fn? equal-v))
    (is (equal-v (d/datom 1 :a 2) 2))
    (is (sut/inter-fn? in-range))
    (is (in-range 12))
    (is (sut/inter-fn? equal-v'))
    (is (equal-v' (d/datom 1 :a 2) 2))
    (is (sut/inter-fn? in-range'))
    (is (in-range' 12))))
