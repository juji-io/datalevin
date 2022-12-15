(ns datalevin.test.listen
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datalevin.core :as d]
   [datalevin.datom :as dd]
   [datalevin.util :as u]
   [datalevin.constants :refer [tx0]]))

(deftest test-listen!
  (let [dir     (u/tmp-dir (str "query-or-" (random-uuid)))
        conn    (d/create-conn dir)
        reports (atom [])]
    (d/transact! conn [[:db/add -1 :name "Alex"]
                       [:db/add -2 :name "Boris"]])
    (d/listen! conn :test #(swap! reports conj %))
    (d/transact! conn [[:db/add -1 :name "Dima"]
                       [:db/add -1 :age 19]
                       [:db/add -2 :name "Evgeny"]] {:some-metadata 1})
    (d/transact! conn [[:db/add -1 :name "Fedor"]
                       [:db/add 1 :name "Alex2"]         ;; should update
                       [:db/retract 2 :name "Not Boris"] ;; should be skipped
                       [:db/retract 4 :name "Evgeny"]])
    (d/unlisten! conn :test)
    (d/transact! conn [[:db/add -1 :name "Geogry"]])

    (is (= (:tx-data (first @reports))
           [(dd/datom 3 :name "Dima"   (+ tx0 2) true)
            (dd/datom 3 :age 19        (+ tx0 2) true)
            (dd/datom 4 :name "Evgeny" (+ tx0 2) true)]))
    (is (= (:tx-meta (first @reports))
           {:some-metadata 1}))
    (is (= (:tx-data (second @reports))
           [(dd/datom 5 :name "Fedor"  (+ tx0 3) true)
            (dd/datom 1 :name "Alex"   (+ tx0 3) false)  ;; update -> retract
            (dd/datom 1 :name "Alex2"  (+ tx0 3) true)   ;;         + add
            (dd/datom 4 :name "Evgeny" (+ tx0 3) false)]))
    (is (= (:tx-meta (second @reports))
           nil))
    (d/close conn)
    (u/delete-files dir)))
