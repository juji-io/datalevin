(ns datalevin.test
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    #?(:clj [clojure.java.shell :as sh])
    datalevin.test.core

    datalevin.test.components
    datalevin.test.conn
    datalevin.test.db
    datalevin.test.entity
    datalevin.test.explode
    datalevin.test.ident
    datalevin.test.index
    datalevin.test.listen
    datalevin.test.lookup-refs
    datalevin.test.lru
    datalevin.test.parser
    datalevin.test.parser-find
    datalevin.test.parser-rules
    datalevin.test.parser-query
    datalevin.test.parser-where
    datalevin.test.pull-api
    datalevin.test.pull-parser
    datalevin.test.query
    datalevin.test.query-aggregates
    datalevin.test.query-find-specs
    datalevin.test.query-fns
    datalevin.test.query-not
    datalevin.test.query-or
    datalevin.test.query-pull
    datalevin.test.query-rules
    datalevin.test.transact
    datalevin.test.validation
    datalevin.test.upsert
    datalevin.test.issues))

(defn ^:export test-clj []
  (datalevin.test.core/wrap-res #(t/run-all-tests #"datalevin\..*")))

(defn ^:export test-cljs []
  (datalevin.test.core/wrap-res #(t/run-all-tests #"datalevin\..*")))

#?(:clj
(defn test-node [& args]
  (let [res (apply sh/sh "node" "test_node.js" args)]
    (println (:out res))
    (binding [*out* *err*]
      (println (:err res)))
    (System/exit (:exit res)))))
