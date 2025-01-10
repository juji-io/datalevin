(ns datalevin.test1
  "more extensive tests"
  (:require
   [clojure.test :as t]
   datalevin.interpret-test
   datalevin.search-utils-test
   datalevin.spill-test
   datalevin.hu-test
   datalevin.concurrent-test
   datalevin.withtxn-test
   datalevin.map-resize-test
   datalevin.bits-test
   datalevin.protocol-test
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
   datalevin.test.tuples
   datalevin.test.validation
   datalevin.test.upsert
   datalevin.test.issues
   datalevin.server-test
   datalevin.client-test
   datalevin.main-test
   datalevin.remote-withtxnkv-test
   datalevin.remote-search-test
   datalevin.remote-sutil-test
   datalevin.remote-kv-test
   datalevin.remote-test
   datalevin.remote-dl-test
   datalevin.remote-entity-test
   datalevin.remote-transact-test)
  (:gen-class))

(defn ^:export test-clj []
  (let [{:keys [fail error]}
        (t/run-tests
          'datalevin.remote-test
          'datalevin.remote-dl-test
          'datalevin.remote-entity-test
          'datalevin.spill-test
          'datalevin.hu-test
          'datalevin.concurrent-test
          'datalevin.withtxn-test
          'datalevin.map-resize-test
          'datalevin.search-utils-test
          'datalevin.bits-test
          'datalevin.protocol-test
          'datalevin.interpret-test
          'datalevin.test.core
          'datalevin.test.components
          'datalevin.test.conn
          'datalevin.test.db
          'datalevin.test.entity
          'datalevin.test.explode
          'datalevin.test.ident
          'datalevin.test.index
          'datalevin.test.listen
          'datalevin.test.lookup-refs
          'datalevin.test.lru
          'datalevin.test.parser
          'datalevin.test.parser-find
          'datalevin.test.parser-rules
          'datalevin.test.parser-query
          'datalevin.test.parser-where
          'datalevin.test.pull-api
          'datalevin.test.pull-parser
          'datalevin.test.query
          'datalevin.test.query-aggregates
          'datalevin.test.query-find-specs
          'datalevin.test.query-fns
          'datalevin.test.query-not
          'datalevin.test.query-or
          'datalevin.test.query-pull
          'datalevin.test.query-rules
          'datalevin.test.tuples
          'datalevin.test.validation
          'datalevin.test.upsert
          'datalevin.test.issues
          'datalevin.server-test
          'datalevin.client-test
          'datalevin.main-test
          'datalevin.remote-kv-test
          'datalevin.remote-withtxnkv-test
          'datalevin.remote-search-test
          'datalevin.remote-sutil-test
          'datalevin.remote-transact-test)]
    (System/exit (if (zero? (+ ^long fail ^long error)) 0 1))))

(defn -main [& _args]
  (println "clojure version" (clojure-version))
  (println "java version" (System/getProperty "java.version"))
  (println
    "running native?"
    (= "executable" (System/getProperty "org.graalvm.nativeimage.kind")))
  (test-clj))
