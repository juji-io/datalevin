(ns datalevin.test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   #?(:clj [clojure.java.shell :as sh])
   datalevin.lmdb-test
   datalevin.util-test
   datalevin.main-test
   datalevin.bits-test
   datalevin.storage-test
   datalevin.protocol-test
   datalevin.server-test
   datalevin.client-test
   datalevin.remote-test
   datalevin.interpret-test
   datalevin.core-test
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
   datalevin.test.issues
   )
  (:gen-class))

(defn ^:export test-clj []
  (let [{:keys [fail error]}
        (t/run-tests
          'datalevin.util-test
          'datalevin.protocol-test
          'datalevin.server-test
          'datalevin.client-test
          'datalevin.remote-test
          'datalevin.interpret-test
          'datalevin.lmdb-test
          'datalevin.main-test
          'datalevin.bits-test
          'datalevin.storage-test
          'datalevin.core-test
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
          'datalevin.test.transact
          'datalevin.test.validation
          'datalevin.test.upsert
          'datalevin.test.issues
          )]
    (System/exit (if (zero? (+ fail error)) 0 1))))

(defn ^:export test-cljs []
  (datalevin.test.core/wrap-res #(t/run-all-tests #"datalevin\..*")))

#?(:clj
   (defn test-node [& args]
     (let [res (apply sh/sh "node" "test_node.js" args)]
       (println (:out res))
       (binding [*out* *err*]
         (println (:err res)))
       (System/exit (:exit res)))))

#?(:clj
   (defn -main [& _args]
     (println "clojure version" (clojure-version))
     (println "java version" (System/getProperty "java.version"))
     (println
       "running native?"
       (= "executable" (System/getProperty "org.graalvm.nativeimage.kind")))
     (test-clj)))
