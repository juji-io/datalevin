(ns datalevin.test1
  "tests for remote operations"
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   #?(:clj [clojure.java.shell :as sh])
   datalevin.server-test
   datalevin.client-test
   datalevin.remote-withtxnkv-test
   datalevin.remote-search-test
   datalevin.remote-sutil-test
   datalevin.remote-kv-test
   datalevin.remote-test
   datalevin.remote-dl-test
   datalevin.remote-withtxn-test
   datalevin.remote-entity-test
   datalevin.remote-transact-test)
  (:gen-class))

(defn ^:export test-clj []
  (let [{:keys [fail error]}
        (t/run-tests
          'datalevin.server-test
          'datalevin.client-test
          'datalevin.remote-kv-test
          'datalevin.remote-withtxnkv-test
          'datalevin.remote-search-test
          'datalevin.remote-sutil-test
          'datalevin.remote-test
          'datalevin.remote-dl-test
          'datalevin.remote-withtxn-test
          'datalevin.remote-entity-test
          'datalevin.remote-transact-test)]
    (System/exit (if (zero? (+ ^long fail ^long error)) 0 1))))

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
