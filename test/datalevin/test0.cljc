(ns datalevin.test0
  "tests for core operations"
  (:require
   [clojure.test :as t :refer [is are deftest testing]]
   datalevin.lmdb-test
   datalevin.search-test
   datalevin.main-test
   datalevin.core-test)
  (:gen-class))

(defn ^:export test-clj []
  (let [{:keys [fail error]}
        (t/run-tests
          'datalevin.lmdb-test
          'datalevin.search-test
          'datalevin.main-test
          'datalevin.core-test
          )]
    (System/exit (if (zero? (+ fail error)) 0 1))))

(defn -main [& _args]
  (println "clojure version" (clojure-version))
  (println "java version" (System/getProperty "java.version"))
  (println
    "running native?"
    (= "executable" (System/getProperty "org.graalvm.nativeimage.kind")))
  (test-clj))
