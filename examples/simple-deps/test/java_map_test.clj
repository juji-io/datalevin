(ns java-map-test
  "Test for collection implementation using guava test-lib.
   Adapted to clojure from 
   - https://github.com/eclipse/eclipse-collections/issues/1196
   - https://github.com/google/guava/blob/master/guava-testlib/src/com/google/common/collect/testing/TestStringMapGenerator.java
   - https://github.com/google/guava/blob/master/guava-testlib/src/com/google/common/collect/testing/MapTestSuiteBuilder.java
   - https://github.com/junit-team/junit4/blob/main/src/main/java/junit/framework/TestCase.java
   - https://github.com/junit-team/junit4/blob/main/src/main/java/junit/framework/TestResult.java
   - https://github.com/junit-team/junit4/blob/main/src/main/java/junit/framework/TestSuite.java
   "
  {:clj-kondo/config '{:lint-as {com.rpl.proxy-plus/proxy+ clojure.core/proxy}}}
  (:require [clojure.test :refer [deftest is run-test testing]]
            [clojure.tools.logging :refer [info]]
            [com.rpl.proxy-plus :refer [proxy+]]
            [datalevin.core :as d]
            [java-map :as jm])
  (:import [com.google.common.collect.testing AbstractTester MapTestSuiteBuilder TestStringMapGenerator]
           [com.google.common.collect.testing.features CollectionFeature CollectionSize MapFeature]
           [java.util.function Supplier]
           [junit.framework Test TestListener TestResult]))

(set! *warn-on-reflection* true)

(defn test-string-generator
  "Implement a string generator for our map.
   Implementation inspired from https://github.com/eclipse/eclipse-collections/issues/1196"
  (^:TestStringMapGenerator
   [^Supplier factory]
   (info "Create string generator")
   (proxy+ []
           TestStringMapGenerator
           (create [_this entries]
                   (let [m ^java.util.Map (.get factory)]
                     (doseq [^java.util.Map$Entry entry entries]
                       (.put m (.getKey entry) (.getValue entry)))
                     m)))))

(def default-features [CollectionSize/ANY,
                       MapFeature/GENERAL_PURPOSE,
                       MapFeature/ALLOWS_NULL_KEYS,
                       MapFeature/ALLOWS_NULL_VALUES,
                       MapFeature/ALLOWS_NULL_ENTRY_QUERIES,
                       CollectionFeature/SUPPORTS_ITERATOR_REMOVE])

(defn generate-map-test-suite
  [^String name ^Supplier factory features]
  (let [m (-> (MapTestSuiteBuilder/using (test-string-generator factory))
              (.named name)
              (.withFeatures ^Iterable features)
              (.createTestSuite))]
    m))

(defn test-result-info
  [^TestResult tr]
  (let [failure-count (.failureCount tr)
        error-count (.errorCount tr)
        run-count (.runCount tr)]
    {:was-successful (.wasSuccessful tr)
     :run-count run-count
     :error-count error-count
     :failure-count failure-count
     :fail+error (+ failure-count error-count)
     :errors (enumeration-seq (.errors tr))
     :failures (enumeration-seq (.failures tr))}))

(defn get-test-name 
  (^String 
   [^Test test]
   (if (instance? AbstractTester test)
     (.getName ^AbstractTester test)
     (str test))))


(defn logging-test-listener 
  (^TestListener
   []
   (reify TestListener
     (addError [_this test throwable]
       (info "Error added" (get-test-name test) ":" throwable))
     (addFailure [_this test assertion-failed-error]
       (info "Failure" (get-test-name test) ":" assertion-failed-error))
     (endTest [_this test]
       (info "End test" (get-test-name test)))
     (startTest [_this test]
       (info "Start test" (get-test-name test))))))

(deftest test-collection-size-any
  (let [result (doto (TestResult.)
                 (.addListener (logging-test-listener))) 
        counter (atom 0)
        db (d/open-kv "/tmp/collection-size-any")
        _ (d/clear-dbi db "test-dbi")
        my-supplier (reify Supplier (get [_this]
                                      (swap! counter inc)
                                      (d/clear-dbi db "test-dbi")
                                      (jm/hashMap db "test-dbi")))
        features [CollectionSize/ANY
                  ;; MapFeature/GENERAL_PURPOSE
                  MapFeature/SUPPORTS_PUT
                  MapFeature/ALLOWS_NULL_VALUES
                  ]
        suite (generate-map-test-suite "collection-size-any-str" my-supplier features)
        _ (.run suite result)
        r (test-result-info result)
        small-result (select-keys r [:was-successful
                                     :run-count
                                     :fail+error
                                     :error-count
                                     :failure-count])]
    (testing "Map is a collection - collection-size-any"
      (is (= {:was-successful false,
              :run-count 745,
              :fail+error 292,
              :error-count 204,
              :failure-count 88} small-result))
      (is (= true (:was-successful small-result))))))


(comment

  (run-test test-collection-size-any)

  (type (make-array java.util.Map$Entry 0))

  (.componentType
   (.getClass (make-array java.util.Map$Entry 0)))

  ;; reset db
  (d/clear (d/open-kv "/tmp/map-test"))

  (let [result (TestResult.)
        counter (atom 0)
        db (d/open-kv "/tmp/map-test")
        my-supplier (reify Supplier (get [_this]
                                      (swap! counter inc)
                                      (jm/hashMap db (str "m-" @counter))))

        suite (generate-map-test-suite "hash-map" my-supplier default-features)
        _ (.run suite result)
        r (test-result-info result)]
    (info "Ran tests with" (select-keys r [:was-successful
                                           :run-count
                                           :fail+error
                                           :error-count
                                           :failure-count])
          "Counter is " @counter)
    (d/close-kv db)
    (def test-result r))

  (doseq [e (take 10 (:errors test-result))]
    (info "Error" e))


  (let [str-supplier (reify Supplier (get [_this] (java.util.HashMap.)))
        data {"a" "b" "c" "d"}
        m (test-string-generator str-supplier)]
    #_(doseq [d test-data]
        (println d))
    (.create m (to-array data))))