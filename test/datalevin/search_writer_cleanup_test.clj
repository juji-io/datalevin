(ns datalevin.search-writer-cleanup-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.search :as search])
  (:import [datalevin DatalevinInterop SearchIndexWriter]
           [java.lang AutoCloseable]))

(defn- writer [commit-fn]
  (let [buffer (atom [])
        backend (reify search/IIndexWriter
                  (write [_ doc-ref doc-text]
                    (swap! buffer conj [doc-ref doc-text]))
                  (commit [_] (commit-fn)))
        constructor (.getDeclaredConstructor SearchIndexWriter
                                             (into-array Class [Object]))]
    (.setAccessible constructor true)
    (.newInstance constructor (object-array [backend]))))

(defn- register-handle! [^SearchIndexWriter writer]
  (let [resource-class (.getSuperclass SearchIndexWriter)
        ;; Exercise HandleResource's real JSON registration and cleanup path.
        method (.getDeclaredMethod resource-class "execJson"
                                   (into-array Class [String]))
        field (.getDeclaredField resource-class "jsonSession")]
    (.setAccessible method true)
    (.invoke method writer (object-array ["api-info"]))
    (.setAccessible field true)
    (.get field writer)))

(defn- commit! [entry ^SearchIndexWriter writer]
  (case entry
    :java (.commit writer)
    :interop (DatalevinInterop/searchCommit writer)))

(defn- assert-released! [^SearchIndexWriter writer session]
  (is (.closed writer))
  (is (empty? (:handles session)))
  (is (empty? (:reverse-handles session)))
  (is (thrown-with-msg? IllegalStateException #"SearchIndexWriter is closed"
                        (.write writer "later" "cannot append"))))

(deftest failed-search-commits-release-the-writer-and-registered-handle
  (doseq [entry [:java :interop]
          fault [(IllegalStateException. "commit failed")
                 (AssertionError. "commit failed")]]
    (testing (str entry " " (.getSimpleName (class fault)))
      (let [^SearchIndexWriter writer (writer #(throw fault))]
        (try
          (.write writer "doc" "pending document")
          (let [session (register-handle! writer)]
            (is (= 1 (count (:handles session))))
            (is (= 1 (count (:reverse-handles session))))
            (is (thrown-with-msg? Throwable #"commit failed" (commit! entry writer)))
            (assert-released! writer session))
          (finally (.close ^AutoCloseable writer)))))))

(deftest successful-search-commits-preserve-the-result-and-release-the-writer
  (doseq [entry [:java :interop]]
    (let [^SearchIndexWriter writer (writer (constantly "committed"))]
      (try
        (.write writer "doc" "pending document")
        (let [session (register-handle! writer)]
          (is (= "committed" (commit! entry writer)))
          (assert-released! writer session)
          (.close ^AutoCloseable writer)
          (is (empty? (:handles session))))
        (finally (.close ^AutoCloseable writer))))))
