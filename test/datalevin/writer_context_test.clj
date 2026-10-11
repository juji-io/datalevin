(ns datalevin.writer-context-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.binding.cpp :as cpp]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.util :as u])
  (:import [datalevin.utl WriterContext]))

(deftest reusable-context-preserves-callbacks-and-member-owned-carriers
  (let [transaction (Object.)
        context (WriterContext. (Thread/currentThread) transaction)
        capture (fn [& _])]
    (.setCallbacks context capture nil nil nil nil)
    (.beginMember context :first {:request :first})
    (is (nil? (.rows context)))
    (is (nil? (.walRows context)))
    (.ensureRows context)
    (let [first-rows (.rows context)
          first-wal-rows (.walRows context)]
      (.ensureRows context)
      (is (identical? first-rows (.rows context)))
      (.endMember context)
      (.beginMember context :second {:request :second})
      (is (identical? capture (.rowCapture context)))
      (is (= :second (.descriptor context)))
      (is (= {:request :second} (.requestContext context)))
      (is (nil? (.rows context)))
      (.ensureRows context)
      (is (not (identical? first-rows (.rows context))))
      (is (not (identical? first-wal-rows (.walRows context)))))
    (is (identical? transaction (.transaction context)))))

(deftest scoped-context-cannot-revive-for-another-member
  (let [context (WriterContext. (Thread/currentThread) (Object.))
        valid? (volatile! true)
        capture (fn [& _])]
    (.beginMember context :first {:request :first})
    (.scopeTo context valid? capture)
    (let [child (.fork context (Thread/currentThread) (Object.))]
      (is (identical? valid? (.validity child)))
      (is (identical? capture (.rowCapture child)))
      (.endMember context)
      (is (false? @(.validity child))))
    (is (false? @valid?))
    (is (identical? valid? (.validity context)))
    (is (thrown? IllegalStateException
                 (.beginMember context :second {:request :second})))
    (is (thrown? IllegalStateException
                 (.setCallbacks context (fn [& _]) nil nil nil nil)))
    (is (thrown? IllegalStateException
                 (.scopeTo context (volatile! true) (fn [& _]))))
    (is (identical? capture (.rowCapture context)))))

(deftest handle-metadata-is-independent-of-writer-state
  (let [dir (u/tmp-dir (str "writer-context-" (random-uuid)))
        db (d/open-kv dir {:wal? false})
        raw (kv/raw-lmdb db)
        escaped (atom nil)
        valid? (volatile! true)]
    (try
      (d/open-dbi db "data")
      (cpp/apply-native-once!
        raw
        (fn [view]
          (let [^WriterContext context (l/writer-context view)
                _ (.scopeTo context valid? nil)
                _ (.setRequestContext context {:request :actual})
                annotated (with-meta view {:label :annotated
                                           :request-context {:request :metadata}
                                           :native-write-valid? (volatile! false)})]
            (reset! escaped annotated)
            (is (not (identical? view annotated)))
            (is (nil? (meta view)))
            (is (= :annotated (:label (meta annotated))))
            (is (identical? context (l/writer-context annotated)))
            (is (= {:request :actual} (l/request-context annotated)))
            (i/transact-kv annotated "data" [[:put 1 42]] :long :long)))
        nil)
      (is (= 42 (d/get-value db "data" 1 :long :long)))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"no longer owned"
                           (l/write-txn @escaped)))
      (vreset! valid? false)
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"body has finished"
                           (l/write-txn (l/mark-write @escaped))))
      (finally (d/close-kv db) (u/delete-files dir)))))
