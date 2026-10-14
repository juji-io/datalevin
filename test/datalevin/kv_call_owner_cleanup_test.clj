(ns datalevin.kv-call-owner-cleanup-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u])
  (:import [datalevin DatalevinInterop KVTransaction PreparedRead]
           [java.lang.ref WeakReference]
           [java.util Map]))

(use-fixtures :each db-fixture)

(defn- field-value [object name]
  (let [field (.getDeclaredField (.getClass ^Object object) name)]
    (.setAccessible field true)
    (.get field object)))

(defn- call-owner [tx] (field-value tx "callOwner"))

(defn- owner-thread [owner]
  (.get ^java.lang.reflect.Field
        (doto (.getDeclaredField (.getClass ^Object (field-value owner "worker")) "thread")
          (.setAccessible true))
        (field-value owner "worker")))

(defn- registry-size []
  (let [field (.getDeclaredField (Class/forName "datalevin.KvCallOwner") "OWNERS")
        ^Map registry (.get (doto field (.setAccessible true)) nil)]
    (locking registry (.size registry))))

(defn- await-gc [done?]
  (let [deadline (+ (System/nanoTime) 10000000000)]
    (loop []
      (cond
        (done?) true
        (> (System/nanoTime) deadline) false
        :else (do (System/gc) (Thread/sleep 25) (recur))))))

(defn- clean-owner! [{:keys [^WeakReference owner ^Thread thread]}]
  ;; Also release a writer after an assertion failure, so regression failures
  ;; cannot strand a daemon or make database teardown wait on its transaction.
  (when-let [value (.get owner)]
    (let [method (.getDeclaredMethod (.getClass ^Object value) "close" (make-array Class 0))]
      (.setAccessible method true)
      (.invoke method value (object-array 0))))
  (.join thread 5000))

(defn- with-kv [f]
  (let [dir (u/tmp-dir (str "kv-call-owner-cleanup-" (random-uuid)))
        db (d/open-kv dir {:wal? false})]
    (try
      (d/open-dbi db "data")
      (d/transact-kv db [[:put "data" "key" "committed" :string :string]])
      (f db)
      (finally
        (d/close-kv db)
        (u/delete-files dir)))))

(defn- abandon-transaction [db]
  (let [^KVTransaction tx (DatalevinInterop/keyValueBeginTransactionBridge db)
        owner (call-owner tx)]
    (.transact tx "data" [[":put" "key" "staged"]] ":string" ":string")
    {:transaction (WeakReference. tx)
     :owner (WeakReference. owner)
     :thread (owner-thread owner)}))

(deftest abandoned-bridge-transactions-abort-and-stop-the-worker
  (with-kv
    (fn [db]
      (let [before (registry-size)
            {:keys [^WeakReference transaction ^WeakReference owner ^Thread thread]
             :as abandoned} (abandon-transaction db)]
        (try
          (is (await-gc #(and (nil? (.get transaction))
                             (nil? (.get owner))
                             (not (.isAlive thread)))))
          (is (= before (registry-size)))
          (is (= "committed" (d/get-value db "data" "key" :string :string)))
          ;; A subsequent write must acquire the writer released by cleanup.
          (when-not (.isAlive thread)
            (let [^KVTransaction tx (DatalevinInterop/keyValueBeginTransactionBridge db)]
              (try
                (.transact tx "data" [[":put" "next" "value"]] ":string" ":string")
                (.commit tx)
                (is (= "value" (d/get-value db "data" "next" :string :string)))
                (finally (.close tx)))))
          (finally (clean-owner! abandoned)))))))

(defn- prepare-and-discard [^KVTransaction tx]
  (let [^PreparedRead read (.prepareGetValue tx "data" ":string" ":string")
        owned (field-value read "prepared")
        native-read (field-value owned "value")]
    (is (= "committed" (.execute read "key")))
    (WeakReference. native-read)))

(deftest prepared-reads-do-not-accumulate-in-the-owner-registry
  (with-kv
    (fn [db]
      (let [^KVTransaction tx (DatalevinInterop/keyValueBeginTransactionBridge db)
            owner (call-owner tx)
            thread (owner-thread owner)]
        (try
          (let [before (registry-size)
                references (mapv (fn [_] (prepare-and-discard tx)) (range 100))]
            (is (= before (registry-size)))
            (is (await-gc #(every? (fn [^WeakReference reference]
                                    (nil? (.get reference))) references)))
            (is (.isAlive ^Thread thread))
            (is (= "committed" (.getValue tx "data" "key" ":string" ":string" true))))
          (finally
            (.close tx)
            (.join ^Thread thread 5000)))))))

(defn- exercise-live-preparation [db]
  (let [^KVTransaction tx (DatalevinInterop/keyValueBeginTransactionBridge db)
        owner (call-owner tx)
        ^PreparedRead read (.prepareGetValue tx "data" ":string" ":string")
        transaction (WeakReference. tx)
        thread (owner-thread owner)]
    (.transact tx "data" [[":put" "key" "staged"]] ":string" ":string")
    ;; Return from the frame holding tx before testing its collection.
    {:transaction transaction :owner (WeakReference. owner)
     :thread thread :prepared read}))

(defn- drop-live-preparation [db]
  (let [{:keys [^WeakReference transaction ^WeakReference owner ^Thread thread
                ^PreparedRead prepared]} (exercise-live-preparation db)]
    (try
      (is (await-gc #(nil? (.get transaction))))
      (is (some? (.get owner)))
      (is (.isAlive thread))
      (is (= "staged" (.execute prepared "key")))
      {:transaction transaction :owner owner :thread thread
       :prepared (WeakReference. prepared)}
      (catch Throwable error
        (clean-owner! {:owner owner :thread thread})
        (throw error)))))

(deftest live-preparations-retain-the-owner-until-they-are-discarded
  (with-kv
    (fn [db]
      (let [{:keys [^WeakReference owner ^WeakReference prepared ^Thread thread]
             :as abandoned} (drop-live-preparation db)]
        (try
          (is (await-gc #(and (nil? (.get prepared))
                             (nil? (.get owner))
                             (not (.isAlive thread)))))
          (is (= "committed" (d/get-value db "data" "key" :string :string)))
          (finally (clean-owner! abandoned)))))))

(deftest finished-preparations-support-explicit-views
  (with-kv
    (fn [db]
      (let [^KVTransaction tx (DatalevinInterop/keyValueBeginTransactionBridge db)
            thread (owner-thread (call-owner tx))
            ^PreparedRead read (.prepareGetValue tx "data" ":string" ":string")]
        (try
          (.transact tx "data" [[":put" "key" "staged"]] ":string" ":string")
          (.commit tx)
          (.join ^Thread thread 5000)
          (is (not (.isAlive ^Thread thread)))
          (is (thrown? RuntimeException (.execute read "key")))
          (is (= "staged" (.execute read db "key")))
          (finally (.close tx)))))))

(deftest generic-bridge-preparations-keep-owner-routing
  (with-kv
    (fn [db]
      (let [^KVTransaction tx (DatalevinInterop/keyValueBeginTransactionBridge db)
            thread (owner-thread (call-owner tx))
            prepared (DatalevinInterop/coreInvoke
                       "prepare-get-value" [tx "data" :string :string true])]
        (try
          (.transact tx "data" [[":put" "key" "staged"]] ":string" ":string")
          (is (= "staged" (DatalevinInterop/coreInvokeBridge
                            "execute-prepared" [prepared "key"])))
          (.commit tx)
          (.join ^Thread thread 5000)
          (is (= "staged" (DatalevinInterop/coreInvokeBridge
                            "execute-prepared" [prepared db "key"])))
          (finally (.close tx)))))))

(deftest prepared-reads-reject-mixed-transaction-owners
  (with-kv
    (fn [first-db]
      (with-kv
        (fn [second-db]
          (let [^KVTransaction first-tx (DatalevinInterop/keyValueBeginTransactionBridge first-db)
                ^KVTransaction second-tx (DatalevinInterop/keyValueBeginTransactionBridge second-db)
                first-thread (owner-thread (call-owner first-tx))
                second-thread (owner-thread (call-owner second-tx))
                ^PreparedRead read (.prepareGetValue first-tx "data" ":string" ":string")]
            (try
              (is (thrown-with-msg? IllegalArgumentException #"different KV transaction owners"
                                   (.execute read second-tx "key")))
              (is (= "committed" (.execute read "key")))
              (finally
                (.close second-tx)
                (.close first-tx)
                (.join ^Thread second-thread 5000)
                (.join ^Thread first-thread 5000)))))))))
