(ns datalevin.scan-cleanup-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.string :as s]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.scan :as scan]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.tx-state.lifetime :as lifetime]
            [datalevin.util :as u])
  (:import [datalevin.binding.cpp Rtx]
           [datalevin.cpp Cursor]
           [datalevin.utl WriterContext]
           [java.lang AutoCloseable]
           [java.util.concurrent.atomic AtomicBoolean]))

(use-fixtures :each db-fixture)

(defn- caught [f]
  (try (f) (catch Throwable e e)))

(defn- matches-error? [fault error]
  (or (identical? fault error)
      (and (instance? clojure.lang.ExceptionInfo error)
           (s/includes? (ex-message error) (ex-message fault)))))

(defn- on-thread [virtual? f]
  (if-not virtual?
    (f)
    (let [result (promise)
          thread (Thread/startVirtualThread
                   ^Runnable (bound-fn [] (deliver result (caught f))))]
      (.join thread 10000)
      (assert (not (.isAlive thread)) "Scan thread did not finish")
      (let [value @result]
        (if (instance? Throwable value) (throw value) value)))))

(defn- with-scan-view
  [virtual? {:keys [acquire-fault record-fault release-fault writer?]} f]
  (let [dir (u/tmp-dir (str "scan-cleanup-" (random-uuid)))
        db (d/open-kv dir {:wal? false})
        raw (kv/raw-lmdb db)
        guard (lifetime/create)]
    (try
      (d/open-dbi db "data")
      (d/transact-kv db [[:put "data" 1 2 :long :long]])
      (vswap! (i/kv-info raw) assoc :native-lifetime guard)
      (on-thread
        virtual?
        (fn []
          (let [base (if writer? (i/open-transact-kv raw) raw)
                native-dbi (i/get-dbi base "data")
                acquired (atom nil)
                cursor (atom nil)
                returns (atom 0)
                cursor-returns (atom 0)
                cursor-closes (atom 0)
                failures (atom [])
                context (doto (WriterContext. (Thread/currentThread) nil)
                          (.setTransactionFailed
                            (fn [error]
                              (swap! failures conj error)
                              (when record-fault (throw record-fault)))))
                ;; Inject faults through explicit protocol implementations while
                ;; borrowing actual native cursors, readers, and lifetime leases.
                dbi (reify l/IDB
                      (get-cursor [_ rtx]
                        (when acquire-fault (throw acquire-fault))
                        (let [cur (l/get-cursor native-dbi rtx)]
                          (reset! cursor cur)
                          cur))
                      (return-cursor [_ cur]
                        (swap! cursor-returns inc)
                        (l/return-cursor native-dbi cur)
                        (when release-fault (throw release-fault)))
                      (close-cursor [_ cur]
                        (swap! cursor-closes inc)
                        (l/close-cursor native-dbi cur))
                      (iterate-key-sample [_ rtx cur indices k-range k-type]
                        (l/iterate-key-sample native-dbi rtx cur indices k-range k-type))
                      (iterate-kv [_ rtx cur k-range k-type v-type]
                        (l/iterate-kv native-dbi rtx cur k-range k-type v-type)))
                view (reify
                       i/ILMDB
                       (check-ready [_] (i/check-ready base))
                       (get-dbi [_ _ _] dbi)
                       (get-rtx [_]
                         (let [rtx (i/get-rtx base)]
                           (reset! acquired rtx)
                           rtx))
                       (return-rtx [_ rtx]
                         (swap! returns inc)
                         (i/return-rtx base rtx))
                       i/IList
                       (list-dbi? [_ _] false)
                       l/IWriting
                       (writing? [_] (boolean writer?))
                       (write-txn [_] (l/write-txn base))
                       l/IWriterContext
                       (writer-context [_] context))]
            (try
              (f {:view view :base base :guard guard :acquired acquired
                  :cursor cursor :returns returns :cursor-returns cursor-returns
                  :cursor-closes cursor-closes :failures failures})
              (finally
                ;; Also clean up on the unfixed path so a leaked lease cannot
                ;; block fixture teardown or retain a virtual-thread reader.
                (when (and @acquired (zero? @returns))
                  (i/return-rtx base @acquired))
                (when writer?
                  (i/abort-transact-kv raw)
                  (i/close-transact-kv raw)))))))
      (finally
        (d/close-kv db)
        (u/delete-files dir)))))

(defn- assert-reader-returned!
  [{:keys [guard acquired returns]} virtual?]
  (is (= 1 @returns))
  (is (zero? (:native-users (lifetime/state guard))))
  (is (= virtual? (.get ^AtomicBoolean (.-closed? ^Rtx @acquired))))
  (when-not virtual?
    (is (zero? @(.-depth ^Rtx @acquired)))))

(defn- read-op [operation view]
  (case operation
    :eager (scan/get-first view "data" [:all] :long :long false)
    :rank (scan/get-by-rank view "data" 0 :long :long false)
    :lazy (scan/range-seq view "data" [:all] :long :long false)))

(deftest cursor-acquisition-failures-return-the-reader
  (doseq [virtual? [false true]
          operation [:eager :rank :lazy]]
    (testing (str operation ", virtual thread: " virtual?)
      (let [fault (AssertionError. "cursor acquisition failed")]
        (with-scan-view
          virtual? {:acquire-fault fault}
          (fn [{:keys [view cursor-returns failures] :as state}]
            (is (matches-error? fault (caught #(read-op operation view))))
            (is (= [fault] @failures))
            (is (zero? @cursor-returns))
            (assert-reader-returned! state virtual?)))))))

(deftest failure-recording-errors-release-cursors-and-readers
  (doseq [virtual? [false true]
          keep? [false true]]
    (let [fault (AssertionError. "scan failed")
          record-fault (AssertionError. "failure recording failed")]
      (with-scan-view
        virtual? {:record-fault record-fault}
        (fn [{:keys [view cursor cursor-returns failures] :as state}]
          (is (identical?
                record-fault
                (caught #(scan/scan view "data"
                           (do (assert (and dbi rtx cur)) (throw fault))
                           (throw e) keep?))))
          (is (= [fault] @failures))
          (is (= 1 @cursor-returns))
          (assert-reader-returned! state virtual?)
          (when virtual? (is (nil? (.ptr ^Cursor @cursor)))))))))

(deftest cursor-release-errors-still-return-the-reader
  (doseq [virtual? [false true]
          operation [:eager :rank]]
    (let [fault (AssertionError. "cursor return failed")]
      (with-scan-view
        virtual? {:release-fault fault}
        (fn [{:keys [view cursor-returns] :as state}]
          (is (matches-error? fault (caught #(read-op operation view))))
          (is (= 1 @cursor-returns))
          (assert-reader-returned! state virtual?))))))

(deftest successful-lazy-scans-transfer-reader-ownership
  (doseq [virtual? [false true]]
    (with-scan-view
      virtual? {}
      (fn [{:keys [view guard returns cursor-returns] :as state}]
        (with-open [^AutoCloseable rows (read-op :lazy view)]
          (is (zero? @returns))
          (is (zero? @cursor-returns))
          (is (= 1 (:native-users (lifetime/state guard))))
          (is (= [[1 2]] (vec (seq rows)))))
        (is (= 1 @cursor-returns))
        (assert-reader-returned! state virtual?)))))

(deftest scans-preserve-the-caller-owned-writer
  (with-scan-view
    false {:writer? true}
    (fn [{:keys [view base guard returns cursor-closes]}]
      (let [writer @(l/write-txn base)]
        (is (= [1 2] (read-op :eager view)))
        (is (= [1 2] (read-op :rank view)))
        (with-open [^AutoCloseable rows (read-op :lazy view)]
          (is (= [[1 2]] (vec (seq rows)))))
        (is (= 3 @cursor-closes))
        (is (identical? writer @(l/write-txn base)))
        (is (zero? @returns))
        (is (= 1 (:native-users (lifetime/state guard))))))))
