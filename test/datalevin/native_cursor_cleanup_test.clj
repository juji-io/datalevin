(ns datalevin.native-cursor-cleanup-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.binding.cpp :as cpp]
            [datalevin.binding.cpp.buffer :as buffer]
            [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u])
  (:import [datalevin.binding.cpp CppLMDB Rtx]
           [datalevin.cpp BufVal Cursor Dbi Txn Util$NotReadyException]
           [datalevin.dtlvnative DTLV]
           [java.util.concurrent CountDownLatch TimeUnit]))

(use-fixtures :each db-fixture)

(defn- with-lmdb [f]
  (let [dir (u/tmp-dir (str "native-cursor-cleanup-" (random-uuid)))
        db (l/open-kv dir {:wal? false})]
    (try
      (i/open-dbi db "data")
      (i/transact-kv db [[:put "data" 1 2 :long :long]])
      (f db (i/get-dbi db "data"))
      (finally
        (i/close-kv db)
        (u/delete-files dir)))))

(defn- cursor-value [dbi rtx ^Cursor cur]
  (l/put-read-key dbi rtx 1 :long)
  (when (.seek cur DTLV/MDB_SET)
    (b/read-buffer (.outBuf (.val cur)) :long)))

(defn- idle-cursor [db dbi]
  (let [rtx (i/get-rtx db)
        cur (l/get-cursor dbi rtx)]
    (try
      (assert (= 2 (cursor-value dbi rtx cur)))
      cur
      (finally
        (l/return-cursor dbi cur)
        (i/return-rtx db rtx)))))

(defn- on-thread [virtual? f]
  (let [result (promise)
        body #(deliver result (try [:ok (f)] (catch Throwable e [:error e])))
        thread (if virtual?
                 (Thread/startVirtualThread ^Runnable body)
                 (doto (Thread. ^Runnable body) (.start)))]
    (.join thread 10000)
    (assert (not (.isAlive thread)) "Reader thread did not finish")
    (let [[status value] @result]
      (if (= :error status) (throw value) value))))

(deftest read-cursors-reuse-within-their-transaction
  (with-lmdb
    (fn [db dbi]
      (let [rtx (i/get-rtx db)
            first-cur (l/get-cursor dbi rtx)
            second-cur (l/get-cursor dbi rtx)]
        (is (not (identical? first-cur second-cur)))
        (is (= 2 (cursor-value dbi rtx first-cur)))
        (is (= 2 (cursor-value dbi rtx second-cur)))
        (l/return-cursor dbi first-cur)
        (l/return-cursor dbi second-cur)
        (i/return-rtx db rtx)
        (let [renewed (i/get-rtx db)
              cur (l/get-cursor dbi renewed)]
          (is (identical? rtx renewed))
          (is (identical? first-cur cur))
          (is (= 2 (cursor-value dbi renewed cur)))
          (l/return-cursor dbi cur)
          (i/return-rtx db renewed))
        (cpp/invalidate-thread-reader! (kv/raw-lmdb db))
        (is (nil? (.ptr ^Cursor first-cur)))
        (is (nil? (.ptr ^Cursor second-cur)))
        (is (= [[1 2]] (i/get-range db "data" [:all] :long :long)))))))

(deftest read-only-terminal-operations-close-pooled-cursors
  (doseq [operation [:abort :commit]]
    (testing (name operation)
      (with-lmdb
        (fn [db dbi]
          (i/open-dbi db "other")
          (with-open [key (BufVal. c/+max-key-size+)
                      val (BufVal. 0)]
            (let [^Txn txn (Txn/createReadOnly
                            (.-env ^CppLMDB (kv/raw-lmdb db)))
                  ^Dbi native-dbi (l/dbi dbi)
                  ^Dbi other-dbi (l/dbi (i/get-dbi db "other"))]
              (try
                (let [^Cursor idle (.getCursor txn native-dbi key val)
                      ^Cursor borrowed (.getCursor txn native-dbi key val)
                      ^Cursor other (.getCursor txn other-dbi key val)]
                  (.release idle)
                  (.release other)
                  (case operation
                    :abort (.abort txn)
                    :commit (.commit txn))
                  ;; Both idle and outstanding cursors, across all DBIs, must
                  ;; be freed immediately rather than waiting for Txn.close().
                  (doseq [^Cursor cur [idle borrowed other]]
                    (is (nil? (.ptr cur)))
                    (.release cur)
                    (.close cur))
                  (is (thrown? Util$NotReadyException
                               (.getCursor txn native-dbi key val)))
                  (.close txn)
                  (.close txn))
                (finally
                  (.close txn))))))))))

(deftest reused-read-cursors-use-the-supplied-buffers
  (with-lmdb
    (fn [db dbi]
      (i/transact-kv db [[:put "data" 3 4 :long :long]])
      (with-open [first-key (BufVal. c/+max-key-size+)
                  first-val (BufVal. 0)
                  next-key (BufVal. c/+max-key-size+)
                  next-val (BufVal. 0)]
        (let [^Txn txn (Txn/createReadOnly
                        (.-env ^CppLMDB (kv/raw-lmdb db)))
              ^Dbi native-dbi (l/dbi dbi)]
          (try
            (buffer/put-bufval first-key 1 :long nil nil)
            (let [^Cursor first-cur (.getCursor txn native-dbi first-key first-val)]
              (is (.seek first-cur DTLV/MDB_SET))
              (is (= 2 (b/read-buffer (.outBuf first-val) :long)))
              (.release first-cur)
              (doseq [[^BufVal key ^BufVal val k v]
                      [[next-key next-val 3 4] [first-key first-val 1 2]]]
                (buffer/put-bufval key k :long nil nil)
                (let [^Cursor cur (.getCursor txn native-dbi key val)]
                  (is (identical? first-cur cur))
                  (is (identical? key (.key cur)))
                  (is (identical? val (.val cur)))
                  (is (.seek cur DTLV/MDB_SET))
                  (is (= k (b/read-buffer (.outBuf (.key cur)) :long)))
                  (is (= v (b/read-buffer (.outBuf (.val cur)) :long)))
                  (.release cur))))
            (finally
              (.close txn))))))))

(deftest virtual-thread-return-closes-its-cursors
  (with-lmdb
    (fn [db dbi]
      (dotimes [_ 20]
        (let [^Cursor cur (on-thread true #(idle-cursor db dbi))]
          (is (nil? (.ptr cur))))))))

(deftest dead-reader-sweep-closes-idle-and-abandoned-cursors
  (with-lmdb
    (fn [db dbi]
      (doseq [returned? [true false]]
        (testing (str "cursor returned to pool: " returned?)
          (let [^Cursor cur
                (on-thread false
                  #(let [rtx (i/get-rtx db)
                         cur (l/get-cursor dbi rtx)]
                     (assert (= 2 (cursor-value dbi rtx cur)))
                     (when returned? (l/return-cursor dbi cur))
                     (i/return-rtx db rtx)
                     cur))]
            (is (some? (.ptr cur)))
            ;; A fresh reader invokes the existing dead-thread sweep.
            (on-thread false #(idle-cursor db dbi))
            (is (nil? (.ptr cur)))))))))

(deftest environment-close-drains-live-thread-cursors
  (with-lmdb
    (fn [db dbi]
      (let [ready (promise)
            release (CountDownLatch. 1)
            thread (doto
                     (Thread.
                       ^Runnable
                       #(do
                          (deliver ready
                                   (try (idle-cursor db dbi)
                                        (catch Throwable e e)))
                          (.await release 10 TimeUnit/SECONDS)))
                     (.start))]
        (try
          (let [value (deref ready 10000 ::timeout)]
            (when (instance? Throwable value) (throw value))
            (is (instance? Cursor value))
            (when (instance? Cursor value)
              (is (.isAlive thread))
              (is (some? (.ptr ^Cursor value)))
              (i/close-kv db)
              (is (nil? (.ptr ^Cursor value)))))
          (finally
            (.countDown release)
            (.join thread 10000)))))))

(deftest stale-reader-replacement-closes-pooled-cursors
  (with-lmdb
    (fn [db dbi]
      (let [^Rtx rtx (i/get-rtx db)
            ^Cursor cur (l/get-cursor dbi rtx)]
        (l/return-cursor dbi cur)
        (i/return-rtx db rtx)
        (.close ^Txn (.-txn rtx))
        (is (nil? (.ptr cur)))
        (is (= [[1 2]] (i/get-range db "data" [:all] :long :long)))))))

(deftest explicit-close-detaches-cursors-from-the-pool
  (with-lmdb
    (fn [db dbi]
      (let [rtx (i/get-rtx db)
            ^Cursor cur (l/get-cursor dbi rtx)]
        (l/return-cursor dbi cur)
        (l/close-cursor dbi cur)
        (is (nil? (.ptr cur)))
        ;; Neither a late return nor repeated close may enqueue or free it again.
        (l/return-cursor dbi cur)
        (l/close-cursor dbi cur)
        (let [replacement (l/get-cursor dbi rtx)]
          (is (not (identical? cur replacement)))
          (is (= 2 (cursor-value dbi rtx replacement)))
          (l/return-cursor dbi replacement))
        (i/return-rtx db rtx)
        (cpp/invalidate-thread-reader! (kv/raw-lmdb db))))))
