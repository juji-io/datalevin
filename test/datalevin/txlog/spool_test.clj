(ns datalevin.txlog.spool-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.kv.txlog :as kvtx]
            [datalevin.txlog.codec :as codec]
            [datalevin.txlog.spool :as spool])
  (:import [datalevin.lmdb DatomKVTxData]
           [datalevin.txlog.spool PendingSpool]
           [java.io Closeable IOException]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]
           [java.nio.file Files LinkOption]
           [java.util Arrays]))

(defn- buffer-bytes ^bytes [^ByteBuffer body]
  (let [bytes (byte-array (.remaining body))]
    (.get (.duplicate body) bytes)
    bytes))

(defn- check-deleted [^PendingSpool pending]
  (let [{:keys [closed? channel path]} @(.-state pending)]
    (is closed?)
    (is (zero? (spool/memory-byte-count pending)))
    (when channel
      (is (not (.isOpen ^FileChannel channel)))
      (is (Files/notExists path (make-array LinkOption 0))))))

(deftest spooled-batches-match-the-ordinary-wal-format
  (doseq [term [nil 13]
          size [10 100000]
          memory-limit [0 128 spool/default-memory-limit]]
    (let [pending (spool/create! memory-limit)
          value (byte-array size (byte 42))
          batches [[[:put "custom" 1 value :id :bytes]]
                   [(DatomKVTxData. 3 (byte-array [1 2 3]) true false)]
                   [[:del "custom" 2 :id]
                    (DatomKVTxData. 4 (byte-array [4 5 6]) false false)]]]
      (try
        (doseq [batch batches] (spool/append! pending batch))
        (is (= 4 (count pending)))
        (is (> (spool/byte-count pending) size))
        (is (not (instance? java.util.Collection pending)))
        (is (= (> (spool/byte-count pending) memory-limit)
               (spool/spilled? pending)))
        (is (<= (spool/memory-byte-count pending)
                (+ memory-limit codec/max-commit-row-header-size)))
        (let [body (spool/payload-buffer! pending {:ha-term term})]
          (try
            (is (= (spool/spilled? pending) (.isDirect ^ByteBuffer body)))
            (codec/patch-commit-row-payload-buffer-header! body 7 11)
            (is (Arrays/equals
                  ^bytes (codec/encode-commit-row-payload
                    7 11 (vec (mapcat identity batches)) {:ha-term term})
                  (buffer-bytes body)))
            (finally (spool/release-payload-buffer! body))))
        (finally (.close ^Closeable pending)))
      (check-deleted pending))))

(deftest batches-own-their-bytes-and-release-the-encoding-arena
  (let [pending (spool/create! 128)
        arena (volatile! (ByteBuffer/allocate (* 2 1024 1024)))
        info (volatile! {:txlog-state {:kv-encode-buffer arena}
                         :txlog-pending-ops pending})
        value (byte-array [1 2 3])]
    (try
      (kvtx/txlog-add-pending! info [[:put "custom" 1 value :id :bytes]])
      (is (nil? @arena))
      (aset-byte value 0 (byte 99))
      (dotimes [i 100]
        (kvtx/txlog-add-pending! info [[:put "custom" (+ i 2) i :id :long]]))
      (is (= 101 (count pending)))
      (is (spool/spilled? pending))
      (is (zero? (spool/memory-byte-count pending)))
      (is (identical? pending (kvtx/txlog-pending-rows info)))
      (let [body (spool/payload-buffer! pending {})]
        (try
          (let [decoded (codec/decode-commit-row-payload (buffer-bytes body))]
            (is (= 101 (count (:ops decoded))))
            (is (= [1 2 3] (vec (nth (first (:ops decoded)) 3)))))
          (finally (spool/release-payload-buffer! body))))
      (kvtx/txlog-reset-pending! info)
      (is (zero? (count (kvtx/txlog-pending-rows info))))
      (finally (.close ^Closeable pending)))
    (check-deleted pending)))

(deftest failed-spool-cannot-commit-a-successful-prefix
  (with-open [^PendingSpool pending (spool/create! 0)]
    (spool/append! pending [[:put "custom" 1 "prefix" :id :string]])
    (testing "a write failure poisons the pending payload"
      (.close ^FileChannel (:channel @(.-state pending)))
      (is (thrown? IOException
                   (spool/append! pending [[:put "custom" 2 "tail" :id :string]])))
      (is (thrown? IOException (spool/payload-buffer! pending {}))))))

(deftest failed-first-batch-is-rejected-even-with-no-pending-rows
  (with-open [^PendingSpool pending (spool/create!)]
    (is (thrown? Exception
                 (spool/append! pending [[:unsupported "custom" 1 nil :id :string]])))
    (is (zero? (count pending)))
    (is (thrown? Exception (spool/check! pending)))))

(deftest memory-limit-is-inclusive-and-spilling-preserves-the-prefix
  (let [rows (mapv #(vector :put "custom" % "value" :id :string) (range 4))
        first-body (codec/encode-commit-row-payload 0 0 [(first rows)])
        row-size (- (alength ^bytes first-body) 28)
        memory-limit (* 2 row-size)
        pending (spool/create! memory-limit)]
    (try
      (testing "empty, below-limit and exactly-at-limit buffers create no file"
        (is (zero? (spool/memory-byte-count pending)))
        (spool/append! pending [])
        (is (zero? (spool/memory-byte-count pending)))
        (doseq [i (range 2)]
          (spool/append! pending [(nth rows i)])
          (is (not (spool/spilled? pending)))
          (is (nil? (:path @(.-state pending))))
          (is (= (* (inc i) row-size) (spool/byte-count pending)))
          (let [body (spool/payload-buffer! pending {})]
            (try
              (is (not (.isDirect ^ByteBuffer body)))
              (is (Arrays/equals
                    ^bytes (codec/encode-commit-row-payload 0 0 (subvec rows 0 (inc i)))
                    (buffer-bytes body)))
              (finally (spool/release-payload-buffer! body))))))
      (testing "the next batch flushes the prefix once and releases its memory"
        (spool/append! pending [(nth rows 2)])
        (is (spool/spilled? pending))
        (is (zero? (spool/memory-byte-count pending)))
        (let [channel (:channel @(.-state pending))]
          (spool/append! pending [(nth rows 3)])
          (is (identical? channel (:channel @(.-state pending)))))
        (let [body (spool/payload-buffer! pending {:ha-term 13})]
          (try
            (codec/patch-commit-row-payload-buffer-header! body 7 11)
            (is (Arrays/equals
                  ^bytes (codec/encode-commit-row-payload 7 11 rows {:ha-term 13})
                  (buffer-bytes body)))
            (finally (spool/release-payload-buffer! body)))))
      (finally (.close ^Closeable pending)))
    (check-deleted pending)))

(deftest oversized-batch-goes-directly-to-disk
  (with-open [^PendingSpool pending (spool/create! 128)]
    (spool/append! pending [[:put "custom" 1 (byte-array 100000) :id :bytes]])
    (is (spool/spilled? pending))
    (is (zero? (spool/memory-byte-count pending)))
    (is (> (spool/byte-count pending) 100000))))

(deftest closing-a-memory-buffer-releases-it-and-rejects-further-writes
  (let [pending (spool/create!)]
    (spool/append! pending [[:put "custom" 1 "value" :id :string]])
    (is (pos? (spool/memory-byte-count pending)))
    (is (not (spool/spilled? pending)))
    (.close ^Closeable pending)
    (check-deleted pending)
    (is (thrown? IOException (spool/check! pending)))
    (is (thrown? IOException
                 (spool/append! pending [[:put "custom" 2 "tail" :id :string]])))))
