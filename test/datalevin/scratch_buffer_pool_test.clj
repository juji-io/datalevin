(ns datalevin.scratch-buffer-pool-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]])
  (:import [datalevin.cpp BufVal]
           [datalevin.dtlvnative DTLV]
           [datalevin.io ScratchBufferPool]
           [java.nio ByteOrder]
           [java.nio.file Files]
           [java.util IdentityHashMap]
           [java.util.concurrent CountDownLatch TimeUnit]
           [java.util.concurrent.atomic AtomicLong]
           [java.util.function LongSupplier]
           [org.bytedeco.javacpp Loader]))

(def ^:private chunk-bytes (* 2 1024 1024))

(defn- pool ^ScratchBufferPool [budget ^AtomicLong clock]
  (ScratchBufferPool. budget 100 (reify LongSupplier (getAsLong [_] (.get clock)))))

(deftest idle-byte-budget-reuse-and-expiry
  (let [clock (AtomicLong.)]
    (with-open [cache (pool (* 2 chunk-bytes) clock)]
      (let [first-buffer (.acquireBuffer cache chunk-bytes)
            second-buffer (.acquireBuffer cache chunk-bytes)
            third-buffer (.acquireBuffer cache chunk-bytes)]
        (is (not (identical? first-buffer second-buffer)))
        (doseq [buffer [first-buffer second-buffer third-buffer]]
          (.releaseBuffer cache buffer))
        (is (= (* 2 chunk-bytes) (.retainedBytes cache)))
        ;; The oldest idle entry was evicted, while the two newest are reusable.
        (let [borrowed (.acquireBuffer cache chunk-bytes)
              other (.acquireBuffer cache chunk-bytes)]
          (is (identical? second-buffer borrowed))
          (is (identical? third-buffer other))
          (is (zero? (.retainedBytes cache)))
          (.order borrowed ByteOrder/LITTLE_ENDIAN)
          (.putLong borrowed 42)
          (.releaseBuffer cache borrowed)
          (let [again (.acquireBuffer cache chunk-bytes)]
            (is (identical? borrowed again))
            (is (zero? (.position again)))
            (is (= ByteOrder/BIG_ENDIAN (.order again)))
            (.releaseBuffer cache again))
          (.set clock (.toNanos TimeUnit/MILLISECONDS 101))
          (.sweep cache)
          (is (zero? (.retainedBytes cache)))
          ;; A checked-out buffer survives expiration and pool closure.
          (.close cache)
          (.putLong other 91)
          (is (= 91 (.getLong other 0)))
          (.releaseBuffer cache other)
          (is (zero? (.retainedBytes cache))))))))

(deftest oversize-entries-bypass-cache-without-evicting-useful-buffers
  (with-open [cache (pool chunk-bytes (AtomicLong.))]
    (let [kept (.acquireBuffer cache chunk-bytes)
          too-large (.acquireBuffer cache (* 2 chunk-bytes))]
      (.releaseBuffer cache kept)
      (.releaseBuffer cache too-large)
      (is (= chunk-bytes (.retainedBytes cache)))
      (let [again (.acquireBuffer cache chunk-bytes)]
        (is (identical? kept again))
        (.releaseBuffer cache again))))
  (with-open [cache (pool 0 (AtomicLong.))]
    (.releaseBuffer cache (.acquireBuffer cache chunk-bytes))
    (is (zero? (.retainedBytes cache)))))

(deftest native-input-and-compression-share-the-same-budget
  (Loader/load DTLV)
  (let [clock (AtomicLong.)]
    (with-open [cache (pool (* 2 chunk-bytes) clock)
                value (BufVal. 16384 cache)]
      (let [small (.inBuf value)
            pointer (.ptr value)]
        (.ensureCapacity value chunk-bytes)
        (let [large (.inBuf value)]
          (.putLong large 42)
          (.flip large)
          (.reset value)
          (is (= 8 (.size value)))
          (.releaseLargeInput value)
          (is (identical? small (.inBuf value)))
          (is (identical? pointer (.ptr value)))
          (dotimes [_ 10]
            (.tryCachedCapacity value chunk-bytes)
            (is (identical? large (.inBuf value)))
            (.releaseLargeInput value))
          (let [codec (.acquireCompression cache)]
            (is (identical? large (.encodingBuffer codec chunk-bytes)))
            (.releaseCompression cache codec)
            (is (= chunk-bytes (.retainedBytes cache)))
            (let [again (.acquireCompression cache)]
              (is (identical? codec again))
              (is (identical? large (.encodingBuffer again chunk-bytes)))
              (.releaseCompression cache again))))
        (.set clock (.toNanos TimeUnit/MILLISECONDS 101))
        (.sweep cache)
        (is (zero? (.retainedBytes cache)))
        ;; A stale size hint alone never allocates a large buffer.
        (.tryCachedCapacity value chunk-bytes)
        (is (identical? small (.inBuf value)))))))

(deftest concurrent-borrowers-never-share-an-active-buffer
  (with-open [cache (pool (* 4 chunk-bytes) (AtomicLong.))]
    (let [ready (CountDownLatch. 8)
          release (CountDownLatch. 1)
          buffers (object-array 8)
          results (mapv (fn [idx]
                          (future
                            (let [buffer (.acquireBuffer cache chunk-bytes)]
                              (try
                                (.putLong buffer (long idx))
                                (aset buffers idx buffer)
                                (.countDown ready)
                                (assert (.await release 10 TimeUnit/SECONDS))
                                (is (= idx (.getLong buffer 0)))
                                (finally (.releaseBuffer cache buffer))))))
                        (range 8))]
      (try
        (is (.await ready 10 TimeUnit/SECONDS))
        (let [identities (IdentityHashMap.)]
          (doseq [buffer buffers] (.put identities buffer true))
          (is (= 8 (.size identities))))
        (finally (.countDown release)))
      (doseq [result results] @result)
      (is (<= (.retainedBytes cache) (* 4 chunk-bytes))))))

(defn -main [& _]
  ;; The child has a 16 MiB direct-memory limit. Cached 4+4 MiB blocks cannot
  ;; satisfy a 10 MiB request, so the allocation must reclaim idle storage.
  (with-open [cache (pool (* 64 1024 1024) (AtomicLong.))]
    (let [a (.acquireBuffer cache (* 4 1024 1024))
          b (.acquireBuffer cache (* 4 1024 1024))]
      (.releaseBuffer cache a)
      (.releaseBuffer cache b)
      (assert (= (* 8 1024 1024) (.retainedBytes cache)))
      (let [large (.acquireBuffer cache (* 10 1024 1024))]
        (assert (zero? (.retainedBytes cache)))
        (.putLong large 42)
        (assert (= 42 (.getLong large 0)))
        (.releaseBuffer cache large))))
  ;; The shared cache's daemon must expire buffers without another acquisition.
  (let [cache (ScratchBufferPool/shared)]
    (.releaseBuffer cache (.acquireBuffer cache chunk-bytes))
    (let [deadline (+ (System/nanoTime) (.toNanos TimeUnit/SECONDS 5))]
      (loop []
        (when (pos? (.retainedBytes cache))
          (assert (< (System/nanoTime) deadline) "Idle buffers did not expire")
          (Thread/sleep 10)
          (recur)))))
  (println "BUFFER_CACHE_OK"))

(deftest tight-direct-memory-limit-and-automatic-idle-expiry
  (let [output (.toFile (Files/createTempFile "buffer-cache-" ".log"
                                             (make-array java.nio.file.attribute.FileAttribute 0)))
        command [(str (io/file (System/getProperty "java.home") "bin" "java"))
                 "--add-opens=java.base/java.nio=ALL-UNNAMED"
                 "-XX:MaxDirectMemorySize=16m"
                 "-Ddatalevin.buffer-cache-idle-ms=100"
                 "-cp" (System/getProperty "java.class.path")
                 "clojure.main" "-m" "datalevin.scratch-buffer-pool-test"]
        child (.start (doto (ProcessBuilder. ^java.util.List command)
                        (.redirectErrorStream true)
                        (.redirectOutput output)))]
    (try
      (is (.waitFor child 30 TimeUnit/SECONDS))
      (when (.isAlive child) (.destroyForcibly child))
      (.waitFor child)
      (is (zero? (.exitValue child)) (slurp output))
      (is (.contains ^String (slurp output) "BUFFER_CACHE_OK"))
      (finally
        (when (.isAlive child) (.destroyForcibly child))
        (.delete output)))))
