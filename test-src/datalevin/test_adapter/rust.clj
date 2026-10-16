(ns ^:no-doc datalevin.test-adapter.rust
  "A synchronous, bounded Nippy process bridge. No reference engine calls."
  (:require
   [datalevin.util :refer [raise]]
   [datalevin.buffer :as buffer]
   [taoensso.nippy :as nippy]
   [datalevin.test-adapter :as adapter]
   [datalevin.test-adapter.bits :as bits])
  (:import
   [java.io DataInputStream DataOutputStream]
   [java.lang ProcessBuilder ProcessBuilder$Redirect]
   [java.nio ByteBuffer ByteOrder]
   [java.util.concurrent Callable ExecutionException ExecutorService Executors
    ThreadFactory TimeUnit TimeoutException]))

(def ^:private ^:const max-frame-bytes (* 16 1024 1024))

(deftype ^:private DbHandle [session id])

(defn- read-frame [^DataInputStream input]
  (let [size (.readInt input)]
    (when-not (<= 1 size max-frame-bytes)
      (raise "Invalid Rust adapter frame size" {:size size}))
    (let [bytes (byte-array size)]
      (.readFully input bytes)
      (nippy/fast-thaw bytes))))

(defn- write-frame! [^DataOutputStream output value]
  (let [bytes (try
                (nippy/fast-freeze value)
                (catch Exception error
                  (adapter/unsupported! :adapter (:op value)
                                        (str "Rust adapter cannot encode the request: "
                                             (ex-message error)))))
        size (alength bytes)]
    (when (> size max-frame-bytes)
      (adapter/unsupported! :adapter (:op value)
                            "Rust adapter request exceeds the frame limit"))
    (.writeInt output size)
    (.write output bytes)
    (.flush output)))

(defn- stop-peer! [{:keys [process input output worker stopped]}]
  (when (compare-and-set! stopped false true)
    ;; Kill before closing streams: another thread may be blocked in read/write.
    (.destroy ^Process process)
    (when-not (.waitFor ^Process process 100 TimeUnit/MILLISECONDS)
      (.destroyForcibly ^Process process)
      (.waitFor ^Process process 1 TimeUnit/SECONDS))
    (.shutdownNow ^ExecutorService worker)
    (doseq [stream [input output]]
      (try (.close ^java.io.Closeable stream) (catch Exception _)))))

(defn- exchange! [{:keys [worker timeout-ms stopped] :as peer} f]
  (locking peer
    (when @stopped
      (raise "Rust adapter process is closed" {}))
    (let [task (.submit ^ExecutorService worker ^Callable (bound-fn [] (f)))]
      (try
        (.get task (long timeout-ms) TimeUnit/MILLISECONDS)
        (catch TimeoutException error
          (stop-peer! peer)
          (throw (ex-info "Rust adapter request timed out"
                          {:timeout-ms timeout-ms} error)))
        (catch ExecutionException error
          (let [cause (.getCause error)]
            (if (adapter/unsupported-data cause)
              ;; Request encoding/size checks fail before writing any bytes.
              (throw cause)
              (do
                (stop-peer! peer)
                (throw (ex-info "Rust adapter transport failed" {} cause))))))))))

(defn- argument [session operation value]
  (if (instance? DbHandle value)
    (if (identical? session (.-session ^DbHandle value))
      {:kind :db :id (.-id ^DbHandle value)}
      (raise "Database belongs to a different Rust adapter session"
                      {:operation operation}))
    {:kind :value :value value}))

(defn- result-value [session operation response]
  (when-not (map? response)
    (raise "Rust adapter response must be a map" {:operation operation}))
  (case (:status response)
    :ok
    (let [{:keys [kind id] :as result} (:result response)
          expects-db? (contains? #{:empty-db :db-with} operation)]
      (cond
        (and expects-db? (= :db kind) (int? id) (pos? (long id)))
        (DbHandle. session id)

        (and (not expects-db?) (= :value kind) (contains? result :value))
        (:value result)

        :else (raise "Invalid Rust adapter result descriptor"
                              {:operation operation :result result})))
    :unsupported
    (adapter/unsupported! :rust operation
                          (or (:message response) "Unsupported Rust operation"))
    :error
    (let [{:keys [class message data]} (:error response)]
      (when-not (and (string? class) (string? message))
        (raise "Invalid Rust adapter error descriptor" {:response response}))
      (throw (case class
               "java.lang.IllegalArgumentException" (IllegalArgumentException. message)
               "java.lang.IllegalStateException" (IllegalStateException. message)
               "clojure.lang.ExceptionInfo" (ex-info message (or data {}))
               (adapter/unsupported! :adapter operation
                                     (str "Exception class is not mapped: " class)))))
    (raise "Invalid Rust adapter response status" {:response response})))

(defn- invoke-peer! [peer session operation arguments]
  (let [request {:op operation
                 :args (mapv #(argument session operation %) arguments)}
        response (exchange! peer
                            #(do (write-frame! (:output peer) request)
                                 (read-frame (:input peer))))]
    (result-value session operation response)))

(defn- serialization-value! [value]
  ;; Datalevin excludes Class values before calling its codec. This is the JVM
  ;; binding's value policy; it does not invoke the reference serializer.
  (when (instance? Class value)
    (raise "Unfreezable type: java.lang.Class" {})))

(defn- serialize-value! ^bytes [peer session value]
  (serialization-value! value)
  (invoke-peer! peer session :bits/serialize [value]))

(defn- big-endian-buffer! [^ByteBuffer buffer]
  (when-not (= ByteOrder/BIG_ENDIAN (.order buffer))
    (throw (IllegalArgumentException.
             (str "ByteBuffer must use BIG_ENDIAN order for DataInput/DataOutput semantics (have "
                  (.order buffer) ").")))))

(defn- serialize-buffer! [peer session ^ByteBuffer buffer value]
  (serialization-value! value)
  (big-endian-buffer! buffer)
  ;; ByteBuffer.put checks capacity/read-only status before changing position.
  ;; Rust performs serialization; the host only copies its output to JVM memory.
  (.put buffer ^bytes (invoke-peer! peer session :bits/serialize [value])))

(defn- acquire-serialized-buffer! ^ByteBuffer [peer session value]
  (let [bytes (serialize-value! peer session value)
        owned (buffer/get-array-buffer (alength bytes))]
    (try
      (big-endian-buffer! owned)
      (.put owned bytes)
      (.flip owned)
      (catch Throwable error
        (buffer/return-array-buffer owned)
        (throw error)))))

(defn- measure-size [peer session value]
  ;; Preserve the original allocation estimate, including its fixed-width
  ;; integer/byte-array cases. Other values are measured using real Rust output.
  (cond
    (bytes? value) (inc (alength ^bytes value))
    (instance? ByteBuffer value) (.remaining ^ByteBuffer value)
    (int? value) 8
    (instance? Byte value) 1
    :else (alength (serialize-value! peer session value))))

(defn- deserialize-buffer! [peer session ^ByteBuffer buffer]
  (let [limit (.limit buffer)]
    (try
      (when-not (= ByteOrder/BIG_ENDIAN (.order buffer))
        (adapter/unsupported! :adapter :bits/deserialize-bf
                              "Little-endian buffer decode errors are not mapped yet"))
      (invoke-peer! peer session :bits/deserialize [(bits/get-bytes buffer)])
      ;; Matches the storage API even on codec errors and support gaps. Returned
      ;; values own their bytes; no borrowed buffer crosses the process boundary.
      (finally (.position buffer limit)))))

(defrecord RustBackend [peer session info]
  adapter/Backend
  (invoke! [_ operation arguments]
    (case operation
      :bits/serialize
      (serialize-value! peer session (first arguments))

      :bits/acquire-serialized-bf
      (acquire-serialized-buffer! peer session (first arguments))

      :bits/measure-size
      (measure-size peer session (first arguments))

      :bits/serialize-bf
      (let [[buffer value] arguments]
        (serialize-buffer! peer session buffer value))

      :bits/deserialize-bf
      (deserialize-buffer! peer session (first arguments))

      :bits/put-buffer
      (let [[^ByteBuffer buffer value value-type] arguments]
        (case value-type
          (:data nil) (serialize-buffer! peer session buffer value)
          :raw (cond
                 (bytes? value) (.put buffer ^bytes value)
                 (instance? ByteBuffer value) (.put buffer (.duplicate ^ByteBuffer value))
                 :else (raise "Raw value must be bytes or ByteBuffer, got "
                               (type value) {:input value}))
          (adapter/unsupported! :adapter operation
                                (str "Rust buffer codec is not implemented for " value-type))))

      :bits/read-buffer
      (let [[buffer value-type] arguments]
        (case value-type
          (:data nil) (deserialize-buffer! peer session buffer)
          :raw (bits/get-bytes buffer)
          (adapter/unsupported! :adapter operation
                                (str "Rust buffer codec is not implemented for " value-type))))

      (invoke-peer! peer session operation arguments)))
  (stop! [_] (stop-peer! peer))
  (backend-info [_] info))

(defn start
  "Start an already-built peer using an argv vector, without a shell."
  ([command] (start command 30000))
  ([command timeout-ms]
   (when-not (and (vector? command) (seq command) (every? string? command)
                  (int? timeout-ms) (pos? (long timeout-ms)))
     (raise "Expected a peer command vector and positive timeout" {}))
   (let [process (.start (doto (ProcessBuilder. ^java.util.List command)
                          (.redirectError ProcessBuilder$Redirect/INHERIT)))
         worker (Executors/newSingleThreadExecutor
                  (reify ThreadFactory
                    (newThread [_ task]
                      (doto (Thread. task "datalevin-test-adapter-io")
                        (.setDaemon true)))))
         peer {:process process
               :input (DataInputStream. (.getInputStream process))
               :output (DataOutputStream. (.getOutputStream process))
               :worker worker :timeout-ms timeout-ms :stopped (atom false)}]
     (try
       (let [hello (exchange! peer #(read-frame (:input peer)))]
         (when-not (and (= "datalevin-test-adapter" (:protocol hello))
                        (= 1 (:version hello)) (= :rust (:backend hello)))
           (raise "Incompatible Rust adapter handshake" {:hello hello}))
         (->RustBackend peer (Object.)
                        {:backend :rust :runtime :rust :command command :peer hello}))
       (catch Throwable error
         (stop-peer! peer)
         (throw error))))))
