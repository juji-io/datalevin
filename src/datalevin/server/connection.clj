;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.server.connection
  "Bound socket/thread admission and network waits for each connection."
  (:require
   [datalevin.buffer :as bf]
   [datalevin.constants :as c]
   [datalevin.protocol :as p]
   [datalevin.protocol.context :as codec]
   [datalevin.server.resources :as resources]
   [datalevin.util :refer [raise]]
   [taoensso.timbre :as log])
  (:import
   [java.nio.channels SelectionKey ServerSocketChannel SocketChannel]
   [java.util Set UUID]
   [java.util.concurrent ConcurrentHashMap Semaphore]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong]))

(defn limits
  "Validate and normalize finite connection limits before acquiring resources."
  [opts]
  (let [settings (merge {:max-connections c/default-server-max-connections
                         :max-unauthenticated-connections
                         c/default-server-max-unauthenticated-connections
                         :authentication-timeout-ms c/default-server-authentication-timeout-ms
                         :read-timeout-ms c/default-server-read-timeout-ms
                         :write-timeout-ms c/default-server-write-timeout-ms}
                        (select-keys opts [:max-connections :max-unauthenticated-connections
                                           :authentication-timeout-ms :read-timeout-ms
                                           :write-timeout-ms]))]
    (doseq [[option value] settings]
      (when-not (and (pos-int? value) (<= (long value) Integer/MAX_VALUE))
        (raise "Server connection limit must be a positive 32-bit integer"
               {:error :server/invalid-connection-limit :option option :value value})))
    (let [settings (update settings :max-unauthenticated-connections
                           #(min (long %) (long (:max-connections settings))))]
      (assoc settings
             :slots (Semaphore. (int (:max-connections settings)))
             :unauthenticated-slots
             (Semaphore. (int (:max-unauthenticated-connections settings)))))))

(defrecord Lease [^Semaphore slots ^Semaphore unauthenticated-slots
                  ^AtomicBoolean pending? ^AtomicBoolean released?
                  ^long authentication-deadline ^long read-timeout-ns
                  ^AtomicLong read-deadline ^long write-timeout-ns
                  ^AtomicLong write-deadline])

(defn- acquire! [{:keys [^Semaphore slots ^Semaphore unauthenticated-slots
                         authentication-timeout-ms read-timeout-ms write-timeout-ms]}]
  (when (.tryAcquire slots)
    (if (.tryAcquire unauthenticated-slots)
      (try
        (->Lease slots unauthenticated-slots (AtomicBoolean. true) (AtomicBoolean. false)
                 (+ (System/nanoTime) (* 1000000 (long authentication-timeout-ms)))
                 (* 1000000 (long read-timeout-ms)) (AtomicLong. 0)
                 (* 1000000 (long write-timeout-ms)) (AtomicLong. 0))
        (catch Throwable t
          (.release unauthenticated-slots)
          (.release slots)
          (throw t)))
      (do (.release slots) nil))))

(defn- release-unauthenticated! [^Lease lease]
  (when (.compareAndSet ^AtomicBoolean (.-pending? lease) true false)
    (.release ^Semaphore (.-unauthenticated-slots lease))))

(defn- release! [^Lease lease]
  (when (.compareAndSet ^AtomicBoolean (.-released? lease) false true)
    (release-unauthenticated! lease)
    (.release ^Semaphore (.-slots lease))))

(defn- lease ^Lease [^SelectionKey key]
  (:connection-lease (some-> key .attachment deref)))

(declare close!)

(defn- timeout! [key phase]
  (close! key)
  (raise "Server connection deadline exceeded"
         {:error :server/connection-timeout :phase phase}))

(defn authenticated!
  "Release anonymous admission only after credentials or a live session token
  have been verified. Authentication never renews the accept-time deadline."
  [^SelectionKey key]
  (when-let [^Lease lease (lease key)]
    (locking lease
      (when (or (not (.isValid key))
                (= Long/MIN_VALUE (.get ^AtomicLong (.-read-deadline lease)))
                (and (.get ^AtomicBoolean (.-pending? lease))
                     (>= (System/nanoTime) (.-authentication-deadline lease))))
        (timeout! key :authentication))
      (release-unauthenticated! lease))))

(defn begin-read!
  "Start one absolute deadline for an entire incoming frame. Additional bytes
  cannot extend it. Dispatch defers this until the first byte when an explicit
  transaction is idle; copy-in always includes waiting for the first byte."
  [key]
  (when-let [^Lease lease (lease key)]
    (let [^AtomicLong ref (.-read-deadline lease)]
      (.compareAndSet ref 0 (+ (System/nanoTime) (.-read-timeout-ns lease)))
      (let [deadline (.get ref)]
        (when (or (= Long/MIN_VALUE deadline) (>= (System/nanoTime) deadline))
          (timeout! key :read))))))

(defn end-read!
  "Stop a frame's read deadline before running its handler. Execution time
  does not consume the next frame's read allowance."
  [key]
  (when-let [^Lease lease (lease key)]
    (let [^AtomicLong ref (.-read-deadline lease)
          deadline (.getAndSet ref 0)]
      (when (or (= Long/MIN_VALUE deadline)
                (and (not (zero? deadline)) (>= (System/nanoTime) deadline)))
        (.set ref Long/MIN_VALUE)
        (timeout! key :read)))))

(defn begin-write!
  "Start one absolute response deadline. Nested writes share the outer deadline;
  return true only to the scope responsible for clearing it."
  [key]
  (when-let [^Lease lease (lease key)]
    (let [^AtomicLong ref (.-write-deadline lease)
          owner? (.compareAndSet ref 0 (+ (System/nanoTime) (.-write-timeout-ns lease)))
          deadline (.get ref)]
      (when (or (= Long/MIN_VALUE deadline) (>= (System/nanoTime) deadline))
        (timeout! key :write))
      owner?)))

(defn end-write!
  "Finish a response without reviving a connection closed by the watchdog."
  [key]
  (when-let [^Lease lease (lease key)]
    (let [^AtomicLong ref (.-write-deadline lease)
          deadline (.getAndSet ref 0)]
      (when (or (= Long/MIN_VALUE deadline)
                (and (not (zero? deadline)) (>= (System/nanoTime) deadline)))
        (.set ref Long/MIN_VALUE)
        (timeout! key :write)))))

(defmacro with-write
  "Bound an entire response, including buffer growth and all copy-out frames."
  [key & body]
  `(let [key# ~key
         owner?# (begin-write! key#)]
     (try ~@body (finally (when owner?# (end-write! key#))))))

(defn write-message
  "Send one server frame within the connection's response deadline."
  [^SelectionKey key write-bf message wire-opts]
  (with-write key
    (p/write-message-blocking (.channel key) write-bf message wire-opts)))

(defn- expire-deadline! [^AtomicLong ref ^long now]
  (let [deadline (.get ref)]
    (and (not (zero? deadline)) (>= now deadline)
         (.compareAndSet ref deadline Long/MIN_VALUE))))

(defn expire!
  "Close expired transports independently of session persistence. Keep their
  admission until the owner exits; cancellation cannot create extra threads."
  [^ConcurrentHashMap connection-keys]
  (doseq [^SelectionKey key (.values connection-keys)]
    (try
      (when (and (.isValid key) (.isOpen (.channel key)))
        (when-let [^Lease lease (lease key)]
          (let [^AtomicLong ref (.-read-deadline lease)
                now (System/nanoTime)
                auth-expired?
                (when (and (.get ^AtomicBoolean (.-pending? lease))
                           (>= now (.-authentication-deadline lease)))
                  (locking lease
                    (when (.get ^AtomicBoolean (.-pending? lease))
                      (.set ref Long/MIN_VALUE)
                      true)))]
            (when (or auth-expired?
                      (expire-deadline! ref now)
                      (expire-deadline! (.-write-deadline lease) now))
              (close! key)))))
      (catch Exception e
        (log/debug e "Failed to close expired connection")))))

(defn- connection-key
  "Retain the handlers' channel/attachment API without registering the blocking
  socket with a selector. Read readiness belongs exclusively to its thread."
  ^SelectionKey [^SocketChannel ch]
  (let [valid? (AtomicBoolean. true)]
    (proxy [SelectionKey] []
      (channel [] ch)
      (selector [] nil)
      (isValid [] (and (.get valid?) (.isOpen ch)))
      (cancel [] (.set valid? false))
      (interestOps
        ([] 0)
        ([_] (throw (UnsupportedOperationException. "Connection owns its reads"))))
      (readyOps [] 0))))

(defn close!
  "Cancel a connection by closing its socket and interrupting its owner. Keep
  the thread registered until it exits so shutdown can still join it."
  [^SelectionKey key]
  (let [{:keys [connection-id connection-threads connections]} (some-> key .attachment deref)]
    (when-let [^Lease lease (lease key)]
      (.set ^AtomicLong (.-read-deadline lease) Long/MIN_VALUE)
      (.set ^AtomicLong (.-write-deadline lease) Long/MIN_VALUE))
    (.cancel key)
    (try
      (.close (.channel key))
      (finally
        (when connections (.remove ^Set connections (.channel key)))
        (when-let [^Thread thread (and connection-threads
                                       (.get ^ConcurrentHashMap connection-threads
                                             connection-id))]
          (when-not (identical? thread (Thread/currentThread))
            (.interrupt thread)))))))

(defn accept!
  "Reserve capacity before allocating buffers or starting a thread. Reject
  without queueing or logging, and release capacity only after owner cleanup."
  ([listener connections connection-keys connection-threads limits serve! context-fn]
   (accept! listener connections connection-keys connection-threads limits
            serve! context-fn codec/create))
  ([^SelectionKey listener
    ^Set connections ^ConcurrentHashMap connection-keys
    ^ConcurrentHashMap connection-threads limits serve! context-fn create-codec]
   (when-let [^SocketChannel ch (.accept ^ServerSocketChannel (.channel listener))]
     (resources/with-acquired
       (fn [own!]
         (own! ch #(.close ^SocketChannel %))
         (if-let [lease (acquire! limits)]
           (let [lease (own! lease release!)
                 id (UUID/randomUUID)
                 accepted-key (volatile! nil)]
             (try
               (.configureBlocking ch true)
               (let [key (connection-key ch)
                     _ (vreset! accepted-key key)
                     context (context-fn key)
                     thread (Thread.
                             ^Runnable
                             (fn []
                               (try
                                 (serve! key)
                                 (finally
                                   (try
                                     (close! key)
                                     (finally
                                       (try
                                         (.close ^java.io.Closeable
                                          (:codec-context @(.attachment key)))
                                         (finally
                                           (.remove connection-keys id)
                                           (.remove connection-threads id)
                                           (release! lease))))))))
                             (str "datalevin-connection-" id))]
                 (.attach key (volatile! {:read-bf (bf/allocate-buffer c/+buffer-size+)
                                          :write-bf (bf/allocate-buffer c/+buffer-size+)
                                          :wire-opts (p/default-wire-opts)
                                          :request-decoder (p/request-decoder)
                                          :context context
                                          :codec-context (create-codec)
                                          :connection-id id
                                          :connection-lease lease
                                          :connection-threads connection-threads
                                          :connections connections}))
                 (.add connections ch)
                 (.put connection-keys id key)
                 (.put connection-threads id thread)
                 (.start thread))
               (catch Throwable t
                 (resources/close-suppressing! t #(.close ch))
                 (when-let [context (some-> ^SelectionKey @accepted-key .attachment deref :codec-context)]
                   (resources/close-suppressing! t #(.close ^java.io.Closeable context)))
                 (.remove connections ch)
                 (.remove connection-keys id)
                 (.remove connection-threads id)
                 (release! lease)
                 (throw t))))
           (.close ch)))))))
