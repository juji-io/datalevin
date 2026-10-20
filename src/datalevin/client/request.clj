;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.client.request
  "A caller-owned budget shared by transport, restoration, and HA attempts."
  (:require [datalevin.command :as cmd])
  (:import [java.util.concurrent.atomic AtomicBoolean AtomicReference]))

(def ^:dynamic *request* nil)
(def ^:dynamic *wire-request* nil)
(def ^:dynamic *stage-deadline* nil)

(defrecord Budget [^long deadline ^long maximum ^long timeout-ms
                   ^AtomicBoolean closed ^AtomicBoolean ha?
                   ^AtomicReference request ^AtomicBoolean sent? ^AtomicBoolean uncertain?
                   ^AtomicBoolean tracked?])

(defn budget [timeout-ms ha-ms closed request]
  (let [now (System/nanoTime)
        deadline (+ now (* 1000000 (long timeout-ms)))]
    (->Budget deadline (+ deadline (* 1000000 (long ha-ms))) timeout-ms
              closed (AtomicBoolean. false) (AtomicReference. request)
              (AtomicBoolean. false) (AtomicBoolean. false) (AtomicBoolean. false))))

(defn deadline ^long []
  (let [^Budget b *request*
        outer (if (.get ^AtomicBoolean (.-ha? b)) (.-maximum b) (.-deadline b))]
    (if *stage-deadline* (min outer (long *stage-deadline*)) outer)))

(defn closed? []
  (boolean (when-let [^Budget b *request*]
             (when-let [^AtomicBoolean closed (.-closed b)] (.get closed)))))

(defn expired? []
  (and *request* (>= (System/nanoTime) (deadline))))

(defn check! [phase]
  (when (closed?)
    (throw (ex-info "This client is closed"
                    {:error :client/closed :termination-reason :client-disconnected})))
  (when (expired?)
    (throw (ex-info "Timeout in making request"
                    {:error :socket/timeout :phase phase
                     :timeout-ms (:timeout-ms *request*)
                     :termination-reason :deadline-exceeded}))))

(defn remaining-ms
  "Ceil a positive remaining interval; a zero selector timeout waits forever."
  ^long [fallback]
  (if *request*
    (do (check! :wait)
        (max 1 (quot (+ 999999 (- (deadline) (System/nanoTime))) 1000000)))
    (long fallback)))

(defn enter-ha! []
  (when-let [^Budget b *request*] (.set ^AtomicBoolean (.-ha? b) true))
  (check! :ha-retry))

(defn await!
  "Wait on an already locked monitor, using the caller's remaining budget."
  [monitor phase]
  (check! phase)
  (let [ns-left (max 1 (- (deadline) (System/nanoTime)))]
    (.wait ^Object monitor (quot ns-left 1000000) (int (rem ns-left 1000000)))))

(defn- same-command? [request message]
  (and request (= (:type request) (:type message))
       (= (or (:db-name request) (first (:args request)))
          (or (:db-name message) (first (:args message))))))

(defn register-request!
  "Capture metadata created inside the public boundary before its first send."
  [message]
  (when-let [^AtomicReference ref (:request *request*)]
    (let [request (.get ref)]
      (when (and (same-command? request message) (nil? (:client-op-id request)))
        (.compareAndSet ref request message)))))

(defn- original? [message]
  (when-let [request (some-> ^AtomicReference (:request *request*) .get)]
    (and (= (:type request) (:type message))
         (= (:client-op-id request) (:client-op-id message))
         (= (or (:db-name request) (first (:args request)))
            (or (:db-name message) (first (:args message)))))))

(defn sending! []
  (when (and *request* (original? *wire-request*)
             (not (cmd/read-only? (:type *wire-request*))))
    (.set ^AtomicBoolean (:sent? *request*) true)))

(defn track-exchange! [message]
  (when (and *request* (original? message))
    (.set ^AtomicBoolean (:tracked? *request*) true)))

(defn failed-attempt! []
  (when-let [^Budget b *request*]
    (when (.getAndSet ^AtomicBoolean (.-sent? b) false)
      (.set ^AtomicBoolean (.-uncertain? b) true))))

(defn failed-exchange!
  "Alternate IClient/IConnection implementations cannot report byte-level
  progress. Conservatively retain uncertainty when their exchange fails."
  [request]
  (when (and *request* (original? request)
             (not (.get ^AtomicBoolean (:tracked? *request*)))
             (not (cmd/read-only? (:type request))))
    (.set ^AtomicBoolean (:uncertain? *request*) true))
  (failed-attempt!))

(defn response! [request response]
  (when (and *request* (original? request))
    (when-not (#{:copy-in-response :copy-out-response} (:type response))
      (.set ^AtomicBoolean (:sent? *request*) false))
    (when (= :command-complete (:type response))
      (.set ^AtomicBoolean (:uncertain? *request*) false))))

(defn failure
  "Keep uncertainty across attempts, including when a later attempt never sent."
  [^Throwable error]
  (if (and *request* (.get ^AtomicBoolean (:uncertain? *request*)))
    (let [req (.get ^AtomicReference (:request *request*))
          existing (:err-data (ex-data error))
          data (merge existing (select-keys req [:client-op-id :client-op-hash
                                        :client-op-response-kind])
                      {:error :ha/write-indeterminate :indeterminate? true
                       :outcome :unknown :retryable? false :type (:type req)
                       :db-name (or (:db-name req) (first (:args req)))
                       :termination-reason
                       (cond (closed?) :client-disconnected
                             (expired?) :deadline-exceeded
                             :else (or (:termination-reason (ex-data error))
                                       :recovery-failed))})]
      (ex-info "Write outcome is unknown; a fresh invocation is not a safe retry"
               (merge (ex-data error) data {:err-data data})
               (if (= :ha/write-indeterminate (:error existing))
                 (or (ex-cause error) error)
                 error)))
    error))

(defmacro with-budget [value & body]
  `(if *request*
     (do ~@body)
     (binding [*request* ~value]
       (try ~@body
            (catch Throwable error#
              (failed-attempt!)
              (throw (failure error#)))))))

