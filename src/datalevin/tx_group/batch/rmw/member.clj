;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.rmw.member
  "Member capture, trusted resolver reuse, and permanently scoped public views."
  (:require [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.rmw.application :as application]
            [datalevin.tx-group.batch.rmw.staging :as staging]
            [datalevin.tx-group.failure :as failure]
            [datalevin.tx-group.phase :as phase])
  (:import [clojure.lang ExceptionInfo]
           [datalevin.lmdb KVTxData]
           [datalevin.utl RowRegions WriterContext]
           [java.util ArrayList Arrays List]))

(defn- terminal-body-failure? [^Throwable t]
  (or (instance? Error t)
      (instance? InterruptedException t)
      (:resized (ex-data t))
      (#{:txlog/runtime-fenced :txlog/runtime-closed
         :txlog/write-deadline-exceeded :txlog/write-interrupted}
       (:error (ex-data t)))
      (when-let [cause (ex-cause t)] (terminal-body-failure? cause))))

(defn clean-body-failure?
  "Tagged user exceptions and expected validation errors can cancel without
   fencing. Native failure records take precedence over callback tags. Untagged
   wrappers must not hide an unexpected engine failure in their cause."
  [^Throwable t]
  (or (and (l/explicit-transaction-timeout-error? t)
           (instance? InterruptedException (ex-cause t)))
      (and (failure/user-body-failure? t)
           (not (terminal-body-failure? t)))
      (and (or (instance? ExceptionInfo t)
               (instance? IllegalArgumentException t))
           (not (batch/pre-dispatch-cancellation? t))
           (not (:resized (ex-data t)))
           (not (#{:txlog/runtime-fenced :txlog/runtime-closed
                   :txlog/write-deadline-exceeded :txlog/write-interrupted}
                 (:error (ex-data t))))
           (or (nil? (ex-cause t)) (clean-body-failure? (ex-cause t))))))

(defn- owner-interruption
  [^Throwable t]
  (when-not (l/explicit-transaction-timeout-error? t)
    (if (instance? InterruptedException t)
      t
      (when-let [cause (ex-cause t)] (owner-interruption cause)))))

(defn- record-failure!
  "Retain the first failure on this native owner's turn."
  [failure t]
  (when-not @failure (vreset! failure t)))

(defn- abort-member!
  "Cancel the batch without changing the member view's scope lifetime."
  [aborted? failure opts]
  (vreset! aborted? true)
  (let [t (batch/cancellation-error
            (ex-info "Batch aborted" {:error :txlog/request-aborted
                                      :outcome :not-committed :retryable? false}))]
    (record-failure! failure t)
    (when-not (:abort-returns? opts) (throw t))))

(defn- charge! [descriptor n]
  (try (batch/charge! descriptor n)
       (catch Throwable t
         (if (= :txlog/pending-budget-exceeded (:error (ex-data t)))
           (batch/cancel-before-dispatch! t)
           (throw t)))))

(defn- owned ^bytes [charge-fn descriptor ^bytes value]
  (charge-fn descriptor (charge/array-bytes 1 (alength value)))
  (Arrays/copyOf value (alength value)))

(defn- capture!
  "Capture owned prepared rows or copy raw input into batch-owned rows,
   deferring eligible native tails. Retain failures even when the body catches
   them, so no failed batch can reach WAL or native commit."
  [raw wdb apply-rows! descriptor rows wal-rows valid? failure
   {:keys [check-row! prepare-rows! charge-fn prepared-request?
           application-error!]
    :or {charge-fn charge!} :as opts} dbi-name txs kt vt]
  (try
    (l/write-txn wdb)
    (when-not @valid?
      (throw (ex-info "Write body has finished"
                      {:error :txlog/transaction-view-invalidated :retryable? false})))
    (when-let [t @failure] (throw t))
    (when-let [flush! (:storage-flush! opts)] (flush!))
    (let [prepared (if prepare-rows!
                     (try
                       (prepare-rows! raw descriptor dbi-name txs kt vt)
                       (catch Throwable t
                         ;; Encoding/validation failed before native application.
                         ;; Classify it just like a rejected user body, retaining
                         ;; the cancellation even if that body catches the error.
                         (if application-error!
                           (application-error! t)
                           (throw t))))
                     txs)
          dbi-name (when-not prepare-rows! dbi-name)]
      (if prepared-request?
        ;; Prepared profiles validate and own their native rows. The raw
        ;; profile validates and copies each row below.
        (staging/capture-prepared! wdb apply-rows! rows wal-rows opts prepared)
        (doseq [row prepared]
          (let [record? (instance? KVTxData row)
                op (if record? (.-op ^KVTxData row) (nth row 0))
                name (or dbi-name (if record? (.-dbi-name ^KVTxData row) (nth row 1)))
                k (if record? (.-k ^KVTxData row) (nth row (if dbi-name 1 2)))
                v (when (#{:put :del-list} op)
                    (if record? (.-v ^KVTxData row) (nth row (if dbi-name 2 3))))
                key-type (if record? (.-kt ^KVTxData row)
                             (if dbi-name kt (nth row (if (= op :put) 4 3) :data)))
                val-type (if record? (.-vt ^KVTxData row)
                             (if dbi-name vt (nth row 5 :data)))
                flags (if record? (.-flags ^KVTxData row)
                          (nth row (if dbi-name (if (= op :put) 3 2)
                                       (if (= op :put) 6 4)) nil))]
            (when-not (and (or (#{:put :del} op)
                               (and prepare-rows! (= :del-list op) (= 1 (count v)))) (= key-type :raw)
                           (or (= op :del) (= val-type :raw)) (empty? flags))
              (throw (ex-info "Operation is outside private native RMW scope"
                              {:error :txlog/unsupported-private-operation
                               :outcome :not-committed :retryable? false})))
            (check-row! name op k v)
            (charge-fn descriptor (+ charge/encoded-row-descriptor charge/vector-wrapper))
            (let [key (when k (owned charge-fn descriptor k))
                  value (case op
                          :put (owned charge-fn descriptor v)
                          :del-list (do (charge-fn descriptor charge/vector-wrapper)
                                        [(owned charge-fn descriptor (first v))])
                          nil)
                  forward (l/kv-tx op name key value :raw :raw)]
              (.add ^ArrayList rows forward)
              (apply-rows! [forward]))))))
    :transacted
    (catch Throwable t
      (when (and (l/blind-unique-collision? t)
                 (:unique-inserts-rolled-back? (ex-data t)))
        (throw t))
      (let [failure-error
            (if (and (= :not-committed (:outcome (ex-data t)))
                     (clean-body-failure? t))
              (batch/cancellation-error t)
              t)]
        (record-failure! failure failure-error)
        (throw failure-error)))))

(defn- resolve-member!
  [b descriptor view failure aborted? opts]
  (phase/phase! :resolution-start b)
  (let [result (try
                 ((batch/op descriptor) view)
                 (catch Throwable t
                   (cond
                     @failure (throw (or (owner-interruption @failure) @failure))
                     (owner-interruption t) (throw (owner-interruption t))
                     (clean-body-failure? t) (batch/cancel-before-dispatch! t)
                     :else (throw t))))]
    (when @aborted?
      (when-let [explicit-abort? (:datalevin.tx-group.batch.rmw/explicit-abort?
                                  (batch/context descriptor))]
        (vreset! explicit-abort? true)))
    (phase/phase! :resolution-complete b)
    (when-let [t @failure]
      (if (and @aborted? (:abort-returns? opts)
               (or (batch/pre-dispatch-cancellation? t) (clean-body-failure? t)))
        (batch/cancel-before-dispatch!
         (ex-info "Batch aborted" {:error :txlog/request-aborted
                                   :outcome :not-committed :retryable? false} t))
        (throw (or (owner-interruption t) t))))
    (batch/check-preparation! b)
    result))

(defn- datalog-member-setup
  "One callback scope for trusted resolvers in a native batch. Descriptor and
  carriers belong to the currently executing member; retained row regions
  themselves are never reset or reused."
  [raw native-db apply-rows! failure opts]
  (let [valid? (volatile! false)
        aborted? (volatile! false)
        wdb (l/mark-write native-db)
        ^WriterContext state (l/writer-context wdb)
        capture (fn [name txs kt vt]
                  (staging/member-carriers! state)
                  (capture! raw wdb apply-rows! (.descriptor state)
                            (.rows state) (.walRows state) valid? failure
                            opts name txs kt vt))
        abort! #(abort-member! aborted? failure opts)
        staged!
        (when (:storage-rows! opts)
          (fn []
            ;; Carry its identity without allocating empty row regions.
            (staging/remember-storage! (:staging opts) wdb (.descriptor state)
                                       nil nil state (l/request-context wdb))))]
    (.setCallbacks state capture staged! (:preparation-flush! opts) abort!
                   (fn [t] (record-failure! failure t)))
    {:state state :valid? valid? :aborted? aborted? :view wdb :native-db native-db}))

(defn- run-datalog-member!
  [b descriptor failure opts setup]
  ((:charge-fn opts charge!) descriptor charge/vector-wrapper)
  (let [{:keys [state valid? aborted? view native-db]} setup
        ^WriterContext state state]
    (.setTransaction state (.transaction ^WriterContext (l/writer-context native-db)))
    (.beginMember state descriptor (batch/context descriptor))
    (vreset! valid? true)
    (vreset! aborted? false)
    (staging/reset-member! (:staging opts))
    ;; Consecutive trusted resolvers update the owned context. Views exposed to
    ;; user code are retired, so their scoped callbacks and validity stay fixed.
    (application/activate-member! (:application opts) view)
    (try
      (let [result (resolve-member! b descriptor view failure aborted? opts)
            rows (or (.rows state) [])
            wal-rows (or (.walRows state) [])
            staged? (staging/member-staged? (:staging opts))]
        (batch/set-data! descriptor
                         (batch/datalog-member-data rows wal-rows result staged?))
        (if (or (seq rows) staged?) 1 0))
      (finally
        (vreset! valid? false)
        (.endMember state)
        (application/activate-member! (:application opts) nil)))))

(defn- run-update-member!
  "Updates expose only a value to user code, so need no escaping view scope."
  [raw native-db apply-rows! b descriptor failure opts]
  ((:charge-fn opts charge!) descriptor charge/vector-wrapper)
  (let [rows (RowRegions.)
        wal-rows (RowRegions.)
        defer? (boolean (:update-tail! opts))
        valid? (volatile! true)
        [name txs kt vt] (resolve-member! b descriptor native-db failure
                                          (volatile! false) opts)]
    (try
      (capture! raw native-db (if defer? identity apply-rows!)
                descriptor rows wal-rows valid? failure
                opts name txs kt vt)
      (let [wal-body (when (and (:encode-body opts) (not (:encode-batch? opts)))
                       (let [estimate (long (if-let [cost (:body-cost opts)]
                                              (cost rows) 0))
                             charge-fn (:charge-fn opts charge!)]
                         (charge-fn descriptor estimate)
                         (let [^bytes body ((:encode-body opts) wal-rows
                                                                (batch/context descriptor))]
                           (charge-fn descriptor (max 0 (- (alength body) estimate)))
                           body)))]
        (when defer?
          (if (and wal-body (> (alength ^bytes wal-body) 4096))
            ((:update-tail! opts) rows native-db)
            (apply-rows! rows)))
        (batch/set-data! descriptor
                         {:rows rows :wal-rows wal-rows :wal-body wal-body
                          :result :transacted}))
      1
      (finally (vreset! valid? false)))))

(defn- run-member!
  [raw native-db apply-rows! batch descriptor failure
   {:keys [body-cost encode-body charge-fn] :or {charge-fn charge!} :as opts}]
  (charge-fn descriptor charge/vector-wrapper)
  (let [^List rows (if (:prepared-request? opts)
                     (RowRegions.) (ArrayList.))
        wal-rows (RowRegions.) valid? (volatile! true)
        aborted? (volatile! false)
        ;; Public bodies can retain their handle. Only trusted resolvers may
        ;; reuse the Datalog view; this handle and its validity never revive.
        wdb (l/mark-write native-db)
        ^WriterContext writer (l/writer-context wdb)
        capture (fn [name txs kt vt]
                  (capture! raw wdb apply-rows! descriptor rows wal-rows valid? failure opts name txs kt vt))
        abort! #(abort-member! aborted? failure opts)
        staged!
        (when (:storage-rows! opts)
          (fn []
            (staging/remember-storage!
             (:staging opts) wdb descriptor rows wal-rows
             nil (l/request-context wdb))))
        _ (.setCallbacks writer capture staged! (:preparation-flush! opts) abort!
                         (fn [t] (record-failure! failure t)))
        _ (.scopeTo writer valid? capture)
        _ (.setRequestContext writer (batch/context descriptor))
        _ (application/activate-member! (:application opts) wdb)]
    (staging/reset-member! (:staging opts))
    (try
      (let [result (resolve-member! batch descriptor wdb failure aborted? opts)
            wal-body (when (and encode-body (not (:encode-batch? opts))
                                (pos? (.size rows)))
                       (let [estimate (long (if body-cost (body-cost rows) 0))]
                         (charge-fn descriptor estimate)
                         (let [^bytes body (encode-body (if (:prepared-request? opts) wal-rows rows)
                                                        (batch/context descriptor))]
                           (charge-fn descriptor (max 0 (- (alength body) estimate)))
                           body)))]
        (batch/set-data! descriptor
                         (if (:encode-batch? opts)
                           {:rows rows :wal-body wal-body :result result
                            :wal-rows wal-rows
                            :storage-staged? (staging/member-staged? (:staging opts))}
                           {:rows rows :wal-body wal-body :result result}))
        (if (and (.isEmpty rows) (not (staging/member-staged? (:staging opts)))) 0 1))
      (finally (vreset! valid? false)
               (.setRequestContext writer nil)
               (application/activate-member! (:application opts) nil)))))

(defn resume-native-reads!
  "Leave resolver preparation before exposing native state to user code.
  An escaping scope gets permanent validity and its own capture callback."
  [application view opts]
  (let [{:keys [raw failure]} application
        apply-rows! #(application/apply-rows! application %)
        ^WriterContext scope (l/writer-context view)]
    (.setRequestContext scope
                        (dissoc (.requestContext scope)
                                :datalog-prepare?))
    ;; The cached preparation callback follows the
    ;; active view, even after an earlier resolver
    ;; escaped to user code and was retired.
    (when (and (.descriptor scope) (nil? (.validity scope)))
      (staging/member-carriers! scope)
      (let [member-valid? (volatile! true)
            descriptor (.descriptor scope)
            rows (.rows scope)
            wal-rows (.walRows scope)
            capture (fn [name txs kt vt]
                      (capture! raw view apply-rows!
                                descriptor rows wal-rows
                                member-valid? failure
                                opts name txs kt vt))]
        (.scopeTo scope member-valid? capture)))))

(defrecord Members [application staging opts apply-rows! ready? member-setup])

(defn create
  "Own member readiness and reusable trusted scopes for one native writer."
  [application staging]
  (let [opts (staging/member-options staging)]
    (->Members application staging opts #(application/apply-rows! application %)
               (volatile! (boolean (:ensure-body-ready! opts))) (volatile! nil))))

(defn apply-member!
  "Resolve one member once, retire its view, and return its logical write weight."
  [^Members members idx]
  (let [{:keys [application staging opts apply-rows! ready? member-setup]} members
        {:keys [raw view batch failure]} application
        wdb view]
    (batch/check-preparation! batch)
    (let [d (batch/batch-at batch idx)]
      ;; The Datalog resolver carries its own existing
      ;; transaction indexes across prepared requests.
      ;; Arbitrary bodies and KV writes need native state.
      (staging/before-member! staging d)
      (when-let [before! (:before-body! opts)] (before! d))
      (if (batch/op d)
        (do
          ;; Recovery/floor readiness belongs to this
          ;; owned transaction, rather than each body.
          (when-not @ready?
            (when-let [ensure! (:ensure-body-ready! opts)]
              (try (ensure! wdb)
                   (catch Throwable t
                     (if (clean-body-failure? t)
                       (batch/cancel-before-dispatch! t)
                       (throw t)))))
            (vreset! ready? true))
          (if (and (:reuse-datalog-member-setup? opts)
                   (:shared-datalog-writer? opts)
                   (:prepared-request? opts)
                   (:encode-batch? opts)
                   (:datalog-prepare? (batch/context d))
                   ;; A lone resolver cannot amortize
                   ;; a shared scope. Queue observation
                   ;; is only a setup hint, never member
                   ;; selection or a wait for a caller.
                   (or @member-setup
                       (> (batch/batch-count batch) 1)
                       (some? (.peek ^java.util.Queue
                               (.ready ^datalevin.tx_group.batch.Collector
                                (batch/batch-collector batch))))))
            (let [setup (or @member-setup
                            (let [setup (datalog-member-setup
                                         raw wdb apply-rows! failure opts)]
                              (vreset! member-setup setup)
                              setup))]
              (try
                (run-datalog-member! batch d failure opts setup)
                (finally
                  ;; User functions can retain the view
                  ;; itself, not just its capture. Never
                  ;; install another request's callbacks
                  ;; on a handle that has escaped.
                  (when (.validity ^WriterContext (:state setup))
                    (vreset! member-setup nil)))))
            (if (:kv-update? (batch/context d))
              (run-update-member! raw wdb apply-rows! batch d failure opts)
              (run-member! raw wdb apply-rows! batch d failure opts))))
        (let [rows (:rows (batch/data d))]
          (apply-rows! rows)
          (if (seq rows) 1 0))))))
