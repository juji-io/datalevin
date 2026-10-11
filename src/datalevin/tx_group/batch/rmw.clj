;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.rmw
  "Coordinate native RMW preparation, WAL dispatch, and commit."
  (:require [datalevin.interface :as i]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.executor :as executor]
            [datalevin.tx-group.batch.rmw.application :as application]
            [datalevin.tx-group.batch.rmw.member :as member]
            [datalevin.tx-group.batch.rmw.staging :as staging]
            [datalevin.tx-group.batch.wal :as wal]
            [datalevin.tx-group.phase :as phase])
  (:import [datalevin.utl RowRegions]
           [java.util ArrayList List]))

(defn clean-body-failure?
  "Classify expected body rejections without hiding native failures."
  [t]
  (member/clean-body-failure? t))

(def ^:dynamic *reuse-datalog-member-setup?*
  "Internal comparison switch for member-setup allocation measurements."
  true)

(def ^:dynamic *defer-storage-encoding?*
  "Internal comparison switch for the final storage drain."
  true)

(defn- encode-members!
  "Freeze consecutive body writes together, preserving prepared KV regions
  and the final Datalog metadata trailer in exactly their native order."
  [b encode-body defer?]
  (let [term (:ha-term (batch/context (batch/batch-at b 0)))
        encode-body (fn [rows opts] (encode-body rows (assoc opts :ha-term term)))
        parts (ArrayList.) rows (volatile! (RowRegions.))
        flush! (fn []
                 (when-not (.isEmpty ^RowRegions @rows)
                   (.add parts @rows)
                   (vreset! rows (RowRegions.))))]
    (dotimes [idx (batch/batch-count b)]
      (let [d (batch/batch-at b idx)
            data (batch/data d)]
        (when-not (= term (:ha-term (batch/context d)))
          (batch/cancel-before-dispatch!
           (ex-info "Cannot combine writes from different HA terms"
                    {:error :ha/write-rejected :reason :leadership-changed
                     :retryable? true})))
        (when-let [body (:wal-body data)]
          (flush!)
          (.add parts body)
          (batch/set-data! d (batch/with-data-wal-body! data nil)))
        (when-let [wal-rows (:wal-rows data)]
          (.append ^RowRegions @rows wal-rows))))
    ;; The final region needs no replacement accumulator.
    (when-not (.isEmpty ^RowRegions @rows)
      (.add parts @rows))
    (when-not (.isEmpty parts) (wal/prepare-body parts encode-body defer?))))

(defn- apply-prefix!
  "Apply and collect ready members, leaving only a frozen suffix for WAL overlap.
  Return the logical write weight and the first unapplied member index."
  [members wal opts batch]
  (let [apply-member! #(member/apply-member! members %)]
    (loop [start (long 0)
           end (long (if (:collect-prepared-prefix? opts)
                       (max 1 (dec (long (batch/batch-count batch))))
                       (min 1 (long (batch/batch-count batch)))))
           weight (long 0)]
      (let [weight (long
                    (loop [idx (long start) weight (long weight)]
                      (if (< idx end)
                        (recur (inc idx)
                               (+ weight (long (apply-member! idx))))
                        weight)))]
        (phase/phase! :native-prefix-applied batch)
        ;; Give already-admitted preparers one scheduling turn before
        ;; freezing a microsecond native prefix. This is a bounded
        ;; hint, never a wait for completion or permission to collect
        ;; an unprepared request. Uncontended writes do not park.
        (when (and (:collect-prepared-prefix? opts)
                   (batch/callers-preparing? batch))
          (java.util.concurrent.locks.LockSupport/parkNanos 1000))
        (let [collected (long (if (:collect? opts) (batch/collect-ready! batch) 0))
              next-end (long (batch/batch-count batch))]
          (cond
            (= end next-end) [weight end]
            ;; Preserve the old collector's collection during native
            ;; application. Apply newly collected prefixes, retaining
            ;; one prepared final member for useful WAL overlap.
            (and (:collect-prepared-prefix? opts) (pos? collected))
            (recur end
                   (long (if (and wal
                                  (nil? (batch/op (batch/batch-at batch (dec next-end))))
                                  (not (:state-dependent? (batch/data (batch/batch-at batch (dec next-end))))))
                           (max end (dec next-end)) next-end)) weight)
            ;; These already-prepared writes need no body evaluation.
            ;; Freeze before append, then apply them alongside WAL I/O.
            (and wal (every? #(let [d (batch/batch-at batch %)]
                                (and (nil? (batch/op d))
                                     (not (:state-dependent? (batch/data d)))))
                             (range end next-end)))
            [(reduce (fn [total idx]
                       (+ (long total)
                          (if (seq (:rows (batch/data (batch/batch-at batch idx)))) 1 0)))
                     weight (range end next-end)) end]
            :else (recur end next-end (long weight))))))))

(defn- freeze-wal!
  "Freeze WAL bodies and the schedule from the completed preparation result."
  [batch opts tail weight suffix-start]
  (let [weight (long weight)
        suffix-start (long suffix-start)]
    (batch/set-accepted-count!
     batch
     (if (:encode-batch? opts)
       (if-let [body (encode-members!
                      batch (:encode-body opts)
                      (or (:storage-tail? tail) (> weight 1) (:large? tail)
                          (and tail (> (.size ^List (:rows tail)) 256))))]
         (let [d (batch/batch-at batch (dec (batch/batch-count batch)))]
           (batch/set-data! d (batch/with-data-wal-body! (batch/data d) body))
           1)
         0)
       weight))
    (batch/freeze-schedule!
     batch weight
     (or (and (some? tail)
              (or (not (:prepared? tail))
                  (> (batch/batch-count batch) 1)
                  (:large? tail)
                  (> (.size ^java.util.List (:rows tail)) 256)))
         (and (< suffix-start (batch/batch-count batch))
              ;; For tiny prepared suffixes the worker handoff costs more
              ;; than applying the rows inline. Larger suffixes still overlap.
              (or (:encode-batch? opts)
                  (not (:collect-prepared-prefix? opts))
                  (> (long (reduce (fn [total idx]
                                     (if-let [^bytes body (:wal-body (batch/data (batch/batch-at batch idx)))]
                                       (+ (long total) (alength body)) total))
                                   0 (range suffix-start (batch/batch-count batch))))
                     4096))))
     ;; A single large frozen tail also repays a worker handoff.
     (and (some? tail)
          (or (not (:prepared? tail)) (:large? tail)
              (> (.size ^java.util.List (:rows tail)) 256))))))

(defn- initialize-writer!
  [view failure batch opts]
  (application/retain-failures! view failure)
  ;; Admission-time isolation can become stale while waiting for this writer.
  (when-let [before! (:before-batch! opts)] (before! batch))
  ;; Readiness belongs to this writer, including an entirely frozen prefix.
  (when-let [ensure! (:ensure-body-ready! opts)]
    (try (ensure! view)
         (catch Throwable t (batch/cancel-before-dispatch! t)))))

(defn- apply-suffix!
  [application staging suffix-start]
  (let [batch (:batch application)]
    (staging/flush-tail! staging)
    (loop [idx (long suffix-start)]
      (when (< idx (batch/batch-count batch))
        (application/apply-rows! application
                                 (:rows (batch/data (batch/batch-at batch idx))))
        (recur (inc idx))))
    (phase/phase! :native-applied batch)))

(defn- dispatch!
  [application staging wal wake! opts weight suffix-start]
  (let [batch (:batch application)
        weight (long weight)
        apply-suffix! #(apply-suffix! application staging suffix-start)]
    (batch/begin-dispatch! batch)
    (application/begin-dispatch! application)
    (phase/phase! :native-dispatch-start batch)
    (if (and wal (pos? weight))
      (if-let [run-policy! (:run-policy! opts)]
        (run-policy! batch (batch/batch-lsn batch) apply-suffix!)
        (let [value (executor/append-group! wal batch (batch/batch-lsn batch))
              durable? (boolean (executor/complete-policy!
                                 wal value (batch/batch-cutoff batch)))]
          (batch/record-wal-policy! batch durable?)
          (when-not durable? (wake!))
          (phase/phase! :wal-complete batch)
          (apply-suffix!)
          value))
      (do (apply-suffix!) nil))))

(defn- before-commit!
  [batch opts token wdb]
  (batch/check-serving! (batch/batch-collector batch))
  (when (batch/expired? batch)
    (throw (ex-info "Write deadline expired before native commit"
                    {:error :txlog/write-deadline-exceeded
                     :outcome :not-committed :retryable? false})))
  (when-let [write-metadata! (:write-metadata! opts)]
    (write-metadata! wdb token)))

(defn execute!
  "Resolve members once, freeze storage and WAL, then apply the remaining tail.
  All members share the native transaction and commit outcome. Writer growth
  replays frozen rows through Application without re-entering member scopes."
  [raw wal next-lsn! wake! check-batch! opts batch]
  (let [failure (volatile! nil)
        token (volatile! nil)
        history (when (:retry-frozen? opts) (ArrayList.))
        opts (assoc opts
                    :reuse-datalog-member-setup? *reuse-datalog-member-setup?*
                    :defer-storage-encoding? *defer-storage-encoding?*)]
    (phase/phase! :native-start batch)
    (application/apply-with-frozen-commit-retry!
     raw batch history
     (fn [wdb]
       (initialize-writer! wdb failure batch opts)
       (let [application (application/create raw wdb batch history failure opts)
             staging (staging/create
                      application wal opts
                      #(member/resume-native-reads! application %1 %2))
             members (member/create application staging)
             [weight suffix-start] (apply-prefix! members wal opts batch)
             [weight suffix-start] (staging/finish! staging weight suffix-start)
             weight (long weight)]
         (freeze-wal! batch opts (staging/pending-tail staging) weight suffix-start)
         (when (and wal (pos? weight))
           (batch/set-lsn! batch (long (next-lsn!)))
           (batch/refresh-wal-bodies! batch))
         (when check-batch! (check-batch! batch))
         (when-let [before! (:before-append! opts)] (before! batch))
         (vreset! token (dispatch! application staging wal wake! opts weight suffix-start))
         (when (zero? weight) (i/abort-transact-kv raw))))
     (fn [wdb _] (before-commit! batch opts @token wdb)))
    (when-let [committed! (:committed! opts)] (committed! batch @token))
    (phase/phase! :native-committed batch)
    (phase/phase! :execution-complete batch)
    (let [values (object-array (batch/batch-count batch))]
      (dotimes [idx (batch/batch-count batch)]
        (aset values idx (:result (batch/data (batch/batch-at batch idx)))))
      values)))
