(ns ^:no-doc datalevin.query.source.runtime
  "Query-scoped ownership and redacted calls across the trusted source boundary."
  (:require [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.value :as value])
  (:import [java.util UUID]))

(def ^:dynamic *runtime* nil)

(def categories
  #{:query-source/version :query-source/unsupported :query-source/invalid
    :query-source/unauthorized :query-source/stale-definition :query-source/snapshot-lost
    :query-source/limit :query-source/cancelled :query-source/provider :query/timeout})

(def ^:private phases
  #{:descriptor :inputs :clauses :projection :window :execution-mode :context
    :types :native-view :native-type :native-value :equality :acquisition :session
    :planning :open :fetch :batch :values :reserve :execution :metrics :close :syntax :explain})

(defn redacted-error [runtime phase error]
  ;; Only errors redacted by this invocation may retain diagnostic context.
  ;; Provider ex-data can contain arbitrary values even under structural keys.
  (if (contains? @(:redacted-errors runtime) error)
    error
    (let [data (ex-data error)
          error
          (ex-info "Query-source operation failed"
             (merge {:error (cond (instance? InterruptedException error) :query-source/cancelled
                                  (categories (:error data)) (:error data)
                                  :else :query-source/provider)
                     :phase (if (phases phase) phase :execution) :query-id (:query-id runtime)}
                    (:location runtime)
                    (when (= :query/timeout (:error data)) {:type :query/timeout})
                    (when (= :closed (:state data)) {:state :closed})
                    (when (and (contains? control/default-limits (:category data))
                               (integer? (:maximum data)) (integer? (:attempted data)))
                      (select-keys data [:category :maximum :attempted]))))]
      (swap! (:redacted-errors runtime) conj error)
      error)))

(defn with-context
  "Locate failures using core query/plan context, never provider exception data."
  [runtime location f]
  (let [runtime (update runtime :location merge
                        (select-keys location [:source-symbol :clause-ids :alternative-id]))]
    (try (f runtime)
         (catch Throwable e
           (throw (redacted-error runtime (or (:phase (ex-data e)) :execution) e))))))

(defn invoke [runtime phase f]
  (let [result (try
                 (source/check-active! (:control runtime))
                 (try (f)
                      (catch Throwable e
                        (source/check-active! (:control runtime))
                        (throw e)))
                 (catch Throwable e
                   (throw (redacted-error runtime phase e))))]
    ;; A provider may successfully return a resource just as its deadline fires.
    ;; Callers first attach ownership, then check active before the next operation.
    result))

(defn own!
  "Attach one resource and return its idempotent close function."
  [runtime close]
  (let [closed (atom false)
        close (fn []
                (when (compare-and-set! closed false true)
                  (try (close)
                       (catch Throwable e
                         (throw (redacted-error runtime :close e))))))]
    (swap! (:cleanup runtime) conj close)
    close))

(defn charge!
  "Reserve before retaining a logical value and its ledger/container overhead."
  [runtime x]
  (source/reserve! (:control runtime)
                   {:retained-bytes (value/add-charge 256 (value/value-charge x))}))

(defn with-runtime [options f]
  (let [control (control/create (assoc options :parent (:control *runtime*)))
        runtime {:control control :query-id (UUID/randomUUID) :cleanup (atom [])
                 :redacted-errors (atom #{})
                 :intermediate-counts? (boolean (:intermediate-counts? options))
                 :explain (atom (cond-> {:sources [] :native-scans 0}
                                  (:intermediate-counts? options) (assoc :streamed-rows 0)))}
        primary (atom nil)]
    (binding [*runtime* runtime]
      (try
        (let [result (f runtime)]
          (source/check-active! control)
          result)
        (catch Throwable e
          (let [error (redacted-error runtime (or (:phase (ex-data e)) :execution) e)]
            (reset! primary error)
            (control/cancel! control (select-keys (ex-data error) [:error :phase]))
            (throw error)))
        (finally
          (let [cleanup-error (atom nil)]
            (doseq [close (reverse @(:cleanup runtime))]
              (try (close)
                   (catch Throwable e
                     (let [error (redacted-error runtime :close e)]
                       (if-let [original (or @primary @cleanup-error)]
                         (.addSuppressed ^Throwable original error)
                         (let [^Throwable cancelled
                               (try (source/check-active! control) nil
                                    (catch Throwable e
                                      (redacted-error runtime
                                        (or (:phase (ex-data e)) :execution) e)))
                               error (if cancelled
                                       (doto cancelled (.addSuppressed error)) error)]
                           ;; Cancellation can first become visible during
                           ;; cleanup. Its category still precedes cleanup errors.
                           (reset! cleanup-error error)
                           (control/cancel! control (select-keys (ex-data error) [:error :phase]))))))))
            (reset! (:cleanup runtime) [])
            (if (or @primary @cleanup-error)
              (control/close! control)
              (try (control/complete! control)
                   (catch Throwable e
                     (reset! cleanup-error
                             (redacted-error runtime (or (:phase (ex-data e)) :execution) e)))))
            (when @cleanup-error (throw @cleanup-error))))))))
