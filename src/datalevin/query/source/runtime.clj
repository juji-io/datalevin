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
    :planning :open :fetch :batch :values :reserve :execution :metrics :close :syntax})

(defn redacted-error [runtime phase error]
  (let [data (ex-data error)]
    (ex-info "Query-source operation failed"
             (merge {:error (cond (instance? InterruptedException error) :query-source/cancelled
                                  (categories (:error data)) (:error data)
                                  :else :query-source/provider)
                     :phase (if (phases phase) phase :execution) :query-id (:query-id runtime)}
                    (when (= :query/timeout (:error data)) {:type :query/timeout})
                    (when (= :closed (:state data)) {:state :closed})
                    (when (and (contains? control/default-limits (:category data))
                               (integer? (:maximum data)) (integer? (:attempted data)))
                      (select-keys data [:category :maximum :attempted]))))))

(defn invoke [runtime phase f]
  (source/check-active! (:control runtime))
  (let [result (try (f)
                    (catch Throwable e
                      (source/check-active! (:control runtime))
                      (throw (redacted-error runtime phase e))))]
    ;; A provider may successfully return a resource just as its deadline fires.
    ;; Callers first attach ownership, then check active before the next operation.
    result))

(defn own!
  "Attach one resource and return its idempotent close function."
  [runtime close]
  (let [closed (atom false)
        close (fn [] (when (compare-and-set! closed false true) (close)))]
    (swap! (:cleanup runtime) conj close)
    close))

(defn charge!
  "Reserve before retaining a logical value and its ledger/container overhead."
  [runtime x]
  (source/reserve! (:control runtime) {:retained-bytes (+' 256 (value/value-charge x))}))

(defn with-runtime [options f]
  (let [control (control/create (assoc options :parent (:control *runtime*)))
        runtime {:control control :query-id (UUID/randomUUID) :cleanup (atom [])
                 :explain (atom {:sources [] :native-scans 0 :streamed-rows 0})}
        primary (atom nil)]
    (binding [*runtime* runtime]
      (try
        (f runtime)
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
                         (reset! cleanup-error error))))))
            (reset! (:cleanup runtime) [])
            (control/close! control)
            (when @cleanup-error (throw @cleanup-error))))))))
