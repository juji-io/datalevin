(ns datalevin.query.source.harness
  "Isolated F0 conformance driver. Eager small fixtures only; not d/q execution."
  (:refer-clojure :exclude [run!])
  (:require [datalevin.query.source :as source]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]))

(defn- invoke [phase f]
  (try (f)
       (catch Throwable e
         (let [data (ex-data e)
               category (:error data)]
           (throw (ex-info "Query-source operation failed"
                           (merge {:error (if (or (= category :query/timeout)
                                                  (and (keyword? category)
                                                       (= "query-source" (namespace category))))
                                           category :query-source/provider)
                                   :phase phase}
                                  (select-keys data [:category :maximum :attempted :state]))))))))

(defn run!
  ([source clauses] (run! source clauses {}))
  ([source clauses {:keys [consume limits timeout-ms bindings required-vars]
                    :or {bindings {} consume (fn [_] nil)}}]
   (let [control (control/create {:limits limits :timeout-ms timeout-ms})
         resources (atom [])
         acquired-session (atom nil)
         primary (atom nil)]
     (try
       (let [descriptor (invoke :descriptor #(source/-source-info source))
             negotiated (contract/negotiate descriptor)
             _ (contract/clauses! descriptor clauses)
             request (merge negotiated {:query-id :f0 :authorization {:principal-id :test}
                                        :consistency :snapshot :control control
                                        :limits (control/limits control)
                                        :source-descriptors [descriptor]})
             session (invoke :acquisition #(source/-open-session source request))
             _ (when-not (satisfies? source/IQuerySourceSession session)
                 (throw (ex-info "Invalid source session" {:error :query-source/invalid :phase :session})))
             _ (reset! acquired-session session)
             _ (swap! resources conj #(source/-close-session! session))
             _ (contract/session-info! request (source/-session-info session))
             plan-request (merge request {:source-symbol '$test :source-descriptor descriptor
                                          :clauses clauses :bindings bindings
                                          :required-vars (or required-vars
                                                             (into #{} (mapcat contract/variables) clauses))
                                          :demand {:quality :exact :ordering [] :offset 0 :limit nil}
                                          :max-alternatives 32})
             response (invoke :planning #(source/-plan-source source session plan-request))
             [alternative] (contract/selection! plan-request response [:scan])
             cursor (invoke :open #(source/-open-cursor
                                    session (merge request (select-keys alternative [:plan])
                                                   {:alternative-id (:id alternative)
                                                    :source-symbol '$test
                                                    :definition-fingerprint (:definition-fingerprint descriptor)})))
             _ (when-not (satisfies? source/IQuerySourceCursor cursor)
                 (throw (ex-info "Invalid source cursor" {:error :query-source/invalid :phase :open})))
             _ (swap! resources conj #(source/-close-cursor! cursor))
             batch-request {:control control :max-rows (:batch-rows (:limits request))
                            :max-bytes (:batch-bytes (:limits request))}]
         (loop [rows []]
           (source/check-active! control)
           (let [batch (invoke :fetch #(source/-next-batch! cursor batch-request))]
             (contract/batch! (:columns alternative) batch-request batch)
             (try (consume batch)
                  (finally (when-let [token (:reservation batch)] (source/release! token))))
             (let [rows (into rows (:rows batch))]
               (if (:exhausted? batch)
                 {:rows rows :columns (:columns alternative) :metrics (source/-cursor-metrics cursor)}
                 (recur rows))))))
       (catch Throwable e
         (reset! primary e)
         (let [reason {:error (or (:error (ex-data e)) :query-source/provider)
                       :phase (or (:phase (ex-data e)) :execution)}]
           (control/cancel! control reason)
           (when-let [session @acquired-session]
             (try (invoke :cancel #(source/-cancel-session! session reason))
                  (catch Throwable cleanup (.addSuppressed e cleanup)))))
         (throw e))
       (finally
         (doseq [close (reverse @resources)]
           (try (invoke :close close)
                (catch Throwable e
                  (if-let [original @primary] (.addSuppressed ^Throwable original e)
                          (reset! primary e)))))
         (control/close! control)
         ;; A cleanup failure on an otherwise successful query is still failure.
         (when (and @primary (= :close (:phase (ex-data @primary))))
           (throw @primary)))))))
