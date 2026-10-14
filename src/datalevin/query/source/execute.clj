(ns ^:no-doc datalevin.query.source.execute
  "Bounded embedded execution of positive query-source conjunctions."
  (:require [datalevin.built-ins :as built-ins]
            [datalevin.parser :as dp]
            [datalevin.query.aggregate :as aggregate]
            [datalevin.query.source :as source]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.native :as native]
            [datalevin.query.source.plan :as plan]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.spill :as spill]
            [datalevin.timeout :as timeout])
  (:import [datalevin.db DB]
           [datalevin.parser FindRel FindColl FindScalar FindTuple]
           [java.util Objects HashSet]))

(defn- match-row [bindings terms row]
  (reduce (fn [bindings [term x]]
            (when bindings
              (case (first term)
                :ignore bindings
                :const (when (= (second term) x) bindings)
                :var (let [sym (second term)]
                       (if (contains? bindings sym)
                         (when (= (bindings sym) x) bindings)
                         (assoc bindings sym x)))))) bindings (map vector terms row)))

(defn- merge-row [left right]
  ;; Match the existing hash join's Java key equality. Predicates and projection
  ;; retain their existing Clojure semantics; neither uses provider collation.
  (when (every? (fn [[k v]] (or (not (contains? left k)) (Objects/equals (get left k) v))) right)
    (merge left right)))

(defn- predicates? [predicates row]
  (every? (fn [{:keys [operator args]}]
            (let [f (get built-ins/query-fns (symbol (name operator)))]
              (apply f (map #(if (= :const (first %)) (second %) (row (second %))) args)))) predicates))

(defn- metrics [cursor]
  (let [m (source/-cursor-metrics cursor)
        ks [:batches :rows :charged-bytes :read-rows :read-bytes :external-requests :elapsed-ns]]
    (when-not (and (map? m) (every? #(contains? m %) ks)
                  (every? #(or (nil? %) (and (integer? %) (<= 0 % Long/MAX_VALUE))) (map m ks)))
      (fail! :query-source/invalid :metrics))
    (select-keys m ks)))

(defn- visit-rows [rows emit]
  (loop [rows (seq rows)]
    (when rows
      (let [result (emit (first rows))]
        (if (reduced? result) result (recur (next rows)))))))

(defn- source-reader [runtime {:keys [session request descriptor symbol alternative]}]
  (fn [emit]
    (let [control (:control runtime)
          cursor (rt/invoke runtime :open
                            #(source/-open-cursor session
                               (merge request {:plan (:plan alternative) :alternative-id (:id alternative)
                                               :source-symbol symbol
                                               :definition-fingerprint (:definition-fingerprint descriptor)})))
          _ (when-not (satisfies? source/IQuerySourceCursor cursor) (fail! :query-source/invalid :open))
          close (rt/own! runtime #(source/-close-cursor! cursor))
          batch-request {:control control :max-rows (get-in request [:limits :batch-rows])
                         :max-bytes (get-in request [:limits :batch-bytes])}
          columns (:columns alternative)]
      (loop []
        (let [batch (rt/invoke runtime :fetch #(source/-next-batch! cursor batch-request))
              _ (contract/batch! columns batch-request batch)
              stopped
              (try
                (visit-rows (:rows batch) (fn [row]
                          (source/check-active! control)
                          (run! value/execution-value! row)
                          (swap! (:explain runtime) update :streamed-rows inc)
                          (let [scratch (source/reserve! control
                                          {:retained-bytes (+' 512 (*' 256 (count columns))
                                                               (value/value-charge row))})]
                            (try (emit (zipmap (map :variable columns) row))
                                 (finally (source/release! scratch))))))
                (finally (when-let [token (:reservation batch)] (source/release! token))))]
          (when-not (or (:exhausted? batch) (reduced? stopped)) (recur))))
      (close)
      (swap! (:explain runtime) update :cursors (fnil conj [])
             {:source symbol :metrics (rt/invoke runtime :metrics #(metrics cursor))}))))

(defn- native-reader [runtime snapshots bindings {:keys [native patterns]}]
  (mapv (fn [pattern]
          (fn [emit]
            (swap! (:explain runtime) update :native-scans inc)
            (native/scan! (:control runtime) (get snapshots (.-store ^DB native))
                          (second (second (:terms pattern)))
                          (fn [row]
                            (when-let [binding (match-row bindings (:terms pattern) row)]
                              (emit binding)))))) patterns))

(defn- result-shape [parsed rows]
  (let [rows (if-let [return (:qreturn-map parsed)]
               (mapv #(zipmap (:symbols return) %) rows) rows)
        find (:qfind parsed)]
    (cond (instance? FindRel find)
          (cond (:qreturn-map parsed) (set rows)
                (or (seq (:qwith parsed)) (some dp/aggregate? (dp/find-elements find))) rows
                :else (spill/new-spillable-set rows {:spill-threshold Long/MAX_VALUE
                                                     :initial-capacity (count rows)}))
          (instance? FindColl find) (mapv first rows)
          (instance? FindScalar find) (ffirst rows)
          (instance? FindTuple find) (first rows))))

(defn- collect-result [runtime {:keys [parsed limit offset]}]
  (let [limit (if (or (instance? FindScalar (:qfind parsed))
                      (instance? FindTuple (:qfind parsed))) 1 limit)
        find-vars (vec (dp/find-vars (:qfind parsed)))
        vars (into find-vars (map :symbol) (:qwith parsed))
        elements (dp/find-elements (:qfind parsed))
        aggregate? (some dp/aggregate? elements)
        seen (HashSet.)
        output (volatile! [])
        consumed (volatile! 0)
        stopped (volatile! false)
        save! (fn [row]
                (let [bytes (+' 256 (value/value-charge row))]
                  ;; The result quota is an error boundary, never a LIMIT.
                  (source/reserve! (:control runtime)
                                   {:result-rows 1 :result-bytes bytes :retained-bytes bytes})
                  (vswap! output conj row)))
        window! (fn [row]
                  (let [n (vswap! consumed inc)]
                    (when (> n offset) (save! row))
                    (when (and limit (>= n (+' offset limit)))
                      (vreset! stopped true)
                      (reduced ::stop))))]
    {:accept (fn [binding]
               (let [row (mapv binding vars)]
                 (when-not (.contains seen row)
                   ;; Also covers the aggregate's temporary grouping lists and
                   ;; projected tuple containers. Input state is quota-bound.
                   (source/reserve! (:control runtime)
                                    {:retained-bytes (+' 512 (*' 3 (value/value-charge row)))})
                   (.add seen row)
                   (when-not aggregate? (window! (mapv row (range (count find-vars))))))))
     :stopped? #(or @stopped (= 0 limit))
     :finish (fn []
               (when (and aggregate? (not= 0 limit))
                 (reduce (fn [_ row]
                           (source/check-active! (:control runtime))
                           (window! (vec row))) nil
                         (aggregate/aggregate elements {} (map #(subvec % 0 (count find-vars)) seen))))
               (let [result (result-shape parsed @output)]
                 (swap! (:explain runtime) assoc :result-rows (count @output))
                 result))}))

(defn- execute! [runtime {:keys [entries fragments bindings predicates] :as prepared} snapshots]
  (let [readers (into (vec (mapcat #(native-reader runtime snapshots bindings %) (filter :native entries)))
                      (map #(source-reader runtime %) fragments))
        readers (if (seq readers) readers [(fn [emit] (emit bindings))])
        tables (mapv (fn [read]
                       (let [rows (volatile! [])]
                         (read (fn [row] (rt/charge! runtime row) (vswap! rows conj row) nil))
                         @rows)) (butlast readers))
        {:keys [accept finish stopped?]} (collect-result runtime prepared)]
    (letfn [(join! [row remaining]
              (source/check-active! (:control runtime))
              (if (stopped?) (reduced ::stop)
                  (if-let [table (first remaining)]
                    (visit-rows table (fn [right]
                              (when-let [joined (merge-row row right)]
                                (let [token (rt/charge! runtime joined)]
                                  (try (join! joined (next remaining))
                                       (finally (source/release! token)))))))
                    (when (predicates? predicates row) (accept row)))))]
      (when (and (every? seq tables) (not (stopped?)))
        ((last readers) (fn [row]
                          (when-let [row (merge-row bindings row)]
                            (join! row tables)))))
      (finish))))

(defn run
  "Execute or explain without consulting native query/result caches."
  [query inputs explain-options]
  (let [raw (if (map? query) query (dp/query->map query))
        parsed (try (dp/parse-query query)
                    (catch Throwable _ (fail! :query-source/invalid :syntax)))
        options (:query-source raw)
        deadline (timeout/effective-deadline (:qtimeout parsed))]
    (binding [timeout/*deadline* deadline]
      (rt/with-runtime {:limits (:limits options) :timeout-ms (timeout/time-left)}
        (fn [runtime]
          (let [prepared (plan/prepare! runtime parsed inputs options)]
            (native/with-views (keep :native (:entries prepared))
              (fn [snapshots]
                (native/validate-types! snapshots (:entries prepared) (:types prepared) (:bindings prepared))
                (let [prepared (plan/acquire-and-plan! runtime prepared)
                      result (when (or (nil? explain-options) (:run? explain-options))
                               (execute! runtime prepared snapshots))]
                  (if explain-options
                    (assoc @(:explain runtime) :query-source? true :executed? (boolean (:run? explain-options)))
                    result))))))))))
