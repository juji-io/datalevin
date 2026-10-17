(ns ^:no-doc datalevin.query.source.execute
  "Bounded embedded execution of positive query-source conjunctions."
  (:require [datalevin.built-ins :as built-ins]
            [datalevin.parser :as dp]
            [datalevin.query.aggregate :as aggregate]
            [datalevin.query.source :as source]
            [datalevin.query.source.arithmetic :as arithmetic]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.inputs :as input]
            [datalevin.query.source.join :as join]
            [datalevin.query.source.native :as native]
            [datalevin.query.source.plan :as plan]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.spill :as spill]
            [datalevin.timeout :as timeout])
  (:import [datalevin.db DB]
           [datalevin.parser FindRel FindColl FindScalar FindTuple]
           [datalevin.utl QueryKey QueryKey$KeySet]))

(defn- match-row [bindings terms row]
  (reduce (fn [bound [term x]]
            (when bound
              (case (first term)
                :ignore bound
                :const (when (QueryKey/equiv (second term) x) bound)
                :var (let [sym (second term)]
                       (if (contains? bound sym)
                         (when (if (contains? bindings sym)
                                 (QueryKey/equals (bound sym) x)
                                 (QueryKey/equiv (bound sym) x)) bound)
                         (assoc bound sym x)))))) bindings (map vector terms row)))

(defn- merge-row [left right]
  ;; Match the existing hash join's Java key equality. Predicates and projection
  ;; retain their existing Clojure semantics; neither uses provider collation.
  (when (every? (fn [[k v]] (or (not (contains? left k)) (QueryKey/equals (get left k) v))) right)
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
    (rt/with-context runtime
      {:source-symbol symbol :alternative-id (:id alternative)
       :clause-ids (vec (sort (into (:covers alternative) (:prefilters alternative))))}
      (fn [runtime]
        (let [control (:control runtime)
              check! #(source/check-active! control)
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
                              (run! #(value/execution-value! % check!) row)
                              (when (:intermediate-counts? runtime)
                                (swap! (:explain runtime) update :streamed-rows inc))
                              (let [scratch (rt/charge! runtime row
                                                       (+ 512 (* 256 (count columns))) 1)]
                                (try (emit (zipmap (map :variable columns) row))
                                     (finally (source/release! scratch))))))
                    (finally (when-let [token (:reservation batch)] (source/release! token))))]
              (when-not (or (:exhausted? batch) (reduced? stopped)) (recur))))
          (rt/invoke runtime :close close)
          (swap! (:explain runtime) update :cursors (fnil conj [])
                 {:source symbol :metrics (rt/invoke runtime :metrics #(metrics cursor))}))))))

(defn- native-reader [runtime snapshots bindings input-data {:keys [native patterns symbol]}]
  (mapv (fn [pattern]
          (let [terms (:terms pattern)
                [entity-tag entity] (first terms)
                [value-tag term-value] (nth terms 2)
                entity-id (case entity-tag
                            :const entity
                            :var (get bindings entity)
                            nil)
                relation (when (= :var entity-tag)
                           ;; The parser requires distinct :in variables, so at
                           ;; most one relational input can bind this entity.
                           (some #(when (contains? (:variables %) entity) %) (:relations input-data)))
                bound-value (case value-tag
                              :const term-value
                              :var (get bindings term-value)
                              nil)
                keys-only? (= :ignore value-tag)
                terms (if keys-only? (pop terms) terms)]
            {:entity-variable (when (and (= :var entity-tag)
                                         (not (contains? bindings entity))
                                         (nil? relation)
                                         (not= :const value-tag)
                                         (not (and (= :var value-tag)
                                                   (contains? bindings term-value)))) entity)
             :variables (into #{} (keep #(when (= :var (first %)) (second %))) terms)
             :read
             (fn [tables emit]
               (rt/with-context runtime {:source-symbol symbol :clause-ids [(:id pattern)]}
                 (fn [runtime]
                   (let [control (:control runtime)
                         rows (or (:rows relation)
                                  (when (and (= :var entity-tag)
                                             (not (contains? bindings entity)))
                                    (some #(when (contains? (first %) entity) %) tables)))
                         scan (fn [entity-id]
                                (source/check-active! control)
                                (swap! (:explain runtime) update :native-scans inc)
                                (let [snapshot (get snapshots (.-store ^DB native))
                                      attribute (second (second terms))
                                      visit (fn [row]
                                              (when-let [binding (match-row bindings terms row)]
                                                (emit binding)))]
                                  (if keys-only?
                                    (native/scan-keys! control snapshot attribute entity-id visit)
                                    (native/scan! control snapshot attribute entity-id bound-value visit))))]
                     (if rows
                       (input/with-entity-ids control rows entity #(visit-rows % scan))
                       (scan entity-id))))))})) patterns))

(defn- readers [runtime snapshots {:keys [entries fragments bindings input-data]}]
  (let [native (vec (mapcat #(native-reader runtime snapshots bindings input-data %)
                           (filter :native entries)))
        source-variables (into #{} (mapcat #(map :variable (:columns (:alternative %)))) fragments)
        [early deferred]
        (loop [pending native, available source-variables, deferred []]
          (if-let [reader (some #(when (contains? available (:entity-variable %)) %) pending)]
            (recur (filterv #(not (identical? reader %)) pending)
                   (into available (:variables reader)) (conj deferred reader))
            [pending deferred]))
        sources (mapv (fn [fragment]
                        (let [read (source-reader runtime fragment)]
                          (fn [_ emit] (read emit)))) fragments)]
    ;; Explicit entity inputs and constant/scalar value constraints keep their
    ;; early native reads, including empty-pattern short circuits. Otherwise,
    ;; build an external relation that binds the entity before native values.
    ;; Native readers borrow IDs from retained tables without replacing their
    ;; original rows, so correlation and duplicate semantics remain intact.
    (into (into (mapv (fn [{:keys [read]}] (fn [_ emit] (read [] emit))) early) sources)
          (map :read deferred))))

(defn- result-shape [parsed rows]
  (let [rows (if-let [return (:qreturn-map parsed)]
               (mapv #(zipmap (:symbols return) %) rows) rows)
        find (:qfind parsed)]
    (cond (instance? FindRel find)
          (cond (:qreturn-map parsed)
                (spill/new-spillable-set rows {:spill-threshold Long/MAX_VALUE
                                              :initial-capacity (count rows)})
                (or (seq (:qwith parsed)) (some dp/aggregate? (dp/find-elements find))) rows
                :else (spill/new-spillable-set rows {:spill-threshold Long/MAX_VALUE
                                                     :initial-capacity (count rows)}))
          (instance? FindColl find) (mapv first rows)
          (instance? FindScalar find) (ffirst rows)
          (instance? FindTuple find) (first rows))))

(defn- collect-result [runtime {:keys [parsed limit offset]}]
  (let [limit (if (or (instance? FindScalar (:qfind parsed))
                      (instance? FindTuple (:qfind parsed))) 1 limit)
        offset (long offset)
        find-vars (vec (dp/find-vars (:qfind parsed)))
        vars (into find-vars (map :symbol) (:qwith parsed))
        elements (dp/find-elements (:qfind parsed))
        aggregate? (some dp/aggregate? elements)
        seen (QueryKey$KeySet.)
        output (volatile! [])
        consumed (volatile! 0)
        stopped (volatile! false)
        save! (fn [row]
                ;; The result quota is an error boundary, never a LIMIT.
                (rt/charge-result! runtime row)
                (vswap! output conj row))
        window! (fn [row]
                  (let [n (long (vswap! consumed #(inc (long %))))]
                    (when (> n offset) (save! row))
                    ;; Subtract the validated offset to avoid overflowing its
                    ;; sum with LIMIT, which can exceed Long/MAX_VALUE.
                    (when (and limit (>= (- n offset) (long limit)))
                      (vreset! stopped true)
                      (reduced ::stop))))]
    {:accept (fn [binding]
               (let [row (mapv binding vars)]
                 (when-not (.contains seen row)
                   ;; Also covers the aggregate's temporary grouping lists and
                   ;; projected tuple containers. Input state is quota-bound.
                   (rt/charge! runtime row 512 3)
                   (.add seen row)
                   (when-not aggregate? (window! (mapv row (range (count find-vars))))))))
     :stopped? #(deref stopped)
     :finish (fn []
               (when aggregate?
                 (reduce (fn [_ row]
                           (source/check-active! (:control runtime))
                           (window! (vec row))) nil
                         (aggregate/aggregate elements {}
                           (map #(subvec % 0 (count find-vars)) seen)
                           #(source/check-active! (:control runtime))
                           #(arithmetic/wrap-aggregate runtime %1 %2))))
               (let [result (result-shape parsed @output)]
                 (swap! (:explain runtime) assoc :result-rows (count @output))
                 result))}))

(defn- execute! [runtime {:keys [bindings input-data predicates] :as prepared} snapshots]
  (let [readers (readers runtime snapshots prepared)
        readers (if (seq readers) readers [(fn [_ emit] (emit bindings))])
        input-tables (mapv :rows (:relations input-data))
        ;; An empty required relation proves the conjunction empty. Reduce
        ;; incrementally so later native scans and source cursors never open.
        tables (when (every? seq input-tables)
                 (reduce (fn [tables read]
                           (let [rows (volatile! [])]
                             (read tables (fn [row] (rt/charge! runtime row) (vswap! rows conj row) nil))
                             (if (seq @rows) (conj tables @rows) (reduced nil))))
                         input-tables (butlast readers)))
        {:keys [accept finish stopped?]} (collect-result runtime prepared)
        steps (volatile! nil)]
    (letfn [(join! [row remaining]
              (source/check-active! (:control runtime))
              (if (stopped?) (reduced ::stop)
                  (if-let [step (first remaining)]
                    (visit-rows (join/candidates step row) (fn [right]
                              (when-let [joined (merge-row row right)]
                                (let [token (rt/charge! runtime joined)]
                                  (try (join! joined (next remaining))
                                       (finally (source/release! token)))))))
                    (when (predicates? predicates row) (accept row)))))]
      (when (and tables (not (stopped?)))
        ((last readers) tables (fn [row]
                          (when-let [row (merge-row bindings row)]
                            (when-not @steps
                              (vreset! steps (if (seq tables)
                                               (join/prepare! runtime row tables) [])))
                            (join! row @steps)))))
      (finish))))

(defn- explain-options! [options]
  (when (some? options)
    (when-not (map? options) (fail! :query-source/invalid :explain))
    (when-not (every? #{:run? :intermediate-counts?} (keys options))
      (fail! :query-source/unsupported :explain))))

(defn run
  "Execute or explain without consulting native query/result caches."
  [query inputs explain-options]
  (let [raw (if (map? query) query (dp/query->map query))
        parsed (try (dp/parse-query query)
                    (catch Throwable _ (fail! :query-source/invalid :syntax)))
        options (:query-source raw)
        deadline (timeout/effective-deadline (:qtimeout parsed))]
    (binding [timeout/*deadline* deadline]
      (rt/with-runtime {:limits (:limits options) :timeout-ms (timeout/time-left)
                        :intermediate-counts? (boolean
                                               (and (:run? explain-options)
                                                    (get explain-options :intermediate-counts? true)))}
        (fn [runtime]
          (explain-options! explain-options)
          (let [prepared (plan/prepare! runtime parsed inputs options)]
            (native/with-views (keep :native (:entries prepared))
              (fn [snapshots]
                (native/validate-types! runtime snapshots (:entries prepared)
                                        (:types prepared) (:input-data prepared))
                (let [prepared (plan/acquire-and-plan! runtime prepared)
                      result (when (or (nil? explain-options) (:run? explain-options))
                               (execute! runtime prepared snapshots))]
                  (if explain-options
                    (assoc @(:explain runtime) :query-source? true :executed? (boolean (:run? explain-options)))
                    result))))))))))
