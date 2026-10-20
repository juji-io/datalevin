(ns ^:no-doc datalevin.query.source.diagnostics
  "Invocation-owned v2 explanations. Retain only allowlisted structural data;
  diagnostic storage belongs to the invocation, including across rule rounds."
  (:require [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]))

(def metric-keys
  [:batches :rows :charged-bytes :read-rows :read-bytes :external-requests :elapsed-ns])

(defn start!
  "Create the explanation owner outside temporary execution workspaces."
  [runtime intermediate?]
  (let [token (control/reserve! (:control runtime) {:retained-bytes 4096})
        tokens (volatile! [token])
        state (atom {:next-program 0 :sources [] :regions [] :cursors [] :actual {}})
        closed? (atom false)]
    (rt/own! runtime #(when (compare-and-set! closed? false true)
                       (reset! state nil)
                       (doseq [token @tokens] (control/release! token))
                       (vreset! tokens [])))
    {:state state :tokens tokens :intermediate? intermediate? :closed? closed?}))

(defn- active! [runtime]
  (control/check-active! (:control runtime))
  (when (some-> runtime :diagnostics :closed? deref)
    (fail! :query-source/invalid :explain)))

(defn- own! [runtime x]
  (active! runtime)
  (let [token (rt/charge-copy! runtime x)]
    (vswap! (get-in runtime [:diagnostics :tokens]) conj token)
    (value/diagnostic-copy x #(active! runtime))))

(defn- reserve! [runtime bytes]
  (active! runtime)
  (vswap! (get-in runtime [:diagnostics :tokens]) conj
    (control/reserve! (:control runtime) {:retained-bytes bytes})))

(defn program!
  "Allocate a query-local ID so separately compiled native bodies stay distinct."
  [runtime]
  (when-let [diagnostics (:diagnostics runtime)]
    (active! runtime)
    (let [id (:next-program @(:state diagnostics))]
      (when (= id Long/MAX_VALUE) (fail! :query-source/limit :explain))
      (swap! (:state diagnostics) update :next-program inc)
      id)))

(defn sources!
  "Retain copied, allowlisted session descriptions."
  [runtime sources]
  (when-let [diagnostics (:diagnostics runtime)]
    (let [sources (own! runtime sources)]
      (swap! (:state diagnostics) update :sources into sources))))

(defn region!
  "Retain one selected region and its shared operator placement."
  [runtime region]
  (when-let [diagnostics (:diagnostics runtime)]
    (let [region (own! runtime region)]
      (swap! (:state diagnostics) update :regions conj region))))

(defn- add-count [a b]
  (when-not (value/at-most? b (- Long/MAX_VALUE (long (or a 0))))
    (fail! :query-source/limit :metrics))
  (+ (long (or a 0)) (long b)))

(defn operator!
  "Accumulate optional inclusive step counts in constant retained space."
  [runtime program-id region-id operator-id input-rows output-rows elapsed-ns]
  (when-let [diagnostics (:diagnostics runtime)]
    (when (:intermediate? diagnostics)
      (active! runtime)
      (let [key [program-id region-id operator-id]
            state (:state diagnostics)]
        (when-not (contains? (:actual @state) key) (reserve! runtime 2048))
        (swap! state update-in [:actual key]
          (fn [before]
            (-> before
                (update :calls add-count 1)
                (update :input-rows add-count input-rows)
                (update :output-rows add-count output-rows)
                (update :elapsed-ns add-count elapsed-ns))))))))

(defn cursor!
  "Admit one actual open's structural identity and optional input count."
  [runtime request dependent?]
  (when-let [diagnostics (:diagnostics runtime)]
    (active! runtime)
    (let [_ (reserve! runtime 1024)
          id (count (:cursors @(:state diagnostics)))
          entry (own! runtime
                  (cond-> (assoc (select-keys request [:source-symbol :region-id :alternative-id :scope-id])
                            :cursor-id id :dependent? dependent?
                            :batch-limits (select-keys (:limits request) [:batch-rows :batch-bytes]))
                    (and dependent? (:intermediate? diagnostics))
                    (assoc :input-rows (count (:bindings request)))))]
      (swap! (:state diagnostics) update :cursors conj entry)
      id)))

(defn batch!
  "Count validated batches separately from provider-reported metrics."
  [runtime cursor-id rows bytes]
  (when-let [diagnostics (:diagnostics runtime)]
    (when (:intermediate? diagnostics)
      (active! runtime)
      (swap! (:state diagnostics) update-in [:cursors cursor-id :actual]
        (fn [before]
          (-> before (update :batches add-count 1)
              (update :rows add-count rows) (update :charged-bytes add-count bytes)))))))

(defn metrics!
  "Validate and copy the known cursor counters, retaining unknown values as nil."
  [runtime cursor-id metrics]
  (when-let [diagnostics (:diagnostics runtime)]
    (active! runtime)
    (let [token (rt/charge-metadata! runtime metrics :metrics)]
      (try
        (when-not (and (value/plain-map? metrics)
                       (every? #(contains? metrics %) metric-keys)
                       (every? #(or (some #{%} metric-keys) (qualified-keyword? %)) (keys metrics))
                       (every? #(or (nil? %) (and (integer? %) (<= 0 % Long/MAX_VALUE)))
                               (map metrics metric-keys)))
          (fail! :query-source/invalid :metrics))
        (let [metrics (own! runtime (select-keys metrics metric-keys))]
          (swap! (:state diagnostics) assoc-in [:cursors cursor-id :metrics] metrics))
        (finally (control/release! token))))))

(defn finish!
  "Own the final explanation before its compilation and diagnostic owner close."
  [runtime executed? final-rows operators]
  (active! runtime)
  (let [diagnostics (:diagnostics runtime)
        {:keys [sources regions cursors actual]} @(:state diagnostics)
        _ (reserve! runtime
            (value/add-charge 2048
              (value/multiply-charge 1024
                (reduce (fn [^long n region] (+ n (count (:operators region))))
                        (count regions) regions))))
        regions (mapv (fn [region]
                        (update region :operators
                          (fn [operators]
                            (mapv (fn [operator]
                                    (if-let [counts (get actual [(:program-id region) (:region-id region)
                                                               (:operator-id operator)])]
                                      (assoc operator :actual counts) operator)) operators)))) regions)]
    (own! runtime
      (cond-> {:query-source? true :spi-version 2 :executed? executed?
               :sources sources :regions regions :cursors cursors :operators operators
               :intermediate-counts? (:intermediate? diagnostics)}
        executed? (assoc :result-rows final-rows)))))
