(ns datalevin.query.source.fake
  "Deterministic, v2-only mapped EAV provider for shared conformance checks.
  Eager test infrastructure, never a production adapter."
  (:require [datalevin.query.source :as spi]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.value :as value :refer [fail!]])
  (:import [datalevin.utl QueryKey]
           [java.io Closeable]))

(defn info
  "Describe a complete finite EAV view. Attribute declarations contain
  :entity-type and :value-type; callers may supply explicit schema guarantees."
  ([attributes] (info attributes {}))
  ([attributes options]
   (let [types (vec (distinct (map (comp :entity-type val) (sort-by (comp str key) attributes))))
         domains (zipmap types (map #(keyword "fixture.domain" (str %)) (range)))
         schema {:domains (into {} (map (fn [[t id]] [id {:entity-type t}])) domains)
                 :attributes (into {} (map (fn [[a t]]
                                             [a {:entity-domain (domains (:entity-type t))
                                                 :reference-domain nil :cardinality :many :absence :no-fact}])) attributes)}]
     (spi/validate-source-info!
       (merge {:spi-versions #{2} :features #{} :required-features #{}
               :source-instance-id (random-uuid) :compute-context-id :fixture
               :definition-id :fixture/eav :definition-fingerprint "fixture:eav:v2"
               :pattern-contract {:kind :eav :attributes attributes} :logical-schema schema
               :scope {:id "fixture:full" :kind :full :parameters {}}
               :completion {:domain :finite :extent :all-matches}
               :read-capabilities #{:snapshot :statement :best-effort}} options)))))

(defn unknown-estimate [dependent?]
  (merge (zipmap (if dependent?
                  [:startup-ns :per-binding-ns :rows-per-binding :row-bytes :requests-per-open
                   :requests-per-binding :transfer-bytes-per-binding :peak-retained-bytes]
                  [:startup-ns :run-ns :rows :row-bytes :requests-per-open :transfer-bytes :peak-retained-bytes])
                (repeat nil))
         {:confidence :unknown :provenance {:kind :unknown :id nil :observed-at-ms nil :valid-for-ms nil}}))

(defn row-info
  "A finite fixture mapping from row IDs to :row/value facts."
  ([] (row-info :string))
  ([type] (info {:row/value {:entity-type :long :value-type type}})))

(defn row-facts
  "Explicit test normalization of [id value] rows; absent values emit no fact."
  [rows]
  (into [] (keep (fn [[id v]] (when (and (some? id) (some? v)) [id :row/value v]))) rows))

(defn- active! [state ctl]
  (when (:closed? @state) (fail! :query-source/invalid :context {:state :closed}))
  (spi/check-active! ctl))

(defn- hook! [config phase request]
  (when-let [hook (get-in config [:hooks phase])] (hook request)))

(defn- reserve-value! [ctl x overhead]
  (let [bytes (volatile! overhead) maximum (:retained-bytes (control/limits ctl))
        observe! (fn [increment]
                   (spi/check-active! ctl)
                   (let [n (vswap! bytes value/add-charge increment)]
                     (when-not (value/at-most? n maximum)
                       (fail! :query-source/limit :reserve
                              {:category :retained-bytes :maximum maximum :attempted n}))))]
    (observe! 0)
    (value/value-charge x observe!)
    (spi/reserve! ctl {:retained-bytes @bytes})))

(defn- facts! [info facts]
  (when-not (vector? facts) (fail! :query-source/invalid :inputs))
  (doseq [fact facts]
    (when-not (and (vector? fact) (= 3 (count fact))) (fail! :query-source/invalid :inputs))
    (let [columns (contract/pattern-columns info {:terms [[:ignore] [:const (nth fact 1)] [:ignore]]})]
      ;; Missing values represent no fact. Validate the declared attribute even
      ;; when an entity/value is absent, then validate all present EAV values.
      (when (and (some? (first fact)) (some? (nth fact 2))) (value/rows! columns [fact]))))
  (value/owned-copy facts))

(defn- match-fact [bindings terms fact]
  (reduce (fn [bound [term x]]
            (when bound
              (case (first term)
                :ignore bound
                :const (when (QueryKey/equiv (second term) x) bound)
                :var (let [v (second term)]
                       (if (contains? bound v)
                         (when (if (contains? bindings v) (QueryKey/equals (bound v) x)
                                   (QueryKey/equiv (bound v) x)) bound)
                         (assoc bound v x)))))) bindings (map vector terms fact)))

(defn- columns [info clause]
  (reduce (fn [out [term column]]
            (if (and (= :var (first term)) (not-any? #(= (second term) (:variable %)) out))
              (conj out (assoc column :variable (second term))) out))
          [] (map vector (:terms clause) (contract/pattern-columns info clause))))

(defn- choices [info request config]
  (mapcat
    (fn [clause]
      (let [available (into {} (map (juxt :variable identity)) (:available-inputs request))
            cols (columns info clause)
            make (fn [kind position]
                   (let [[tag x] (when position (nth (:terms clause) position))
                         column (some #(when (= x (:variable %)) %) cols)
                         requires (if (and position (= :var tag) (not (contains? (:bindings request) x)))
                                    [column] [])]
                     (when (or (nil? position) (= :const tag) (contains? (:bindings request) x)
                               (and column (= column (available x))))
                       {:id (keyword (name kind) (str (:id clause))) :clause clause :columns cols
                        :requires requires :kind kind})))
            modes (case (get config :mode :scan)
                    :scan [[:attribute-enumeration nil]]
                    :lookup [[:entity-lookup 0]]
                    :value [[:value-lookup 2]]
                    :both [[:attribute-enumeration nil] [:value-lookup 2] [:entity-lookup 0]])]
        (keep (fn [[kind position]] (make kind position)) modes)))
    (filter #(= :pattern (:kind %)) (:clauses request))))

(defn- batch-bytes [columns rows ids check! maximum]
  (let [total (volatile! 0)
        observe! (fn [increment]
                   (check!)
                   (let [n (vswap! total value/add-charge increment)]
                     (when-not (value/at-most? n maximum)
                       (fail! :query-source/limit :batch
                              {:category :batch-bytes :maximum maximum :attempted n}))))]
    (if ids (value/dependent-batch-charge-bytes columns rows ids observe!)
        (value/batch-charge-bytes columns rows observe!))))

(defrecord Cursor [plan snapshot bindings request state position metrics reservation events config limiter]
  spi/IQuerySourceCursor
  (-next-batch! [_ {:keys [control max-rows max-bytes] :as fetch}]
    (active! state control)
    (when-not (identical? control (:control request)) (fail! :query-source/invalid :fetch))
    (hook! config :fetch fetch)
    (when (:page-requests? config)
      (with-open [_permit ^Closeable (spi/acquire-request! limiter)]
        (swap! events conj [:request :page])
        (swap! metrics update :external-requests inc)))
    (let [dependent? (boolean (seq (:requires plan)))
          inputs (when dependent? @bindings) facts @snapshot
          width (count (:columns plan)) scratch (volatile! [])
          check! #(active! state control)
          ;; Cursor position is published only after the owned handoff succeeds.
          next-match
          (if dependent?
            (fn [[binding-index fact-index]]
            (loop [b binding-index f fact-index]
              (check!)
              (cond
                (= b (count inputs)) nil
                (= f (count facts)) (recur (inc b) 0)
                :else
                (let [fact (nth facts f) input (nth inputs b)
                      bound (merge (:bindings plan) (zipmap (map :variable (:requires plan)) (:values input)))
                      match (when (and (some? (first fact)) (some? (nth fact 2)))
                              (match-fact bound (:terms (:clause plan)) fact))]
                  (swap! metrics update :read-rows inc)
                  (if match
                    {:position [b (inc f)] :row (mapv match (map :variable (:columns plan)))
                     :binding-id (:binding-id input)}
                    (recur b (inc f)))))))
            (fn [fact-index]
              (loop [f fact-index]
                (check!)
                (when (< f (count facts))
                  (let [fact (nth facts f)
                        match (when (and (some? (first fact)) (some? (nth fact 2)))
                                (match-fact (:bindings plan) (:terms (:clause plan)) fact))]
                    (swap! metrics update :read-rows inc)
                    (if match {:position (inc f) :row (mapv match (map :variable (:columns plan)))}
                        (recur (inc f))))))))]
      (try
        (let [{:keys [rows ids next-position exhausted?]}
              (loop [p @position rows [] ids (when dependent? [])]
                (if (= (count rows) max-rows)
                  {:rows rows :ids ids :next-position p :exhausted? false}
                  (if-let [{next-position :position :keys [row binding-id]} (next-match p)]
                    (let [token (spi/reserve! control {:retained-bytes (+ 256 (* 32 width))})
                          _ (vswap! scratch conj token)
                          candidate (conj rows row) candidate-ids (when dependent? (conj ids binding-id))
                          fits? (try (batch-bytes (:columns plan) candidate candidate-ids check! max-bytes) true
                                     (catch clojure.lang.ExceptionInfo e
                                       (if (and (seq rows) (= :batch-bytes (:category (ex-data e)))) false (throw e))))]
                      (if fits? (recur next-position candidate candidate-ids)
                          {:rows rows :ids ids :next-position p :exhausted? false}))
                    {:rows rows :ids ids :next-position (if dependent? [(count inputs) 0] (count facts))
                     :exhausted? true})))
              bytes (batch-bytes (:columns plan) rows ids check! max-bytes)
              token (when (pos? bytes) (spi/reserve! control {:retained-bytes bytes :in-flight-batches 1}))]
          (try
            (let [owned (value/owned-copy rows check!)
                  owned-ids (when dependent? (value/owned-copy ids check!))
                  batch (cond-> {:columns (:columns plan) :rows owned :row-count (count rows)
                                 :byte-count bytes :reservation token :exhausted? exhausted?}
                          dependent? (assoc :binding-ids owned-ids))]
              (check!)
              (reset! position next-position)
              (swap! metrics #(-> % (update :batches inc) (update :rows + (count rows)) (update :charged-bytes + bytes)))
              (swap! events conj [:batch (select-keys batch [:row-count :byte-count :exhausted? :binding-ids])])
              (if-let [transform (:batch config)] (transform batch) batch))
            (catch Throwable e (when token (spi/release! token)) (throw e))))
        (finally (doseq [token @scratch] (spi/release! token))))))
  (-cursor-metrics [_] @metrics)
  (-close-cursor! [_]
    (when-not (:closed? @state)
      (swap! state assoc :closed? true)
      (reset! snapshot nil)
      (when bindings (reset! bindings nil))
      (spi/release! reservation)
      (swap! events conj [:close-cursor])
      (hook! config :close-cursor request))
    nil))

(defrecord Session [owner info descriptor snapshot request state plans cursors reservations events config limiter]
  spi/IQuerySourceSession
  (-session-info [_] info)
  (-open-cursor [_ open]
    (active! state (:control request))
    (when-not (= (:definition-fingerprint descriptor) (:definition-fingerprint open))
      (fail! :query-source/stale-definition :open))
    (let [plan (get @plans (:plan open)) ctl (:control request)
          dependent? (boolean (seq (:requires plan)))]
      (when-not (and plan (= (:id plan) (:alternative-id open)) (= (:source-symbol plan) (:source-symbol open))
                    (= (select-keys request [:control :query-id :spi-version :features :authorization :consistency :limits])
                       (select-keys open [:control :query-id :spi-version :features :authorization :consistency :limits]))
                    (= (:region-id plan) (:region-id open)) (= (:scope-id plan) (:scope-id open))
                    (= (:definition-fingerprint descriptor) (:definition-fingerprint open)))
        (fail! :query-source/invalid :open))
      (if dependent?
        (spi/binding-charge-bytes (:requires plan) (:bindings open))
        (when (contains? open :bindings) (fail! :query-source/invalid :open)))
      (hook! config :open open)
      (let [reservation (if dependent? (reserve-value! ctl (:bindings open) 1024)
                            (spi/reserve! ctl {:retained-bytes 1024}))]
        (try
          (let [cursor (->Cursor plan (atom @snapshot) (when dependent? (atom (value/owned-copy (:bindings open))))
                                 request (atom {}) (atom (if dependent? [0 0] 0))
                                 (atom {:batches 0 :rows 0 :charged-bytes 0 :read-rows 0 :read-bytes nil
                                        :external-requests 0 :elapsed-ns nil}) reservation events config limiter)]
            (swap! cursors conj cursor)
            (swap! events conj [:open open])
            cursor)
          (catch Throwable e (spi/release! reservation) (throw e))))))
  (-cancel-session! [_ reason] (swap! events conj [:cancel reason]) nil)
  (-close-session! [_]
    (when-not (:closed? @state)
      (swap! state assoc :closed? true)
      (let [error (volatile! nil)
            attempt! (fn [f]
                       (try (f) (catch Throwable e
                                  (if-let [primary @error] (.addSuppressed ^Throwable primary e)
                                      (vreset! error e)))))]
        (doseq [cursor @cursors] (attempt! #(spi/-close-cursor! cursor)))
        (reset! cursors []) (reset! plans {}) (reset! snapshot nil)
        (doseq [reservation @reservations] (spi/release! reservation))
        (reset! reservations [])
        (attempt! #(.close ^Closeable limiter))
        (swap! events conj [:close-session])
        (attempt! #(hook! config :close-session request))
        (when-let [error @error] (throw error))))
    nil))

(defrecord EAVSource [descriptor data config events]
  spi/IQuerySource
  (-source-info [_] (swap! events conj [:descriptor]) descriptor)
  (-open-session [this request]
    (let [ctl (:control request) {:keys [facts version]} @data
          actual (get config :actual-consistency (:consistency request))
          reservation (reserve-value! ctl facts 2048)
          constraints {:max-concurrent-requests 1 :rate-limit nil}
          limiter (try (spi/request-limiter ctl constraints)
                       (catch Throwable e (spi/release! reservation) (throw e)))]
      (try
        (hook! config :acquire request)
        (when (:setup-request? config)
          (with-open [_permit ^Closeable (spi/acquire-request! limiter)] (swap! events conj [:request :setup])))
        (let [info (merge (select-keys descriptor [:source-instance-id :compute-context-id])
                          {:spi-version 2 :features #{} :session-id (random-uuid)
                           :authorization-token :fixture/authorized :consistency actual :acquired-at-ms 0
                           :data-version (when (= :snapshot actual) version)
                           :consumption (get config :consumption {:opens :many :max-open-cursors 1})
                           :request-constraints constraints})]
          (swap! events conj [:acquire request])
          (->Session this info descriptor (atom facts) request (atom {}) (atom {}) (atom [])
                     (atom [reservation]) events config limiter))
        (catch Throwable e (spi/release! reservation) (.close ^Closeable limiter) (throw e)))))
  (-plan-source [this session request]
    (active! (:state session) (:control request))
    (when-not (identical? this (:owner session)) (fail! :query-source/invalid :planning))
    (when-not (and (= 2 (:spi-version request))
                   (= descriptor (:source-descriptor request))
                   (identical? (:control (:request session)) (:control request))
                   (= (:query-id (:request session)) (:query-id request))
                   (= (get-in descriptor [:scope :id]) (:scope-id request)))
      (fail! :query-source/invalid :planning))
    (hook! config :plan request)
    (swap! events conj [:plan request])
    (let [plans (vec (choices descriptor request config))]
      {:alternatives
       (mapv (fn [{:keys [id clause columns requires kind] :as plan}]
               (let [handle (Object.)
                     plan (merge plan (select-keys request [:source-symbol :region-id :scope-id :bindings]))
                     reservation (reserve-value! (:control request) plan 1024)]
                 (swap! (:reservations session) conj reservation)
                 (swap! (:plans session) assoc handle plan)
                 {:id id :plan handle :covers #{(:id clause)} :prefilters #{} :requires requires :columns columns
                  :scope-id (:scope-id request) :completion :all-bindings :access {:kind kind :anchor-clause (:id clause)}
                  :properties {:replay (get config :replay :identical) :ordering [] :blocking-state :none}
                  :constraints (get config :constraints {:max-binding-rows 256 :max-binding-bytes 262144
                                                         :max-request-binding-rows 32})
                  :estimate (get config :estimate (unknown-estimate (boolean (seq requires))))
                  :explain {:label "Mapped EAV fixture"}})) plans)})))

(defn source
  "Create a v2-only eager fixture. :mode selects :scan, :lookup, :value or :both;
  hooks and envelope transforms are test-only fault injection."
  ([info facts] (source info facts {}))
  ([info facts config]
   (spi/validate-source-info! info)
   (->EAVSource info (atom {:facts (facts! info facts) :version (random-uuid)}) config (atom []))))

(defn replace-facts! [source facts]
  (reset! (:data source) {:facts (facts! (:descriptor source) facts) :version (random-uuid)})
  nil)
