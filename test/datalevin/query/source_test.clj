(ns datalevin.query.source-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.query.source :as source]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.fake :as fake]
            [datalevin.query.source.harness :as harness]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value]))

(def tuple-info
  {:spi-versions #{1} :source-instance-id :fixture :compute-context-id :local
   :definition-id :pairs :definition-fingerprint "fake-definition:v1:pairs-long-string"
   :features #{} :required-features #{}
   :pattern-contract {:kind :tuple :columns [{:type :long :nullable? false}
                                            {:type :string :nullable? true}]}})

(def patterns [{:id 0 :kind :pattern :terms [[:var '?id] [:var '?name]]}])
(defn error-data [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))
(defn category [f] (:error (error-data f)))

(deftest nested-type-validation-preserves-cancellation
  (doseq [matches? [true false]
          category [:query-source/cancelled :query/timeout]]
    (let [ctl (control/create)
          descriptor {:kind :custom :name :test/checked :version 1 :fingerprint "v1"}
          visited (atom [])
          head (reify value/IQuerySourceValue
                 (-logical-type [_]
                   (swap! visited conj :head)
                   (control/cancel! ctl {:error category :phase :execution})
                   (if matches? descriptor (assoc descriptor :fingerprint "v2")))
                 (-retained-bytes [_] 64))
          tail (reify value/IQuerySourceValue
                 (-logical-type [_] (swap! visited conj :tail) descriptor)
                 (-retained-bytes [_] 64))
          type {:kind :tuple :elements [{:kind :tuple :elements [descriptor]} descriptor]}]
      (try
        (is (= category (:error (error-data #(value/matches-type? type [[head] tail]
                                                                (fn [] (source/check-active! ctl)))))))
        (is (= [:head] @visited))
        (finally (control/close! ctl))))))

(deftest retained-value-preflight-preserves-charges-and-rejects-overflow
  (let [ctl (control/create {:limits {:retained-bytes 32768}})
        x (with-meta [{:date (java.util.Date. 1)} '(1 2)] {:bytes (byte-array 16)})]
    (try
      (let [token (rt/charge! {:control ctl} x)]
        (is (= (+ 256 (value/value-charge x)) (:retained-bytes (control/usage ctl))))
        (source/release! token)
        (is (every? zero? (vals (control/usage ctl)))))
      (finally (control/close! ctl))))
  (let [ctl (control/create {:limits {:retained-bytes Long/MAX_VALUE}})
        huge (reify value/IQuerySourceValue
               (-logical-type [_] {:kind :custom :name :test/huge :version 1
                                   :fingerprint "huge:v1" :parameters {}})
               (-retained-bytes [_] Long/MAX_VALUE))]
    (try
      (let [error (error-data #(rt/charge! {:control ctl} huge))]
        (is (= :query-source/limit (:error error)))
        (is (= :retained-bytes (:category error)))
        (is (> (:attempted error) Long/MAX_VALUE))
        (is (every? zero? (vals (control/usage ctl)))))
      (finally (control/close! ctl)))))

(deftest immutable-time-values-use-the-data-contract
  (doseq [x [(java.time.Instant/ofEpochSecond 1 123456789)
             (java.time.Duration/ofSeconds -1 123456789)
             (java.time.Period/of 1 -2 3)]]
    (is (value/matches-type? :data x))
    (is (identical? x (value/owned-copy x)))
    (is (not (value/matches-type? :instant x)))
    ;; Descriptor/session metadata still uses the immutable EDN subset.
    (is (false? (value/portable? x))))
  (is (value/matches-type? :instant (java.util.Date. 1))))

(deftest portable-envelopes-include-clojure-metadata
  (doseq [x ['id [] {} #{} '() (sorted-map) (sorted-set)]
          mutable [(java.util.Date.) (byte-array [1]) (atom :value) (Object.)]]
    (is (false? (value/portable? (with-meta x {:hidden mutable}))))
    (is (false? (value/portable? {:nested [(with-meta x {:hidden mutable})]}))))
  (let [metadata {:source (with-meta 'declared {:revision [1 :stable]})}
        descriptor (with-meta tuple-info metadata)]
    (is (true? (value/portable? descriptor)))
    (is (identical? descriptor (source/validate-source-info! descriptor))))
  (is (= :query-source/invalid
         (category #(source/validate-source-info!
                      (with-meta tuple-info {:hidden (atom :mutable)}))))))

(deftest descriptor-contract
  (is (= tuple-info (source/validate-source-info! tuple-info)))
  (is (= {:spi-version 1 :features #{}}
         (contract/negotiate (assoc tuple-info :features #{:unknown/optional}))))
  (is (= :query-source/version
         (category #(contract/negotiate (assoc tuple-info :spi-versions #{2})))))
  (is (= :query-source/unsupported
         (category #(contract/negotiate
                      (assoc tuple-info :features #{:unknown/required}
                             :required-features #{:unknown/required})))))
  (doseq [bad [(dissoc tuple-info :definition-fingerprint)
               (assoc tuple-info :definition-fingerprint "")
               (assoc tuple-info :runtime (Object.))
               (assoc-in tuple-info [:pattern-contract :columns 0 :type] :int)
               (assoc-in tuple-info [:pattern-contract :columns 0 :nullable?] :yes)
               (assoc tuple-info :pattern-contract {:kind :eav :attributes {}})
               (assoc tuple-info :pattern-contract {:kind :eav :attributes []})
               (assoc tuple-info :pattern-contract {:kind :eav :attributes {"bad" {}}})]]
    (is (= :query-source/invalid (category #(source/validate-source-info! bad)))))
  (let [custom {:kind :custom :name :test/ref :version 1 :fingerprint "type:v1:test"
                :parameters {:domain [:physical :order 0] :key-types [:long :string]}}
        info (assoc tuple-info :pattern-contract
                    {:kind :eav :attributes {:order/id {:entity-type custom
                                                       :value-type {:kind :tuple :elements [:long :string]}}
                                             :order/customer {:entity-type custom
                                                              :value-type (assoc-in custom [:parameters :domain]
                                                                                    [:physical :customer 0])}}})]
    (is (= info (source/validate-source-info! info)))
    (doseq [attribute [[:var '?a] [:ignore] [:const :unknown]]]
      (is (= :query-source/unsupported
             (category #(contract/clauses! info [{:id 0 :kind :pattern
                                                 :terms [[:var '?e] attribute [:ignore]]}])))))))

(deftest f0-cursor-open-uses-only-common-context-and-selection
  (let [inner (fake/source tuple-info [[1 "Ada"] [2 "Bea"]])
        acquired (atom nil)
        planned (atom nil)
        opened (atom nil)
        delegate (atom nil)
        provider
        (reify source/IQuerySource
          (-source-info [_] tuple-info)
          (-open-session [_ request]
            (reset! acquired request)
            (let [session (source/-open-session inner request)]
              (reset! delegate session)
              (reify source/IQuerySourceSession
                (-session-info [_] (source/-session-info session))
                (-open-cursor [_ request]
                  (reset! opened request)
                  (source/-open-cursor session request))
                (-cancel-session! [_ reason] (source/-cancel-session! session reason))
                (-close-session! [_] (source/-close-session! session)))))
          (-plan-source [_ _ request]
            (let [response (source/-plan-source inner @delegate request)]
              (reset! planned {:request request :alternative (first (:alternatives response))})
              response)))
        result (harness/run! provider patterns {:bindings {'?name "Ada"}})
        common [:spi-version :features :query-id :authorization :consistency :control :limits]]
    (is (= [[1 "Ada"]] (:rows result)))
    (is (= (into (set common) [:alternative-id :plan :source-symbol :definition-fingerprint])
           (set (keys @opened))))
    (is (= (select-keys @acquired common) (select-keys @opened common)))
    (is (identical? (:control @acquired) (:control @opened)))
    (is (= '$test (:source-symbol @opened)))
    (is (= (:definition-fingerprint tuple-info) (:definition-fingerprint @opened)))
    (is (= (get-in @planned [:alternative :id]) (:alternative-id @opened)))
    (is (identical? (get-in @planned [:alternative :plan]) (:plan @opened)))
    (is (= {'?name "Ada"} (get-in @planned [:request :bindings])))
    (is (= :close-session (last @(:events inner))))))

(deftest owned-batches-snapshots-and-duplicates
  (let [input [[1 "Ada"] [2 nil] [3 "Ada"] [3 "Ada"]]
        fake (fake/source tuple-info input)
        retained (atom [])
        result (harness/run! fake patterns
                             {:limits {:batch-rows 1}
                              :consume #(do (swap! retained conj (:rows %))
                                            (reset! (:rows fake) [[99 "new"]]))})]
    (is (= input (:rows result)))
    (is (= input (vec (mapcat identity @retained))))
    (is (= 4 (get-in result [:metrics :batches])))
    (is (= [:open-session :plan :open-cursor :fetch :fetch :fetch :fetch
            :close-cursor :close-session] @(:events fake)))
    (is (= [[99 "new"]] (:rows (harness/run! fake patterns)))))
  (is (= [] (:rows (harness/run! (fake/source tuple-info []) patterns)))))

(deftest malformed-constant-is-rejected-before-acquisition
  (let [fake (fake/source tuple-info [[1 "a"]])]
    (is (= :query-source/invalid
           (category #(harness/run! fake [{:id 0 :kind :pattern
                                          :terms [[:const "wrong-type"] [:ignore]]}]))))
    (is (empty? @(:events fake)))))

(deftest eav-missing-is-absent-including-wildcards
  (let [info (assoc tuple-info :pattern-contract
                    {:kind :eav :attributes {:name {:entity-type :long :value-type :string}}})
        fake (fake/source info [[1 :name "Ada"] [2 :name nil] [3 :name "Grace"]])]
    (doseq [value [[:var '?name] [:ignore]]]
      (is (= #{1 3}
             (set (map first (:rows (harness/run! fake [{:id 0 :kind :pattern
                                                       :terms [[:var '?id] [:const :name] value]}])))))))
    (reset! (:events fake) [])
    (is (= :query-source/unsupported
           (category #(harness/run! fake [{:id 0 :kind :pattern
                                          :terms [[:var '?id] [:const :missing] [:ignore]]}]))))
    (is (empty? @(:events fake)))))

(deftest scalar-predicates-and-repeated-variables
  (let [info (assoc-in tuple-info [:pattern-contract :columns 1] {:type :long :nullable? false})
        fake (fake/source info [[1 1] [1 2] [2 2] [3 3]])]
    (is (= [[2] [3]] (:rows (harness/run!
                             fake [{:id 0 :kind :pattern :terms [[:var '?n] [:var '?n]]}
                                   {:id 1 :kind :predicate :operator :> :semantics-version 1
                                    :args [[:var '?n] [:const 1]]}]))))))

(deftest resource-reservations-are-atomic-query-scoped-and-transfer-once
  (let [c (control/create {:limits {:retained-bytes 10 :external-requests 2}})
        other (control/create)
        token (source/reserve! c {:retained-bytes 10 :external-requests 1 :in-flight-batches 1})]
    (try
      (is (= :query-source/limit (category #(source/reserve! c {:retained-bytes 1 :external-requests 1}))))
      (is (= 1 (:external-requests (control/usage c))))
      (is (= :query-source/invalid (category #(control/accept! other token 10))))
      (is (nil? (control/accept! c token 10)))
      (is (= :query-source/invalid (category #(control/accept! c token 10))))
      (source/release! token)
      (source/release! token)
      (is (= 0 (:retained-bytes (control/usage c))))
      (is (= 1 (:external-requests (control/usage c))))
      (is (= :query-source/invalid (category #(control/accept! c token 0))))
      (finally (control/close! c) (control/close! other)))))

(deftest cancellation-signals-registration-races-and-timeouts
  (let [c (control/create) before (promise) after (promise) removed (atom false)
        registration (source/register-cancel! c #(reset! removed %))]
    (source/unregister-cancel! registration)
    (source/register-cancel! c #(deliver before %))
    (control/cancel! c {:error :query-source/cancelled :phase :fetch :secret "hidden"})
    (source/register-cancel! c #(deliver after %))
    (is (= {:error :query-source/cancelled :phase :fetch} (deref before 2000 :timeout)))
    (is (= @before (deref after 2000 :timeout)))
    (is (false? @removed))
    (is (source/cancelled? c))
    (is (= :query-source/cancelled (category #(source/check-active! c))))
    (control/close! c))
  (let [c (control/create {:timeout-ms 0})]
    (is (zero? (source/remaining-time-ms c)))
    (is (= :query/timeout (category #(source/check-active! c))))
    (control/close! c)))

(deftest resource-limits-and-deadlines-preserve-large-integers
  (let [c (control/create {:limits {:retained-bytes Long/MAX_VALUE}
                          :timeout-ms Long/MAX_VALUE})
        token (source/reserve! c {:retained-bytes (dec Long/MAX_VALUE) :in-flight-batches 1})]
    (try
      (is (nil? (source/check-active! c)))
      (let [error (error-data #(source/reserve! c {:retained-bytes 2}))]
        (is (= :query-source/limit (:error error)))
        (is (= (inc' Long/MAX_VALUE) (:attempted error))))
      (is (= (dec Long/MAX_VALUE) (:retained-bytes (control/usage c))))
      (is (= :query-source/invalid
             (category #(control/accept! c token (inc' Long/MAX_VALUE)))))
      (is (= :query-source/invalid
             (category #(control/accept! c token Double/NaN))))
      (is (nil? (control/accept! c token (biginteger (dec Long/MAX_VALUE)))))
      (source/release! token)
      (is (zero? (:retained-bytes (control/usage c))))
      (finally (control/close! c)))))

(deftest concurrent-reservations-and-cancellation-registration
  (let [c (control/create {:limits {:retained-bytes 16}})
        start (promise)
        workers (mapv (fn [_] (future @start
                                     (try (source/reserve! c {:retained-bytes 8})
                                          (catch clojure.lang.ExceptionInfo _ nil)))) (range 8))]
    (deliver start true)
    (let [tokens (keep deref workers)]
      (is (= 2 (count tokens)))
      (is (= 16 (:retained-bytes (control/usage c))))
      (run! source/release! tokens)
      (is (zero? (:retained-bytes (control/usage c)))))
    (control/close! c))
  (dotimes [_ 20]
    (let [c (control/create) start (promise) signal (promise)
          registration (future @start (source/register-cancel! c #(deliver signal %)))
          cancellation (future @start (control/cancel! c {:error :query-source/cancelled :phase :test}))]
      (deliver start true)
      @registration
      @cancellation
      (is (= :query-source/cancelled (:error (deref signal 2000 {}))))
      (control/close! c))))

(deftest owned-byte-values-survive-advance-and-close
  (let [bytes (byte-array [1 2 3])
        info (assoc tuple-info :pattern-contract {:kind :tuple :columns [{:type :bytes :nullable? false}]})
        clauses [{:id 0 :kind :pattern :terms [[:var '?bytes]]}]
        fake (fake/source info (vec (repeat 5 [bytes])))
        batches (atom [])
        result (harness/run! fake clauses {:limits {:batch-rows 2}
                                           :consume (fn [batch]
                                                      (swap! batches conj (:row-count batch))
                                                      (aset-byte bytes 0 (byte 9)))})
        values (mapv first (:rows result))]
    (is (= [2 2 1] @batches))
    (is (= (repeat 5 [1 2 3]) (mapv vec values)))
    (is (= 5 (count (set values))))
    ;; Ownership holds between rows in one batch as well as across advances.
    (aset-byte ^bytes (first values) 0 (byte 7))
    (is (= (repeat 4 [1 2 3]) (mapv vec (next values))))
    (is (= :close-session (last @(:events fake))))))

(deftest owned-metadata-survives-advance-and-close
  (doseq [x [[1] {:a 1} #{1} '(1) 'value (sorted-map :a 1) (sorted-set 1 2)]]
    (let [bytes (byte-array [1 2])
          date (java.util.Date. 1000)
          x (with-meta x {:bytes bytes :nested (with-meta 'leaf {:date date})})
          info (assoc tuple-info :pattern-contract
                      {:kind :tuple :columns [{:type :data :nullable? false}]})
          clauses [{:id 0 :kind :pattern :terms [[:var '?value]]}]
          s (fake/source info (vec (repeat 5 [x])))
          result (harness/run! s clauses
                              {:limits {:batch-rows 2}
                               :consume (fn [_] (aset-byte bytes 0 (byte 9)) (.setTime date 9000))})
          values (mapv first (:rows result))
          stamp #(-> % meta :nested meta :date)]
      (is (= (repeat 5 x) values))
      (doseq [v values]
        (is (= (class x) (class v)))
        (is (= [1 2] (vec (:bytes (meta v)))))
        (is (= 1000 (.getTime ^java.util.Date (stamp v)))))
      (aset-byte ^bytes (:bytes (meta (first values))) 0 (byte 8))
      (.setTime ^java.util.Date (stamp (first values)) 8000)
      (doseq [v (next values)]
        (is (= [1 2] (vec (:bytes (meta v)))))
        (is (= 1000 (.getTime ^java.util.Date (stamp v)))))
      (is (= :close-session (last @(:events s)))))))

(deftest custom-comparators-require-the-custom-value-contract
  (doseq [x [(sorted-map-by #(compare %2 %1) :a 1 :b 2)
             (sorted-set-by #(compare %2 %1) 1 2)]]
    (is (not (value/portable? x)))
    (is (= :query-source/unsupported (category #(value/value-charge x))))
    (is (= :query-source/unsupported (category #(value/owned-copy x))))
    (let [info (assoc tuple-info :pattern-contract
                      {:kind :tuple :columns [{:type :data :nullable? false}]})
          s (fake/source info [[x]])]
      (is (= :query-source/invalid
             (category #(harness/run! s [{:id 0 :kind :pattern :terms [[:const x]]}]))))
      (is (empty? @(:events s))))))

(deftest acquisition-and-planning-failures-clean-up-only-acquired-resources
  (doseq [[phase category expected]
          [[:acquisition :query-source/unauthorized []]
           [:planning :query-source/stale-definition [:open-session :cancel-session :close-session]]
           [:open-cursor :query-source/snapshot-lost [:open-session :plan :cancel-session :close-session]]]]
    (let [fake (fake/source tuple-info [[1 "a"]]
                            {phase #(throw (ex-info "sensitive detail" {:error category}))})]
      (is (= category (:error (error-data #(harness/run! fake patterns)))))
      (is (= expected @(:events fake))))))

(deftest unsupported-values-and-overflow-do-not-escape-as-partial-results
  (is (= :query-source/unsupported
         (category #(source/batch-charge-bytes [{:type :data :nullable? false}]
                                              [[(Object.)]]))))
  (let [fake (fake/source tuple-info [[1 "small"] [2 (apply str (repeat 10000 "x"))]])]
    (is (= :query-source/limit
           (category #(harness/run! fake patterns {:limits {:batch-rows 1 :batch-bytes 4096}}))))
    (is (= [:cancel-session :close-cursor :close-session] (take-last 3 @(:events fake))))))

(deftest primary-failure-survives-cleanup-and-provider-details-are-redacted
  (let [fake (fake/source tuple-info [[1 "a"]]
                          {:fetch #(throw (Exception. "secret SQL"))
                           :close-cursor #(throw (Exception. "secret cleanup"))})
        error (try (harness/run! fake patterns) nil (catch Exception e e))]
    (is (= :query-source/provider (:error (ex-data error))))
    (is (= :fetch (:phase (ex-data error))))
    (is (nil? (.getCause error)))
    (is (= 1 (count (.getSuppressed error))))
    (is (not (.contains (str error) "secret")))
    (is (= :close-session (last @(:events fake))))))

(deftest stale-cursor-open-and-idempotent-close
  (let [c (control/create)
        fake (fake/source tuple-info [[1 "a"]])
        context {:spi-version 1 :features #{} :consistency :snapshot :control c}
        session (source/-open-session fake context)]
    (is (= :query-source/stale-definition
           (category #(source/-open-cursor session {:definition-fingerprint "changed"}))))
    (source/-close-session! session)
    (source/-close-session! session)
    (is (= :closed (:state (error-data #(source/-open-cursor session {})))))
    (is (= :snapshot (:consistency (source/-session-info session))))
    (control/close! c)))

(deftest plan-and-batch-structural-validation
  (let [c (control/create)
        fake (fake/source tuple-info [[1 "a"]])
        context {:spi-version 1 :features #{} :consistency :snapshot :control c}
        session (source/-open-session fake context)
        request (merge context {:source-symbol '$test :source-descriptor tuple-info :clauses patterns
                                :bindings {} :required-vars #{'?id '?name}
                                :demand {:quality :exact :ordering [] :offset 0 :limit nil}
                                :max-alternatives 32})
        response (source/-plan-source fake session request)]
    (try
      (is (= response (contract/alternatives! request response)))
      (let [handle (Object.)
            opaque (assoc-in response [:alternatives 0 :plan] handle)]
        (is (identical? handle
                       (get-in (contract/alternatives! request opaque)
                               [:alternatives 0 :plan]))))
      (doseq [bad [(assoc-in response [:alternatives 0 :covers] #{99})
                   (assoc-in response [:alternatives 0 :prefilters] #{0})
                   (update-in response [:alternatives 0 :columns] pop)
                   (assoc-in response [:alternatives 0 :estimate :rows] Double/NaN)
                   (assoc-in response [:alternatives 0 :estimate :rows] (bigdec "-1E-1000"))]]
        (is (= :query-source/invalid (category #(contract/alternatives! request bad)))))
      (is (= :query-source/unsupported
             (category #(contract/alternatives! request
                                                (assoc-in response [:alternatives 0 :requires] ['?id])))))
      (is (= :query-source/unsupported (category #(contract/selection! request {:alternatives []} []))))
      (doseq [bad [{:columns [] :rows [] :row-count 0 :byte-count 0 :exhausted? false}
                   {:columns [] :rows [[]] :row-count 1 :byte-count 0 :exhausted? true}
                   nil]]
        (is (= :query-source/invalid
               (category #(contract/batch! [] {:control c :max-rows 10 :max-bytes 1000} bad)))))
      (finally (source/-close-session! session) (control/close! c)))))

(deftest batch-accounting-validates-each-custom-value-once
  (let [calls (atom 0)
        type {:kind :custom :name :test/charged :version 1 :fingerprint "v1"}
        x (reify value/IQuerySourceValue
            (-logical-type [_] type)
            (-retained-bytes [_] (swap! calls inc) 64))]
    (doseq [[t row] [[:data [x]] [type [x]]
                    [{:kind :tuple :elements [:long :data]} [[1 x]]]]]
      (let [columns [{:type t :nullable? false}]
            rows [row]
            expected (+' 64 (value/value-charge columns) (value/value-charge rows))]
        (reset! calls 0)
        (is (= expected (source/batch-charge-bytes columns rows)))
        (is (= 1 @calls))))))

(deftest incremental-batch-charges-include-metadata-and-overflow
  (let [huge (reify value/IQuerySourceValue
               (-logical-type [_] {:kind :custom :name :test/huge :version 1 :fingerprint "v1"})
               (-retained-bytes [_] Long/MAX_VALUE))]
    (doseq [x [nil (java.util.Date. 1) (byte-array [1 2])
               (with-meta 'item {:when (java.time.Instant/ofEpochSecond 1 123)})
               (with-meta [1N 2/3 1.23M] {:hidden (sorted-map :a (sorted-set 1 2))})
               {:values (list #{1 2} [huge huge])}]]
      (let [columns [{:type :data :nullable? true}]
            rows (with-meta [(with-meta [x] {:row :metadata})] {:batch :metadata})
            observed (volatile! 0)
            bytes (value/batch-charge-bytes columns rows #(vswap! observed +' %))]
        (is (= (+' 64 (value/value-charge columns) (value/value-charge rows)) bytes))
        (is (= bytes @observed)))))
  (is (zero? (source/batch-charge-bytes [] []))))

(deftest batch-rows-accept-all-persistent-vector-representations
  (let [columns [{:type :long :nullable? false} {:type :data :nullable? false}]
        expected (source/batch-charge-bytes columns [[1 :ok]])]
    (doseq [row [[1 :ok] (subvec [:prefix 1 :ok :suffix] 1 3) (first {1 :ok})]]
      (is (vector? row))
      (is (<= expected (source/batch-charge-bytes columns [row]))))))

(deftest vector-views-charge-their-parent-and-detached-copies-do-not
  (let [parent (with-meta [(.repeat "x" 8192) :visible (byte-array 8192)]
                         {:hidden (byte-array 8192)})
        view (with-meta (subvec parent 1 2) {:visible (java.util.Date. 1)})
        copied (value/owned-copy view)
        observed (volatile! 0)
        retained (value/value-charge view #(vswap! observed +' %))]
    (is (= view copied))
    (is (> retained (value/value-charge parent)))
    (is (= retained @observed))
    (is (= (value/value-charge copied) (value/copy-charge view)))
    (is (< (value/copy-charge view) 1024))
    (is (not (identical? (:visible (meta view)) (:visible (meta copied))))))
  (let [view (subvec [(Object.) :visible] 1 2)
        columns [{:type :data :nullable? false}]]
    (is (value/matches-type? :data view))
    (is (= [[view]] (value/rows! columns [[view]])))
    (is (= :query-source/unsupported
           (category #(source/batch-charge-bytes columns [[view]]))))
    (is (pos? (source/batch-charge-bytes columns (value/owned-copy [[view]]))))))

(deftest empty-batches-with-retained-state-need-a-reservation
  (let [rows (with-meta [] {:payload (byte-array 1024)})
        ctl (control/create)
        bytes (source/batch-charge-bytes [] rows)
        request {:control ctl :max-rows 1 :max-bytes 8192}
        batch {:columns [] :rows rows :row-count 0 :byte-count bytes :exhausted? true}]
    (try
      (is (pos? bytes))
      (is (= :query-source/invalid (category #(contract/batch! [] request batch))))
      (let [token (source/reserve! ctl {:retained-bytes bytes :in-flight-batches 1})
            batch (assoc batch :reservation token)]
        (is (= batch (contract/batch! [] request batch)))
        (source/release! token)
        (is (every? zero? (vals (control/usage ctl)))))
      (finally (control/close! ctl)))))

(deftest copied-fake-batches-report-the-detached-charge
  (let [view (subvec [(byte-array 1024) 2] 1 2)
        info (assoc-in tuple-info [:pattern-contract :columns 1 :type] :data)
        result (harness/run! (fake/source info [[1 [2]]]) patterns
                             {:bindings {'?name view}})]
    (is (= [[1 [2]]] (:rows result)))
    (is (not (instance? clojure.lang.APersistentVector$SubVector
                        (get-in result [:rows 0 1]))))
    (is (= (source/batch-charge-bytes (:columns result) (:rows result))
           (get-in result [:metrics :charged-bytes])))))
