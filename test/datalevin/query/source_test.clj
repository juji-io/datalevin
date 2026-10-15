(ns datalevin.query.source-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.query.source :as source]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.fake :as fake]
            [datalevin.query.source.harness :as harness]))

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
