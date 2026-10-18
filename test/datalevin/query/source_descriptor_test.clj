(ns datalevin.query.source-descriptor-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.descriptor :as descriptor]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.session :as session]
            [datalevin.query.source.value :as value]
            [datalevin.query.source-session-test :as fixture]))

(def ^:private customer-type
  {:kind :custom :name :example/customer :version 1 :fingerprint "customer:v1"
   :parameters {:domain :customer :epoch 7}})

(def ^:private info
  (-> fixture/descriptor
      (assoc-in [:pattern-contract :attributes :row/customer]
                {:entity-type :long :value-type customer-type})
      (assoc-in [:logical-schema :domains :customer] {:entity-type customer-type})
      (assoc-in [:logical-schema :attributes :row/customer]
                {:entity-domain :row :reference-domain :customer :cardinality :many :absence :no-fact})))

(defn- error [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(defn- provider [info events]
  (reify source/IQuerySource
    (-source-info [_] (swap! events conj :descriptor) info)
    (-open-session [_ _] (throw (Exception. "unexpected acquisition")))
    (-plan-source [_ _ _] (throw (Exception. "unexpected planning")))))

(deftest full-schema-reference-types-scope-and-completion-are-admitted-without-io
  (doseq [cardinality [:one :many :unknown] extent [:all-matches :declared-view]]
    (let [d (-> info (assoc-in [:logical-schema :attributes :row/value :cardinality] cardinality)
                (assoc-in [:completion :extent] extent)
                (assoc :scope {:id "scope:tenant" :kind :restricted :parameters {:tenant :one}}))]
      (is (identical? d (descriptor/source-info! d))))))

(deftest every-required-envelope-and-logical-guarantee-field-is-validated
  (doseq [key (keys info)]
    (is (= :query-source/invalid (:error (error #(descriptor/source-info! (dissoc info key)))))))
  (doseq [path [[:scope :id] [:scope :kind] [:scope :parameters]
                [:completion :domain] [:completion :extent]
                [:logical-schema :domains] [:logical-schema :attributes]
                [:logical-schema :domains :row :entity-type]
                [:logical-schema :attributes :row/value :entity-domain]
                [:logical-schema :attributes :row/value :reference-domain]
                [:logical-schema :attributes :row/value :cardinality]
                [:logical-schema :attributes :row/value :absence]]]
    (let [d (update-in info (pop path) dissoc (peek path))]
      (is (= :query-source/invalid (:error (error #(descriptor/source-info! d))))))))

(deftest obsolete-revisions-and-positional-descriptors-have-no-negotiation-mode
  (doseq [versions [#{1} #{1 2} #{3}]]
    (is (= :query-source/version
           (:error (error #(descriptor/source-info! (assoc info :spi-versions versions)))))))
  (doseq [versions [[] #{} #{:v2} nil]]
    (is (= :query-source/invalid
           (:error (error #(descriptor/source-info! (assoc info :spi-versions versions)))))))
  (is (= :query-source/unsupported
         (:error (error #(descriptor/source-info!
                          (assoc info :pattern-contract {:kind :tuple :columns []})))))))

(deftest domain-membership-and-complete-nominal-types-must-agree
  (doseq [mutate [(fn [d] (assoc-in d [:logical-schema :attributes :row/value :entity-domain] :missing))
                  (fn [d] (assoc-in d [:logical-schema :attributes :row/customer :reference-domain] :missing))
                  (fn [d] (assoc-in d [:logical-schema :domains :row :entity-type] :bigint))
                  (fn [d] (assoc-in d [:logical-schema :domains :customer :entity-type :version] 2))
                  (fn [d] (assoc-in d [:logical-schema :domains :customer :entity-type :fingerprint] "other"))
                  (fn [d] (assoc-in d [:logical-schema :domains :customer :entity-type :parameters :epoch] 8))
                  (fn [d] (update-in d [:logical-schema :attributes] dissoc :row/value))
                  (fn [d] (assoc-in d [:logical-schema :attributes :private/extra]
                                   (get-in d [:logical-schema :attributes :row/value])))
                  (fn [d] (assoc-in d [:logical-schema :domains nil] {:entity-type :long}))
                  (fn [d] (assoc-in d [:logical-schema :domains] {}))]]
    (is (= :query-source/invalid (:error (error #(descriptor/source-info! (mutate info))))))))

(deftest cardinality-absence-scope-and-completion-cannot-weaken-structural-guarantees
  (doseq [[path v] [[[:logical-schema :attributes :row/value :cardinality] :single]
                    [[:logical-schema :attributes :row/value :absence] :null]
                    [[:scope :id] ""] [[:scope :kind] :stream] [[:scope :parameters] []]
                    [[:scope :parameters] {:tenant :one}]
                    [[:completion :domain] :unbounded] [[:completion :extent] :partial]
                    [[:read-capabilities] #{}] [[:read-capabilities] #{:serializable}]]]
    (is (= :query-source/invalid
           (:error (error #(descriptor/source-info! (assoc-in info path v))))))))

(deftest qualified-envelope-annotations-are-inert-and-unknown-fields-are-invalid
  (doseq [path [[] [:scope] [:completion] [:logical-schema] [:logical-schema :domains :row]
                [:logical-schema :attributes :row/value] [:pattern-contract]
                [:pattern-contract :attributes :row/value]]]
    (let [annotated (if (seq path) (update-in info path assoc :example/annotation {:note "inert"})
                        (assoc info :example/annotation {:note "inert"}))
          invalid (if (seq path) (update-in info path assoc :unknown "not-an-extension")
                      (assoc info :unknown "not-an-extension"))]
      (is (= annotated (descriptor/source-info! annotated)))
      (is (= :query-source/invalid (:error (error #(descriptor/source-info! invalid))))))))

(deftest capture-is-once-owned-and-reused-without-reentering-the-provider
  (rt/with-runtime {}
    (fn [runtime]
      (let [events (atom []) parent [:unused :tenant :unused]
            raw (assoc info :scope {:id "scope:tenant" :kind :restricted
                                   :parameters {:tenant (with-meta (subvec parent 1 2) {:inert true})}})
            source (provider raw events) captured (descriptor/capture! runtime source)
            prepared (session/prepare! runtime [{:symbol '$a :descriptor captured :source source}
                                                {:symbol '$b :descriptor captured :source source}] nil)]
        (is (= [:descriptor] @events))
        (is (= raw captured))
        (is (not (identical? raw captured)))
        (is (nil? (value/vector-parent (get-in captured [:scope :parameters :tenant]))))
        (is (identical? captured (get-in prepared [:groups 0 :entries 0 :descriptor])))
        (is (identical? captured (get-in prepared [:groups 0 :entries 1 :descriptor])))))))

(deftest invalid-later-alias-is-fully-checked-before-acquiring-any-session
  (rt/with-runtime {}
    (fn [runtime]
      (let [events (atom []) source (provider info events)
            before (control/usage (:control runtime))
            invalid (assoc-in info [:logical-schema :attributes :row/customer :reference-domain] :missing)]
        (is (= :query-source/invalid
               (:error (error #(session/prepare! runtime
                                 [{:symbol '$a :source source :descriptor info}
                                  {:symbol '$b :source source :descriptor invalid}] nil)))))
        (is (empty? @events))
        (is (= before (control/usage (:control runtime))))))))

(deftest accepted-scope-ids-cannot-change-parameters-across-definitions
  (rt/with-runtime {}
    (fn [runtime]
      (let [events (atom []) source (provider info events)
            a (assoc info :scope {:id "scope:tenant" :kind :restricted :parameters {:tenant :one}})
            b (-> a (assoc :definition-id :other) (assoc-in [:scope :parameters :tenant] :two))
            before (control/usage (:control runtime))]
        (is (= :query-source/invalid
               (:error (error #(session/prepare! runtime
                                 [{:symbol '$a :source source :descriptor a}
                                  {:symbol '$b :source source :descriptor b}] nil)))))
        (is (empty? @events))
        (is (= before (control/usage (:control runtime))))
        (is (= 1 (count (:groups (session/prepare! runtime
                                  [{:symbol '$a :source source :descriptor a}
                                   {:symbol '$b :source source :descriptor (assoc b :scope (:scope a))}]
                                  nil)))))))))

(deftest capture-admits-discarded-metadata-before-validation-and-rolls-back-quotas
  (rt/with-runtime {:limits {:retained-bytes 24000}}
    (fn [runtime]
      (let [events (atom []) visited? (atom false)
            opaque (reify Object (toString [_] (reset! visited? true) "do not print"))
            raw (assoc info :example/annotation [(apply str (repeat 30000 "x")) opaque])
            before (control/usage (:control runtime))]
        (is (= :query-source/limit
               (:error (error #(descriptor/capture! runtime (provider raw events))))))
        (is (= [:descriptor] @events))
        (is (false? @visited?))
        (is (= before (control/usage (:control runtime))))))))

(deftest capture-does-not-call-value-hooks-in-invalid-descriptor-metadata
  (rt/with-runtime {}
    (fn [runtime]
      (let [calls (atom 0)
            opaque (reify value/IQuerySourceValue
                     (-logical-type [_] (swap! calls inc) {})
                     (-retained-bytes [_] (swap! calls inc) 64))
            before (control/usage (:control runtime))]
        (is (= :query-source/invalid
               (:error (error #(descriptor/capture! runtime
                                 (provider (assoc info :example/annotation opaque) (atom [])))))))
        (is (zero? @calls))
        (is (= before (control/usage (:control runtime))))))))
