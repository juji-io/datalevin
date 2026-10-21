(ns datalevin.query.source-lowering-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.lowering :as lowering]
            [datalevin.query.source.runtime :as rt])
  (:import [java.util Date]))

(def descriptor
  {:pattern-contract {:kind :eav
                      :attributes {:row/name {:entity-type :long :value-type :string}
                                   :row/age {:entity-type :long :value-type :long}
                                   :row/ref {:entity-type :long :value-type :long}
                                   :tag/self {:entity-type :keyword :value-type :keyword}}}
   :logical-schema {:domains {:row {:entity-type :long} :tag {:entity-type :keyword}}
                    :attributes {:row/name {:entity-domain :row :reference-domain nil
                                            :cardinality :one :absence :no-fact}
                                  :row/age {:entity-domain :row :reference-domain nil
                                           :cardinality :one :absence :no-fact}
                                  :row/ref {:entity-domain :row :reference-domain :row
                                           :cardinality :many :absence :no-fact}
                                  :tag/self {:entity-domain :tag :reference-domain nil
                                            :cardinality :many :absence :no-fact}}}})

(defn pattern [entity attribute value]
  {:id 7 :kind :pattern :terms [entity attribute value]})

(defn- error [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(deftest exact-constant-mismatches-prune-without-coercing-equivalent-values
  (rt/with-runtime {}
    (fn [runtime]
      (doseq [[e a v bindings] [[[:const "not-an-id"] :row/name [:var '?v] {}]
                                 [[:var '?e] :row/name [:const 42] {}]
                                 [[:var '?e] :row/age [:var '?v] {'?v "not-a-long"}]
                                 [[:const nil] :row/name [:var '?v] {}]]]
        (let [result (lowering/lower! runtime descriptor (pattern e [:const a] v) bindings #{})]
          (is (= :incompatible-constant (:empty-proof result)))
          (is (empty? (:branches result)))))
      (doseq [e [1 1N (int 1) (biginteger 1)]]
        (let [result (lowering/lower! runtime descriptor
                       (pattern [:const e] [:const :row/name] [:var '?v]) {} #{})]
          (is (nil? (:empty-proof result)))
          (is (= 1 (count (:branches result)))))))))

(deftest known-attributes-specialize-without-dispatch-and-preserve-the-original-binding
  (rt/with-runtime {}
    (fn [runtime]
      (doseq [[middle bindings restore] [[[:const :row/name] {} {}]
                                         [[:var '?a] {'?a :row/name} {'?a :row/name}]]]
        (let [result (lowering/lower! runtime descriptor (pattern [:var '?e] middle [:var '?v]) bindings #{'?a})
              [branch] (:branches result)]
          (is (= :fixed (:mode result)))
          (is (= :row/name (:attribute branch)))
          (is (= [[:var '?e] [:const :row/name] [:var '?v]] (get-in branch [:clause :terms])))
          (is (= restore (:restore branch)))
          (is (= [:long :string] (mapv :type (:columns branch))))
          (is (= :one (get-in branch [:guarantees :cardinality]))))))))

(deftest unknown-attributes-have-canonical-heterogeneous-complete-branches
  (rt/with-runtime {}
    (fn [runtime]
      (doseq [[middle available mode] [[[:var '?a] #{} :union]
                                       [[:var '?a] #{'?a} :dispatch]
                                       [[:ignore] #{} :union]]]
        (let [result (lowering/lower! runtime descriptor (pattern [:var '?e] middle [:var '?v]) {} available)]
          (is (= mode (:mode result)))
          (is (= [:row/age :row/name :row/ref :tag/self] (mapv :attribute (:branches result))))
          (is (= [[:long :long] [:long :string] [:long :long] [:keyword :keyword]]
                 (mapv #(mapv :type (:columns %)) (:branches result))))
          (is (= :row (get-in result [:branches 2 :guarantees :reference-domain])))
          (is (= (if (= :var (first middle)) (mapv #(hash-map '?a %) [:row/age :row/name :row/ref :tag/self])
                       [{} {} {} {}])
                 (mapv :restore (:branches result)))))))))

(deftest repeated-attribute-variables-unify-entity-and-value-before-planning
  (rt/with-runtime {}
    (fn [runtime]
      (doseq [terms [[[:var '?a] [:var '?a] [:var '?v]]
                     [[:var '?e] [:var '?a] [:var '?a]]
                     [[:var '?a] [:var '?a] [:var '?a]]]]
        (let [result (lowering/lower! runtime descriptor {:id 0 :kind :pattern :terms terms} {} #{})
              [branch] (:branches result)]
          (is (= [:tag/self] (mapv :attribute (:branches result))))
          (is (= (mapv #(if (= [:var '?a] %) [:const :tag/self] %) terms)
                 (get-in branch [:clause :terms])))
          (is (not-any? #(= '?a (:variable %)) (:columns branch)))
          (is (= {'?a :tag/self} (:restore branch))))))))

(deftest repeated-entity-value-types-have-an-exact-no-match-proof
  (rt/with-runtime {}
    (fn [runtime]
      (let [result (lowering/lower! runtime descriptor (pattern [:var '?x] [:const :row/name] [:var '?x]) {} #{})]
        (is (empty? (:branches result)))
        (is (= :incompatible-repeated-variable (:empty-proof result)))))))

(deftest overlapping-types-cannot-be-discarded-as-an-empty-branch
  (rt/with-runtime {}
    (fn [runtime]
      (let [d (assoc-in descriptor [:pattern-contract :attributes :row/name :value-type] :data)
            before (control/usage (:control runtime))]
        (is (= :query-source/unsupported
               (:error (error #(lowering/lower! runtime d
                                               (pattern [:var '?x] [:const :row/name] [:var '?x]) {} #{})))))
        (is (= before (control/usage (:control runtime))))))))

(deftest undeclared-keywords-prove-no-facts-without-calling-a-provider
  (rt/with-runtime {}
    (fn [runtime]
      (doseq [[term bindings] [[[:const :unknown/attribute] {}]
                               [[:var '?a] {'?a :unknown/attribute}]]]
        (let [result (lowering/lower! runtime descriptor (pattern [:var '?e] term [:var '?v]) bindings #{'?a})]
          (is (empty? (:branches result)))
          (is (= :undeclared-attribute (:empty-proof result))))))))

(deftest constants-and-input-metadata-are-owned-before-provider-execution
  (rt/with-runtime {}
    (fn [runtime]
      (let [date (Date. 100) bytes (byte-array [1])
            d (assoc-in descriptor [:pattern-contract :attributes :row/name :value-type] :data)
            literal (with-meta {:date date} {:bytes bytes})
            result (lowering/lower! runtime d (pattern [:var '?e] [:const :row/name] [:const literal]) {} #{})
            value (get-in result [:branches 0 :clause :terms 2 1])]
        (.setTime date 999) (aset-byte bytes 0 (byte 9))
        (is (= 100 (.getTime ^Date (:date value))))
        (is (= [1] (vec (:bytes (meta value)))))))))

(deftest metadata-expansion-and-failures-release-owned-planning-state
  (rt/with-runtime {:limits {:retained-bytes 10000}}
    (fn [runtime]
      (let [before (control/usage (:control runtime))
            result (error #(lowering/lower! runtime descriptor (pattern [:var '?e] [:ignore] [:var '?v]) {} #{}))]
        (is (= :query-source/limit (:error result)))
        (is (= before (control/usage (:control runtime)))))))
  (rt/with-runtime {}
    (fn [runtime]
      (let [before (control/usage (:control runtime))
            result (lowering/lower! runtime descriptor (pattern [:var '?e] [:ignore] [:var '?v]) {} #{})]
        ((:close result))
        (is (= before (control/usage (:control runtime))))
        (is (true? @(:closed? result)))))))
