(ns datalevin.query.source-binding-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.value :as value]))

(def requires [{:variable '?id :type :long :nullable? false}])
(def columns [{:variable '?value :type :string :nullable? false}])

(defn- error [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(deftest input-free-and-dependent-batch-shapes-have-distinct-charges
  (let [rows [["a"] ["b"]]
        observed (volatile! 0)
        dependent (value/dependent-batch-charge-bytes columns rows [7 7] #(vswap! observed +' %))]
    (is (= dependent @observed))
    (is (= dependent (source/batch-charge-bytes columns rows [7 7])))
    (is (< (source/batch-charge-bytes columns rows) dependent))
    (is (zero? (source/batch-charge-bytes columns [])))
    (is (pos? (source/batch-charge-bytes columns [] [])))
    (is (= :query-source/invalid (:error (error #(source/batch-charge-bytes columns rows nil)))))
    (is (= :query-source/invalid (:error (error #(source/batch-charge-bytes columns rows [7])))))
    (doseq [ids [[-1 7] [(inc' Long/MAX_VALUE) 7] [1.0 7] [nil 7]]]
      (is (= :query-source/invalid (:error (error #(source/batch-charge-bytes columns rows ids))))))))

(deftest dependent-input-charges-include-metadata-and-backing-storage
  (let [bindings [{:binding-id 7 :values [42]}]
        observed (volatile! 0)
        charge (value/binding-charge-bytes requires bindings #(vswap! observed +' %))]
    (is (= charge @observed (source/binding-charge-bytes requires bindings)))
    (is (< charge (source/binding-charge-bytes requires (with-meta bindings {:note "retained metadata"}))))
    (let [backing (subvec (into bindings [{:binding-id 8 :values [99]}]) 0 1)]
      (is (< charge (source/binding-charge-bytes requires backing)))))
  (let [base (source/batch-charge-bytes columns [["a"]] [7])
        ids (subvec [7 (apply str (repeat 1000 "retained"))] 0 1)]
    (is (< base (source/batch-charge-bytes columns [["a"]] ids)))
    (is (< base (source/batch-charge-bytes columns [["a"]] (with-meta [7] {:retained [1 2 3]}))))))

(deftest dependent-input-validates-types-ids-and-unique-typed-keys
  (doseq [bindings [[] nil [{:binding-id -1 :values [42]}]
                    [{:binding-id 0 :values [42]} {:binding-id 0 :values [43]}]
                    [{:binding-id 0 :values [42]} {:binding-id 1 :values [42]}]
                    [{:binding-id 0 :values [42.0]}]
                    [{:binding-id 0 :values [nil]}]
                    [{:binding-id 0 :values [42 43]}]
                    [{:binding-id 0 :values '(42)}]
                    [{:binding-id 0 :values [42] :unknown true}]
                    [{:values [42]}]]]
    (is (= :query-source/invalid (:error (error #(source/binding-charge-bytes requires bindings))))))
  (doseq [requires [[] [{:variable '?id :type :unknown :nullable? true}]
                     [{:variable '?id :type :long :nullable? :yes}]
                     [{:variable '?id :type :long :nullable? true}
                      {:variable '?id :type :long :nullable? true}]]]
    (is (= :query-source/invalid
           (:error (error #(source/binding-charge-bytes requires [{:binding-id 0 :values [nil]}])))))))

(deftest typed-byte-keys-and-tuples-use-query-key-equality
  (let [requires [{:variable '?key :type {:kind :tuple :elements [:long :bytes]} :nullable? false}]
        one {:binding-id 0 :values [[42 (byte-array [1 2])]]}
        duplicate {:binding-id 1 :values [[42 (byte-array [1 2])]]}
        different {:binding-id 1 :values [[42 (byte-array [1 3])]]}]
    (is (= :query-source/invalid (:error (error #(source/binding-charge-bytes requires [one duplicate])))))
    (is (pos? (source/binding-charge-bytes requires [one different]))))
  (let [requires [{:variable '?key :type :bigdec :nullable? false}]]
    (is (pos? (source/binding-charge-bytes requires [{:binding-id 0 :values [1M]}
                                                    {:binding-id 1 :values [1.00M]}])))))

(deftest sizing-stops-before-visiting-an-oversized-single-key
  (let [visited (atom false)
        opaque (reify value/IQuerySourceValue
                 (-logical-type [_] {:kind :custom :name :test/key :version 1 :fingerprint "key"})
                 (-retained-bytes [_] (reset! visited true) 64))
        requires [{:variable '?key :type :data :nullable? false}]
        bindings [{:binding-id 0 :values [(with-meta [(apply str (repeat 10000 "x")) opaque] {:owned true})]}]
        observed (volatile! 0)
        observer (fn [increment]
                   (when (> (vswap! observed +' increment) 4096)
                     (throw (ex-info "binding cap" {:error :query-source/limit}))))]
    (is (= :query-source/limit (:error (error #(value/binding-charge-bytes requires bindings observer)))))
    (is (false? @visited)))
  (let [ctl (control/create)
        observer (fn [_] (control/check-active! ctl))
        cancel (reify value/IQuerySourceValue
                 (-logical-type [_] {:kind :custom :name :test/key :version 1 :fingerprint "key"})
                 (-retained-bytes [_]
                   (control/cancel! ctl {:error :query-source/cancelled :phase :execution}) 64))]
    (try
      (is (= :query-source/cancelled
             (:error (error #(value/binding-charge-bytes [{:variable '?key :type :data :nullable? false}]
                                                         [{:binding-id 0 :values [[cancel "later"]]}] observer)))))
      (finally (control/close! ctl)))))

(deftest binding-caps-are-positive-batch-limits-not-cumulative-ledger-entries
  (let [ctl (control/create)
        child (control/create {:parent ctl :limits {:binding-rows 32 :binding-bytes 4096}})]
    (try
      (is (= 256 (:binding-rows (control/limits ctl))))
      (is (= 262144 (:binding-bytes (control/limits ctl))))
      (is (= 32 (:binding-rows (control/limits child))))
      (is (= 4096 (:binding-bytes (control/limits child))))
      (is (not (contains? (control/usage ctl) :binding-rows)))
      (is (not (contains? (control/usage ctl) :binding-bytes)))
      (doseq [k [:binding-rows :binding-bytes] n [0 -1 1.5]]
        (is (= :query-source/invalid (:error (error #(control/create {:limits {k n}}))))))
      (finally (control/close! child) (control/close! ctl)))))
