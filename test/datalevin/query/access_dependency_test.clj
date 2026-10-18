(ns datalevin.query.access-dependency-test
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is]]
            [datalevin.query.access.dependency :as dependency])
  (:import [java.util Random]))

(defn- column
  ([v] (column v :long false))
  ([v t nullable?] {:variable v :type t :nullable? nullable?}))

(defn- choice [id requires columns]
  {:id id :covers #{id} :requires (mapv column requires) :columns (mapv column columns)
   :after #{} :resources #{}})

(defn- run [inputs work choices & [options]]
  (dependency/schedule
    (merge {:inputs (mapv column inputs) :required-work (set work) :required-vars #{}
            :choices choices :check! (fn []) :reserve! (fn [_]) :max-states 4096}
           options)))

(deftest dependencies-include-local-operators-and-explicit-boundaries
  (let [choices [(choice :dependent ['?b] ['?c])
                 (assoc (choice :project ['?c] ['?result]) :after #{:residual})
                 (choice :native ['?a] ['?b])
                 (choice :residual ['?c] [])
                 (choice :seed [] ['?a])]
        result (run [] (map :id choices) choices)]
    (is (= :complete (:status result)))
    (is (= [:seed :native :dependent :residual :project] (mapv :id (:choices result))))
    (is (= #{'?a '?b '?c '?result} (set (keys (:columns result)))))))

(deftest potential-outputs-do-not-seed-cycles-or-self-dependencies
  (doseq [choices [[(choice 0 ['?a] ['?a])]
                   [(choice 0 ['?a] ['?b]) (choice 1 ['?b] ['?a])]
                   [(assoc (choice 0 [] ['?a]) :after #{1})
                    (assoc (choice 1 [] ['?b]) :after #{0})]]]
    (is (= :infeasible (:status (run [] (map :id choices) choices))))))

(deftest complete-types-nullability-and-selected-exports-determine-readiness
  (let [need (choice 0 ['?a] ['?b])]
    (doseq [col [(column '?a :string false) (column '?a :long true)]]
      (is (= :infeasible (:status (run [] [0] [need] {:inputs [col]})))))
    (is (= :complete (:status (run ['?a] [0] [need]))))
    (is (= :complete (:status (run [] [0] [(assoc need :requires [(column '?a :long true)])]
                                  {:inputs [(column '?a :long true)]}))))
    (is (= :infeasible (:status (run ['?a] [0] [(assoc need :columns [(column '?a :string false)])])))))
  (let [t {:kind :custom :name :example/entity :version 1 :fingerprint "one" :parameters {:epoch 0}}
        other (assoc t :parameters {:epoch 1})]
    (is (false? (boolean (dependency/supplies? (column '?e t false) (column '?e other false)))))))

(deftest data-admits-specific-types-and-joins-retain-the-more-precise-contract
  (let [t {:kind :custom :name :example/entity :version 1 :fingerprint "entity:v1" :parameters {:epoch 7}}
        known {'?e (column '?e :data true)}
        precise (column '?e t false)]
    (is (dependency/supplies? precise (column '?e :data false)))
    (is (false? (boolean (dependency/supplies? (column '?e :data false) precise))))
    (is (= {'?e precise} (dependency/extend-columns known [precise])))
    (is (= {'?e precise} (dependency/extend-columns {'?e precise} [(column '?e :data true)])))
    (is (= :complete (:status (run ['?e] [0]
                               [(assoc (choice 0 [] ['?v]) :requires [(column '?e :data false)])]))))))

(deftest permissions-and-coverage-are-independent-of-provided-columns
  (let [a (assoc (choice 0 [] ['?a]) :resources #{:once})
        b (assoc (choice 1 ['?a] ['?b]) :resources #{:once})
        fallback (choice 0 [] ['?a])]
    (is (= :infeasible (:status (run [] [0 1] [a b]))))
    (is (= [fallback b] (:choices (run [] [0 1] [a fallback b]))))
    (is (= :infeasible (:status (run [] [0] [(assoc a :unavailable? true)])))))
  (let [fused (assoc (choice 0 [] ['?a]) :covers #{0 1})
        overlapping (assoc (choice 2 [] ['?b]) :covers #{1 2})]
    (is (= :infeasible (:status (run [] [0 1 2] [fused overlapping]))))
    (is (= :complete (:status (run [] [0 1] [fused]))))))

(deftest missing-work-and-live-exports-cannot-return-a-partial-plan
  (is (= :infeasible (:status (run [] [0 1] [(choice 0 [] ['?a])]))))
  (is (= :infeasible (:status (run [] [0] [(choice 0 [] ['?a])] {:required-vars #{'?b}})))))

(deftest admission-cancellation-and-search-bound-abort-before-success
  (let [choices [(choice 0 [] ['?a]) (choice 1 ['?a] ['?b])]
        charges (atom [])]
    (is (= :bound (:status (run [] [0 1] choices {:max-states 2 :reserve! #(swap! charges conj %)}))))
    (is (seq @charges))
    (is (every? pos? @charges))
    (doseq [callback [:check! :reserve!]]
      (is (thrown-with-msg? Exception #"stop"
            (run [] [0 1] choices {callback (fn [& _] (throw (Exception. "stop")))}))))))

(deftest long-chains-use-bounded-explicit-stack
  (let [n 1100
        choices (mapv (fn [i] (choice i (if (zero? i) [] [(symbol (str "?" (dec i)))])
                                     [(symbol (str "?" i))])) (range n))
        result (run [] (range n) choices {:max-states (inc n)})]
    (is (= :complete (:status result)))
    (is (= (vec (range n)) (mapv :id (:choices result))))))

(defn- exhaustive
  "Independent eager oracle: enumerate every legal next choice, without the
  planner's failed-state memo or its iterative stack."
  [known done used work choices]
  (if (= done work) []
      (some (fn [{:keys [requires columns covers resources after unavailable?] :as c}]
              (when (and (not unavailable?) (empty? (set/intersection done covers))
                         (set/subset? after done) (empty? (set/intersection used resources))
                         (every? #(= % (get known (:variable %))) requires))
                (let [produced (into {} (map (juxt :variable identity)) columns)]
                  (when (every? (fn [[v col]] (or (not (contains? known v)) (= col (known v)))) produced)
                    (when-let [tail (exhaustive (merge known produced) (into done covers)
                                               (into used resources) work choices)]
                      (cons c tail)))))) choices)))

(deftest generated-small-graphs-agree-with-exhaustive-eager-search
  (let [random (Random. 481723) variables ['?a '?b '?c '?d]]
    (dotimes [_ 180]
      (let [n (inc (.nextInt random 5))
            choices (mapv (fn [i]
                            (let [work (quot i 2)]
                              (assoc (choice work
                                             (if (.nextBoolean random) [(nth variables (.nextInt random 4))] [])
                                             [(nth variables (.nextInt random 4))])
                                     :resources (if (.nextBoolean random) #{(.nextInt random 2)} #{})
                                     :after (if (.nextBoolean random) #{(.nextInt random n)} #{}))))
                          (range (* 2 n)))
            seeds (if (.nextBoolean random) ['?a] [])
            expected (exhaustive (into {} (map (fn [v] [v (column v)])) seeds)
                                 #{} #{} (set (range n)) choices)
            actual (run seeds (range n) choices)]
        (is (= (if (some? expected) :complete :infeasible) (:status actual)))
        (when expected (is (= (vec expected) (:choices actual))))))))
