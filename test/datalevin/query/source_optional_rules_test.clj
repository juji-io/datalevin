(ns datalevin.query.source-optional-rules-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.parser :as parser]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.planning :as planning]
            [datalevin.query.source.program :as program]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value]
            [datalevin.query.source-planning-test :as fixture]
            [datalevin.util :as u])
  (:import [java.util Random UUID]))

(defn- error [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))
(defn- events-of [events kind] (filterv #(= kind (first %)) @events))
(defn- query! [query arguments & [options]]
  (rt/with-runtime (or options {})
    (fn [runtime]
      (let [result (program/query! runtime query arguments)]
        (is (every? zero? (vals (control/usage (:control runtime)))))
        result))))

(def copy-rules
  '[[(copied ?out ?input) [(identity ?input) ?out]]
    [(copied ?result ?value) (copied ?result ?value)]])

(defn- closure [edges]
  (loop [paths (set edges)]
    (let [next (into paths (for [[a b] paths [c d] edges :when (= b c)] [a d]))]
      (if (= paths next) paths (recur next)))))

(deftest observable-optional-inputs-preserve-caller-representations
  (let [result (query! '[:find ?tag ?out :in $rows % [[?input ?tag]] :where ($rows copied ?out ?input)]
                 [[] copy-rules [[1 :long] [1N :bigint] [1.00M :decimal] [1 :long]]])
        by-tag (into {} result)]
    (is (= 3 (count result)))
    (is (instance? Long (:long by-tag)))
    (is (instance? clojure.lang.BigInt (:bigint by-tag)))
    (is (= 2 (.scale ^java.math.BigDecimal (:decimal by-tag)))))
  (is (instance? clojure.lang.BigInt
        (query! '[:find ?out . :in $rows % :where ($rows copied ?out 1N)] [[] copy-rules]))))

(deftest generated-sparse-input-positions-retain-required-key-correlations
  (let [random (Random. 244948)
        rules '[[(reach [?from] ?to ?tag ?echo) [?from ?to] [(identity ?tag) ?echo]]
                [(reach [?start] ?end ?label ?copy) [?start ?mid] (reach ?mid ?end ?label ?copy)]]]
    (dotimes [_ 20]
      (let [edges (vec (repeatedly (.nextInt random 16)
                                  #(vector (.nextInt random 6) (.nextInt random 6))))
            inputs (vec (repeatedly 7 #(vector (.nextInt random 6) (keyword (str "tag" (.nextInt random 4))))))
            expected (set (for [[start tag] inputs [a b] (closure edges) :when (= start a)] [start tag b tag]))]
        (is (= expected
               (query! '[:find ?start ?tag ?end ?copy :in $rows % [[?start ?tag]]
                         :where ($rows reach ?start ?end ?tag ?copy)] [edges rules inputs])))))))

(deftest mutual-recursion-translates-different-input-positions
  (let [rules '[[(left ?out ?input) [(identity ?input) ?out]]
                [(left ?out ?input) (right ?input ?out)]
                [(right ?seed ?copy) (left ?copy ?seed)]]
        result (query! '[:find ?out . :in $rows % ?input :where ($rows left ?out ?input)] [[] rules 1N])]
    (is (instance? clojure.lang.BigInt result))))

(deftest sparse-literal-inputs-select-exact-demand-partitions
  (let [rules (into copy-rules
                '[[(selected [?seed] ?tag ?out) (copied ?out 1) [(ground :long) ?tag]]
                  [(selected [?seed] ?tag ?out) (copied ?out 1N) [(ground :bigint) ?tag]]])
        result (query! '[:find ?tag ?out :in $rows % ?seed :where ($rows selected ?seed ?tag ?out)]
                 [[] rules :seed])]
    (is (= #{[:long Long] [:bigint clojure.lang.BigInt]}
           (set (map (fn [[tag x]] [tag (class x)]) result))))))

(deftest repeated-optional-inputs-retain-the-bound-position-before-a-free-tail
  (let [rules '[[(echo ?tag ?input ?other) [(ground :tag) ?tag] [(identity ?input) ?local] [(ground 1N) ?other]]
                [(echo ?tag ?input ?other) (echo ?tag ?input ?other)]]
        result (query! '[:find ?a . :in $rows % ?a :where ($rows echo _ ?a ?a)] [[] rules 1])]
    (is (instance? Long result))
    (is (= 1 result))))

(deftest repeated-sparse-inputs-cannot-match-another-numeric-demand
  (let [rules '[[(copied ?tag ?a ?out ?b) [(ground :tag) ?tag] [(identity ?a) ?local] [(identity ?b) ?out]]
                [(copied ?tag ?a ?out ?b) (copied ?tag ?a ?out ?b)]
                [(selected [?seed] ?tag ?out) (copied _ 1 ?out 1N) [(ground :cross) ?tag]]
                [(selected [?seed] ?tag ?out) [(ground 1) ?a] (copied _ ?a ?out ?a) [(ground :same) ?tag]]]
        result (query! '[:find ?tag ?out :in $rows % ?seed :where ($rows selected ?seed ?tag ?out)]
                 [[] rules :seed])]
    (is (= #{[:cross clojure.lang.BigInt] [:same Long]}
           (set (map (fn [[tag x]] [tag (class x)]) result))))))

(deftest wrappers-forward-required-inputs-through-optional-heads
  (let [rules '[[(leaf [?id] ?out) [?id ?out]] [(leaf [?id] ?out) (leaf ?id ?out)]
                [(outer ?out ?id) (leaf ?id ?out)]]]
    (is (= #{[1 :one] [2 :two]}
           (query! '[:find ?id ?out :in $rows % [?id ...] :where ($rows outer ?out ?id)]
             [[[1 :one] [2 :two]] rules [1 2]])))
    (rt/with-runtime {}
      (fn [runtime]
        (let [capture (planning/capture! runtime [{:symbol '$rows :native [[1 :one]]}]
                        (parser/parse-query '[:find ?out :in $rows % ?id :where ($rows outer ?out ?id)])
                        (parser/parse-rules rules) {} #{'?id} nil)
              p (program/compile! runtime capture [{:variable '?id :type :long :nullable? false}] [[1]])]
          (is (= [1] (get-in p [:core-specs [0] :input-positions]))))))))

(deftest positive-local-producers-satisfy-callee-inputs-without-promoting-the-caller
  (let [rules '[[(leaf [?id] ?out) [?id ?out]] [(leaf [?id] ?out) (leaf ?id ?out)]
                [(selected ?id ?out) [$seed ?id] (leaf ?id ?out)]]]
    (is (= #{[1 :one] [2 :two]}
           (query! '[:find ?id ?out :in $rows $seed % :where ($rows selected ?id ?out)]
             [[[1 :one] [2 :two] [3 :three]] [[1] [2]] rules])))))

(deftest forwarded-input-inference-converges-through-a-long-rule-chain
  (let [n 18
        rules (into '[[(leaf [?id] ?out) [?id ?out]] [(leaf [?id] ?out) (leaf ?id ?out)]]
                (for [i (range n)]
                  [(list (symbol (str "wrapper" i)) '?out '?id)
                   (if (zero? i) '(leaf ?id ?out)
                       (list (symbol (str "wrapper" (dec i))) '?out '?id))]))
        query {:find ['?out] :in ['$rows '% '?id]
               :where [(list '$rows (symbol (str "wrapper" (dec n))) '?out '?id)]}]
    (is (= #{[:present]}
           (query! query [[[1 :present]] rules 1] {:limits {:retained-bytes 8000000}})))))

(deftest optional-input-dependencies-wait-for-real-producers-and-preserve-bags
  (let [q '[:find ?tag ?out :in $rows $values % :where ($rows copied ?out ?input) [$values ?input ?tag]]]
    (is (= #{[:long 1] [:bigint 1N]}
           (query! q [[] [[1 :long] [1N :bigint] [1 :long]] copy-rules]))))
  (rt/with-runtime {}
    (fn [runtime]
      (let [capture (planning/capture! runtime [{:symbol '$rows :native []}]
                      (parser/parse-query '[:find ?tag ?out :in $rows % [[?input ?tag]]
                                            :where ($rows copied ?out ?input)])
                      (parser/parse-rules copy-rules) {} #{'?input '?tag} nil)
            p (program/compile! runtime capture
                [{:variable '?input :type :data :nullable? false} {:variable '?tag :type :keyword :nullable? false}]
                [[1 :a] [1 :a] [1N :b]])
            result (program/execute! runtime p)]
        (is (= {[:a Long] 2 [:b clojure.lang.BigInt] 1}
               (frequencies (map (fn [^objects row]
                                   [(aget row (int (get (:attrs result) '?tag)))
                                    (class (aget row (int (get (:attrs result) '?out))))]) (:tuples result)))))))))

(deftest unseeded-optional-dependency-cycles-reject-before-acquisition
  (let [events (atom [])
        failure (error (fn [] (rt/with-runtime {}
                               (fn [runtime]
                                 (program/query! runtime '[:find ?a ?b :in $rows $s %
                                                           :where ($rows copied ?a ?b) ($rows copied ?b ?a)]
                                   [[] (fixture/provider events {}) copy-rules])))))]
    (is (= :query-source/unsupported (:error failure)))
    (is (empty? (events-of events :acquire))))
  (let [result (query! '[:find ?a ?b :in $rows % ?a :where ($rows copied ?a ?b) ($rows copied ?b ?a)]
                 [[] copy-rules 1N])]
    (is (= #{[clojure.lang.BigInt clojure.lang.BigInt]} (set (map #(mapv class %) result))))))

(deftest missing-observable-producers-and-ambiguous-literals-reject-before-acquisition
  (doseq [[rules call]
          [[copy-rules '(copied ?out ?input)] [copy-rules '(copied ?out _)]
           ['[[(copied ?out ?input) [?input _] [(identity ?input) ?out]]
              [(copied ?out ?input) (copied ?out ?input)]] '(copied ?out 1N)]]]
    (let [events (atom []) saved (atom nil)
          failure (error (fn [] (rt/with-runtime {}
                                 (fn [runtime]
                                   (reset! saved runtime)
                                   (program/query! runtime
                                     {:find ['?out] :in ['$rows '$s '%] :where [(apply list '$rows call)]}
                                     [[[1 :one]] (fixture/provider events {}) rules])))))]
      (is (= :query-source/unsupported (:error failure)))
      (is (empty? (events-of events :acquire)))
      (is (every? zero? (vals (control/usage (:control @saved))))))))

(deftest private-branch-functions-do-not-observe-an-unexported-caller-head
  (let [rules '[[(selected ?key ?value) [$seed ?key ?value]
                 (or-join [?key]
                   (and [?key ?value] [(identity ?value) ?private])
                   (and [?key ?other] [(identity ?other) ?private]))]
                [(selected ?key ?value) (selected ?key ?value)]]]
    (is (= #{[1 :outer]}
           (query! '[:find ?key ?value :in $rows $seed % :where ($rows selected ?key ?value)]
             [[[1 :inner]] [[1 :outer]] rules])))))

(deftest observable-heads-enumerate-local-facts-through-mutual-recursion
  (let [rules '[[(left ?copy ?value) [?value _] [(identity ?value) ?copy]]
                [(left ?copy ?value) (right ?value ?copy)]
                [(right ?input ?out) (left ?out ?input)]]
        rows [[1 :one] [2 :two] [1 :duplicate] [nil :absent]]]
    (is (= #{[1 1] [2 2]}
           (query! '[:find ?copy ?value :in $rows % :where ($rows left ?copy ?value)] [rows rules])))
    (is (= #{[1] [2]}
           (query! '[:find ?copy :in $rows % :where ($rows left ?copy _)] [rows rules])))
    (is (= #{} (query! '[:find ?copy :in $rows % :where ($rows left ?copy _)] [[] rules])))))

(deftest partial-observable-inputs-select-independent-enumeration-variants
  (let [rules '[[(copies ?a ?b ?left ?right) [?a ?b] [(identity ?a) ?left] [(identity ?b) ?right]]
                [(copies ?a ?b ?left ?right) (copies ?a ?b ?left ?right)]]
        rows [[1 2] [1 3N] [1N 3N] [4N 2] [4N 3N]]
        typed (fn [result] (set (map #(mapv (juxt class identity) %) result)))
        classes (fn [result] (set (map #(mapv class %) result)))]
    (is (= (typed (map #(into % %) rows))
           (typed (query! '[:find ?a ?b ?left ?right :in $rows % :where ($rows copies ?a ?b ?left ?right)] [rows rules]))))
    (is (= #{[Long Long Long Long] [Long clojure.lang.BigInt Long clojure.lang.BigInt]}
           (classes (query! '[:find ?a ?b ?left ?right :in $rows % ?a :where ($rows copies ?a ?b ?left ?right)]
                      [rows rules 1]))))
    (is (= #{[Long Long Long Long] [clojure.lang.BigInt Long clojure.lang.BigInt Long]}
           (classes (query! '[:find ?a ?b ?left ?right :in $rows % ?b :where ($rows copies ?a ?b ?left ?right)]
                      [rows rules 2]))))))

(deftest enumeration-and-bound-demands-do-not-share-their-answers
  (let [rules '[[(copied ?out ?value) [?value _] [(identity ?value) ?out]]
                [(copied ?out ?value) (copied ?out ?value)]
                [(selected [?seed] ?tag ?out) (copied ?out _) [(ground :enumerated) ?tag]]
                [(selected [?seed] ?tag ?out) [$inputs ?value ?tag] (copied ?out ?value)]]
        result (query! '[:find ?tag ?out :in $rows $inputs % ?seed :where ($rows selected ?seed ?tag ?out)]
                 [[[1 :one] [2N :two] [3.00M :three]] [[2N :bigint] [3.00M :decimal]] rules :seed])
        by-tag (into {} (remove #(= :enumerated (first %))) result)]
    (is (= #{:bigint :decimal} (set (keys by-tag))))
    (is (= #{Long clojure.lang.BigInt java.math.BigDecimal}
           (set (map (comp class second) (filter #(= :enumerated (first %)) result)))))
    (is (instance? clojure.lang.BigInt (:bigint by-tag)))
    (is (= 2 (.scale ^java.math.BigDecimal (:decimal by-tag))))))

(deftest nonlinear-observable-recursion-enumerates-a-complete-closure
  (let [random (Random. 99173)
        rules '[[(path ?x ?y ?copy) [?x ?y] [(identity ?y) ?copy]]
                [(path ?x ?y ?copy) (path ?x ?mid _) (path ?mid ?y ?copy)]]]
    (dotimes [_ 12]
      (let [edges (vec (repeatedly (.nextInt random 15) #(vector (.nextInt random 5) (.nextInt random 5))))]
        (is (= (set (map (fn [[a b]] [a b b]) (closure edges)))
               (query! '[:find ?a ?b ?copy :in $rows % :where ($rows path ?a ?b ?copy)] [edges rules])))))))

(deftest union-interfaces-distinguish-enumerated-and-required-observable-heads
  (let [rules '[[(copied ?out ?input)
                 (or-join [?out ?input]
                   (and [?input :left] [(identity ?input) ?out])
                   (and [?input :right] [(identity ?input) ?out]))]
                [(copied ?out ?input) (copied ?out ?input)]]]
    (is (= #{[1] [2]}
           (query! '[:find ?out :in $rows % :where ($rows copied ?out _)]
             [[[1 :left] [2 :right]] rules]))))
  (let [rules '[[(copied ?out ?input)
                 (or-join [?out ?input] [(identity ?input) ?out] [(ground ?input) ?out])]
                [(copied ?out ?input) (copied ?out ?input)]]]
    (is (instance? clojure.lang.BigInt
          (query! '[:find ?out . :in $rows % ?input :where ($rows copied ?out ?input)] [[] rules 1N])))
    (let [events (atom [])
          failure (error (fn [] (rt/with-runtime {}
                                 (fn [runtime]
                                   (program/query! runtime '[:find ?out :in $rows $s % :where ($rows copied ?out _)]
                                     [[] (fixture/provider events {}) rules])))))]
      (is (= :query-source/unsupported (:error failure)))
      (is (empty? (events-of events :acquire))))))

(deftest observable-enumeration-completes-lower-negative-strata
  (let [rules '[[(blocked ?x ?copy) [$blocked ?x] [(identity ?x) ?copy]]
                [(blocked ?x ?copy) (blocked ?x ?copy)]
                [(kept ?x ?copy) [?x _] [(identity ?x) ?copy]
                 (not-join [?x] (blocked ?x _))]]]
    (is (= #{[1 1] [3 3]}
           (query! '[:find ?x ?copy :in $rows $blocked % :where ($rows kept ?x ?copy)]
             [[[1 :one] [2 :two] [3 :three]] [[2]] rules])))))

(deftest adornment-expansion-is-bounded-before-provider-acquisition
  (let [inputs (mapv #(symbol (str "?input" %)) (range 13))
        outputs (mapv #(symbol (str "?output" %)) (range 13))
        head (apply list 'wide (concat inputs outputs))
        rules [(into [head inputs] (map (fn [input output] [(list 'identity input) output]) inputs outputs))
               [head head]]
        events (atom []) saved (atom nil)
        failure (error (fn [] (rt/with-runtime {}
                               (fn [runtime]
                                 (reset! saved runtime)
                                 (program/query! runtime
                                   {:find [(first outputs)] :in ['$rows '$s '%] :where [(apply list '$rows head)]}
                                   [[(vec (range 13))] (fixture/provider events {}) rules])))))]
    (is (= :query-source/limit (:error failure)))
    (is (= :planning (:phase failure)))
    (is (empty? (events-of events :acquire)))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest optional-function-inputs-work-inside-negative-bodies
  (let [rules '[[(kept ?x) (not-join [?x] [(identity ?x) ?copy])]]]
    (is (= #{[nil]}
           (query! '[:find ?x :in $rows % [?x ...] :where ($rows kept ?x)] [[] rules [nil 1]])))))

(deftest optional-inputs-remain-owned-through-unions-and-aliases
  (let [bytes (byte-array [1 2 3])
        rules '[[(copied ?out ?input) [(identity ?input) ?out]]
                [(copied ?out ?input) (copied ?out ?input)]
                [(both ?out ?input) (or-join [?out ?input] ($a copied ?out ?input) ($b copied ?out ?input))]]
        result (query! '[:find ?out . :in $a $b % ?input :where ($a both ?out ?input)]
                 [[] [] rules {:nested [bytes]}])]
    (aset-byte bytes 0 (byte 99))
    (is (= [1 2 3] (vec (first (:nested result)))))))

(defn- with-db [f]
  (let [dir (str u/tmp-dir "spi-v2-optional-rules-" (UUID/randomUUID))
        conn (d/get-conn dir {:edge {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}})]
    (try (d/transact! conn [{:db/id 1 :edge 2} {:db/id 2 :edge 3}]) (f conn)
         (finally (d/close conn) (u/delete-files dir)))))

(deftest native-observable-enumeration-keeps-typed-exports-and-pinned-views
  (with-db
    (fn [conn]
      (let [events (atom []) delegate (fixture/provider events {:lookup-only? true})
            provider (reify source/IQuerySource
                       (-source-info [_] (source/-source-info delegate))
                       (-open-session [_ request] (source/-open-session delegate request))
                       (-plan-source [_ session request]
                         (d/transact! conn [{:db/id 1 :edge 1}])
                         (source/-plan-source delegate session request)))
            rules '[[(reach ?x ?y) [?x :edge ?y]]
                    [(reach ?x ?y) [?x :edge ?mid] (reach ?mid ?y)]
                    [(copied ?x ?to ?copy) (reach ?x ?to) [(identity ?x) ?copy]]]]
        (is (= #{[1 2 1 "two"]}
               (query! '[:find ?x ?to ?copy ?name :in $n $s %
                         :where ($n copied ?x ?to ?copy) [$s ?to :row/value ?name]]
                 [@conn provider rules])))
        (is (= [{:variable '?to :type :long :nullable? false}]
               (filterv #(= '?to (:variable %)) (:available-inputs (second (first (events-of events :plan)))))))
        (is (= #{[1]} (d/q '[:find ?e :where [?e :edge 1]] @conn)))))))

(deftest sparse-optional-inputs-preserve-pinned-native-exports
  (with-db
    (fn [conn]
      (let [events (atom []) delegate (fixture/provider events {:lookup-only? true})
            provider (reify source/IQuerySource
                       (-source-info [_] (source/-source-info delegate))
                       (-open-session [_ request] (source/-open-session delegate request))
                       (-plan-source [_ session request]
                         (d/transact! conn [{:db/id 3 :edge 4}])
                         (source/-plan-source delegate session request)))
            rules '[[(reach [?from] ?to ?label ?echo) [?from :edge ?to] [(identity ?label) ?echo]]
                    [(reach [?from] ?to ?label ?echo) [?from :edge ?mid] (reach ?mid ?to ?label ?echo)]]]
        (is (= #{[2 :tag "two"]}
               (query! '[:find ?to ?echo ?name :in $n $s % ?from ?label
                         :where ($n reach ?from ?to ?label ?echo) [$s ?to :row/value ?name]]
                 [@conn provider rules 1 :tag])))
        (is (= [{:variable '?to :type :long :nullable? false}]
               (filterv #(= '?to (:variable %)) (:available-inputs (second (first (events-of events :plan)))))))
        (is (= #{[3 4]} (d/q '[:find ?a ?b :where [?a :edge ?b] [(= ?b 4)]] @conn)))))))

(deftest sparse-demand-storage-releases-between-recursive-rounds
  (let [n 100 rules '[[(reach [?from] ?to ?label ?echo) [?from ?to] [(identity ?label) ?echo]]
                      [(reach [?from] ?to ?label ?echo) (reach ?from ?mid ?label ?echo) [?mid ?to]]]]
    (is (= (set (map #(vector % :tag) (range 1 (inc n))))
           (query! '[:find ?to ?echo :in $rows % ?from ?label :where ($rows reach ?from ?to ?label ?echo)]
             [(mapv #(vector % (inc %)) (range n)) rules 0 :tag]
             {:limits {:retained-bytes 6000000}})))))

(deftest cancellation-during-sparse-input-ownership-releases-all-storage
  (let [saved (atom nil) armed? (atom false) visits (atom 0)
        item (reify value/IQuerySourceValue
               (-logical-type [_] {:kind :custom :name :test/optional-key :version 1 :fingerprint "v1"})
               (-retained-bytes [_]
                 (when @armed?
                   (swap! visits inc)
                   (control/cancel! (:control @saved) {:error :query-source/cancelled}))
                 64))
        failure (error (fn [] (rt/with-runtime {}
                               (fn [runtime]
                                 (reset! saved runtime)
                                 (let [capture (planning/capture! runtime [{:symbol '$rows :native []}]
                                                 (parser/parse-query '[:find ?out :in $rows % ?input
                                                                       :where ($rows copied ?out ?input)])
                                                 (parser/parse-rules copy-rules) {} #{'?input} nil)
                                       p (program/compile! runtime capture
                                           [{:variable '?input :type :data :nullable? false}] [[item]])]
                                   (reset! armed? true)
                                   (program/execute! runtime p))))))]
    (is (= :query-source/cancelled (:error failure)))
    (is (pos? @visits))
    (is (every? zero? (vals (control/usage (:control @saved)))))))
