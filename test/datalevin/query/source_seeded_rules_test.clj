(ns datalevin.query.source-seeded-rules-test
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

(defn- closure [edges]
  (loop [paths (set edges)]
    (let [next (into paths (for [[a b] paths [c d] edges :when (= b c)] [a d]))]
      (if (= paths next) paths (recur next)))))

(def copy-rules
  '[[(copied [?x] ?out) [(identity ?x) ?out]]
    [(copied [?x] ?out) (copied ?x ?out)]])

(deftest generated-required-prefixes-retain-correlations-through-recursive-demands
  (let [random (Random. 141421)
        rules '[[(reach [?node ?label] ?to ?echo) [?node ?to] [(identity ?label) ?echo]]
                [(reach [?from ?tag] ?end ?copy) [?from ?mid] (reach ?mid ?tag ?end ?copy)]]]
    (dotimes [_ 25]
      (let [edges (vec (repeatedly (.nextInt random 18)
                                  #(vector (.nextInt random 6) (.nextInt random 6))))
            seeds (vec (repeatedly 7 #(vector (.nextInt random 6) (keyword (str "tag" (.nextInt random 4))))))
            expected (set (for [[start label] seeds [a b] (closure edges) :when (= start a)]
                            [start label b label]))]
        (is (= expected
               (query! '[:find ?start ?label ?to ?echo :in $rows % [[?start ?label]]
                         :where ($rows reach ?start ?label ?to ?echo)] [edges rules seeds])))))))

(deftest recursive-required-constraints-use-whole-input-tuples
  (let [rules '[[(reach [?node ?limit] ?to) [?node ?to] [(<= ?to ?limit)]]
                [(reach [?node ?limit] ?to) [?node ?mid] [(<= ?mid ?limit)] (reach ?mid ?limit ?to)]]
        edges [[1 2] [2 3] [3 4] [4 1]]]
    (is (= #{[1 2 2] [3 4 1] [3 4 2] [3 4 3] [3 4 4]}
           (query! '[:find ?start ?limit ?to :in $rows % [[?start ?limit]]
                     :where ($rows reach ?start ?limit ?to)] [edges rules [[1 2] [3 4]]])))))

(deftest mutual-required-recursion-and-free-callers-share-demand-completion
  (let [edges (mapv #(vector % (inc %)) (range 6))
        rules '[[(odd [?start] ?end) [?start ?end]]
                [(odd [?start] ?end) [?start ?next] (even ?next ?end)]
                [(even [?start] ?end) [?start ?next] (odd ?next ?end)]
                [(selected ?start ?end) [$seed ?start] (even ?start ?end)]]]
    (is (= #{[0 2] [0 4] [0 6] [1 3] [1 5]}
           (query! '[:find ?a ?b :in $rows $seed % :where ($rows selected ?a ?b)]
             [edges [[0] [1]] rules])))))

(deftest source-qualified-required-relations-and-zero-column-demands-remain-independent
  (let [rules '[[(reach [?x] ?y) [?x ?y]]
                [(reach [?x] ?y) [?x ?mid] (reach ?mid ?y)]
                [(both [?x] ?y) (or-join [?x ?y] ($a reach ?x ?y) ($b reach ?x ?y))]
                [(exists ?flag) (both :root _) [(ground :found) ?flag]]]
        a [[:root :a] [:a :b]] b [[:root :c] [:c :d]]]
    (is (= #{[:a] [:b] [:c] [:d]}
           (query! '[:find ?to :in $a $b % ?from :where ($a both ?from ?to)] [a b rules :root])))
    (is (= :found
           (query! '[:find ?v . :in $a $b % :where ($a exists ?v)] [a b rules])))
    (is (nil? (query! '[:find ?v . :in $a $b % :where ($a exists ?v)] [[] [] rules])))))

(deftest seed-only-recursion-preserves-typed-values-and-outer-witnesses
  (let [seeds [[1 :long] [1N :bigint] [1.00M :decimal] [1 :long]]
        result (query! '[:find ?tag ?out :in $rows % [[?x ?tag]] :where ($rows copied ?x ?out)]
                 [[] copy-rules seeds])
        by-tag (into {} result)]
    (is (= #{:long :bigint :decimal} (set (keys by-tag))))
    (is (instance? Long (:long by-tag)))
    (is (instance? clojure.lang.BigInt (:bigint by-tag)))
    (is (= 2 (.scale ^java.math.BigDecimal (:decimal by-tag)))))
  (rt/with-runtime {}
    (fn [runtime]
      (let [seeds [[1 :a] [1 :a] [1N :b]]
            capture (planning/capture! runtime [{:symbol '$rows :native []}]
                      (parser/parse-query '[:find ?tag ?out :in $rows % [[?x ?tag]]
                                            :where ($rows copied ?x ?out)])
                      (parser/parse-rules copy-rules) {} #{'?x '?tag} nil)
            p (program/compile! runtime capture
                [{:variable '?x :type :data :nullable? false}
                 {:variable '?tag :type :keyword :nullable? false}] seeds)
            result (program/execute! runtime p)
            actual (map (fn [^objects row]
                          [(aget row (int (get (:attrs result) '?tag)))
                           (class (aget row (int (get (:attrs result) '?out))))]) (:tuples result))]
        (is (= {[:a Long] 2 [:b clojure.lang.BigInt] 1} (frequencies actual)))))))

(deftest required-literals-select-their-own-demand-partition
  (let [rules (into copy-rules
                '[[(selected [?seed] ?tag ?out) (copied 1 ?out) [(ground :long) ?tag]]
                  [(selected [?seed] ?tag ?out) (copied 1N ?out) [(ground :bigint) ?tag]]])
        result (query! '[:find ?tag ?out :in $rows % ?seed :where ($rows selected ?seed ?tag ?out)]
                 [[] rules :seed])
        by-tag (into {} result)]
    (is (= 2 (count result)))
    (is (instance? Long (:long by-tag)))
    (is (instance? clojure.lang.BigInt (:bigint by-tag))))
  (doseq [x [1N 1.00M [:nested 1N] {:k 1.00M}]]
    (let [result (query! {:find ['?out '.] :in ['$rows '%] :where [(list '$rows 'copied x '?out)]}
                   [[] copy-rules])]
      (is (= x result))
      (is (= (class x) (class result))))))

(deftest repeated-required-arguments-retain-the-required-representation
  (let [rules '[[(echo [?a] ?b) [(ground 1N) ?b]] [(echo [?a] ?b) (echo ?a ?b)]]
        result (query! '[:find ?a . :in $rows % ?a :where ($rows echo ?a ?a)] [[] rules 1])]
    (is (instance? Long result))
    (is (= 1 result)))
  (let [rules '[[(twice [?a ?b] ?out) [(identity ?a) ?out] [(= ?a ?b)]]
                [(twice [?a ?b] ?out) (twice ?a ?b ?out)]]]
    (is (= #{[1 1] [2 2]}
           (query! '[:find ?a ?out :in $rows % [?a ...] :where ($rows twice ?a ?a ?out)]
             [[] rules [1 2]])))))

(deftest repeated-required-keys-cannot-match-another-demand-by-numeric-equivalence
  (let [rules '[[(copied [?a ?b] ?out) [(identity ?b) ?out]]
                [(copied [?a ?b] ?out) (copied ?a ?b ?out)]
                [(selected [?seed] ?tag ?out) (copied 1 1N ?out) [(ground :cross) ?tag]]
                [(selected [?seed] ?tag ?out) [(ground 1) ?a] (copied ?a ?a ?out) [(ground :same) ?tag]]]
        result (query! '[:find ?tag ?out :in $rows % ?seed :where ($rows selected ?seed ?tag ?out)]
                 [[] rules :seed])]
    (is (= #{[:cross clojure.lang.BigInt] [:same Long]}
           (set (map (fn [[tag x]] [tag (class x)]) result))))))

(deftest required-values-are-owned-through-unions-and-caller-mutation
  (let [bytes (byte-array [1 2 3])
        rules '[[(copied [?x] ?out)
                 (or-join [?x ?out] [(identity ?x) ?out] (copied ?x ?out))]]
        result (query! '[:find ?out . :in $rows % ?x :where ($rows copied ?x ?out)]
                 [[] rules {:nested [bytes]}])]
    (aset-byte bytes 0 (byte 99))
    (is (= [1 2 3] (vec (first (:nested result)))))))

(deftest variable-functions-over-local-values-need-no-optional-head-binding
  (let [rules '[[(label ?e ?out) [?e ?local] [(identity ?local) ?out]]
                [(label ?e ?out) (label ?e ?out)]]]
    (is (= #{[1 :a] [2 :b]}
           (query! '[:find ?e ?out :in $rows % :where ($rows label ?e ?out)]
             [[[1 :a] [2 :b]] rules])))))

(deftest unsatisfied-required-inputs-and-ambiguous-literals-reject-before-acquisition
  (doseq [[rules call]
          [[copy-rules '(copied ?missing ?out)]
           [copy-rules '(copied _ ?out)]
           ['[[(left [?x] ?out) (right ?x ?out)] [(right [?x] ?out) (left ?x ?out)]]
            '(left ?missing ?out)]
           [(conj copy-rules '[(forwarded ?x ?out) (copied ?x ?out)])
            '(forwarded ?missing ?out)]
           ['[[(reach [?x] ?out) [?x ?out]] [(reach [?x] ?out) (reach ?x ?out)]]
            '(reach 1N ?out)]
           ['[[(reach [?x] ?out) [?x ?out]] [(reach [?x] ?out) (reach ?x ?out)]]
            '(reach 1.00M ?out)]]]
    (let [events (atom []) saved (atom nil)
          failure (error (fn [] (rt/with-runtime {}
                                 (fn [runtime]
                                   (reset! saved runtime)
                                   (program/query! runtime
                                     {:find ['?out] :in ['$rows '$s '%] :where [(apply list '$rows call)]}
                                     [[[1 2]] (fixture/provider events {}) rules])))))]
      (is (= :query-source/unsupported (:error failure)))
      (is (empty? (events-of events :acquire)))
      (is (empty? (events-of events :open)))
      (is (every? zero? (vals (control/usage (:control @saved)))))))
  (let [rules '[[(reach [?x] ?out) [?x ?out]] [(reach [?x] ?out) (reach ?x ?out)]]]
    (is (= #{[2]}
           (query! '[:find ?out :in $rows % :where ($rows reach 1 ?out)] [[[1 2]] rules])))))

(defn- with-db [f]
  (let [dir (str u/tmp-dir "spi-v2-seeded-rules-" (UUID/randomUUID))
        conn (d/get-conn dir {:edge {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}})]
    (try (d/transact! conn [{:db/id 1 :edge 2} {:db/id 2 :edge 3}]) (f conn)
         (finally (d/close conn) (u/delete-files dir)))))

(deftest required-native-recursion-exports-typed-free-heads-from-pinned-views
  (with-db
    (fn [conn]
      (let [events (atom []) delegate (fixture/provider events {:lookup-only? true})
            provider (reify source/IQuerySource
                       (-source-info [_] (source/-source-info delegate))
                       (-open-session [_ request] (source/-open-session delegate request))
                       (-plan-source [_ session request]
                         (d/transact! conn [{:db/id 3 :edge 4}])
                         (source/-plan-source delegate session request)))
            rules '[[(reach [?x] ?y) [?x :edge ?y]]
                    [(reach [?x] ?y) [?x :edge ?mid] (reach ?mid ?y)]]]
        (is (= #{[1 2 "two"]}
               (query! '[:find ?a ?b ?v :in $n $s % [?a ...]
                         :where ($n reach ?a ?b) [$s ?b :row/value ?v]] [@conn provider rules [1]])))
        (is (= [{:variable '?b :type :long :nullable? false}]
               (filterv #(= '?b (:variable %)) (:available-inputs (second (first (events-of events :plan)))))))
        (is (= 1 (count (events-of events :acquire))))
        (is (= #{[3 4]} (d/q '[:find ?a ?b :where [?a :edge ?b] [(= ?b 4)]] @conn)))))))

(deftest demand-storage-releases-within-a-fixed-long-chain-budget
  (doseq [[n budget rules]
          [[180 6000000 '[[(reach [?x] ?y) [?x ?y]]
                          [(reach [?x] ?y) (reach ?x ?mid) [?mid ?y]]]]
           [40 8000000 '[[(reach [?x] ?y) [?x ?y]]
                         [(reach [?x] ?y) [?x ?mid] (reach ?mid ?y)]]]]]
    (is (= (set (map vector (range 1 (inc n))))
           (query! '[:find ?to :in $rows % ?start :where ($rows reach ?start ?to)]
             [(mapv #(vector % (inc %)) (range n)) rules 0]
             {:limits {:retained-bytes budget}})))))

(deftest demand-registration-cancellation-releases-all-staging-and-body-storage
  (let [saved (atom nil) armed? (atom false) visits (atom 0)
        item (reify value/IQuerySourceValue
               (-logical-type [_] {:kind :custom :name :test/demand-key :version 1 :fingerprint "v1"})
               (-retained-bytes [_]
                 (when @armed?
                   (swap! visits inc)
                   (control/cancel! (:control @saved) {:error :query-source/cancelled}))
                 64))
        failure (error (fn [] (rt/with-runtime {}
                               (fn [runtime]
                                 (reset! saved runtime)
                                 (let [capture (planning/capture! runtime [{:symbol '$rows :native []}]
                                                 (parser/parse-query '[:find ?out :in $rows % ?x
                                                                       :where ($rows copied ?x ?out)])
                                                 (parser/parse-rules copy-rules) {} #{'?x} nil)
                                       p (program/compile! runtime capture
                                           [{:variable '?x :type :data :nullable? false}] [[item]])]
                                   (reset! armed? true)
                                   (program/execute! runtime p))))))]
    (is (= :query-source/cancelled (:error failure)))
    (is (pos? @visits))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest seeded-runtime-quota-failure-releases-staged-demands
  (let [saved (atom nil)
        failure (error (fn [] (rt/with-runtime {}
                               (fn [runtime]
                                 (reset! saved runtime)
                                 (let [capture (planning/capture! runtime [{:symbol '$rows :native []}]
                                                 (parser/parse-query '[:find ?out :in $rows % [?x ...]
                                                                       :where ($rows copied ?x ?out)])
                                                 (parser/parse-rules copy-rules) {} #{'?x} nil)
                                       p (program/compile! runtime capture
                                           [{:variable '?x :type :long :nullable? false}]
                                           (mapv vector (range 100)))
                                       ctl (:control runtime)
                                       remaining (- (:retained-bytes (control/limits ctl))
                                                    (:retained-bytes (control/usage ctl)))
                                       token (source/reserve! ctl {:retained-bytes (- remaining 50000)})]
                                   (try (program/execute! runtime p) (finally (source/release! token))))))))]
    (is (= :query-source/limit (:error failure)))
    (is (every? zero? (vals (control/usage (:control @saved)))))))
