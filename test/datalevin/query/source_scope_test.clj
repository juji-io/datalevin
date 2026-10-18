(ns datalevin.query.source-scope-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.parser :as parser]
            [datalevin.relation :as relation]
            [datalevin.rules.relation :as rule-relation]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.region :as region]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.scope :as scope]
            [datalevin.query.source.value :as value]
            [datalevin.query.source-lowering-test :as fixture])
  (:import [java.util Date Random AbstractList]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defn- column
  ([v t] (column v t false))
  ([v t nullable?] {:variable v :type t :nullable? nullable?}))

(defn- error [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(defn- compile!
  ([runtime query rules] (compile! runtime query rules {} #{}))
  ([runtime query rules bindings inputs]
   (region/annotate! runtime (parser/parse-query query)
     [{:symbol '$s :descriptor fixture/descriptor
       :source (reify source/IQuerySource
                 (-source-info [_] (throw (Exception. "unexpected descriptor call")))
                 (-open-session [_ _] (throw (Exception. "unexpected acquisition")))
                 (-plan-source [_ _ _] (throw (Exception. "unexpected planning"))))}
      {:symbol '$n :native []}]
     (parser/parse-rules rules) bindings inputs)))

(defn- child-id [branch] (if (map? branch) (:region-id branch) branch))

(defn- site-index [compiled kind]
  (first (keep-indexed #(when (= kind (:kind %2)) %1) (:sites compiled))))

(defn- branch-schema [compiled branch]
  (let [id (child-id branch)]
    {:region-id id :columns (->> (get-in compiled [:regions id :patterns])
                                (mapcat :columns) (reduce #(assoc %1 (:variable %2) %2) {}) vals vec)}))

(defn- interfaces! [runtime compiled kind parent]
  (let [index (site-index compiled kind)]
    (scope/interfaces! runtime compiled index parent
                       (mapv #(branch-schema compiled %) (get-in compiled [:sites index :branches])))))

(defn- rel [columns rows]
  (relation/relation! (zipmap columns (range)) (FastList. (mapv object-array rows))))

(defn- rows [rel] (mapv vec (:tuples rel)))

(deftest required-rule-inputs-translate-only-through-the-captured-arguments
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime
                       '[:find ?name ?private :in $s % ?person
                         :where [$s ?private :row/name ?tag] ($s person ?person ?name)]
                       '[[(person [?x] ?v) [?x :row/name ?v] [?x :row/age ?hidden]]]
                       {} #{'?person})
            boundary (interfaces! runtime compiled :rule
                                  [(column '?person :long) (column '?private :long) (column '?tag :string)])
            [branch] (:branches boundary)]
        (is (= [(column '?person :long)] (:inputs boundary)))
        (is (= [(column '?x :long)] (:inputs branch)))
        (is (= ['?x '?v] (:set-key branch)))
        (is (= ['?x '?v] (:projection branch)))
        (is (= [(column '?name :string) (column '?person :long)] (:columns boundary)))
        (is (not (.contains (pr-str (dissoc boundary :owner :close :closed?)) "?private")))
        (is (not (.contains (pr-str (dissoc boundary :owner :close :closed?)) "?hidden")))
        (is (= :set-union (:combine boundary)))
        (is (= :query-source/unsupported
               (:error (error #(interfaces! runtime compiled :rule [])))))))))

(deftest rule-branches-retain-precise-types-and-canonical-head-position
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e ?v :in $s % :where ($s choose ?e ?v)]
                               '[[(choose ?x ?a) [?x :row/age ?a]]
                                 [(choose ?entity ?name) [?entity :row/name ?name]]])
            boundary (interfaces! runtime compiled :rule [])
            [a b] (:branches boundary)
            output (scope/finish! runtime boundary
                     [{:region-id (:region-id b) :relation (rel ['?name '?entity] [["one" 1] ["one" 1]])}
                      {:region-id (:region-id a) :relation (rel ['?a '?x] [[7 1] [7 1]])}])]
        (is (= [(column '?e :long) (column '?v :data)] (:columns boundary)))
        (is (= [[:long :long] [:long :string]] (mapv #(mapv :type (:columns %)) [a b])))
        (is (= [['?x '?a] ['?entity '?name]] (mapv :set-key [a b])))
        (is (= #{[1 7] [1 "one"]} (set (rows output))))))))

(deftest branch-output-order-cannot-change-boundary-mapping-or-export-types
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e ?v :in $s % :where ($s choose ?e ?v)]
                               '[[(choose ?e ?v) [?e :row/age ?v]]
                                 [(choose ?x ?value) [?x :row/name ?value]]])
            index (site-index compiled :rule)
            schemas (mapv #(branch-schema compiled %) (get-in compiled [:sites index :branches]))
            a (scope/interfaces! runtime compiled index [] schemas)
            b (scope/interfaces! runtime compiled index [] (vec (reverse schemas)))]
        (is (= (:branches a) (:branches b)))
        (is (= (:columns a) (:columns b)))))))

(deftest nested-rule-interfaces-use-the-inner-calls-exported-variables
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?caller :in $s % :where ($s outer ?caller)]
                               '[[(outer ?x) (inner ?x)] [(inner ?e) [?e :row/name ?hidden]]])
            index-for (fn [name] (first (keep-indexed #(when (= name (get-in %2 [:node :name :symbol])) %1)
                                                                      (:sites compiled))))
            inner-index (index-for 'inner)
            inner (scope/interfaces! runtime compiled inner-index []
                    (mapv (partial branch-schema compiled) (get-in compiled [:sites inner-index :branches])))
            outer-index (index-for 'outer)
            outer-id (get-in compiled [:sites outer-index :branches 0 :region-id])
            outer (scope/interfaces! runtime compiled outer-index []
                    [{:region-id outer-id :columns (:columns inner)}])
            inner-result (scope/finish! runtime inner
                           [{:region-id (get-in inner [:branches 0 :region-id])
                             :relation (rel ['?e '?hidden] [[1 "one"] [1 "another witness"]])}])
            output (scope/finish! runtime outer [{:region-id outer-id :relation inner-result}])]
        (is (= [(column '?x :long)] (:columns inner)))
        (is (= [(column '?caller :long)] (:columns outer)))
        (is (= [[1]] (rows output)))))))

(deftest unions-preserve-nullability-and-values-from-every-branch
  (doseq [left [:long :string :data] right [:long :string :data]
          left-null? [false true] right-null? [false true]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [compiled (compile! runtime
                         '[:find ?v :in $s
                           :where (or-join [?v]
                                    (and [$s ?e :row/name _] [(identity nil) ?v])
                                    (and [$s ?e :row/age _] [(identity nil) ?v]))] [])
              index (site-index compiled :union)
              ids (get-in compiled [:sites index :branches])
              boundary (scope/interfaces! runtime compiled index []
                         [{:region-id (first ids) :columns [(column '?v left left-null?)]}
                          {:region-id (second ids) :columns [(column '?v right right-null?)]}])
              value-for (fn [t nullable?] (if nullable? nil (if (= :string t) "value" 7)))
              values [(value-for left left-null?) (value-for right right-null?)]
              output (scope/finish! runtime boundary
                       [{:region-id (first ids) :relation (rel ['?v] [[(first values)]])}
                        {:region-id (second ids) :relation (rel ['?v] [[(second values)]])}])]
          (is (= [(column '?v (if (= left right) left :data) (or left-null? right-null?))]
                 (:columns boundary)))
          (is (= (set (map vector values)) (set (rows output)))))))))

(deftest full-head-set-projection-precedes-wildcard-and-repeated-call-arguments
  (doseq [argument ['_ '?x]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [query {:find ['?x] :in ['$s '%] :where [(list '$s 'pair '?x argument)]}
              compiled (compile! runtime query '[[(pair ?a ?b) [?a :row/ref ?b]]])
              boundary (interfaces! runtime compiled :rule [])
              [branch] (:branches boundary)
              body (rel ['?b '?hidden '?a] [[1 :a 1] [1 :b 1] [2 :c 1] [2 :d 2] [4 :e 3]])
              output (scope/finish! runtime boundary [{:region-id (:region-id branch) :relation body}])]
          (is (= ['?a '?b] (:set-key branch)))
          (is (= #{'?x} (set (keys (:attrs output)))))
          (is (= (if (= '_ argument) #{[1] [2] [3]} #{[1] [2]}) (set (rows output))))
          (is (every? #(= 1 (alength ^objects %)) (:tuples output)))
          (when (= '_ argument)
            (is (not= '_ (second (:call-args boundary))))))))))

(deftest literal-and-known-scalar-rule-arguments-become-scoped-owned-seeds
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?v :in $s % :where ($s named 1 ?v)]
                               '[[(named [?e] ?v) [?e :row/name ?v]]])
            boundary (interfaces! runtime compiled :rule [])
            [branch] (:branches boundary)]
        (is (empty? (:inputs boundary)))
        (is (= [(column '?e :long)] (:inputs branch)))
        (is (= {'?e 1} (:bindings branch)))
        (is (= [["one"]]
               (rows (scope/finish! runtime boundary
                       [{:region-id (:region-id branch) :relation (rel ['?e '?v] [[1 "one"] [2 "two"]])}])))))))
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?v :in $s % ?e :where ($s named ?e ?v)]
                               '[[(named [?id] ?v) [?id :row/name ?v]]] {'?e 1} #{'?e})
            boundary (interfaces! runtime compiled :rule [(column '?e :long)])]
        (is (empty? (:inputs boundary)))
        (is (= {'?id 1} (get-in boundary [:branches 0 :bindings])))))))

(deftest incompatible-literal-arguments-prove-empty-without-reading-a-branch
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?v :in $s % :where ($s named "not-a-long" ?v)]
                               '[[(named [?e] ?v) [?e :row/name ?v]]])
            boundary (interfaces! runtime compiled :rule [])
            output (scope/finish! runtime boundary [])]
        (is (:empty? boundary))
        (is (empty? (rows output)))
        (is (= #{'?v} (set (keys (:attrs output)))))))))

(deftest union-interfaces-project-only-exports-and-combine-nullability-without-coercion
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime
                       '[:find ?e ?v ?private :in $s ?e
                         :where [$s ?private :row/name ?tag]
                         (or-join [?e ?v]
                           (and [$s ?e :row/age ?v] [$s ?e :row/name ?private])
                           (and [$s ?e :row/name ?v] [$s ?e :row/age ?private]))]
                       [] {} #{'?e})
            boundary (interfaces! runtime compiled :union [(column '?e :long) (column '?private :long)])
            [a b] (:branches boundary)
            output (scope/finish! runtime boundary
                     [{:region-id (:region-id a) :relation (rel ['?e '?v '?private] [[1 7 "a"] [1 7 "b"]])}
                      {:region-id (:region-id b) :relation (rel ['?e '?private '?v] [[1 1 "one"] [1 2 "one"]])}])]
        (is (= [(column '?e :long)] (:inputs boundary)))
        (is (every? #(= [(column '?e :long)] (:inputs %)) [a b]))
        (is (= [(column '?e :long) (column '?v :data)] (:columns boundary)))
        (is (= #{[1 7] [1 "one"]} (set (rows output))))))))

(deftest attribute-branches-restore-keywords-and-use-bag-union
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e ?a ?v :in $s :where [$s ?e ?a ?v]] [])
            boundary (interfaces! runtime compiled :attribute [])
            [age name ref tag] (:branches boundary)
            result (scope/finish! runtime boundary
                     [{:region-id (:region-id age) :relation (rel ['?e '?v] [[1 7] [1 7]])}
                      {:region-id (:region-id name) :relation (rel ['?e '?v] [[1 "one"]])}
                      {:region-id (:region-id ref) :relation (rel ['?e '?v] [[1 7]])}
                      {:region-id (:region-id tag) :relation (rel ['?e '?v] [[:tag/self :tag/self]])}])]
        (is (= :bag-union (:combine boundary)))
        (is (false? (:set-boundary? boundary)))
        (is (= [(column '?a :keyword) (column '?e :data) (column '?v :data)] (:columns boundary)))
        (is (= {:row/age 2 :row/name 1 :row/ref 1 :tag/self 1}
               (frequencies (map first (rows result)))))
        (is (every? nil? (map :set-key (:branches boundary))))))))

(deftest dispatch-narrows-only-the-routed-attribute-input
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e ?v :in $s ?a :where [$s ?e ?a ?v]] [] {} #{'?a})
            boundary (interfaces! runtime compiled :attribute [(column '?a :data true)])]
        (is (= [(column '?a :data true)] (:inputs boundary)))
        (is (every? #(= [(column '?a :keyword)] (:inputs %)) (:branches boundary)))
        (is (every? #(keyword? (get (:restore %) '?a)) (:branches boundary)))
        (is (= :query-source/unsupported (:error (error #(interfaces! runtime compiled :attribute [])))))))))

(deftest zero-column-attribute-branches-preserve-facts-and-rule-boundaries-project-existentially
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?bound :in $s ?bound :where [$s _ _ _]] [] {} #{'?bound})
            boundary (interfaces! runtime compiled :attribute [(column '?bound :long)])
            ids (mapv :region-id (:branches boundary))
            output (scope/finish! runtime boundary
                     (mapv (fn [i id] {:region-id id :relation (rel [] (repeat i []))}) (range) ids))]
        (is (empty? (:columns boundary)))
        (is (= 6 (count (rows output))))
        (is (every? empty? (rows output))))))
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?bound :in $s % ?bound :where ($s named _)]
                               '[[(named ?e) [?e :row/name _]]] {} #{'?bound})
            boundary (interfaces! runtime compiled :rule [(column '?bound :long)])
            id (get-in boundary [:branches 0 :region-id])
            output (scope/finish! runtime boundary [{:region-id id :relation (rel ['?e] [[1] [2] [1]])}])]
        (is (empty? (:columns boundary)))
        (is (= [[]] (rows output)))))))

(deftest a-routed-attribute-value-cannot-be-replaced-by-another-branchs-keyword
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e ?a ?v :in $s :where [$s ?e ?a ?v]] [])
            boundary (interfaces! runtime compiled :attribute [])
            results (mapv (fn [b]
                            {:region-id (:region-id b)
                             :relation (rel ['?e '?a '?v]
                                           (if (= :row/age (get-in b [:restore '?a]))
                                             [[1 :row/name 7]] []))}) (:branches boundary))
            before (control/usage (:control runtime))]
        (is (= :query-source/invalid (:error (error #(scope/finish! runtime boundary results)))))
        (is (= before (control/usage (:control runtime))))))))

(deftest rule-heads-cannot-be-pruned-just-because-the-call-uses-a-wildcard
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e :in $s % :where ($s pair ?e _)]
                               '[[(pair ?a ?b) [?a :row/ref ?b]]])
            index (site-index compiled :rule) id (get-in compiled [:sites index :branches 0 :region-id])]
        (is (= :query-source/unsupported
               (:error (error #(scope/interfaces! runtime compiled index []
                                  [{:region-id id :columns [(column '?a :long)]}])))))))))

(deftest schemas-and-results-cannot-cross-call-sites-or-include-foreign-branches
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?a ?b :in $s % :where ($s named ?a) ($s named ?b)]
                               '[[(named ?e) [?e :row/name _]]])
            indices (keep-indexed #(when (= :rule (:kind %2)) %1) (:sites compiled))
            [a b] (mapv #(scope/interfaces! runtime compiled % []
                           (mapv (partial branch-schema compiled) (get-in compiled [:sites % :branches]))) indices)
            before (control/usage (:control runtime))]
        (is (= :query-source/invalid
               (:error (error #(scope/finish! runtime a
                                 [{:region-id (get-in b [:branches 0 :region-id])
                                   :relation (rel ['?e] [[1]])}])))))
        (is (= before (control/usage (:control runtime))))
        (is (= :query-source/invalid
               (:error (error #(scope/interfaces! runtime compiled (first indices) []
                                 [(branch-schema compiled (first (get-in compiled [:sites (second indices) :branches])))])))))))))

(deftest finished-results-own-byte-and-mutable-values-after-producer-close
  (let [bytes (byte-array [1 2]) date (Date. 50) output (atom nil)]
    (rt/with-runtime {}
      (fn [runtime]
        (let [compiled (compile! runtime '[:find ?e ?v :in $s % :where ($s pair ?e ?v)]
                                 '[[(pair ?e ?v) [?e :row/ref ?v]]])
              index (site-index compiled :rule) id (get-in compiled [:sites index :branches 0 :region-id])
              boundary (scope/interfaces! runtime compiled index []
                         [{:region-id id :columns [(column '?e :data) (column '?v :data)]}])]
          (reset! output (scope/finish! runtime boundary [{:region-id id :relation (rel ['?e '?v] [[bytes date]])}])))))
    (aset-byte bytes 0 (byte 9))
    (.setTime date 99)
    (let [[[copied-bytes ^Date copied-date]] (rows @output)]
      (is (= [1 2] (vec copied-bytes)))
      (is (= 50 (.getTime copied-date))))))

(deftest returned-schema-names-remain-charged-after-compiler-and-boundary-close
  (rt/with-runtime {}
    (fn [runtime]
      (let [variable (symbol (str "?" (apply str (repeat 40000 "x"))))
            query {:find [variable] :in ['$s '%] :where [(list '$s 'named variable)]}
            compiled (compile! runtime query '[[(named ?e) [?e :row/name _]]])
            boundary (interfaces! runtime compiled :rule [])
            id (get-in boundary [:branches 0 :region-id])
            output (scope/finish! runtime boundary [{:region-id id :relation (rel ['?e] [[1]])}])]
        ((:close boundary))
        ((:close compiled))
        (is (= [[1]] (rows output)))
        (is (>= (:retained-bytes (control/usage (:control runtime)))
                (value/copy-charge (vec (keys (:attrs output))))))))))

(deftest malformed-branch-values-and-routing-violations-unwind-all-admissions
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e ?v :in $s % :where ($s named ?e ?v)]
                               '[[(named ?e ?v) [?e :row/name ?v]]])
            boundary (interfaces! runtime compiled :rule []) id (get-in boundary [:branches 0 :region-id])
            before (control/usage (:control runtime))]
        (doseq [body [(rel ['?e '?v] [[1 "okay"] [2 7]])
                      (rel ['?e] [[1]])
                      (relation/relation! {'?e 0 '?v 8} (FastList. [(object-array [1 "bad-index"])]))]]
          (is (= :query-source/invalid
                 (:error (error #(scope/finish! runtime boundary [{:region-id id :relation body}])))))
          (is (= before (control/usage (:control runtime)))))))))

(deftest scope-ownership-and-quota-failures-release-prefixes-immediately
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e :in $s % :where ($s named ?e)]
                               '[[(named ?e) [?e :row/name _]]])
            before (control/usage (:control runtime)) boundary (interfaces! runtime compiled :rule [])
            ctl (:control runtime) remaining (- (:retained-bytes (control/limits ctl)) (:retained-bytes (control/usage ctl)))
            held (control/reserve! ctl {:retained-bytes (- remaining 10000)})
            admitted (control/usage ctl) id (get-in boundary [:branches 0 :region-id])]
        (try
          (is (= :query-source/limit
                 (:error (error #(scope/finish! runtime boundary
                                  [{:region-id id :relation (rel ['?e] (mapv vector (range 100)))}])))))
          (is (= admitted (control/usage ctl)))
          (finally (control/release! held)))
        ((:close boundary))
        (is (= before (control/usage ctl)))
        (is (= :query-source/invalid (:error (error #(scope/check! runtime boundary)))))))))

(deftest empty-wide-heads-admit-their-attribute-maps-before-allocation
  (rt/with-runtime {}
    (fn [runtime]
      (let [variables (mapv #(symbol (str "?v" %)) (range 256))
            query {:find [(first variables)] :in ['$s '%] :where [(list* '$s 'wide variables)]}
            rules [[(list* 'wide variables) ['?entity :row/name '_]
                    [(list 'identity (vec (repeat 256 1))) variables]]]
            compiled (compile! runtime query rules)
            index (site-index compiled :rule) id (get-in compiled [:sites index :branches 0 :region-id])
            boundary (scope/interfaces! runtime compiled index []
                       [{:region-id id :columns (mapv #(column % :long) variables)}])
            ctl (:control runtime) remaining (- (:retained-bytes (control/limits ctl)) (:retained-bytes (control/usage ctl)))
            held (control/reserve! ctl {:retained-bytes (- remaining 20000)})
            before (control/usage ctl)]
        (try
          (is (= :query-source/limit
                 (:error (error #(scope/finish! runtime boundary
                                  [{:region-id id :relation (rel variables [])}])))))
          (is (= before (control/usage ctl)))
          (finally (control/release! held)))))))

(deftest compiled-and-runtime-ownership-remain-required-after-interface-capture
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e :in $s % :where ($s named ?e)]
                               '[[(named ?e) [?e :row/name _]]])
            boundary (interfaces! runtime compiled :rule [])]
        (rt/with-runtime {}
          (fn [other] (is (= :query-source/invalid (:error (error #(scope/check! other boundary)))))))
        ((:close compiled))
        (is (= :query-source/invalid (:error (error #(scope/check! runtime boundary)))))))))

(deftest shared-rule-argument-equality-supports-bytes-and-preserves-numeric-equivalence
  (let [a (byte-array [1 2]) b (byte-array [1 2])]
    (is (= 1 (count (rows (rule-relation/map-rule-result (rel ['?a '?b] [[a b]]) ['?a '?b] ['?x '?x])))))
    (is (= [[42]] (rows (rule-relation/map-rule-result (rel ['?a '?b] [[a 42]]) ['?a '?b] [b '?x]))))
    (is (= [[42]] (rows (rule-relation/map-rule-result (rel ['?a '?b] [[1N 42]]) ['?a '?b] [1 '?x]))))))

(deftest generated-rule-head-results-match-the-eager-query-engine
  (let [random (Random. 512893)
        query '[:find ?e :in $s % :where ($s pair ?e _)]
        rules '[[(pair ?e ?v) [?e :row/ref ?v]]]]
    (dotimes [_ 45]
      (let [facts (vec (distinct (repeatedly 15 #(vector (long (.nextInt random 5)) :row/ref
                                                       (long (.nextInt random 5))))))]
        (rt/with-runtime {}
          (fn [runtime]
            (let [compiled (compile! runtime query rules)
                  boundary (interfaces! runtime compiled :rule []) id (get-in boundary [:branches 0 :region-id])
                  body (rel ['?e '?v '?hidden]
                            (mapcat (fn [[e _ v]] [[e v :a] [e v :b]]) facts))
                  output (scope/finish! runtime boundary [{:region-id id :relation body}])]
              (is (= (d/q query facts rules) (set (rows output)))))))))))

(deftest cancellation-stops-before-reading-any-branch-value
  (let [read? (atom false) saved (atom nil)
        result (error (fn [] (rt/with-runtime {}
                         (fn [runtime]
                           (reset! saved runtime)
                           (let [compiled (compile! runtime '[:find ?e :in $s % :where ($s named ?e)]
                                                    '[[(named ?e) [?e :row/name _]]])
                                 boundary (interfaces! runtime compiled :rule [])
                                 id (get-in boundary [:branches 0 :region-id])
                                 body (relation/relation! {'?e 0}
                                        (proxy [AbstractList] []
                                          (size [] Integer/MAX_VALUE)
                                          (get [_] (reset! read? true) (object-array [1]))))]
                             (control/cancel! (:control runtime) {:error :query-source/cancelled :phase :execution})
                             (scope/finish! runtime boundary [{:region-id id :relation body}]))))))]
    (is (= :query-source/cancelled (:error result)))
    (is (false? @read?))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftype StopKey [id action hits]
  value/IQuerySourceValue
  (-logical-type [_] {:kind :custom :name :test/stop-key :version 1 :fingerprint "stop:v1"})
  (-retained-bytes [_] 64)
  Object
  (hashCode [_] (swap! hits inc) (action) (int id))
  (equals [_ other] (and (instance? StopKey other) (= id (.-id ^StopKey other)))))

(deftest cancellation-during-shared-projection-stops-before-hashing-the-next-row
  (let [hits (atom 0) second-hits (atom 0) saved (atom nil)
        result
        (error
          (fn []
            (rt/with-runtime {}
              (fn [runtime]
                (reset! saved runtime)
                (let [compiled (compile! runtime '[:find ?e :in $s % :where ($s named ?e)]
                                         '[[(named ?e) [?e :row/name _]]])
                      index (site-index compiled :rule) id (get-in compiled [:sites index :branches 0 :region-id])
                      boundary (scope/interfaces! runtime compiled index []
                                 [{:region-id id :columns [(column '?e :data)]}])
                      stop #(control/cancel! (:control runtime) {:error :query-source/cancelled :phase :projection})]
                  (scope/finish! runtime boundary
                    [{:region-id id :relation (rel ['?e] [[(StopKey. 1 stop hits)]
                                                         [(StopKey. 2 (fn []) second-hits)]])}]))))))]
    (is (= :query-source/cancelled (:error result)))
    (is (= 1 @hits))
    (is (zero? @second-hits))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest reentrant-boundary-close-during-shared-projection-unwinds-current-work
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e :in $s % :where ($s named ?e)]
                               '[[(named ?e) [?e :row/name _]]])
            before (control/usage (:control runtime))
            index (site-index compiled :rule) id (get-in compiled [:sites index :branches 0 :region-id])
            boundary (scope/interfaces! runtime compiled index []
                       [{:region-id id :columns [(column '?e :data)]}])
            hits (atom 0) key (StopKey. 1 (:close boundary) hits)]
        (is (= :query-source/invalid
               (:error (error #(scope/finish! runtime boundary [{:region-id id :relation (rel ['?e] [[key]])}])))))
        (is (= 1 @hits))
        (is (= before (control/usage (:control runtime))))))))
