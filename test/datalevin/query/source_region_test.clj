(ns datalevin.query.source-region-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.parser :as dp]
            [datalevin.core :as d]
            [datalevin.query.access :as access]
            [datalevin.query.plan :as plan]
            [datalevin.query.source :as source]
            [datalevin.query.source.access :as source-access]
            [datalevin.query.source.binding :as binding]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.cursor :as cursor]
            [datalevin.query.source.region :as region]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.session :as session]
            [datalevin.query.source-lowering-test :as fixture]))

(def ^:private descriptor
  (assoc fixture/descriptor :spi-versions #{2} :source-instance-id :regions :compute-context-id :local
         :definition-id :rows :definition-fingerprint "rows:v2" :features #{} :required-features #{}
         :scope {:id "scope:rows" :kind :full :parameters {}}
         :completion {:domain :finite :extent :all-matches} :read-capabilities #{:snapshot}))

(defn- entries []
  [{:symbol '$s :descriptor descriptor
    :source (reify source/IQuerySource
              (-source-info [_] (throw (Exception. "unexpected descriptor callback")))
              (-open-session [_ _] (throw (Exception. "unexpected acquisition")))
              (-plan-source [_ _ _] (throw (Exception. "unexpected planning"))))}
   {:symbol '$n :native []}])

(defn- error [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(defn- compile!
  ([runtime query] (compile! runtime query {}))
  ([runtime query {:keys [rules bindings input-vars] :or {rules [] bindings {} input-vars #{}}}]
   (region/annotate! runtime (dp/parse-query query) (entries) (dp/parse-rules rules) bindings input-vars)))

(defn- context [runtime]
  {:spi-version 2 :features #{} :query-id (:query-id runtime) :authorization {:principal-id :test}
   :consistency :snapshot :control (:control runtime) :limits (control/limits (:control runtime))})

(defn- request! [runtime r & [inputs]]
  (region/request! runtime r (first (entries)) (context runtime) (or inputs []) 32))

(defn- patterns [compiled]
  (mapcat :patterns (:regions compiled)))

(defn- kind [compiled k]
  (filterv #(= k (:kind %)) (:sites compiled)))

(deftest conjunctions-share-a-region-and-repeated-clauses-have-occurrence-identities
  (rt/with-runtime {}
    (fn [runtime]
      (let [q '[:find ?e ?name :in $s :where [$s ?e :row/name ?name] [$s ?e :row/name ?name]
                [(not= ?name "hidden provider predicate")]]
            normalized (dp/parse-query q)
            parsed (update normalized :qwhere #(into [(first %) (first %)] (rest %)))
            compiled (region/annotate! runtime parsed (entries) [] {} #{})
            [r] (:regions compiled)
            request (request! runtime r)]
        (is (= 1 (count (:regions compiled))))
        (is (= [0 1] (mapv (comp :id :clause) (:patterns r))))
        (is (= 1 (count (:core-clauses r))))
        (is (= #{'?e '?name} (:required-vars request)))
        (is (= 2 (count (:clauses request))))
        (is (every? #(= :pattern (:kind %)) (:clauses request)))
        (is (not (.contains (pr-str request) "hidden provider predicate")))
        (is (= parsed (:parsed compiled)))))))

(deftest variable-attribute-expansion-splits-only-its-access-node
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?a ?v ?name :in $s
                                        :where [$s ?e ?a ?v] [$s ?e :row/name ?name]])
            [root & children] (:regions compiled)
            [site] (kind compiled :attribute)
            requests (mapv #(request! runtime %) children)]
        (is (= [0 1 2 3 4] (mapv :region-id (:regions compiled))))
        (is (= [1 2 3 4] (:branches site)))
        (is (= [[:row/age] [:row/name] [:row/ref] [:tag/self]]
               (mapv #(mapv (comp second second :terms) (:clauses %)) requests)))
        (is (= 1 (count (:patterns root))))
        (is (every? #(= 1 (count (:clauses %))) requests))
        (is (= #{'?e '?v} (:required-vars (first requests))))
        (is (= #{'?e '?a '?v} (:exports site)))
        (is (= #{'?e '?name} (:input-vars (first children))))
        (is (= #{0 1 2 3 4} (set (map :region-id (cons (request! runtime root) requests)))))
        (is (= 5 (count (set (map (comp :id :clause) (patterns compiled))))))))))

(deftest scalar-attributes-stay-in-the-original-region-and-runtime-attributes-dispatch
  (rt/with-runtime {}
    (fn [runtime]
      (let [q '[:find ?v :in $s ?a :where [$s ?e ?a ?v]]
            known (compile! runtime q {:bindings {'?a :row/name} :input-vars #{'?a}})
            unknown (compile! runtime q {:input-vars #{'?a}})
            [r] (:regions known)
            request (request! runtime r)]
        (is (= 1 (count (:regions known))))
        (is (empty? (kind known :attribute)))
        (is (= {'?a :row/name} (get-in r [:patterns 0 :restore])))
        (is (= [:const :row/name] (get-in request [:clauses 0 :terms 1])))
        (is (not (contains? (:required-vars request) '?a)))
        (is (= :dispatch (:mode (first (kind unknown :attribute)))))
        (is (= #{'?a} (:required-inputs (first (kind unknown :attribute)))))))))

(deftest union-branch-exports-do-not-import-sibling-or-caller-hidden-variables
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime
                       '[:find ?e ?private :in $s ?seed
                         :where [$s ?private :row/age ?seed]
                         (or-join [?e]
                           (and [$s ?e :row/name ?private] [(= ?private "one")])
                           (and [$s ?e :row/name ?private] [(= ?private "two")]))]
                       {:bindings {'?seed 7} :input-vars #{'?seed}})
            [root left right] (:regions compiled)
            [site] (kind compiled :union)]
        (is (= #{'?e} (:exports site)))
        (is (= [1 2] (:branches site)))
        (is (empty? (:input-vars left)))
        (is (empty? (:bindings left)))
        (is (= 1 (count (:patterns root))))
        (is (= 1 (count (:core-clauses left)) (count (:core-clauses right))))
        (is (= [:const :row/name] (get-in (request! runtime left) [:clauses 0 :terms 1])))
        (is (not (.contains (pr-str (request! runtime left)) "two")))
        (is (= :query-source/invalid
               (:error (error #(request! runtime left [{:variable '?seed :type :long :nullable? false}])))))))))

(deftest rule-calls-preserve-head-set-projection-and-each-call-site-scope
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime
                       '[:find ?a ?b :in $s % :where ($s person ?a) ($s person ?b)]
                       {:rules '[[(person ?x) [?x :row/name ?hidden]]
                                 [(person ?x) [?x :row/age ?hidden]]]})
            sites (kind compiled :rule)
            children (rest (:regions compiled))]
        (is (= 2 (count sites)))
        (is (= 5 (count (:regions compiled))))
        (is (= [[0] [1]] (mapv :path sites)))
        (is (every? :set-boundary? sites))
        (is (= [#{'?a} #{'?b}] (mapv :exports sites)))
        (is (= [['?x] ['?x] ['?x] ['?x]] (mapv :head-vars (mapcat :branches sites))))
        (is (= [{'?x [:var '?a]} {'?x [:var '?b]}]
               (mapv #(get-in % [:branches 0 :arguments]) sites)))
        (is (every? #(= #{'?x} (:required-vars %)) children))
        (is (every? #(= #{'?x '?hidden} (:required-vars (request! runtime %))) children))
        (is (every? #(not-any? #{'?a '?b} (:input-vars %)) children))))))

(deftest nested-calls-translate-only-explicit-head-arguments-and-constants
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime
                       '[:find ?v :in $s % :where ($s outer :row/name ?v)]
                       {:rules '[[(outer ?attribute ?value) (inner ?attribute ?value)]
                                 [(inner ?a ?v) [1 ?a ?v]]]})
            sites (kind compiled :rule)
            inner (first (filter #(= 'inner (get-in % [:node :name :symbol])) sites))
            inner-region (nth (:regions compiled) (get-in inner [:branches 0 :region-id]))
            request (request! runtime inner-region)]
        (is (= 2 (count sites)))
        (is (= {'?a :row/name} (:bindings inner-region)))
        (is (= [:const :row/name] (get-in request [:clauses 0 :terms 1])))
        (is (= {'?a :row/name} (get-in inner-region [:patterns 0 :restore])))
        (is (= #{'?v} (:required-vars request)))
        (is (not (contains? (:bindings request) '?attribute)))))))

(deftest rule-head-wildcards-and-repeated-arguments-do-not-remove-the-set-key
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?x :in $s % :where ($s pair ?x _)]
                               {:rules '[[(pair ?a ?b) [?a :row/ref ?b]]]})
            [site] (kind compiled :rule)
            [branch] (:branches site)]
        (is (= ['?a '?b] (:head-vars branch)))
        (is (= {'?a [:var '?x] '?b [:ignore]} (:arguments branch)))
        (is (= #{'?a '?b} (:required-vars (nth (:regions compiled) (:region-id branch)))))))))

(deftest native-rules-remain-existing-core-nodes-including-recursion
  (rt/with-runtime {}
    (fn [runtime]
      (let [q '[:find ?e ?n :in $s $n %
                :where [$s ?e :row/name ?name] ($n reachable ?n)]
            compiled (compile! runtime q {:rules '[[(reachable ?x) [?x :next _]]
                                                   [(reachable ?x) [?x :next ?y] (reachable ?y)]]})
            [r] (:regions compiled)]
        (is (= 1 (count (:regions compiled))))
        (is (empty? (kind compiled :rule)))
        (is (= (second (:qwhere (dp/parse-query q))) (get-in r [:core-clauses 0 :node])))
        (is (= 1 (count (:clauses (request! runtime r)))))))))

(deftest source-overrides-propagate-through-rule-and-union-bodies
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e :in $s % :where ($s choose ?e)]
                               {:rules '[[(choose ?x) (or [?x :row/name "one"] [?x :row/name "two"])]]})]
        (is (= ['$s '$s] (mapv :source-symbol (patterns compiled))))
        (is (= 4 (count (:regions compiled))))
        (is (= 1 (count (kind compiled :union))))))))

(deftest undeclared-attributes-mark-empty-conjunctions-without-skipping-validation
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e :in $s
                                        :where [$s ?e :not/exposed _] [$s ?e :row/name _]])
            [r] (:regions compiled)]
        (is (:empty? r))
        (is (= 1 (count (:patterns r))))
        (is (= :undeclared-attribute (:proof (first (kind compiled :empty)))))
        (is (= :query-source/invalid (:error (error #(request! runtime r)))))))))

(deftest an-empty-parent-eliminates-descendant-access-work-before-provider-planning
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e ?v :in $s
                                        :where [$s ?e :missing/attribute _] [$s ?e _ ?v]])]
        (is (= 5 (count (:regions compiled))))
        (is (every? #(false? (:live? %)) (:regions compiled)))
        (is (every? #(= 0 (:parent-region %)) (rest (:regions compiled))))))))

(deftest invalid-recursive-external-or-foreign-source-graphs-release-prefixes
  (doseq [[query rules expected]
          [['[:find ?e :in $s % :where ($s cycle ?e)]
            '[[(cycle ?e) [?e :row/ref ?v] (cycle ?v)]] :query-source/unsupported]
           ['[:find ?e :in $s $missing :where [$s ?e :row/name _] [$missing ?e :row/age _]]
            [] :query-source/invalid]
           ['[:find ?e :in $s % :where ($s missing ?e)] [] :query-source/invalid]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [before (control/usage (:control runtime))
              result (error #(compile! runtime query {:rules rules}))]
          (is (= expected (:error result)))
          (is (= before (control/usage (:control runtime)))))))))

(deftest stable-identities-ignore-source-map-and-unrelated-rule-declaration-order
  (rt/with-runtime {}
    (fn [runtime]
      (let [q (dp/parse-query '[:find ?e :in $s % :where ($s person ?e) ($s person ?e)])
            rules '[[(person ?x) [?x _ _]] [(unused ?z) [?z :row/age _]]]
            a (region/annotate! runtime q (entries) (dp/parse-rules rules) {} #{})
            b (region/annotate! runtime q (vec (reverse (entries))) (dp/parse-rules (reverse rules)) {} #{})
            identity (fn [compiled]
                       (mapv (fn [r] [(:region-id r) (:path r)
                                      (mapv (fn [p] [(:source-symbol p) (:clause p)]) (:patterns r))])
                             (:regions compiled)))]
        (is (= (identity a) (identity b)))))))

(deftest planning-state-is-bounded-owned-and-closed-before-reuse
  (rt/with-runtime {:limits {:retained-bytes 17000}}
    (fn [runtime]
      (let [before (control/usage (:control runtime))]
        (is (= :query-source/limit
               (:error (error #(compile! runtime '[:find ?v :in $s :where [$s ?e _ ?v]])))))
        (is (= before (control/usage (:control runtime)))))))
  (rt/with-runtime {}
    (fn [runtime]
      (let [before (control/usage (:control runtime))
            compiled (compile! runtime '[:find ?e :in $s :where [$s ?e :row/name _]])
            [r] (:regions compiled)]
        ((:close compiled))
        (is (= before (control/usage (:control runtime))))
        (is (= :query-source/invalid (:error (error #(request! runtime r)))))))))

(deftest typed-request-exports-cannot-leak-private-compiler-or-context-data
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e :in $s :where [$s ?e :row/name ?name]])
            [r] (:regions compiled)
            request (region/request! runtime r (first (entries)) (assoc (context runtime) :private :do-not-send)
                                     [{:variable '?e :type :long :nullable? false}] 32)]
        (is (identical? descriptor (:source-descriptor request)))
        (is (every? #(not (contains? request %)) [:owner :path :patterns :core-clauses :private :close :restore]))
        (is (= [{:variable '?e :type :long :nullable? false}] (:available-inputs request)))
        (is (= :query-source/invalid
               (:error (error #(request! runtime r [{:variable '?hidden :type :long :nullable? false}])))))))))

(deftest request-exports-come-from-visible-patterns-and-shared-step-interfaces
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e ?name :in $s :where [$s ?e :row/name ?name]])
            [r] (:regions compiled)
            columns [{:variable '?seed :type :long :nullable? false}
                     {:variable '?private :type :string :nullable? false}]
            operator (source-access/core-operator! runtime 0 0 [] columns #{}
                       (plan/map->InitStep {:in #{} :cols ['?seed '?private]}))
            other (source-access/core-operator! runtime 0 1 []
                    [{:variable '?private :type :long :nullable? false}] #{}
                    (plan/map->InitStep {:in #{} :cols ['?private]}))
            inputs (source-access/available-inputs! runtime r [] [operator other])
            request (request! runtime r inputs)]
        (is (= [{:variable '?e :type :long :nullable? false}
                {:variable '?name :type :string :nullable? false}] inputs))
        (is (= inputs (:available-inputs request)))
        (is (not (.contains (pr-str request) "?private")))
        (is (not (.contains (pr-str request) "?seed")))
        (is (= :query-source/invalid
               (:error (error #(source-access/available-inputs! runtime r [(first inputs)] [])))))
        ((:close compiled))
        (is (= :query-source/invalid
               (:error (error #(source-access/available-inputs! runtime r [] [operator])))))))))

(deftest export-derivation-rolls-back-immediately-if-workspace-admission-fails
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e :in $s :where [$s ?e :row/name _]])
            [r] (:regions compiled) ctl (:control runtime)
            remaining (- (:retained-bytes (control/limits ctl)) (:retained-bytes (control/usage ctl)))
            held (control/reserve! ctl {:retained-bytes (- remaining 300)})
            before (control/usage ctl)]
        (try
          (is (= :query-source/limit
                 (:error (error #(source-access/available-inputs! runtime r [] [])))))
          (is (= before (control/usage ctl)))
          (finally (control/release! held)))))))

(deftest requests-cannot-substitute-another-captured-definition-or-invocation
  (rt/with-runtime {}
    (fn [runtime]
      (let [compiled (compile! runtime '[:find ?e :in $s :where [$s ?e :row/name _]])
            [r] (:regions compiled)
            foreign (assoc (first (entries)) :descriptor (assoc descriptor :definition-fingerprint "foreign"))
            before (control/usage (:control runtime))]
        (is (= :query-source/invalid
               (:error (error #(region/request! runtime r foreign (context runtime) [] 32)))))
        (is (= before (control/usage (:control runtime))))
        (rt/with-runtime {}
          (fn [other]
            (is (= :query-source/invalid (:error (error #(request! other r)))))))))))

(def ^:private facts
  [[1 :row/name "one"] [1 :row/age 7] [1 :row/ref 7] [1 :row/ref 8]
   [2 :row/name "two"] [2 :row/age 7]
   [:tag/self :tag/self :tag/self] [:other :tag/self :value]])

(defn- backend [events]
  (reify source/IQuerySource
    (-source-info [_] (throw (Exception. "use the captured descriptor")))
    (-open-session [_ _]
      (swap! events conj :acquire)
      (reify source/IQuerySourceSession
        (-session-info [_]
          {:spi-version 2 :features #{} :session-id :regions/session
           :source-instance-id :regions :compute-context-id :local
           :authorization-token :policy :consistency :snapshot :acquired-at-ms 0 :data-version :v1
           :consumption {:opens :many :max-open-cursors 1}
           :request-constraints {:max-concurrent-requests 1 :rate-limit nil}})
        (-open-cursor [_ open]
          (swap! events conj [:open (:region-id open)])
          (let [{:keys [request columns]} (:plan open)
                _ (is (= (:region-id request) (:region-id open)))
                _ (is (= (:source-symbol request) (:source-symbol open)))
                _ (is (= (:scope-id request) (:scope-id open)))
                _ (is (not (contains? open :bindings)))
                terms (get-in request [:clauses 0 :terms])
                pattern (mapv (fn [[tag x]] (case tag :const x :var x :ignore '_)) terms)
                rows (if (seq columns)
                       (vec (d/q {:find (mapv :variable columns) :where [pattern]} facts))
                       (into [] (comp (filter (fn [fact]
                                                (every? (fn [[[tag x] actual]]
                                                          (or (= :ignore tag) (= x actual)))
                                                        (map vector terms fact))))
                                      (map (constantly []))) facts))]
            (reify source/IQuerySourceCursor
              (-next-batch! [_ fetch]
                (let [bytes (source/batch-charge-bytes columns rows)]
                  (cond-> {:columns columns :rows rows :row-count (count rows) :byte-count bytes :exhausted? true}
                    (pos? bytes) (assoc :reservation (source/reserve! (:control fetch)
                                                     {:in-flight-batches 1 :retained-bytes bytes})))))
              (-cursor-metrics [_] {})
              (-close-cursor! [_] (swap! events conj :cursor-close)))))
        (-cancel-session! [_ _])
        (-close-session! [_] (swap! events conj :session-close))))
    (-plan-source [_ _ request]
      (swap! events conj [:plan (:region-id request)])
      (let [clause (first (:clauses request))
            _ (is (= 1 (count (:clauses request))))
            attr (get-in clause [:terms 1 1])
            types (get-in descriptor [:pattern-contract :attributes attr])
            columns (into [] (comp (filter #(= :var (ffirst %)))
                                   (map (fn [[[ _ v] t]] {:variable v :type t :nullable? false}))
                                   (distinct))
                          (map vector (:terms clause) [(:entity-type types) :keyword (:value-type types)]))]
        {:alternatives [{:id :scan :plan {:request request :columns columns}
                         :covers #{(:id clause)} :prefilters #{} :requires [] :columns columns
                         :scope-id (:scope-id request) :completion :all-bindings
                         :access {:kind :attribute-enumeration :anchor-clause (:id clause)}
                         :properties {:replay :unstable :ordering [] :blocking-state :none}
                         :constraints {:max-binding-rows 100 :max-binding-bytes 65536 :max-request-binding-rows 1}
                         :estimate nil :explain {:label "private backend"}}]}))))

(defn- read-lowered-facts! [runtime compiled events]
  (let [source (backend events) entry {:symbol '$s :source source :descriptor descriptor}
        [group] (session/acquire! runtime (session/prepare! runtime [entry] nil))
        ids (delay (binding/id-space! runtime))
        implementation (cursor/method runtime group ids)
        schedules
        (into []
              (for [r (:regions compiled) :when (and (seq (:patterns r)) (:live? r))
                    :let [request (region/request! runtime r entry (:context group) [] 32)
                          response (source/-plan-source source (:session group) request)
                          candidates (mapv #(source-access/candidate! runtime request % (:consumption group) implementation)
                                           (:alternatives response))
                          restore (get-in r [:patterns 0 :restore])
                          input-columns (mapv (fn [v] {:variable v :type :keyword :nullable? false}) (keys restore))]]
                {:region r :restore restore
                 :schedule (source-access/schedule! runtime (:region-id r) input-columns [request] candidates)}))]
    (source-access/validate-query! runtime (mapv :schedule schedules))
    (is (not-any? #(and (vector? %) (= :open (first %))) @events))
    (let [result
          (into []
                (mapcat (fn [{:keys [region restore schedule]}]
                          (let [p (first (:plans schedule))
                                input-cols (vec (keys restore))
                                step (plan/access-step (:expr p) (:path p) (access/complete-demand :exact nil)
                                                       (:bounds p) (:work p) input-cols)
                                output (plan/-execute step nil [(mapv restore input-cols)])
                                terms (get-in region [:patterns 0 :clause :terms])]
                            (map (fn [tuple]
                                   (let [row (zipmap (:cols step) (vec tuple))]
                                     (mapv (fn [[tag x]] (if (= :const tag) x (get row x))) terms))) output))))
                schedules)]
      (is (false? (realized? ids)))
      (is (= (count schedules) (count (filter #(and (vector? %) (= :open (first %))) @events))))
      (vec (distinct result)))))

(deftest compiled-attribute-accesses-through-shared-steps-agree-with-eager-eav-evaluation
  (doseq [query
          ['[:find ?e ?a ?v :in $s :where [$s ?e ?a ?v]]
           '[:find ?v :with ?a :in $s :where [$s ?e ?a ?v]]
           '[:find (count ?v) . :in $s :where [$s ?e _ ?v]]
           '[:find ?a ?v :in $s :where [$s ?a ?a ?v]]
           '[:find ?a :in $s :where [$s ?a ?a ?a]]
           '[:find ?v :in $s :where [$s ?e ?a ?v] [(not= ?a :row/name)]]
           '[:find ?e ?a ?b :in $s :where [$s ?e ?a ?v] [$s ?e ?b ?w]]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [compiled (compile! runtime query) events (atom [])
              lowered (read-lowered-facts! runtime compiled events)]
          (is (= (d/q query facts) (d/q query lowered)) (pr-str query)))))))
