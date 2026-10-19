(ns ^:no-doc datalevin.query.source.program
  "Compile captured v2 regions into shared, materialized plan steps. Scoped
  calls execute complete child regions once and join their projected results
  back to the original outer witnesses. No alternate Datalog interpreter."
  (:refer-clojure :exclude [run!])
  (:require [clojure.set :as set]
            [datalevin.join :as join]
            [datalevin.parser :as parser]
            [datalevin.query.access :as query-access]
            [datalevin.query.execute :as execute]
            [datalevin.query.execute.result :as result]
            [datalevin.query.plan :as plan]
            [datalevin.query.resolve :as resolve]
            [datalevin.query.source.access :as access]
            [datalevin.query.source.arithmetic :as arithmetic]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.inputs :as inputs]
            [datalevin.query.source.native :as native]
            [datalevin.query.source.native-plan :as native-plan]
            [datalevin.query.source.planning :as planning]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.scope :as scope]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.relation :as relation]
            [datalevin.spill :as spill])
  (:import [datalevin.parser Constant DefaultSrc FindColl FindRel FindScalar FindTuple Function Pattern Predicate Variable]
           [datalevin.utl QueryKey QueryKey$KeySet]
           [java.util AbstractList List]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defn- ensure! [ok] (when-not ok (fail! :query-source/invalid :execution)))
(defn- unsupported! [] (fail! :query-source/unsupported :planning))
(defn- fail-program! [runtime compilation error]
  (let [error (rt/redacted-error runtime (or (:phase (ex-data error)) :execution) error)]
    (control/cancel! (:control runtime) (select-keys (ex-data error) [:error :phase]))
    (try ((:close compilation))
         (catch Throwable secondary (.addSuppressed ^Throwable error secondary)))
    (throw error)))
(defn- columns-map [columns] (into {} (map (juxt :variable identity)) columns))
(defn- child-id [branch] (if (map? branch) (:region-id branch) branch))
(defn- canonical [variables check!]
  (vec (sort #(access/compare-keys %1 %2 check!) variables)))
(defn- column [v t nullable?] {:variable v :type t :nullable? nullable?})

(defn- merge-column [a b]
  (cond (nil? a) b
        (= (:type a) (:type b)) (update b :nullable? #(and % (:nullable? a)))
        (= :data (:type a)) b
        (= :data (:type b)) a
        :else (column (:variable b) :data (and (:nullable? a) (:nullable? b)))))

(defn- add-columns [schema columns]
  (reduce #(update %1 (:variable %2) merge-column %2) schema columns))

(defn- reserve! [runtime rows bytes]
  (let [ctl (:control runtime) maximum (:retained-bytes (control/limits ctl))
        attempted (+' (:retained-bytes (control/usage ctl)) bytes)]
    (control/check-active! ctl)
    (when (or (> rows Integer/MAX_VALUE) (> attempted maximum))
      (fail! :query-source/limit :reserve
             {:category :retained-bytes :maximum maximum :attempted attempted}))
    (control/reserve! ctl {:retained-bytes bytes})))

(defn- retain! [runtime token]
  (rt/own! runtime #(control/release! token)))

(defn- containers! [runtime rows width]
  (reserve! runtime rows (+' 2048 (*' 128 width) (*' rows (+' 192 (*' 32 width))))))

(defn- checked-list [check! ^List tuples]
  (proxy [AbstractList] []
    (size [] (check!) (.size tuples))
    (get [i] (check!) (.get tuples i))))

(defn- joined! [runtime check! left right]
  (join/hash-join-bounded left right
    {:check! check! :reserve! #(reserve! runtime %1 %2)
     :retain! #(retain! runtime %) :release! control/release!}))

(defrecord ^:private StepFactory [in cols build]
  plan/IStepFactory
  (-instantiate-step [_ input-cols] (build input-cols)))

(defrecord ^:private MaterializedStep [in cols execute]
  plan/IStep
  (-type [_] :scoped)
  (-execute [_ _ tuples] (execute tuples))
  (-execute-pipe [_ _ _ _] (unsupported!))
  (-sample [_ _ _] (unsupported!))
  (-explain [_ _] "Shared scoped operation"))

(defn- pure-spec [node schema check!]
  (let [args (:args node)
        variables (into #{} (keep #(when (instance? Variable %) (:symbol %))) args)
        ;; A local predicate consumes bindings, including nulls, before a fact
        ;; can narrow their schema. Possible fact outputs are not its inputs.
        inputs (mapv #(column % :data true) (canonical variables check!))]
    (cond
      (instance? Predicate node)
      (do (when-not (and (#{'= 'not= '< '<= '> '>= 'nil? 'some?} (get-in node [:fn :symbol]))
                        (every? #(or (instance? Variable %) (instance? Constant %)) args))
            (unsupported!))
          {:requires inputs
           :columns (if (and (= 'some? (get-in node [:fn :symbol])) (= 1 (count args))
                             (instance? Variable (first args)))
                      [(column (:symbol (first args)) :data false)] [])})
      (instance? Function node)
      (let [f (get-in node [:fn :symbol]) arg (first args)
            variable (get-in node [:binding :variable :symbol])]
        (when-not (and (#{'identity 'ground} f) (= 1 (count args)) variable
                      (or (instance? Variable arg) (instance? Constant arg))) (unsupported!))
        {:requires (if (instance? Variable arg)
                     [(assoc (or (schema (:symbol arg)) (column (:symbol arg) :data true)) :nullable? true)] inputs)
         :columns [(if (instance? Constant arg)
                     (column variable (value/inferred-type (:value arg)) false)
                     (assoc (or (schema (:symbol arg)) (column variable :data true))
                            :variable variable :nullable? false))]})
      :else (unsupported!))))

(defn- pure-factory [runtime check! node spec literal-output]
  (let [exports (mapv :variable (:columns spec))]
    (->StepFactory (set (map :variable (:requires spec))) exports
      (fn [input-cols]
        (let [output-cols (into input-cols (remove (set input-cols)) exports)
              attrs (zipmap input-cols (range))
              form (parser/source node)
              temporary (when literal-output
                          (loop []
                            (check!)
                            (let [v (gensym "?literal_result__")]
                              (if (contains? attrs v) (recur) v))))
              forms (if temporary
                      ;; Keep function nil filtering and predicate equality in
                      ;; the existing resolver. The temporary never crosses
                      ;; this scalar step's public column layout.
                      [(assoc form 1 temporary) [(list '= literal-output temporary)]]
                      [form])]
          (->MaterializedStep (set input-cols) output-cols
            (fn [tuples]
              (check!)
              (let [^List tuples tuples
                    token (containers! runtime (if temporary (*' 2 (.size tuples)) (.size tuples))
                                       (+ (count output-cols) (if temporary 1 0)))
                    success? (volatile! false)]
                (try
                  ;; These admitted scalar operators cannot decode a source,
                  ;; expand a collection, or allocate arithmetic workspaces.
                  (let [context {:rels [(relation/relation! attrs (checked-list check! tuples))]
                                 :sources {} :rules {}}
                        resolved (reduce resolve/resolve-clause context forms)
                        result (reduce #(joined! runtime check! %1 %2) (:rels resolved))
                        ^List rows (:tuples result)
                        output (FastList. (.size rows))]
                    (dotimes [i (.size rows)]
                      (check!)
                      (let [^objects row (.get rows i)]
                        (.add output (object-array (mapv #(aget row (int (get (:attrs result) %))) output-cols)))))
                    (retain! runtime token)
                    (vreset! success? true)
                    output)
                  (finally (when-not @success? (control/release! token))))))))))))

(defn- branch-input! [runtime check! boundary branch input-cols ^List tuples]
  (let [columns (:inputs branch) width (count columns)
        token (containers! runtime (.size tuples) width)
        success? (volatile! false)]
    (try
      (let [seen (QueryKey$KeySet.) output (FastList.) attrs (zipmap input-cols (range))]
        (dotimes [i (.size tuples)]
          (check!)
          (let [^objects outer (.get tuples i)
                routed? (every? (fn [[v x]]
                                  (or (not (contains? attrs v))
                                      (QueryKey/equals x (aget outer (int (attrs v)))))) (:restore branch))]
            (when routed?
              (let [values
                    (mapv (fn [{:keys [variable type nullable?]}]
                            (check!)
                            (let [[tag actual] (get (:arguments branch) variable)
                                  source (if (= :rule (:kind boundary)) actual variable)
                                  x (if (contains? (:bindings branch) variable)
                                      (get (:bindings branch) variable)
                                      (do (ensure! (and (or (not= :rule (:kind boundary)) (= :var tag))
                                                       (contains? attrs source)))
                                          (aget outer (int (attrs source)))))]
                              (ensure! (if (nil? x) nullable? (value/matches-type? type x check!))) x)) columns)]
                (when (.add seen values) (.add output (object-array values)))))))
        (retain! runtime token)
        (vreset! success? true)
        (relation/relation! (zipmap (map :variable columns) (range)) output))
      (finally (when-not @success? (control/release! token))))))

(defn- scope-factory [runtime check! boundary run-region]
  (->StepFactory (set (map :variable (:inputs boundary))) (mapv :variable (:columns boundary))
    (fn [input-cols]
      (let [output-cols (into input-cols (remove (set input-cols)) (map :variable (:columns boundary)))]
        (->MaterializedStep (set input-cols) output-cols
          (fn [tuples]
            (check!)
            (scope/check! runtime boundary)
            (let [results (mapv (fn [branch]
                                  (let [input (branch-input! runtime check! boundary branch input-cols tuples)]
                                    {:region-id (:region-id branch)
                                     :relation (run-region (:region-id branch) input)}))
                                (filterv (complement :empty?) (:branches boundary)))
                  result (scope/finish! runtime boundary results)
                  outer (relation/relation! (zipmap input-cols (range)) tuples)
                  result (joined! runtime check! outer result)]
              (ensure! (= output-cols (mapv key (sort-by val (:attrs result)))))
              (:tuples result))))))))

(defn- schemas [compiled native-specs root-columns check!]
  (reduce
    (fn [schemas r]
      (let [id (:region-id r)
            initial (if (= id (:root-region compiled)) (columns-map root-columns)
                        (into {} (map #(vector % (column % :data true))) (:input-vars r)))
            fact-columns (concat (mapcat :columns (:patterns r))
                                 (mapcat #(get-in native-specs [(:path %) :columns]) (:core-clauses r)))
            produced (set (map :variable fact-columns))
            initial (add-columns initial fact-columns)
            ;; A known constraint does not change the type of a body producer.
            ;; Its compatibility is checked at the captured scope boundary.
            initial (reduce-kv (fn [schema v x]
                                 (if (contains? produced v) schema
                                     (assoc schema v (column v (value/inferred-type x) (nil? x)))))
                               initial (:bindings r))
            initial
            (reduce
              (fn [schema site]
                (let [branches
                      (mapv (fn [branch]
                              (let [body (get schemas (child-id branch))]
                                (if (= :rule (:kind site))
                                  (reduce (fn [m [v [tag actual]]]
                                            (if (= :var tag)
                                              (update m actual merge-column
                                                      (assoc (or (body v) (column v :data true)) :variable actual)) m))
                                          {} (:arguments branch))
                                  (add-columns body
                                    (map (fn [[v _]] (column v :keyword false))
                                         (get-in compiled [:regions (child-id branch) :patterns 0 :restore]))))))
                            (filter #(get-in compiled [:regions (child-id %) :live?]) (:branches site)))
                      outputs
                      (mapv (fn [v]
                              (let [cs (mapv #(or (get % v) (get schema v) (column v :data true)) branches)]
                                (column v (if (and (seq cs) (apply = (map :type cs))) (:type (first cs)) :data)
                                        (boolean (some :nullable? cs))))) (canonical (:exports site) check!))]
                  (add-columns schema outputs))) initial
              (filter #(= id (:parent-region %)) (:sites compiled)))
            [final _ constraints]
            (reduce (fn [[schema produced constraints] {:keys [node path]}]
                      (check!)
                      (let [columns (:columns (if (instance? Pattern node) (get native-specs path)
                                                  (pure-spec node schema check!)))]
                        [(reduce (fn [schema column]
                                   (let [v (:variable column)]
                                     (cond (contains? (:literal-vars r) v) schema
                                           (and (contains? (:bindings r) v) (not (contains? produced v)))
                                           (assoc schema v column)
                                           :else (add-columns schema [column])))) schema columns)
                         (into produced (map :variable) columns)
                         (into constraints (filter #(contains? (:literal-vars r) (:variable %))) columns)]))
                    [initial produced (vec (mapcat #(get-in native-specs [(:path %) :constant-constraints])
                                                  (:core-clauses r)))] (:core-clauses r))]
        (assoc schemas id (with-meta final {:constant-constraints constraints})))) {} (reverse (:regions compiled))))

(defn- branch-output [id schema]
  {:region-id id :columns (vec (vals schema))
   :constant-constraints (:constant-constraints (meta schema))})

(defn- scope-parent-columns [site schema known-inputs child-schemas check!]
  (let [needed
        (if (= :rule (:kind site))
          (into (set/intersection (:exports site) (set (map :variable known-inputs)))
                (mapcat (fn [branch]
                          (keep (fn [v]
                                  (let [[tag actual] (get (:arguments branch) v)]
                                    (when (= :var tag) actual))) (:required-inputs branch)))) (:branches site))
          (set/union (:required-inputs site)
                     (set/intersection (:exports site) (set (map :variable known-inputs)))
                     (into #{} (mapcat #(set/difference (:exports site) (set (keys (get child-schemas (child-id %))))))
                           (:branches site))))]
    (mapv #(or (schema %) (column % :data true)) (canonical needed check!))))

(defrecord ^:private Program [runtime compilation planned root input regions state runner])

(defn- terminal-query! [compiled]
  (let [parsed (:parsed compiled) constants (get-in compiled [:regions (:root-region compiled) :bindings])]
    (when (or (seq (:qorder parsed)) (seq (:qhaving parsed))) (unsupported!))
    (doseq [element (parser/find-elements (:qfind parsed))]
      (when-not (or (instance? Variable element)
                    (and (parser/aggregate? element)
                         (#{'count 'sum 'avg 'min 'max} (get-in element [:fn :symbol]))
                         (= 1 (count (:args element))) (instance? Variable (first (:args element)))))
        (unsupported!)))
    (when (and (not (instance? FindRel (:qfind parsed))) (or (:qlimit parsed) (:qoffset parsed)))
      (unsupported!))
    (reduce (fn [parsed key]
              (let [x (get parsed key) x (if (symbol? x) (get constants x ::missing) x)]
                (ensure! (or (nil? x) (= -1 x)
                             (and (integer? x) (<= (if (= key :qlimit) 1 0) x Long/MAX_VALUE))))
                (assoc parsed key x))) parsed [:qlimit :qoffset])))

(defn- input! [runtime check! compiled input-columns input-tuples]
  (let [root (:root-region compiled) constants (get-in compiled [:regions root :bindings])
        supplied (columns-map input-columns)
        columns (into input-columns
                  (map (fn [v] (column v (value/inferred-type (get constants v)) (nil? (get constants v)))))
                  (canonical (set/difference (set (keys constants)) (set (keys supplied))) check!))
        _ (ensure! (set/subset? (set (map :variable columns))
                               (get-in compiled [:regions root :input-vars])))
        tuples (FastList.)
        source (if (nil? input-tuples) (do (ensure! (empty? input-columns)) [[]]) input-tuples)
        input-vars (mapv :variable input-columns) read-item! (inputs/item-reader source)]
    (loop []
      (check!)
      (when-let [[tuple] (read-item!)]
        (retain! runtime (containers! runtime 1 (count columns)))
        (let [row (inputs/tuple-row (:control runtime) input-vars tuple)
              values (mapv (fn [{:keys [variable type nullable?]}]
                             (let [x (if (contains? row variable) (get row variable) (get constants variable))]
                               (ensure! (if (nil? x) nullable? (value/matches-type? type x check!))) x)) columns)]
          (retain! runtime (rt/charge-copy! runtime values))
          (.add tuples (object-array (value/owned-copy values check!))))
        (recur)))
    {:columns columns :relation (relation/relation! (zipmap (map :variable columns) (range)) tuples)}))

(defn- boundaries! [runtime check! compiled native-specs root-columns]
  (let [all-schemas (schemas compiled native-specs root-columns check!) boundaries (volatile! [])]
    (letfn [(region! [id inputs]
              (check!)
              (let [schema (add-columns (get all-schemas id) inputs)]
                (doseq [[index site] (map-indexed vector (:sites compiled))
                        :when (and (= id (:parent-region site)) (seq (:branches site)))]
                  (let [parent (scope-parent-columns site schema inputs all-schemas check!)
                        outputs (mapv (fn [b] (branch-output (child-id b) (get all-schemas (child-id b)))) (:branches site))
                        boundary (scope/interfaces! runtime compiled index parent outputs)]
                    (vswap! boundaries conj boundary)
                    (doseq [branch (:branches boundary) :when (not (:empty? branch))]
                      (region! (:region-id branch) (:inputs branch)))))))]
      (when (get-in compiled [:regions (:root-region compiled) :live?])
        (region! (:root-region compiled) root-columns))
      @boundaries)))

(defn- compile-program!
  "Compile captured regions and real typed input tuples into executable shared
  steps. This is the revised internal compiler, not a caller-supplied interface
  fixture. Scalar predicates/functions and positive rule/union/attribute sites
  use existing engine operators. Unsupported core operators reject before
  acquisition. Native initializations use the existing planner and the explicit
  controlled storage boundary within the caller's pinned views."
  [runtime compilation input-columns input-tuples snapshots]
  (ensure! (and (identical? (:control runtime) (get-in compilation [:runtime :control]))
                (= (:query-id runtime) (get-in compilation [:runtime :query-id]))))
  (let [runtime (:runtime compilation) compiled (:compiled compilation)
        root (:root-region compiled) runner (atom nil) state (atom :compiling)
        check! #(do (control/check-active! (:control runtime))
                    (ensure! (not= :closed @state)))
        run-region (fn [id input] (check!) (ensure! @runner) (@runner id input))
        _ (rt/own! runtime #(do (reset! state :closed) (reset! runner nil)))
        _ (terminal-query! compiled)
        entries (into {} (map (juxt :symbol identity)) (:entries compilation))
        native-specs
        (into {} (for [r (:regions compiled) {:keys [node path literal-terms]} (:core-clauses r)
                       :when (instance? Pattern node)
                       :let [source (:source node)
                             source (if (or (nil? source) (instance? DefaultSrc source))
                                      (:default-source r) (:symbol source))
                             database (:native (get entries source))]]
                   [path (assoc (native-plan/spec! runtime snapshots database node literal-terms) :database database)]))
        input (input! runtime check! compiled input-columns input-tuples)
        root-columns (:columns input)
        proofs (boundaries! runtime check! compiled native-specs root-columns)
        compilation (planning/refine! runtime compilation proofs)
        _ (doseq [proof proofs] ((:close proof)))
        tables (volatile! nil)
        planned
          (planning/plan! runtime compilation
            (fn [runtime compiled]
                (let [all-schemas (schemas compiled native-specs root-columns check!)
                      interfaces (atom {})]
                  (letfn [(region! [id inputs]
                            (check!)
                            (let [r (nth (:regions compiled) id)
                                  schema (add-columns (get all-schemas id) inputs)
                                  core (volatile! {}) operators (volatile! [])
                                  add! (fn [path requires columns step]
                                         (let [op-id (count @operators)
                                               op (access/core-operator! runtime id op-id requires columns #{} step)]
                                           (vswap! core assoc path op-id) (vswap! operators conj op)))]
                              (doseq [{:keys [node path]} (:core-clauses r)]
                                (let [native? (instance? Pattern node)
                                      spec (if native? (get native-specs path) (pure-spec node schema check!))
                                      output (get-in node [:binding :variable :symbol])
                                      literal-output (when (contains? (:literal-vars r) output) output)
                                      spec (if literal-output
                                             (-> spec
                                                 (update :requires #(conj (filterv (fn [c] (not= literal-output (:variable c))) %)
                                                                           (column literal-output :data true)))
                                                 (assoc :columns [])) spec)
                                      factory (if native?
                                                (native-plan/factory! runtime snapshots (:database spec) node spec
                                                  (:core-clauses r) check! #(joined! runtime check! %1 %2))
                                                (pure-factory runtime check! node spec literal-output))]
                                  (add! path (:requires spec) (:columns spec) factory)))
                              (doseq [[index site] (map-indexed vector (:sites compiled))
                                      :when (= id (:parent-region site))]
                                (let [parent (scope-parent-columns site schema inputs all-schemas check!)
                                      branch-outputs
                                      (mapv (fn [b]
                                              (let [id (child-id b)]
                                                (branch-output id (get all-schemas id)))) (:branches site))
                                      boundary (scope/interfaces! runtime compiled index parent branch-outputs)]
                                  (doseq [branch (:branches boundary) :when (not (:empty? branch))]
                                    (region! (:region-id branch) (:inputs branch)))
                                  (add! (:path site) (:inputs boundary) (:columns boundary)
                                        (scope-factory runtime check! boundary run-region))))
                              (swap! interfaces assoc id {:inputs inputs :operators @operators :core @core})))]
                    (when (:live? (nth (:regions compiled) root)) (region! root root-columns))
                    (vreset! tables @interfaces)
                    @interfaces))))
          schedules (planning/schedules runtime planned)
          executable
          (into {}
                (map (fn [schedule]
                       (let [id (:region-id schedule) inputs (get-in @tables [id :inputs])
                             [cols steps]
                             (reduce (fn [[cols steps] {:keys [kind value]}]
                                       (let [step (if (= :access kind)
                                                    (plan/access-step (:expr value) (:path value)
                                                      (query-access/complete-demand :exact nil)
                                                      (:bounds value) (:work value) cols)
                                                    (let [step (:step value)]
                                                      (if (satisfies? plan/IStepFactory step)
                                                        (plan/-instantiate-step step cols) step)))]
                                         [(:cols step) (conj steps step)]))
                                     [(mapv :variable inputs) []] (:operators schedule))]
                         [id {:inputs (mapv :variable inputs) :cols cols :steps steps}]))) schedules)]
      (reset! runner
        (fn [id input]
          (check!)
          (let [{:keys [inputs cols steps]} (get executable id)]
            (ensure! (and inputs (= inputs (mapv key (sort-by val (:attrs input))))))
            (let [tuples (reduce (fn [tuples step]
                                   (check!)
                                   (let [result (plan/-execute step nil tuples)] (check!) result))
                                 (:tuples input) steps)]
              (relation/relation! (zipmap cols (range)) tuples)))))
      (reset! state :ready)
      (->Program runtime compilation planned root (:relation input) executable state runner)))

(defn compile!
  "Compile a captured region graph and typed input rows into shared steps.
  All failed construction, including final step assembly, unwinds immediately."
  ([runtime compilation input-columns input-tuples]
   (compile! runtime compilation input-columns input-tuples {}))
  ([runtime compilation input-columns input-tuples snapshots]
   (ensure! (and (identical? (:control runtime) (get-in compilation [:runtime :control]))
                 (= (:query-id runtime) (get-in compilation [:runtime :query-id]))))
   (try (compile-program! runtime compilation input-columns input-tuples snapshots)
        (catch Throwable e (fail-program! (:runtime compilation) compilation e)))))

(defn execute!
  "Execute a compiled region graph once, with no replay of its external paths.
  Return an owned internal Relation; public result shaping runs before cleanup."
  [runtime program]
  (ensure! (and (instance? Program program)
                (identical? (:control runtime) (get-in program [:runtime :control]))
                (= (:query-id runtime) (get-in program [:runtime :query-id]))
                (= :planned @(get-in program [:compilation :state]))
                (compare-and-set! (:state program) :ready :running)))
  (control/check-active! (:control runtime))
  (try
    (if (get (:regions program) (:root program))
      (@(:runner program) (:root program) (:input program))
      (relation/relation! {} (FastList.)))
    (catch Throwable e (fail-program! (:runtime program) (:compilation program) e))
    (finally (compare-and-set! (:state program) :running :finished))))

(defn result!
  "Complete a compiled query through the shared collector, aggregates, windows
  and return-shape processor. Realize all lazy work and own public values before
  the invocation closes. No external rows or session plans enter native caches."
  [runtime program]
  (ensure! (and (instance? Program program)
                (identical? (:control runtime) (get-in program [:runtime :control]))
                (= (:query-id runtime) (get-in program [:runtime :query-id]))))
  (try
   (let [rel (execute! runtime program) runtime (:runtime program)
        parsed (terminal-query! (get-in program [:compilation :compiled]))
        find (:qfind parsed)
        all-vars (into (vec (parser/find-vars find)) (map :symbol) (:qwith parsed))
        check! #(control/check-active! (:control runtime))
        ^List tuples (:tuples rel) n (.size tuples) width (count all-vars)
        ;; Projection, set indexes, group keys/lists, return maps and window
        ;; vectors are all sized before invoking their shared constructors.
        token (reserve! runtime n (+' 8192 (*' 512 width) (*' n (+' 4096 (*' 128 width)))))
        _ (retain! runtime token)
        collected (spill/new-spillable-set nil {:spill-threshold Long/MAX_VALUE :initial-capacity n})
        context {:rels [(assoc rel :tuples (checked-list check! tuples))] :sources {} :rules {}}
        _ (doseq [^objects tuple (result/-collect context all-vars)]
            (check!) (.cons ^clojure.lang.IPersistentCollection collected (vec tuple)))
        context (assoc context :result-set collected :terminal-collected-symbols all-vars)
        scalar-row? (volatile! false)
        post-process
        (fn [find return-map tuples]
          (check!)
          (when (instance? FindScalar find) (vreset! scalar-row? (boolean (seq tuples))))
          (cond
            (and return-map (instance? FindRel find))
            (result/tuples->return-map return-map tuples {:spill-threshold Long/MAX_VALUE})
            (and return-map (instance? FindTuple find))
            (first (result/tuples->return-map return-map [(first tuples)] {:spill-threshold Long/MAX_VALUE}))
            :else (execute/-post-process find return-map tuples)))
        process (execute/result-processor parsed
                  {:check! check! :wrap-aggregate #(arithmetic/wrap-aggregate runtime %1 %2)
                   :post-process post-process})
        raw (process parsed context)
        own-row! (fn [row]
                   (check!)
                   (retain! runtime (rt/charge-result! runtime row))
                   (retain! runtime (rt/charge-copy! runtime row))
                   (value/owned-copy row check!))]
    (check!)
    (cond
      (instance? FindScalar find) (if @scalar-row? (first (own-row! [raw])) nil)
      (instance? FindTuple find) (when (some? raw) (own-row! raw))
      (instance? FindColl find) (mapv #(first (own-row! [%])) raw)
      (set? raw) (into (spill/new-spillable-set nil {:spill-threshold Long/MAX_VALUE :initial-capacity n})
                      (map own-row!) raw)
      :else (mapv own-row! raw)))
   (catch Throwable e (fail-program! (:runtime program) (:compilation program) e))))

(defn run!
  "Compile and finish a captured query inside native read views pinned for the
  entire invocation. No borrowed native context may escape this callback."
  [runtime compilation input-columns input-tuples]
  (ensure! (and (identical? (:control runtime) (get-in compilation [:runtime :control]))
                (= (:query-id runtime) (get-in compilation [:runtime :query-id]))))
  (try
    (native/with-views (keep :native (:entries compilation))
      (fn [snapshots]
        (result! runtime (compile! runtime compilation input-columns input-tuples snapshots))))
    (catch Throwable e (fail-program! (:runtime compilation) compilation e))))
