(ns ^:no-doc datalevin.query.source.native-rules
  "Compile positive native rule bodies with the existing region compiler.
  Rule references read completed invocation-owned relations; the shared
  fixpoint kernel supplies iteration, ownership and set semantics."
  (:require [datalevin.parser :as parser]
            [datalevin.query.source.access :as access]
            [datalevin.query.source.collection :as collection]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.recursion :as recursion]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.relation :as relation]
            [datalevin.rules.relation :as rule-relation])
  (:import [datalevin.parser And Constant DefaultSrc Function Not Or Pattern RuleExpr Variable]
           [java.util AbstractList List]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic ^:private *iteration* nil)

(defn source-symbol [node inherited]
  (let [source (:source node)]
    (if (or (nil? source) (instance? DefaultSrc source)) inherited (:symbol source))))

(defn- head-vars [branch]
  (mapv :symbol (concat (get-in branch [:vars :required]) (get-in branch [:vars :free]))))

(defn- variables [node]
  (vec (distinct (keep #(when (instance? Variable %) (:symbol %)) (:args node)))))

(defn- retain! [runtime token]
  (rt/own! runtime #(control/release! token)))

(defn- containers! [runtime rows width]
  (when (> rows Integer/MAX_VALUE) (fail! :query-source/limit :reserve))
  (retain! runtime
    (control/reserve! (:control runtime)
      {:retained-bytes (+' 2048 (*' 128 width) (*' rows (+' 512 (*' 64 width))))})))

(defn- checked [check! rel]
  (let [^List tuples (:tuples rel)]
    (assoc rel :tuples (proxy [AbstractList] []
                         (size [] (check!) (.size tuples))
                         (get [i] (check!) (.get tuples i))))))

(defn reference-spec
  "Emit a relation-reference interface only in an explicit body compiler context."
  [compiled node source]
  (let [key [source (get-in node [:name :symbol])]]
    (when-let [head (get (:rule-relations compiled) key)]
      (when-not (= (count head) (count (:args node))) (fail! :query-source/invalid :planning))
      {:requires [] :columns (mapv #(hash-map :variable % :type :data :nullable? true) (variables node))
       :rule-reference key :head-vars head})))

(defn reference!
  "Borrow the current complete relation on the serial iteration's owning thread."
  [runtime key]
  (control/check-active! (:control runtime))
  (when-not (and (identical? (:control runtime) (:control *iteration*))
                 (= (:query-id runtime) (:query-id *iteration*))
                 (contains? (:tables *iteration*) key))
    (fail! :query-source/invalid :execution))
  (get (:tables *iteration*) key))

(defn call!
  "Apply constants, repeated arguments and wildcards to a complete head set.
  Shared matching preserves predicate equivalence; shared distinct projection
  retains rule-call set semantics after ignored head positions disappear."
  [runtime check! node rel]
  (check!)
  (let [mapped (collection/match! runtime (:args node) (:tuples rel) check!)
        vars (variables node)]
    (containers! runtime (.size ^List (:tuples mapped)) (count vars))
    (let [view (checked check! mapped)
          result (relation/project-distinct view vars)]
      (check!)
      (if (identical? (:tuples view) (:tuples result)) (assoc result :tuples (:tuples mapped)) result))))

(defn- graph!
  [runtime entries rules root source]
  (containers! runtime (+ (count entries) (count rules)) 2)
  (let [check! #(control/check-active! (:control runtime))
        entries (into {} (map (juxt :symbol identity)) entries)
        rules (into {} (map #(vector (get-in % [:name :symbol]) %)) rules)
        graph (atom {})]
    (letfn [(visit-node! [node inherited depth calls]
              (check!)
              (when (> depth 128) (fail! :query-source/limit :planning))
              (containers! runtime 0 1)
              (let [source (source-symbol node inherited)]
                (cond
                  (instance? Not node) (fail! :query-source/unsupported :planning)
                  (and (instance? Function node) (some #(instance? Variable %) (:args node)))
                  ;; A caller's literal/required representation must survive
                  ;; observable scalar production. That needs seeded bodies,
                  ;; rather than filtering an unbound global relation later.
                  (fail! :query-source/unsupported :planning)
                  (instance? Pattern node)
                  (when-not (and (contains? entries source) (:native (get entries source)))
                    (fail! :query-source/unsupported :planning))
                  (instance? RuleExpr node)
                  (let [key [source (get-in node [:name :symbol])]]
                    (vswap! calls conj key)
                    (visit-rule! node source (inc depth)))
                  (or (instance? Or node) (instance? And node))
                  (doseq [clause (:clauses node)] (visit-node! clause source (inc depth) calls)))))
            (visit-rule! [node source depth]
              (check!)
              (when (> depth 128) (fail! :query-source/limit :planning))
              (let [name (get-in node [:name :symbol]) key [source name]
                    rule (get rules name) head (head-vars (first (:branches rule)))]
                (when-not (and rule (= (count head) (count (:args node))))
                  (fail! :query-source/invalid :planning))
                (when-not (contains? @graph key)
                  (containers! runtime (count (:branches rule)) (count head))
                  (doseq [branch (:branches rule)]
                    ;; Demand-seeded required heads need a separate correlated
                    ;; input interface. Never evaluate them as unbound globals.
                    (when (or (seq (get-in branch [:vars :required]))
                              (not= (count (head-vars branch)) (count (set (head-vars branch)))))
                      (fail! :query-source/unsupported :planning)))
                  (swap! graph assoc key {:head-vars head})
                  (let [calls (volatile! #{})]
                    (doseq [branch (:branches rule) clause (:clauses branch)]
                      (visit-node! clause source (inc depth) calls))
                    (swap! graph assoc key {:head-vars head :source source
                                            :branches (:branches rule) :dependencies @calls})))))]
      (visit-rule! root source 0)
      @graph)))

(defn- head-columns!
  "Propagate an upper bound of possible producer types through positive heads.
  Missing producers are bottom; an actual :data producer is top. Predicate
  filters never invent a narrower type. This analysis executes no rule body."
  [runtime ids heads programs]
  (let [check! #(control/check-active! (:control runtime))
        seeds (volatile! {}) edges (volatile! [])
        union-column (fn [a b]
                       (cond (nil? a) b (nil? b) a
                             :else {:type (if (= (:type a) (:type b)) (:type a) :data)
                                    :nullable? (or (:nullable? a) (:nullable? b))}))
        seed! (fn [v column]
                (containers! runtime 1 2)
                (vswap! seeds update v union-column (select-keys column [:type :nullable?])))
        edge! (fn [from to] (containers! runtime 1 2) (vswap! edges conj [from to]))]
    (doseq [id ids [branch-index {:keys [head-vars program]}] (map-indexed vector (get programs id))]
      (let [local #(vector id branch-index %)]
        (doseq [r (get-in program [:compiled :regions]) {:keys [node path]} (:core-clauses r)]
          (check!)
          (cond
            (instance? Pattern node)
            (doseq [column (:columns (get (:core-specs program) path))]
              (seed! (local (:variable column)) column))
            (instance? RuleExpr node)
            (let [key (:rule-reference (get (:core-specs program) path))]
              (doseq [[i arg] (map-indexed vector (:args node)) :when (instance? Variable arg)]
                (edge! [key :head i] (local (:symbol arg)))))
            (instance? Function node)
            (let [arg (first (:args node)) output (get-in node [:binding :variable :symbol])]
              (when (instance? Constant arg)
                (seed! (local output) {:type (value/inferred-type (:value arg))
                                       :nullable? (nil? (:value arg))})))))
        (doseq [[i v] (map-indexed vector head-vars)] (edge! (local v) [id :head i]))))
    (let [types (loop [types @seeds]
                  (check!)
                  (let [next (reduce (fn [types [from to]]
                                       (check!)
                                       (if-let [column (get types from)]
                                         (update types to union-column column) types)) types @edges)]
                    (if (= types next) types (recur next))))]
      (into {} (map (fn [id]
                      [id (mapv (fn [i v]
                                  (assoc (or (get types [id :head i]) {:type :data :nullable? true}) :variable v))
                                (range) (get heads id))])) ids))))

(defn prepare!
  "Compile every reachable positive native body before provider acquisition.
  compile-body! supplies the existing region compiler, returning a captured
  runtime and a reusable shared-step runner. No body executes during preparation.
  Required heads, variable-argument functions and negation reject until their
  seeded/stratified emitter is available. Full totals make nonlinear positive
  recursion complete."
  [runtime entries rules node source compile-body!]
  (let [check! #(control/check-active! (:control runtime))
        graph (graph! runtime entries rules node source)
        ids (vec (sort #(access/compare-keys %1 %2 check!) (keys graph)))
        heads (into {} (map (fn [[id rule]] [id (:head-vars rule)])) graph)
        names (into #{} (map second) ids)
        body-rules (filterv #(contains? names (get-in % [:name :symbol])) rules)
        empty-tables (into {} (map (fn [[id head]] [id (relation/relation! (zipmap head (range)) (FastList.))])) heads)
        programs
        (into {} (map (fn [id]
                        (let [rule (get graph id)]
                          [id (mapv (fn [branch]
                                      (check!)
                                      (let [head (head-vars branch)
                                            query (parser/map->Query
                                                    {:qfind (parser/->FindRel (mapv parser/->Variable head))
                                                     :qin [] :qwhere (:clauses branch)})]
                                        {:head-vars head
                                         :program (compile-body! query
                                                    {:rule-relations heads :default-source (:source rule)} body-rules)}))
                                    (:branches rule))]))) ids)
        owners (vec (mapcat (fn [id] (map #(get-in % [:program :runtime]) (get programs id))) ids))
        head-columns (head-columns! runtime ids heads programs)
        call-columns
        (reduce (fn [columns [arg column]]
                  (if (instance? Variable arg)
                    (let [v (:symbol arg) before (get columns v)]
                      (assoc columns v {:variable v :type (if (or (nil? before) (= (:type before) (:type column)))
                                                           (:type column) :data)
                                        :nullable? (or (:nullable? before) (:nullable? column))})) columns))
                {} (map vector (:args node) (get head-columns [source (get-in node [:name :symbol])])))
        dependencies (into {} (map (fn [[id rule]] [id (:dependencies rule)])) graph)
        run-rule!
        (fn [workspace tables id]
          (binding [*iteration* {:control (:control runtime) :query-id (:query-id runtime) :tables tables}]
            (reduce
              (fn [acc {:keys [head-vars program]}]
                (check!)
                (let [body ((:run! program)) width (count head-vars)]
                  (containers! workspace (.size ^List (:tuples body)) width)
                  (let [projected (rule-relation/project-rule-result (checked check! body) head-vars)
                        projected (rule-relation/rename-rel-attrs projected (get heads id))
                        n (+' (.size ^List (:tuples acc)) (.size ^List (:tuples projected)))]
                    (containers! workspace n width)
                    (relation/sum-rel acc projected))))
              (get empty-tables id) (get programs id))))]
    {:requires [] :columns (mapv call-columns (variables node))
     :native-rule-program true
     :solve! (fn []
               (check!)
               (let [handle (recursion/evaluate! runtime empty-tables ids dependencies
                              (fn [workspace totals _ id] (run-rule! workspace totals id))
                              {:owners owners :initialize! (fn [workspace id] (run-rule! workspace empty-tables id))})]
                 (get ((:relations handle)) [source (get-in node [:name :symbol])])))}))
