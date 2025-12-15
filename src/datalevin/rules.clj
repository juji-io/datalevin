;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.rules
  "Rules evaluation engine"
  (:require
   [clojure.set :as set]
   [clojure.edn :as edn]
   [clojure.walk :as walk]
   [datalevin.parser :as dp]
   [datalevin.db :as db]
   [datalevin.query-util :as qu]
   [datalevin.join :as j]
   [datalevin.util :as u :refer [raise concatv]]
   [datalevin.relation :as r])
  (:import
   [datalevin.relation Relation]
   [datalevin.db DB]
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.util List HashSet]))

(defn parse-rules
  [rules]
  (let [rules (if (string? rules) (edn/read-string rules) rules)]
    (dp/parse-rules rules) ;; validation
    (group-by ffirst rules)))

(defn- rule-head
  [clause]
  (if (qu/source? (first clause))
    (second clause)
    (first clause)))

(defn- rule-body
  [clause]
  (if (qu/source? (first clause))
    (nnext clause)
    (rest clause)))

;; stratification

(defprotocol INode
  (lowlink [this])
  (index [this])
  (on-stack [this])
  (set-lowlink [this l])
  (set-index [this i])
  (set-on-stack [this s]))

(deftype Node [value
               ^:unsynchronized-mutable index
               ^:unsynchronized-mutable lowlink
               ^:unsynchronized-mutable on-stack]
  INode
  (lowlink [_] lowlink)
  (index [_] index)
  (on-stack [_] on-stack)
  (set-lowlink [_ l] (set! lowlink l))
  (set-index [_ i] (set! index i))
  (set-on-stack [_ s] (set! on-stack s)))

(defn- init-nodes
  [graph]
  (let [vs      (keys graph)
        v->node (zipmap vs (map #(Node. % nil nil false) vs))]
    (reduce (fn [m [v node]] (assoc m node (map v->node (graph v))))
            {} v->node)))

(defn tarjans-scc
  "returns SCCs in reverse topological order"
  [graph]
  (let [nodes (init-nodes graph)
        cur   (volatile! 0)
        stack (volatile! '())
        sccs  (volatile! '())
        connect
        (fn connect [^Node node]
          (set-index node @cur)
          (set-lowlink node @cur)
          (vswap! cur u/long-inc)
          (vswap! stack conj node)
          (set-on-stack node true)
          (doseq [^Node tgt (nodes node)]
            (if (nil? (index tgt))
              (do (connect tgt)
                  (set-lowlink node (min ^long (lowlink node)
                                         ^long (lowlink tgt))))
              (when (on-stack tgt)
                (set-lowlink node (min ^long (lowlink node)
                                       ^long (index tgt))))))
          (when (= (lowlink node) (index node))
            (let [w   (volatile! nil)
                  scc (volatile! #{})]
              (while (not= @w node)
                (let [^Node n (peek @stack)]
                  (vswap! stack pop)
                  (set-on-stack n false)
                  (vswap! scc conj (.-value n))
                  (vreset! w n)))
              (vswap! sccs conj @scc))))]
    (doseq [node (keys nodes)]
      (when (nil? (index node)) (connect node)))
    ;; (println "sccs" @sccs)
    @sccs))

(defn dependency-graph
  [rules]
  (let [graph
        (reduce-kv
          (fn [g head branches]
            (reduce
              (fn [g branch]
                (let [[_ & clauses] branch]
                  (reduce
                    (fn [g clause]
                      (if (sequential? clause)
                        (let [sym (first clause)]
                          (if (rules sym)
                            (update g head (fnil conj #{}) sym)
                            g))
                        g))
                    g clauses)))
              (update g head #(or % #{})) ;; Ensure node exists
              branches))
          {} rules)]
    ;; Preserve stratification metadata on the graph for reuse
    ;; (SCC + temporal cache).
    (with-meta graph {:sccs               (delay (tarjans-scc graph))
                      :temporal-idx-cache (volatile! {})})))

(defn- dependency-sccs
  [deps]
  (if-let [sccs (:sccs (meta deps))]
    (if (delay? sccs) @sccs sccs)
    (tarjans-scc deps)))

;; SNE

(defn- empty-rel-for-rule
  "Create an empty relation with attributes matching the rule head args"
  [rule-name rules]
  (let [head-vars (rest (ffirst (get rules rule-name)))]
    (r/relation! (zipmap head-vars (range)) (FastList.))))

(defn- project-rule-result
  "Project body-rel to head-vars"
  [body-rel head-vars]
  (if (seq head-vars)
    (let [attrs       (:attrs body-rel)
          idxs-arr    (object-array (mapv attrs head-vars))
          n           (alength idxs-arr)
          tuples      ^List (:tuples body-rel)
          m           (.size tuples)
          final-attrs (zipmap head-vars (range))]
      (r/relation!
        final-attrs
        (let [res (FastList. m)]
          (dotimes [i m]
            (let [from ^objects (.get tuples i)
                  to   (object-array n)]
              (dotimes [j n]
                (aset to j (aget from (aget idxs-arr j))))
              (.add res to)))
          res)))
    body-rel))

(defn- head-indices
  [attrs rule-head-vars]
  (long-array
    (mapv (fn [v]
            (if-let [i (attrs v)]
              i
              (raise "Missing var in rule-rel attrs"
                     {:var v :attrs attrs :head rule-head-vars})))
          rule-head-vars)))

(defn- const-indices
  [^longs head-idxs call-args]
  (into []
        (keep-indexed (fn [idx arg]
                        (when (not (qu/free-var? arg))
                          [(aget head-idxs idx) arg])))
        call-args))

(defn- const-check
  [const-checks ^objects tuple]
  (loop [[c & more] const-checks]
    (if c
      (let [[idx arg] c]
        (if (= (aget tuple idx) arg)
          (recur more)
          false))
      true)))

(defn- var-positions
  [call-args]
  (u/reduce-indexed
    (fn [m arg idx]
      (if (qu/free-var? arg)
        (update m arg (fnil conj []) idx)
        m))
    {} call-args))

(defn- equality-indices
  [^longs head-idxs var->positions]
  (into []
        (keep (fn [[_ idxs]]
                (when (< 1 (count idxs))
                  (mapv #(aget head-idxs %) idxs))))
        var->positions))

(defn- unique-vars
  [call-args]
  (into []
        (comp (filter qu/free-var? )
           (distinct))
        call-args))

(defn- projection-indices
  [var->positions ^longs head-idxs unique-call-vars]
  (long-array (mapv #(aget head-idxs (first (var->positions %)))
                    unique-call-vars)))

(defn- equality-check
  [equality-idxs ^objects tuple]
  (loop [[idxs & more] equality-idxs]
    (if idxs
      (if (let [v0 (aget tuple (nth idxs 0))]
            (loop [rest-idxs (rest idxs)]
              (cond
                (empty? rest-idxs)                    true
                (= v0 (aget tuple (first rest-idxs))) (recur (rest rest-idxs))
                :else                                 false)))
        (recur more)
        false)
      true)))

(defn- map-rule-result
  "transforms evaluated rule-rel into a result rel tailored to the rule call"
  [rule-rel rule-head-vars call-args]
  (let [attrs            (:attrs rule-rel)
        ^List tuples     (:tuples rule-rel)
        ^longs head-idxs (head-indices attrs rule-head-vars)
        const-checks     (const-indices head-idxs call-args)
        var->positions   (var-positions call-args)
        equality-idxs    (equality-indices head-idxs var->positions)
        unique-call-vars (unique-vars call-args)
        ^longs projection-idxs
        (projection-indices var->positions head-idxs unique-call-vars)
        n                (alength projection-idxs)]
    (r/relation!
      (zipmap unique-call-vars (range))
      (let [acc (FastList.)]
        (dotimes [i (.size tuples)]
          (let [^objects tuple (.get tuples i)]
            (when (and (const-check const-checks tuple)
                       (equality-check equality-idxs tuple))
              (let [to (object-array n)]
                (dotimes [j n]
                  (aset to j (aget tuple (aget projection-idxs j))))
                (.add acc to)))))
        acc))))

(defn- rename-rule
  [branches]
  (let [mapping (volatile! {})]
    (walk/postwalk
      (fn [x]
        (if (qu/free-var? x)
          (if-let [n (@mapping x)]
            n
            (let [n (gensym (str (name x) "__"))]
              (vswap! mapping assoc x n)
              n))
          x))
      branches)))

(defn- rule-call?
  [context clause]
  (when (sequential? clause)
    (let [head (rule-head clause)]
      (and (symbol? head)
           (not (qu/free-var? head))
           (not (qu/rule-head head))
           ((:rules context) head)))))

(defn- clause-required-vars
  "Vars that must be bound before evaluating a clause."
  [clause context]
  (cond
    ;; predicate style list, but not a rule call
    (and (sequential? clause)
         (not (vector? clause))
         (not (rule-call? context clause)))
    (set (filter qu/free-var? (flatten clause)))

    ;; function binding clause: [ (f ?in ...) ?out ...]
    (and (vector? clause)
         (sequential? (first clause)))
    (set (filter qu/free-var? (rest (first clause))))

    :else #{}))

(defn- clause-bound-vars
  "Vars that become bound after a clause is evaluated."
  [clause context]
  (cond
    (and (sequential? clause)
         (not (vector? clause)))
    (if (rule-call? context clause)
      (set (filter qu/free-var? (rest clause)))
      (set (filter qu/free-var? clause)))

    (vector? clause)
    (set (filter qu/free-var? (flatten clause)))

    :else #{}))

(defn- clause-free-vars
  [clause]
  (set (filterv qu/free-var? (flatten clause))))

(defn- context-bound-vars
  [context]
  (into #{} (mapcat #(keys (:attrs %))) (:rels context)))

(defn- clause-source
  [clause context]
  (let [src-sym (if (qu/source? (first clause))
                  (first clause)
                  '$)]
    (get-in context [:sources src-sym])))

(defn- clause->pattern [clause] (mapv #(if (qu/free-var? %) nil %) clause))

(defn- estimate-clause-size
  [clause context]
  (cond
    (and (vector? clause)
         (dp/parse-pattern clause)
         (not (dp/parse-pred clause))
         (not (dp/parse-fn clause)))
    (if-let [^DB db (clause-source clause context)]
      (try
        (or (db/-count (.-store db) (clause->pattern clause))
            Long/MAX_VALUE)
        (catch Exception _ Long/MAX_VALUE))
      Long/MAX_VALUE)

    (sequential? clause)
    (if (rule-call? context clause)
      Long/MAX_VALUE
      0)

    :else Long/MAX_VALUE))

(defn- reorder-clauses
  [clauses context]
  (let [bound (volatile! (context-bound-vars context))]
    (loop [remaining clauses
           ordered   []]
      (if (empty? remaining)
        ordered
        (let [candidates-indices
              (keep-indexed
                (fn [idx clause]
                  (when (set/subset?
                          (clause-required-vars clause context) @bound)
                    idx))
                remaining)

              candidates-indices
              (if (seq candidates-indices)
                candidates-indices
                (range (count remaining)))

              ^long best-idx
              (first (sort-by
                       (fn [idx]
                         (let [clause (nth remaining idx)]
                           [(- (count (set/intersection
                                        (clause-free-vars clause) @bound)))
                            (estimate-clause-size clause context)]))
                       candidates-indices))

              best-clause (nth remaining best-idx)]

          (vswap! bound set/union (clause-bound-vars best-clause context))
          (recur (concatv (take best-idx remaining)
                          (drop (inc best-idx) remaining))
                 (conj ordered best-clause)))))))

(defn- eval-rule-body
  [context rule-name rule-branches resolve-fn]
  (reduce
    (fn [rel branch]
      (let [[[_ & args] & clauses] branch
            ordered-clauses        (reorder-clauses clauses context)
            body-res-context       (reduce resolve-fn context ordered-clauses)
            body-rel               (reduce j/hash-join (:rels body-res-context))
            projected-rel          (project-rule-result body-rel args)]
        (r/sum-rel rel projected-rel)))
    (empty-rel-for-rule rule-name (:rules context))
    rule-branches))

(defn- branch-requires?
  [branch var context]
  (loop [clauses (rest branch) bound #{}]
    (if (empty? clauses)
      false
      (let [clause       (first clauses)
            required     (clause-required-vars clause context)
            next-clauses (rest clauses)]
        (cond
          (and (seq? clause) (= 'and (first clause)))
          (recur (concatv (rest clause) next-clauses) bound)

          (and (some #{var} required) (not (bound var)))
          true

          :else
          (recur next-clauses
                 (into bound (clause-bound-vars clause context))))))))

(defn- required-seeds
  [branches head-vars context]
  (into #{}
        (keep-indexed
          (fn [idx var]
            (when (some #(branch-requires? % var context) branches)
              idx)))
        head-vars))

(defn- unique-seeds
  [rel idx]
  (let [^List tuples (:tuples rel)]
    (if tuples
      (let [seen (HashSet.)
            res  (FastList.)]
        (dotimes [i (.size tuples)]
          (let [^objects tuple (.get tuples i)
                v              (aget tuple idx)]
            (when (.add seen v)
              (.add res (object-array [v])))))
        res)
      (FastList.))))

(defn- bound-arg-indices
  "Indices of args that are already bound (via outer context or constants)."
  [args context]
  (let [bound (context-bound-vars context)]
    (into #{}
          (keep-indexed (fn [idx arg]
                          (cond
                            (not (qu/free-var? arg)) idx
                            (bound arg)              idx
                            :else                    nil)))
          args)))

(def ^:dynamic *temporal-elimination* false)
(def ^:dynamic *auto-optimize-temporal* true)

(defn- recursive-branch?
  [branch scc]
  (let [clauses (rest branch)]
    (some
      (fn [clause]
        (when (sequential? clause)
          (scc (rule-head clause))))
      clauses)))

(defn- vector-binds-var?
  "True if a binding clause produces the given var as an output."
  [v clause]
  (and (vector? clause)
       (some #(and (symbol? %) (= v %))
             (remove sequential? (rest clause)))))

(defn- stable-head-idxs
  "Conservative check for head vars that stay unchanged through recursion.
   Only used for single-rule strata to safely push bound arguments
   (magic-ish seeds)."
  [branches stratum-set]
  (if (not= 1 (count stratum-set))
    #{}
    (let [head-vars (rest (ffirst branches))]
      (into #{}
            (keep-indexed
              (fn [idx hv]
                (when (every?
                        (fn [branch]
                          (let [clauses (rest branch)]
                            (and
                              (not-any? #(vector-binds-var? hv %) clauses)
                              (every? (fn [clause]
                                        (let [args (rule-body clause)]
                                          (= hv (nth args idx))))
                                      ;; recursive calls
                                      (filterv
                                        (fn [clause]
                                          (when (sequential? clause)
                                            (stratum-set (rule-head clause))))
                                        clauses)))))
                        branches)
                  idx)))
            head-vars))))

(defn- variable-dependency
  "look for [(inc ?x) ?y], [(+ ?x 1) ?y], ..."
  [clauses]
  (reduce
    (fn [deps clause]
      (if (vector? clause)
        (let [[call out-var] clause]
          (if (seq? call)
            (let [[f v c] call]
              (cond
                (= 'inc f) (assoc-in deps [out-var v] 1)
                (= '+ f)   (cond
                             (number? c) (assoc-in deps [out-var v] c)
                             (number? v) (assoc-in deps [out-var c] v)
                             :else       deps)))
            deps))
        deps))
    {} clauses))

(defn- extract-attr-dependencies
  [rule-name branches scc-rules]
  (let [edges (volatile! [])]
    (doseq [branch branches]
      (let [[head & clauses] branch
            head-vars        (rest head)
            var-origins      (volatile! {})]

        (doseq [clause clauses]
          (when (sequential? clause)
            (let [c-head (rule-head clause)]
              (when (scc-rules c-head)
                (let [args (rest clause)]
                  (doseq [[idx arg] (map-indexed vector args)]
                    (when (symbol? arg)
                      (vswap! var-origins
                              update arg (fnil conj #{}) [c-head idx]))))))))

        (let [var-deps (variable-dependency clauses)]
          (doseq [[h-idx h-var] (map-indexed vector head-vars)]
            (if-let [origins (@var-origins h-var)]
              (doseq [origin origins]
                (vswap! edges conj [origin [rule-name h-idx] 0]))

              (doseq [[src-var offset] (get var-deps h-var)]
                (when-let [origins (@var-origins src-var)]
                  (doseq [origin origins]
                    (vswap! edges conj
                            [origin [rule-name h-idx] offset])))))))))
    @edges))

(defn- build-attributes-graph
  [scc rules]
  (let [all-edges (mapcat
                    (fn [rname]
                      (extract-attr-dependencies rname (rules rname)
                                                 (set scc)))
                    scc)]
    (reduce
      (fn [g [src tgt weight]]
        (update g src (fnil conj []) {:target tgt :weight weight}))
      {} all-edges)))

(defn- find-temporal-candidates
  [ag]
  (let [simple-graph (reduce-kv (fn [m k v]
                                  (assoc m k (map :target v)))
                                {} ag)]
    (filterv (fn [comp]
               (let [comp-set (set comp)]
                 (some (fn [u]
                         (some (fn [edge]
                                 (and (comp-set (:target edge))
                                      (pos? ^long (:weight edge))))
                               (ag u)))
                       comp)))
             (tarjans-scc simple-graph))))

(defn- build-apdg
  [scc rules temporal-comp ag]
  (let [comp-set   (set temporal-comp)
        scc-set    (set scc)
        ;; Precompute max weights for edges within the temporal component keyed
        ;; by rule heads, so clause scanning is O(1) lookups.
        weight-map (reduce-kv
                     (fn [m src edges]
                       (if (comp-set src)
                         (reduce (fn [m {:keys [target weight]}]
                                   (if (comp-set target)
                                     (update m [(first src) (first target)]
                                             (fnil max 0) weight)
                                     m))
                                 m edges)
                         m))
                     {} ag)]
    (reduce
      (fn [graph r-head]
        (reduce
          (fn [g clause]
            (if (sequential? clause)
              (let [c-head (rule-head clause)]
                (if (scc-set c-head)
                  (update g c-head (fnil conj [])
                          {:target r-head
                           :weight (weight-map [c-head r-head] 0)})
                  g))
              g))
          graph (mapcat rest (rules r-head))))
      {} scc)))

(defn- check-positive-cycles
  [graph nodes]
  (let [zero-graph (reduce-kv
                     (fn [m k edges]
                       (let [zeros (filterv #(zero? ^long (:weight %)) edges)]
                         (if (seq zeros)
                           (assoc m k (mapv :target zeros))
                           m)))
                     {} graph)
        zero-graph (merge (zipmap nodes (repeat [])) zero-graph)]
    (every? (fn [comp]
              (let [size (count comp)]
                (cond
                  (> size 1) false
                  (= size 1) (let [node      (first comp)
                                   neighbors (zero-graph node)]
                               (not (some #{node} neighbors)))
                  :else      true)))
            (tarjans-scc zero-graph))))

(defn- temporal-index
  ([scc rules] (temporal-index scc rules nil))
  ([scc rules cache]
   (let [cache-map (when cache @cache)]
     (if (and cache-map (cache-map scc))
       (cache-map scc)
       (let [ag         (build-attributes-graph scc rules)
             candidates (find-temporal-candidates ag)
             res
             (loop [cands candidates]
               (when (seq cands)
                 (let [cand   (first cands)
                       apdg   (build-apdg scc rules cand ag)
                       valid? (check-positive-cycles apdg scc)]
                   (if valid?
                     (let [rname (first scc)
                           match (some #(when (= (first %) rname) %) cand)]
                       (if match
                         (second match)
                         (recur (rest cands))))
                     (recur (rest cands))))))]
         (when cache (vswap! cache assoc scc res))
         res)))))

(defn- rel-not-empty [^Relation rel] (< 0 (.size ^List (:tuples rel))))

(defn solve-stratified
  [context rule-name args resolve-clause-fn]
  (let [rules      (:rules context)
        deps       (or (:rules-deps context) (dependency-graph rules))
        cached-rel (get-in context [:rule-rels rule-name])
        head-vars  (rest (ffirst (rules rule-name)))]
    (if cached-rel
      (map-rule-result cached-rel head-vars args)
      (let [sccs       (dependency-sccs deps)
            stratum    (some #(when (% rule-name) %) sccs)
            recursive? (or (< 1 (count stratum))
                           ((deps rule-name) rule-name))]
        (if-not stratum
          (raise "Rule not found in strata" {:rule rule-name})
          (let [stratum-set    (set stratum)
                temporal-cache (:temporal-idx-cache (meta deps))
                ;; 1. Rename rules in stratum to avoid collision & freshen vars
                renamed-rules-map
                (reduce
                  (fn [m rname]
                    (assoc m rname (rename-rule (rules rname))))
                  {} stratum)

                full-renamed-rules (merge rules renamed-rules-map)

                ;; Detection of Temporal Elimination
                temporal-idx   (when recursive?
                                 (temporal-index stratum renamed-rules-map
                                                 temporal-cache))
                temporal-elim? (or *temporal-elimination*
                                   (and *auto-optimize-temporal* temporal-idx))

                ;; 2. Determine Required Seeds
                entry-renamed-branches (renamed-rules-map rule-name)
                entry-renamed-head     (rest (ffirst entry-renamed-branches))

                required-indices (required-seeds entry-renamed-branches
                                                 entry-renamed-head context)
                bound-indices    (bound-arg-indices args context)

                ;; When recursion preserves certain head vars unchanged, we can
                ;; safely seed on those bound args (lightweight magic-set style)
                stable-indices (stable-head-idxs entry-renamed-branches
                                                 stratum-set)
                magic-indices  (set/intersection bound-indices stable-indices)

                ;; 3. Build Seed Relations (only for required vars or constants)
                constant-indices     (into #{}
                                           (keep-indexed
                                             (fn [idx arg]
                                               (when (not (qu/free-var? arg))
                                                 idx)))
                                           args)
                stable-const-indices (set/intersection stable-indices
                                                       constant-indices)

                ;; Seeding recursive strata with constant arguments can drop
                ;; necessary intermediate bindings (e.g. recursive calls that
                ;; introduce new head values), so only seed on required vars
                ;; when recursion is involved. Constants are also safe if the
                ;; corresponding head var is stable through recursion.
                seed-indices (sort
                               (if recursive?
                                 (set/union required-indices magic-indices
                                            stable-const-indices)
                                 (set/union required-indices constant-indices
                                            bound-indices)))

                ;; warm start: only when a single outer relation already covers
                ;; all head vars
                warm-start
                (when (and recursive? (every? qu/free-var? args))
                  (when-let [rel (some #(when (every? (set entry-renamed-head)
                                                      (keys (:attrs %)))
                                          %)
                                       (:rels context))]
                    (project-rule-result rel entry-renamed-head)))

                seed-rels
                (reduce
                  (fn [rels idx]
                    (let [hv  (nth entry-renamed-head idx)
                          arg (nth args idx)]
                      (if (qu/free-var? arg)
                        ;; Bind to Outer Context Var
                        (let [outer-rels (filterv #((:attrs %) arg)
                                                  (:rels context))]
                          (if (seq outer-rels)
                            (into
                              rels
                              (map (fn [rel]
                                     (let [idx ((:attrs rel) arg)]
                                       ;; View: Rename attr
                                       (r/relation! {hv 0}
                                                    (unique-seeds rel idx)))))
                              outer-rels)
                            rels))
                        ;; Bind to Constant
                        (conj rels
                              (r/relation! {hv 0}
                                           (doto (FastList.)
                                             (.add (object-array [arg]))))))))
                  [] seed-indices)

                clean-context
                (assoc (select-keys context [:sources :rule-rels :rules-deps])
                       :rules-deps deps)

                ;; Split branches into base (non-recursive) and recursive
                base-branches-map
                (reduce
                  (fn [m rname]
                    (let [branches (renamed-rules-map rname)
                          base     (filterv
                                     #(not (recursive-branch? % stratum-set))
                                     branches)]
                      (if (seq base) (assoc m rname base) m)))
                  {} stratum)

                rec-branches-map
                (reduce
                  (fn [m rname]
                    (let [branches (renamed-rules-map rname)
                          rec      (filterv
                                     #(recursive-branch? % stratum-set)
                                     branches)]
                      (if (seq rec) (assoc m rname rec) m)))
                  {} stratum)

                ;; Track recursive dependencies so we can skip work when no
                ;; relevant deltas arrived in an iteration.
                stratum-deps
                (reduce
                  (fn [m rname]
                    (assoc m rname (reduce
                                     (fn [acc branch]
                                       (reduce
                                         (fn [acc clause]
                                           (if (sequential? clause)
                                             (let [head (rule-head clause)]
                                               (if (stratum-set head)
                                                 (conj acc head)
                                                 acc))
                                             acc))
                                         acc (rest branch)))
                                     #{} (renamed-rules-map rname))))
                  {} stratum)

                empty-stratum-rels
                (zipmap stratum
                        (mapv #(empty-rel-for-rule % full-renamed-rules)
                              stratum))

                ;; 4. Evaluate Base Cases
                start-totals
                (reduce
                  (fn [acc rname]
                    (let [branches (base-branches-map rname)]
                      (assoc acc rname (if branches
                                         (eval-rule-body
                                           (assoc clean-context
                                                  :rules full-renamed-rules
                                                  :rels seed-rels)
                                           rname branches resolve-clause-fn)
                                         (empty-rel-for-rule
                                           rname full-renamed-rules)))))
                  {} stratum)

                start-totals (if warm-start
                               (update start-totals rule-name
                                       #(r/sum-rel warm-start %))
                               start-totals)

                final-totals
                (loop [totals          start-totals
                       deltas          start-totals
                       deltas-present? (some rel-not-empty (vals start-totals))
                       iter            0]
                  (if (not deltas-present?)
                    totals
                    (let [iter-context
                          (assoc clean-context
                                 :rules full-renamed-rules
                                 :rels seed-rels
                                 :rule-rels (merge (:rule-rels context) deltas))

                          candidates
                          (reduce
                            (fn [acc rname]
                              (let [branches (rec-branches-map rname)
                                    deps     (stratum-deps rname)
                                    dep-delta?
                                    (some (fn [dep]
                                            (let [rel (deltas dep)]
                                              (and rel (rel-not-empty rel))))
                                          deps)]
                                (assoc acc rname
                                       (if (and branches dep-delta?)
                                         (eval-rule-body
                                           iter-context rname
                                           branches resolve-clause-fn)
                                         (empty-stratum-rels rname)))))
                            {} stratum)

                          new-deltas
                          (reduce
                            (fn [acc rname]
                              (let [cand-rel (candidates rname)
                                    old-rel
                                    (if temporal-elim?
                                      (deltas rname)
                                      (get totals rname
                                           (empty-rel-for-rule
                                             rname full-renamed-rules)))
                                    diff     (r/difference cand-rel old-rel)]
                                (if (rel-not-empty diff)
                                  (assoc acc rname diff)
                                  acc)))
                            {} stratum)

                          new-totals
                          (if temporal-elim?
                            (if (some rel-not-empty (vals new-deltas))
                              new-deltas
                              totals)
                            (reduce
                              (fn [acc rname]
                                (let [diff (new-deltas rname)]
                                  (if diff
                                    (update acc rname r/sum-rel diff)
                                    acc)))
                              totals stratum))]

                      (recur new-totals new-deltas
                             (seq new-deltas) (inc iter)))))]

            (map-rule-result (final-totals rule-name)
                             entry-renamed-head args)))))))
