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
   [datalevin.query-util :as qu]
   [datalevin.join :as j]
   [datalevin.util :as u :refer [cond+ raise conjv concatv]]
   [datalevin.relation :as r])
  (:import
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.util List]))

(defn parse-rules
  [rules]
  (let [rules (if (string? rules) (edn/read-string rules) rules)]
    (dp/parse-rules rules) ;; validation
    (group-by ffirst rules)))

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
  "returns SCCs in reverse topological order (bottom-up)"
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
    @sccs))

(defn- dependency-graph
  [rules]
  (reduce-kv
    (fn [g head branches]
      (reduce
        (fn [g branch]
          (let [[_ & clauses] branch]
            (reduce
              (fn [g clause]
                (if (sequential? clause)
                  (let [sym (first clause)]
                    (if (contains? rules sym)
                      (update g head (fnil conj #{}) sym)
                      g))
                  g))
              g clauses)))
        (update g head #(or % #{})) ;; Ensure node exists
        branches))
    {} rules))

(defn stratify [rules] (-> rules dependency-graph tarjans-scc))

;; SNE

(defn- empty-rel-for-rule
  "Create an empty relation with attributes matching the rule head args"
  [rule-name rules]
  (let [head-vars (rest (ffirst (get rules rule-name)))
        attrs     (zipmap head-vars (range))]
    (r/relation! attrs (FastList.))))

(defn- project-rule-result
  "Project body-rel to head-vars"
  [body-rel head-vars]
  (if (seq head-vars)
    (let [final-attrs (zipmap head-vars (range))
          idxs        (mapv (:attrs body-rel) head-vars)]
      (r/relation! final-attrs
                   (u/map-fl
                     (fn [t]
                       (let [tg (u/tuple-get t)]
                         (object-array (map #(tg t %) idxs))))
                     (:tuples body-rel))))
    body-rel))

(defn- eval-rule-body
  [context rule-name rule-branches resolve-fn]
  (reduce
    (fn [rel branch]
      (let [[[_ & args] & clauses] branch
            body-res-context       (reduce resolve-fn context clauses)
            body-rel               (reduce j/hash-join (:rels body-res-context))
            projected-rel          (project-rule-result body-rel args)]
        (r/sum-rel rel projected-rel)))
    (empty-rel-for-rule rule-name (:rules context))
    rule-branches))

(defn- map-rule-result
  [rule-rel rule-head-vars call-args]
  (let [attrs (:attrs rule-rel)
        head-idxs
        (mapv (fn [v]
                (if-let [i (get attrs v)]
                  i
                  (raise "Missing var in rule-rel attrs"
                         {:var v :attrs attrs :head rule-head-vars})))
              rule-head-vars)
        const-checks (->> (map-indexed vector call-args)
                          (keep (fn [[idx arg]]
                                  (when (not (qu/free-var? arg))
                                    [(nth head-idxs idx) arg])))
                          vec)
        var->positions
        (reduce
          (fn [m [idx arg]]
            (if (qu/free-var? arg)
              (update m arg (fnil conj []) idx)
              m))
          {} (map-indexed vector call-args))
        equality-idxs (->> var->positions
                           (keep (fn [[_ idxs]]
                                   (when (< 1 (count idxs))
                                     (mapv #(nth head-idxs %) idxs))))
                           vec)
        unique-call-vars (vec (distinct (filter qu/free-var? call-args)))
        projection-idxs (mapv (fn [v]
                                (let [pos (first (get var->positions v))]
                                  (nth head-idxs pos)))
                              unique-call-vars)
        new-attrs        (zipmap unique-call-vars (range))]
    (r/relation!
      new-attrs
      (reduce
        (fn [^List acc tuple]
          (let [tg (u/tuple-get tuple)]
            (if (and
                  (loop [[c & more] const-checks]
                    (if c
                      (let [[idx arg] c]
                        (if (= (tg tuple idx) arg)
                          (recur more)
                          false))
                      true))
                  (loop [[idxs & more] equality-idxs]
                    (if idxs
                      (let [v0 (tg tuple (nth idxs 0))]
                        (if (loop [rest-idxs (rest idxs)]
                              (cond
                                (empty? rest-idxs) true
                                (= v0 (tg tuple (first rest-idxs)))
                                (recur (rest rest-idxs))
                                :else false))
                          (recur more)
                          false))
                      true)))
              (do
                (.add acc (object-array (map #(tg tuple %) projection-idxs)))
                acc)
              acc)))
        (FastList.)
        (:tuples rule-rel)))))

(defn- rename-rule
  [branches]
  (let [mapping (volatile! {})]
    (walk/postwalk
      (fn [x]
        (if (and (symbol? x) (qu/free-var? x))
          (if-let [n (get @mapping x)]
            n
            (let [n (gensym (str (name x) "__"))]
              (vswap! mapping assoc x n)
              n))
          x))
      branches)))

(defn- rule-call?
  [context clause]
  (when (sequential? clause)
    (let [head (if (qu/source? (first clause))
                 (second clause)
                 (first clause))]
      (boolean
        (and (symbol? head)
             (not (qu/free-var? head))
             (not (contains? qu/rule-head head))
             (contains? (:rules context) head))))))

(defn- clause-required-vars
  "Vars that must be bound before evaluating a clause."
  [clause context]
  (cond
    ;; predicate style list, but not a rule call
    (and (sequential? clause)
         (not (vector? clause))
         (not (rule-call? context clause)))
    (set (filter qu/free-var? clause))

    ;; function binding clause: [ (f ?in ...) ?out ...]
    (and (vector? clause)
         (sequential? (first clause)))
    (set (filter qu/free-var? (rest (first clause))))

    :else
    #{}))

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

    :else
    #{}))

(defn- branch-requires?
  [branch var context]
  (let [clauses (rest branch)]
    (loop [cs clauses bound #{}]
      (if (empty? cs)
        false
        (let [c            (first cs)
              required     (clause-required-vars c context)
              next-clauses (rest cs)]
          (cond
            (and (seq? c) (= 'and (first c)))
            (recur (concat (rest c) next-clauses) bound)

            (and (some #{var} required) (not (contains? bound var)))
            true

            :else
            (recur next-clauses (into bound (clause-bound-vars c context)))))))))

(defn- required-seeds
  [branches head-vars context]
  (let [head-vec (vec head-vars)]
    (set
      (keep-indexed
        (fn [idx var]
          (when (some #(branch-requires? % var context) branches)
            idx))
        head-vec))))

(def ^:dynamic *temporal-elimination* false)
(def ^:dynamic *auto-optimize-temporal* true)

(defn- recursive-branch? [branch scc context]
  (let [clauses (rest branch)]
    (boolean
      (some (fn [clause]
              (when (sequential? clause)
                (let [head (if (qu/source? (first clause)) (second clause) (first clause))]
                  (contains? scc head))))
            clauses))))

(defn- variable-dependency [clauses head-vars]
  (reduce
    (fn [deps clause]
      (cond
        (vector? clause)
        (let [head    (first clause)
              out-var (second clause)]
          (cond
            (and (seq? head) (= 'inc (first head)))
            (assoc-in deps [out-var (second head)] 1)

            (and (seq? head) (= '+ (first head)))
            (let [v (second head)
                  c (nth head 2)]
              (if (number? c)
                (assoc-in deps [out-var v] c)
                deps))

            (and (seq? head) (= '+ (first head)) (number? (second head)))
            (let [c (second head)
                  v (nth head 2)]
              (assoc-in deps [out-var v] c))

            :else deps))
        :else deps))
    {} clauses))

(defn- extract-attr-dependencies [rule-name branches scc-rules]
  (let [edges (atom [])]
    (doseq [branch branches]
      (let [[head & clauses] branch
            head-vars (rest head)
            var-origins (atom {})]

        (doseq [clause clauses]
          (when (sequential? clause)
            (let [c-head (if (qu/source? (first clause)) (second clause) (first clause))]
              (when (contains? scc-rules c-head)
                (let [args (rest clause)]
                  (doseq [[idx arg] (map-indexed vector args)]
                    (when (symbol? arg)
                      (swap! var-origins update arg (fnil conj #{}) [c-head idx]))))))))

        (let [var-deps (variable-dependency clauses head-vars)]
          (doseq [[h-idx h-var] (map-indexed vector head-vars)]
            (if-let [origins (@var-origins h-var)]
              (doseq [origin origins]
                (swap! edges conj [origin [rule-name h-idx] 0]))

              (doseq [[src-var offset] (get var-deps h-var)]
                (when-let [origins (@var-origins src-var)]
                  (doseq [origin origins]
                    (swap! edges conj [origin [rule-name h-idx] offset])))))))))
    @edges))

(defn- build-attributes-graph [scc rules]
  (let [scc-set (set scc)]
    (let [all-edges (mapcat (fn [rname]
                              (extract-attr-dependencies rname (get rules rname) scc-set))
                            scc)]
      (reduce (fn [g [src tgt weight]]
                (update g src (fnil conj []) {:target tgt :weight weight}))
              {}
              all-edges))))

(defn- find-temporal-candidates [ag]
  (let [simple-graph (reduce-kv (fn [m k v]
                                  (assoc m k (map :target v)))
                                {} ag)
        ag-sccs (tarjans-scc simple-graph)]

    (filter (fn [comp]
              (let [comp-set (set comp)]
                (some (fn [u]
                        (some (fn [edge]
                                (and (contains? comp-set (:target edge))
                                     (pos? (:weight edge))))
                              (get ag u)))
                      comp)))
            ag-sccs)))

(defn- build-apdg [scc rules temporal-comp]
  (let [comp-set (set temporal-comp)
        apdg (atom {})]
    (doseq [r-head scc]
      (let [branches (get rules r-head)]
        (doseq [branch branches]
          (let [[head & clauses] branch]
            (doseq [clause clauses]
              (when (sequential? clause)
                (let [c-head (if (qu/source? (first clause)) (second clause) (first clause))]
                  (when (contains? (set scc) c-head)
                    (let [rule-edges (extract-attr-dependencies r-head [branch] (set scc))
                          relevant-edges (filter (fn [[src tgt w]]
                                                   (and (= (first src) c-head)
                                                        (= (first tgt) r-head)
                                                        (contains? comp-set src)
                                                        (contains? comp-set tgt)))
                                                 rule-edges)
                          weight (if (seq relevant-edges)
                                   (apply max (map last relevant-edges))
                                   0)]
                      (swap! apdg update c-head (fnil conj []) {:target r-head :weight weight})
                      )))))))))
    @apdg))

(defn- check-positive-cycles [graph nodes]
  (let [zero-graph (reduce-kv (fn [m k edges]
                                (let [zeros (filter #(zero? (:weight %)) edges)]
                                  (if (seq zeros)
                                    (assoc m k (map :target zeros))
                                    m)))
                              {} graph)
        zero-graph (merge (zipmap nodes (repeat [])) zero-graph)
        sccs (tarjans-scc zero-graph)]

    (every? (fn [comp]
              (cond
                (> (count comp) 1) false
                (= (count comp) 1)
                (let [node (first comp)
                      neighbors (get zero-graph node)]
                  (not (some #{node} neighbors)))
                :else true))
            sccs)))

(defn- temporal-index [scc rules]
  (let [ag (build-attributes-graph scc rules)
        candidates (find-temporal-candidates ag)]
    (loop [cands candidates]
      (if (empty? cands)
        nil
        (let [cand (first cands)
              apdg (build-apdg scc rules cand)
              valid? (check-positive-cycles apdg scc)]
          (if valid?
            (let [rname (first scc)
                  match (first (filter #(= (first %) rname) cand))]
              (if match
                (second match)
                (recur (rest cands))))
            (recur (rest cands))))))))

(defn solve-stratified
  [context rule-name args resolve-clause-fn]
  (let [rules      (:rules context)
        cached-rel (get-in context [:rule-rels rule-name])
        head-vars  (rest (first (first (get rules rule-name))))]
    (if cached-rel
      (map-rule-result cached-rel head-vars args)
      (let [sccs       (stratify rules)
            stratum    (first (filter #(contains? (set %) rule-name) sccs))
            deps       (dependency-graph rules)
            recursive? (or (< 1 (count stratum))
                           (contains? (get deps rule-name) rule-name))]
        (if-not stratum
          (raise "Rule not found in strata" {:rule rule-name})
          (let [;; 1. Rename rules in stratum to avoid collision & freshen vars
                renamed-rules-map
                (reduce
                  (fn [m rname]
                    (assoc m rname (rename-rule (get rules rname))))
                  {} stratum)

                full-renamed-rules (merge rules renamed-rules-map)

                ;; Detection of Temporal Elimination
                temporal-idx   (when recursive? (temporal-index stratum renamed-rules-map))
                temporal-elim? (or *temporal-elimination*
                                   (and *auto-optimize-temporal* temporal-idx))

                ;; 2. Determine Required Seeds
                entry-renamed-branches (get renamed-rules-map rule-name)
                entry-renamed-head     (rest (first (first entry-renamed-branches)))

                required-indices (required-seeds entry-renamed-branches entry-renamed-head context)

                ;; 3. Build Seed Relations (only for required vars or constants)
                constant-indices (into #{} (keep-indexed (fn [idx arg]
                                                           (when (not (qu/free-var? arg))
                                                             idx))
                                                         args))
                ;; Seeding recursive strata with constant arguments can drop
                ;; necessary intermediate bindings (e.g. recursive calls that
                ;; introduce new head values), so only seed on required vars
                ;; when recursion is involved.
                seed-indices (sort (seq (if recursive?
                                          required-indices
                                          (set/union required-indices constant-indices))))

                ;; warm start: only when a single outer relation already covers all head vars
                warm-start
                (when (and recursive?
                           (every? qu/free-var? args))
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
                        (let [outer-rels (filter #(contains? (:attrs %) arg) (:rels context))]
                          (if (seq outer-rels)
                            (into rels
                                  (map (fn [rel]
                                         (let [idx (get (:attrs rel) arg)]
                                           (assoc rel :attrs {hv idx}))) ;; View: Rename attr
                                       outer-rels))
                            rels))
                        ;; Bind to Constant
                        (conj rels (r/relation! {hv 0} (doto (FastList.) (.add (object-array [arg]))))))))
                  []
                  seed-indices)

                clean-context (select-keys context [:sources :rule-rels]) ;; :rules updated below

                ;; Split branches into base (non-recursive) and recursive
                stratum-set (set stratum)
                base-branches-map
                (reduce (fn [m rname]
                          (let [branches (get renamed-rules-map rname)
                                base     (filterv #(not (recursive-branch? % stratum-set context)) branches)]
                            (if (seq base) (assoc m rname base) m)))
                        {} stratum)

                rec-branches-map
                (reduce (fn [m rname]
                          (let [branches (get renamed-rules-map rname)
                                rec      (filterv #(recursive-branch? % stratum-set context) branches)]
                            (if (seq rec) (assoc m rname rec) m)))
                        {} stratum)

                ;; Track recursive dependencies so we can skip work when no
                ;; relevant deltas arrived in an iteration.
                stratum-deps
                (reduce
                  (fn [m rname]
                    (let [branches (get renamed-rules-map rname)
                          deps     (reduce
                                     (fn [acc branch]
                                       (reduce
                                         (fn [acc clause]
                                           (if (sequential? clause)
                                             (let [head (if (qu/source? (first clause))
                                                          (second clause)
                                                          (first clause))]
                                               (if (contains? stratum-set head)
                                                 (conj acc head)
                                                 acc))
                                             acc))
                                         acc
                                         (rest branch)))
                                     #{}
                                     branches)]
                      (assoc m rname deps)))
                  {} stratum)

                empty-stratum-rels
                (zipmap stratum
                        (map #(empty-rel-for-rule % full-renamed-rules) stratum))

                ;; 4. Evaluate Base Cases
                start-totals
                (reduce
                  (fn [acc rname]
                    (let [branches (get base-branches-map rname)
                          rel      (if branches
                                     (eval-rule-body (assoc clean-context
                                                            :rules full-renamed-rules
                                                            :rels seed-rels)
                                                     rname branches resolve-clause-fn)
                                     (empty-rel-for-rule rname full-renamed-rules))]
                      (assoc acc rname rel)))
                  {} stratum)

                start-totals (if warm-start
                               (update start-totals rule-name
                                       (fn [rel]
                                         (r/sum-rel rel warm-start)))
                               start-totals)

                final-totals
                (loop [totals    start-totals
                       deltas    start-totals
                       iteration 0]
                  (if (or (every? #(empty? (:tuples %)) (vals deltas))
                          (> iteration 1000000))
                    totals
                    (let [iter-context (assoc clean-context
                                              :rules full-renamed-rules
                                              :rels seed-rels
                                              :rule-rels (merge (:rule-rels context) deltas))
                          candidates
                          (reduce
                            (fn [acc rname]
                              (let [branches (get rec-branches-map rname)
                                    deps     (get stratum-deps rname)
                                    dep-delta?
                                    (some (fn [dep]
                                            (let [rel (get deltas dep)]
                                              (and rel (not (empty? (:tuples rel))))))
                                          deps)
                                    rel      (if (and branches dep-delta?)
                                               (eval-rule-body iter-context rname
                                                               branches
                                                               resolve-clause-fn)
                                               (get empty-stratum-rels rname))]
                                (assoc acc rname rel)))
                            {} stratum)

                          new-deltas
                          (reduce
                            (fn [acc rname]
                              (let [cand-rel (get candidates rname)
                                    old-rel  (if temporal-elim?
                                               (get deltas rname)
                                               (get totals rname (empty-rel-for-rule rname full-renamed-rules)))
                                    diff     (r/difference cand-rel old-rel)]
                                (if (not-empty (:tuples diff))
                                  (assoc acc rname diff)
                                  acc)))
                            {} stratum)

                          new-totals
                          (if temporal-elim?
                            (if (some #(not-empty (:tuples %)) (vals new-deltas))
                              new-deltas
                              totals)
                            (reduce
                              (fn [acc rname]
                                (let [diff (get new-deltas rname)]
                                  (if diff
                                    (update acc rname r/sum-rel diff)
                                    acc)))
                              totals stratum))]

                      (recur new-totals new-deltas (inc iteration)))))]

            (let [rule-rel (get final-totals rule-name)]
              (map-rule-result rule-rel entry-renamed-head args))))))))
