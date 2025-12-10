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
   [datalevin.util :as u :refer [cond+ raise conjv concatv index-of]]
   [datalevin.relation :as r])
  (:import
   [org.eclipse.collections.impl.list.mutable FastList]))

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
        ;; 1. Filter
        filtered-tuples
        (filter
          (fn [tuple]
            (let [tg (u/tuple-get tuple)]
              (every?
                true?
                (for [[v arg] (map vector rule-head-vars call-args)
                      :when   (not (qu/free-var? arg))]
                  (if-let [idx (get attrs v)]
                    (= (tg tuple idx) arg)
                    (raise "Missing var in rule-rel attrs (filter)"
                           {:var v :attrs attrs :head rule-head-vars}))))))
          (:tuples rule-rel))

        ;; 2. Enforce repeated vars equality
        var-indices (group-by second (map-indexed vector call-args))

        consistent-tuples
        (filter
          (fn [tuple]
            (let [tg (u/tuple-get tuple)]
              (every?
                (fn [[_ idxs]]
                  (let [vals (map (fn [idx]
                                    (let [v (nth rule-head-vars idx)]
                                      (if-let [i (get attrs v)]
                                        (tg tuple i)
                                        (raise "Missing var in rule-rel attrs (equality)"
                                               {:var v :attrs attrs :head rule-head-vars}))))
                                  (map first idxs))]
                    (apply = vals)))
                var-indices)))
          filtered-tuples)

        ;; 3. Project and Rename
        unique-call-vars (distinct (filter qu/free-var? call-args))
        new-attrs        (zipmap unique-call-vars (range))

        new-tuples
        (map
          (fn [tuple]
            (let [tg (u/tuple-get tuple)]
              (object-array
                (map
                  (fn [target-var]
                    (let [idx      (u/index-of #(= % target-var) call-args)
                          rule-var (nth rule-head-vars idx)]
                      (if-let [i (get attrs rule-var)]
                        (tg tuple i)
                        (raise "Missing var in rule-rel attrs (project)"
                               {:var rule-var :attrs attrs :head rule-head-vars}))))
                  unique-call-vars))))
          consistent-tuples)]
    (r/relation! new-attrs (u/map-fl identity new-tuples))))

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
          (when (every? #(branch-requires? % var context) branches)
            idx))
        head-vec))))

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

                ;; 2. Determine Required Seeds
                entry-renamed-branches (get renamed-rules-map rule-name)
                entry-renamed-head     (rest (first (first entry-renamed-branches)))

                required-indices (required-seeds entry-renamed-branches entry-renamed-head context)

                ;; 3. Build Seed Relations (required seeds; constants only when non-recursive)
                seed-indices (if recursive?
                               (filter #(contains? required-indices %)
                                       (range (count args)))
                               (filter #(or (contains? required-indices %)
                                            (not (qu/free-var? (nth args %))))
                                       (range (count args))))

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

                start-totals (reduce (fn [m r]
                                       (if (contains? m r)
                                         m
                                         (assoc m r (empty-rel-for-rule r full-renamed-rules))))
                                     (select-keys (:rule-rels context) stratum)
                                     stratum)

                start-totals (if warm-start
                               (update start-totals rule-name
                                       (fn [rel]
                                         (r/sum-rel rel warm-start)))
                               start-totals)

                final-totals
                (loop [totals    start-totals
                       iteration 0]
                  (let [iter-context (assoc clean-context
                                            :rules full-renamed-rules
                                            :rels seed-rels
                                            :rule-rels (merge (:rule-rels context) totals))
                        new-totals
                        (reduce
                          (fn [acc rname]
                            (let [branches (get full-renamed-rules rname)
                                  rel      (eval-rule-body iter-context rname
                                                           branches
                                                           resolve-clause-fn)]
                              (assoc acc rname rel)))
                          totals stratum)

                        deltas
                        (reduce
                          (fn [acc rname]
                            (let [old-rel (get totals rname (empty-rel-for-rule rname full-renamed-rules))
                                  new-rel (get new-totals rname)
                                  diff    (r/difference new-rel old-rel)]
                              (if (not-empty (:tuples diff))
                                (assoc acc rname diff)
                                acc)))
                          {} stratum)]

                    (if (empty? deltas)
                      totals
                      (recur new-totals (inc iteration)))))]

            (let [rule-rel (get final-totals rule-name)]
              (map-rule-result rule-rel entry-renamed-head args))))))))
