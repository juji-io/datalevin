(ns ^:no-doc datalevin.rules
  "Rules evaluation engine"
  (:require
   [clojure.edn :as edn]
   [clojure.walk :as walk]
   [datalevin.parser :as dp]
   [datalevin.query-util :as qu]
   [datalevin.util :as u :refer [raise cond+ conjv concatv]]
   [datalevin.relation :as r]))

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
  [graph]
  (let [nodes (init-nodes graph)
        cur   (volatile! 0)
        stack (volatile! '())
        sccs  (volatile! [])
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
    (reverse @sccs)))

(defn parse-rules
  [rules]
  (let [rules (if (string? rules) (edn/read-string rules) rules)]
    (dp/parse-rules rules) ;; validation
    (group-by ffirst rules)))

(defn solve-rule
  [context clause])

;; (def rule-seqid (atom 0))

;; (defn expand-rule
;;   [clause context]
;;   (let [[rule & call-args] clause
;;         seqid              (swap! rule-seqid inc)
;;         branches           (get (:rules context) rule)]
;;     (for [branch branches
;;           :let   [[[_ & rule-args] & clauses] branch
;;                   replacements (zipmap rule-args call-args)]]
;;       (walk/postwalk
;;         #(if (qu/free-var? %)
;;            (u/some-of
;;              (replacements %)
;;              (symbol (str (name %) "__auto__" seqid)))
;;            %)
;;         clauses))))

;; (defn remove-pairs
;;   [xs ys]
;;   (let [pairs (sequence (comp (map vector)
;;                            (remove (fn [[x y]] (= x y))))
;;                         xs ys)]
;;     [(map first pairs)
;;      (map peek pairs)]))

;; (defn rule-gen-guards
;;   [rule-clause used-args]
;;   (let [[rule & call-args] rule-clause
;;         prev-call-args     (get used-args rule)]
;;     (for [prev-args prev-call-args
;;           :let      [[call-args prev-args] (remove-pairs call-args prev-args)]]
;;       [(concatv ['-differ?] call-args prev-args)])))

;; (defn split-guards
;;   [clauses guards]
;;   (let [bound-vars (collect-vars clauses)
;;         pred       (fn [[[_ & vars]]] (every? bound-vars vars))]
;;     [(filter pred guards)
;;      (remove pred guards)]))

#_(defn solve-rule
    [context clause]
    (let [final-attrs     (filter qu/free-var? clause)
          final-attrs-map (zipmap final-attrs (range))
          ;;         clause-cache    (atom {}) ;; TODO
          solve           (fn [prefix-context clauses]
                            (reduce -resolve-clause prefix-context clauses))
          empty-rels?     (fn [context]
                            (some #(empty? (:tuples %)) (:rels context)))]
      (loop [stack (list {:prefix-clauses []
                          :prefix-context context
                          :clauses        [clause]
                          :used-args      {}
                          :pending-guards {}})
             rel   (r/relation! final-attrs-map (FastList.))]
        (if-some [frame (first stack)]
          (let [[clauses [rule-clause & next-clauses]]
                (split-with #(not (rule? context %)) (:clauses frame))]
            (if (nil? rule-clause)

              ;; no rules -> expand, collect, sum
              (let [context (solve (:prefix-context frame) clauses)
                    tuples  (u/distinct-by vec (-collect context final-attrs))
                    new-rel (r/relation! final-attrs-map tuples)]
                (recur (next stack) (r/sum-rel rel new-rel)))

              ;; has rule -> add guards -> check if dead -> expand rule -> push to stack, recur
              (let [[rule & call-args] rule-clause
                    guards
                    (rule-gen-guards rule-clause (:used-args frame))
                    [active-gs pending-gs]
                    (split-guards (concatv (:prefix-clauses frame) clauses)
                                  (concatv guards (:pending-guards frame)))]
                (if (some #(= % '[(-differ?)]) active-gs) ;; trivial always false case like [(not= [?a ?b] [?a ?b])]

                  ;; this branch has no data, just drop it from stack
                  (recur (next stack) rel)

                  (let [prefix-clauses (concatv clauses active-gs)
                        prefix-context (solve (:prefix-context frame)
                                              prefix-clauses)]
                    (if (empty-rels? prefix-context)

                      ;; this branch has no data, just drop it from stack
                      (recur (next stack) rel)

                      ;; need to expand rule to branches
                      (let [used-args (assoc (:used-args frame) rule
                                             (conj (get (:used-args frame)
                                                        rule [])
                                                   call-args))
                            branches  (expand-rule rule-clause context)]
                        (recur (concatv
                                 (for [branch branches]
                                   {:prefix-clauses prefix-clauses
                                    :prefix-context prefix-context
                                    :clauses        (concatv branch next-clauses)
                                    :used-args      used-args
                                    :pending-guards pending-gs})
                                 (next stack))
                               rel))))))))
          rel))))
