(ns ^:no-doc datalevin.query
  (:require
   [clojure.edn :as edn]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [clojure.pprint :as pp]
   [datalevin.db :as db]
   [datalevin.relation :as r]
   [datalevin.built-ins :as built-ins]
   [datalevin.util :as u :refer [raise cond+ conjv concatv]]
   [datalevin.lru :as lru]
   [datalevin.spill :as sp]
   [datalevin.parser :as dp]
   [datalevin.pull-api :as dpa]
   [datalevin.timeout :as timeout]
   [datalevin.constants :as c])
  (:import
   [clojure.lang ILookup LazilyPersistentVector]
   [datalevin.relation Relation]
   [datalevin.parser BindColl BindIgnore BindScalar BindTuple Constant
    FindColl FindRel FindScalar FindTuple PlainSymbol RulesVar SrcVar
    Variable Pattern Predicate]
   [org.eclipse.collections.impl.map.mutable UnifiedMap]
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.util List]))

(defn spy
  ([x] (pp/pprint x) x)
  ([x msg] (print msg "--> ") (pp/pprint x) x))

;; ----------------------------------------------------------------------------

(def ^:dynamic *query-cache* (lru/cache 32 :constant))

(declare -collect -resolve-clause resolve-clause)

;; Records

(defrecord Context
    [parsed-q rels sources rules opt-clauses late-clauses graph plan])

;; Utilities

(defn single
  [coll]
  (assert (nil? (next coll)) "Expected single element")
  (first coll))

(defn intersect-keys
  [attrs1 attrs2]
  (set/intersection (set (keys attrs1)) (set (keys attrs2))))

(defn- looks-like?
  [pattern form]
  (cond
    (= '_ pattern)    true
    (= '[*] pattern)  (sequential? form)
    (symbol? pattern) (= form pattern)

    (sequential? pattern)
    (if (= (last pattern) '*)
      (and (sequential? form)
           (every? (fn [[pattern-el form-el]] (looks-like? pattern-el form-el))
                   (map vector (butlast pattern) form)))
      (and (sequential? form)
           (= (count form) (count pattern))
           (every? (fn [[pattern-el form-el]] (looks-like? pattern-el form-el))
                   (map vector pattern form))))
    :else ;; (predicate? pattern)
    (pattern form)))

(defn source? [sym] (and (symbol? sym) (= \$ (first (name sym)))))

(defn free-var? [sym] (and (symbol? sym) (= \? (first (name sym)))))

(defn placeholder? [sym]
  (and (symbol? sym) (str/starts-with? (name sym) "?placeholder")))

(defn lookup-ref? [form] (looks-like? [keyword? '_] form))

;;

(defn parse-rules
  [rules]
  (let [rules (if (string? rules) (edn/read-string rules) rules)]
    (dp/parse-rules rules) ;; validation
    (group-by ffirst rules)))

(defn empty-rel
  ^Relation [binding]
  (let [vars (->> (dp/collect-vars-distinct binding)
                  (map :symbol))]
    (r/relation! (zipmap vars (range)) (FastList.))))

(defprotocol IBinding
  ^Relation (in->rel [binding value]))

(extend-protocol IBinding
  BindIgnore
  (in->rel [_ _]
    (r/prod-rel))

  BindScalar
  (in->rel [binding value]
    (r/relation! {(get-in binding [:variable :symbol]) 0}
                 (doto (FastList.) (.add (into-array Object [value])))))

  BindColl
  (in->rel [binding coll]
    (cond
      (not (u/seqable? coll))
      (raise "Cannot bind value " coll " to collection " (dp/source binding)
             {:error :query/binding, :value coll, :binding (dp/source binding)})
      (empty? coll)
      (empty-rel binding)
      :else
      (->> coll
           (map #(in->rel (:binding binding) %))
           (reduce r/sum-rel))))

  BindTuple
  (in->rel [binding coll]
    (cond
      (not (u/seqable? coll))
      (raise "Cannot bind value " coll " to tuple " (dp/source binding)
             {:error :query/binding, :value coll, :binding (dp/source binding)})
      (< (count coll) (count (:bindings binding)))
      (raise "Not enough elements in a collection " coll " to bind tuple "
             (dp/source binding)
             {:error :query/binding, :value coll, :binding (dp/source binding)})
      :else
      (reduce r/prod-rel
              (map #(in->rel %1 %2) (:bindings binding) coll)))))

(defn resolve-in
  [context [binding value]]
  (cond
    (and (instance? BindScalar binding)
         (instance? SrcVar (:variable binding)))
    (update context :sources assoc (get-in binding [:variable :symbol]) value)
    (and (instance? BindScalar binding)
         (instance? RulesVar (:variable binding)))
    (assoc context :rules (parse-rules value))
    :else
    (update context :rels conj (in->rel binding value))))

(defn resolve-ins
  [context values]
  (let [bindings (get-in context [:parsed-q :qin])]
    (reduce resolve-in context (zipmap bindings values))))

;;

(def ^{:dynamic true
       :doc "List of symbols in current pattern that might potentiall be resolved to refs"}
  *lookup-attrs* nil)

(def ^{:dynamic true
       :doc "Default pattern source. Lookup refs, patterns, rules will be resolved with it"}
  *implicit-source* nil)

(defn getter-fn
  [attrs attr]
  (let [idx (attrs attr)]
    (if (contains? *lookup-attrs* attr)
      (if (int? idx)
        (let [idx (int idx)]
          (fn contained-int-getter-fn [tuple]
            (let [eid (if (u/array? tuple)
                        (aget ^objects tuple idx)
                        (nth tuple idx))]
              (cond
                (number? eid)     eid ;; quick path to avoid fn call
                (sequential? eid) (db/entid *implicit-source* eid)
                (u/array? eid)    (db/entid *implicit-source* eid)
                :else             eid))))
        ;; If the index is not an int?, the target can never be an array
        (fn contained-getter-fn [tuple]
          (let [eid (.valAt ^ILookup tuple idx)]
            (cond
              (number? eid)     eid ;; quick path to avoid fn call
              (sequential? eid) (db/entid *implicit-source* eid)
              (u/array? eid)    (db/entid *implicit-source* eid)
              :else             eid))))
      (if (int? idx)
        (let [idx (int idx)]
          (fn int-getter [tuple]
            (if (u/array? tuple)
              (aget ^objects tuple idx)
              (nth tuple idx))))
        ;; If the index is not an int?, the target can never be an array
        (fn getter [tuple] (.valAt ^ILookup tuple idx))))))

(defn tuple-key-fn
  [attrs common-attrs]
  (let [n (count common-attrs)]
    (if (== n 1)
      (getter-fn attrs (first common-attrs))
      (let [^objects getters-arr (into-array Object common-attrs)]
        (loop [i 0]
          (if (< i n)
            (do
              (aset getters-arr i (getter-fn attrs (aget getters-arr i)))
              (recur (unchecked-inc i)))
            (fn [tuple]
              (let [^objects arr (make-array Object n)]
                (loop [i 0]
                  (if (< i n)
                    (do
                      (aset arr i ((aget getters-arr i) tuple))
                      (recur (unchecked-inc i)))
                    (LazilyPersistentVector/createOwning arr)))))))))))

(defn -group-by
  [f init coll]
  (let [^UnifiedMap ret (UnifiedMap.)]
    (doseq [x    coll
            :let [k (f x)]]
      (.put ret k (conj (.getIfAbsentPut ret k init) x)))
    ret))

(defn hash-attrs [key-fn tuples] (-group-by key-fn '() tuples))

(defn hash-join
  [rel1 rel2]
  (let [tuples1      (:tuples rel1)
        tuples2      (:tuples rel2)
        attrs1       (:attrs rel1)
        attrs2       (:attrs rel2)
        common-attrs (vec (intersect-keys attrs1 attrs2))
        keep-attrs1  (keys attrs1)
        keep-attrs2  (->> attrs2
                          (reduce-kv (fn keeper [vec k _]
                                       (if (attrs1 k)
                                         vec
                                         (conj! vec k)))
                                     (transient []))
                          persistent!) ; keys in attrs2-attrs1
        keep-idxs1   (to-array (vals attrs1))
        keep-idxs2   (to-array (->Eduction (map attrs2) keep-attrs2))
        key-fn1      (tuple-key-fn attrs1 common-attrs)
        key-fn2      (tuple-key-fn attrs2 common-attrs)]
    (if (< (count tuples1) (count tuples2))
      (let [^UnifiedMap hash (hash-attrs key-fn1 tuples1)]
        (r/relation! (zipmap (concatv keep-attrs1 keep-attrs2) (range))
                     (reduce
                       (fn outer [acc tuple2]
                         (let [key (key-fn2 tuple2)]
                           (if-some [tuples1 (.get hash key)]
                             (reduce
                               (fn inner [^List acc tuple1]
                                 (.add acc
                                       (r/join-tuples
                                         tuple1 keep-idxs1 tuple2 keep-idxs2))
                                 acc)
                               acc tuples1)
                             acc)))
                       (FastList.)
                       tuples2)))
      (let [^UnifiedMap hash (hash-attrs key-fn2 tuples2)]
        (r/relation! (zipmap (concatv keep-attrs1 keep-attrs2) (range))
                     (reduce
                       (fn outer [acc tuple1]
                         (let [key (key-fn1 tuple1)]
                           (if-some [tuples2 (.get hash key)]
                             (reduce
                               (fn inner [^List acc tuple2]
                                 (.add acc
                                       (r/join-tuples
                                         tuple1 keep-idxs1 tuple2 keep-idxs2))
                                 acc)
                               acc tuples2)
                             acc)))
                       (FastList.)
                       tuples1))))))

(defn subtract-rel
  [a b]
  (let [{attrs-a :attrs, tuples-a :tuples} a
        {attrs-b :attrs, tuples-b :tuples} b

        attrs    (vec (intersect-keys attrs-a attrs-b))
        key-fn-b (tuple-key-fn attrs-b attrs)
        hash     (hash-attrs key-fn-b tuples-b)
        key-fn-a (tuple-key-fn attrs-a attrs)]
    (assoc a :tuples
           (filterv #(nil? (.get ^UnifiedMap hash (key-fn-a %))) tuples-a))))

(defn lookup-pattern-db
  [db pattern]
  (let [search-pattern (mapv #(if (or (= % '_) (free-var? %)) nil %)
                             pattern)
        datoms         (db/-search db search-pattern)
        attr->prop     (->> (map vector pattern ["e" "a" "v"])
                            (filter (fn [[s _]] (free-var? s)))
                            (into {}))]
    (r/relation! attr->prop datoms)))

(defn matches-pattern?
  [pattern tuple]
  (loop [tuple   tuple
         pattern pattern]
    (if (and tuple pattern)
      (let [t (first tuple)
            p (first pattern)]
        (if (or (= p '_) (free-var? p) (= t p))
          (recur (next tuple) (next pattern))
          false))
      true)))

(defn lookup-pattern-coll
  [coll pattern]
  (let [data      (filter #(matches-pattern? pattern %) coll)
        attr->idx (->> (map vector pattern (range))
                       (filter (fn [[s _]] (free-var? s)))
                       (into {}))]
    (r/relation! attr->idx (u/map-fl to-array data))))

(defn normalize-pattern-clause
  [clause]
  (if (source? (first clause))
    clause
    (into ['$] clause)))

(defn lookup-pattern
  [source pattern]
  (if (db/-searchable? source)
    (lookup-pattern-db source pattern)
    (lookup-pattern-coll source pattern)))

(defn collapse-rels
  [rels new-rel]
  (loop [rels    rels
         new-rel new-rel
         acc     []]
    (if-some [rel (first rels)]
      (if (not-empty (intersect-keys (:attrs new-rel) (:attrs rel)))
        (recur (next rels) (hash-join rel new-rel) acc)
        (recur (next rels) new-rel (conj acc rel)))
      (conj acc new-rel))))

(defn- rel-with-attr
  [context sym]
  (some #(when (contains? (:attrs %) sym) %) (:rels context)))

(defn- context-resolve-val
  [context sym]
  (when-some [rel (rel-with-attr context sym)]
    (when-some [tuple (first (:tuples rel))]
      (let [tg (if (u/array? tuple) r/typed-aget get)]
        (tg tuple ((:attrs rel) sym))))))

(defn- rel-contains-attrs? [rel attrs] (some #(contains? (:attrs rel) %) attrs))

(defn- rel-prod-by-attrs
  [context attrs]
  (let [rels       (filter #(rel-contains-attrs? % attrs) (:rels context))
        production (reduce r/prod-rel rels)]
    [(update context :rels #(remove (set rels) %)) production]))

(defn- dot-form [f] (when (and (symbol? f) (str/starts-with? (name f) ".")) f))

(defn- dot-call
  [f args]
  (let [obj   (first args)
        oc    (.getClass ^Object obj)
        fname (subs (name f) 1)
        as    (rest args)
        res   (if (zero? (count as))
                (. (.getDeclaredMethod oc fname nil) (invoke obj nil))
                (. (.getDeclaredMethod
                     oc fname
                     (into-array Class (map #(.getClass ^Object %) as)))
                   (invoke obj (into-array Object as))))]
    (when (not= res false) res)))

(defn- make-call
  [f args]
  (if (dot-form f)
    (dot-call f args)
    (apply f args)))

(defn -call-fn
  [context rel f args]
  (let [sources              (:sources context)
        attrs                (:attrs rel)
        len                  (count args)
        ^objects static-args (make-array Object len)
        ^objects tuples-args (make-array Object len)]
    (dotimes [i len]
      (let [arg (nth args i)]
        (if (symbol? arg)
          (if-some [source (get sources arg)]
            (aset static-args i source)
            (aset tuples-args i (get attrs arg)))
          (aset static-args i arg))))
    (fn [tuple]
      ;; TODO raise if not all args are bound
      (dotimes [i len]
        (when-some [tuple-idx (aget tuples-args i)]
          (let [tg (if (u/array? tuple) r/typed-aget get)
                v  (tg tuple tuple-idx)]
            (aset static-args i v))))
      (make-call f static-args))))

(defn- resolve-sym
  [sym]
  (when (symbol? sym)
    (when-let [v (or (resolve sym)
                     (when (find-ns 'pod.huahaiy.datalevin)
                       (ns-resolve 'pod.huahaiy.datalevin sym)))]
      @v)))

(defonce pod-fns (atom {}))

(defn- resolve-pred
  [f context clause]
  (let [fun (if (fn? f)
              f
              (or (get built-ins/query-fns f)
                  (context-resolve-val context f)
                  (dot-form f)
                  (resolve-sym f)
                  (when (nil? (rel-with-attr context f))
                    (raise "Unknown function or predicate '" f " in " clause
                           {:error :query/where, :form clause,
                            :var   f}))))]
    (if-let [s (:pod.huahaiy.datalevin/inter-fn fun)]
      (@pod-fns s)
      fun)))

(defn filter-by-pred
  [context clause]
  (let [[[f & args]]         clause
        pred                 (resolve-pred f context clause)
        [context production] (rel-prod-by-attrs context (filter symbol? args))

        new-rel (if pred
                  (let [tuple-pred (-call-fn context production pred args)]
                    (update production :tuples #(filter tuple-pred %)))
                  (assoc production :tuples []))]
    (update context :rels conj new-rel)))

(defn bind-by-fn
  [context clause]
  (let [[[f & args] out]     clause
        binding              (dp/parse-binding out)
        fun                  (resolve-pred f context clause)
        [context production] (rel-prod-by-attrs context (filter symbol? args))
        new-rel
        (if fun
          (let [tuple-fn (-call-fn context production fun args)
                rels     (for [tuple (:tuples production)
                               :let  [val (tuple-fn tuple)]
                               :when (not (nil? val))]
                           (r/prod-rel
                             (r/relation! (:attrs production)
                                          (doto (FastList.) (.add tuple)))
                             (in->rel binding val)))]
            (if (empty? rels)
              (r/prod-rel production (empty-rel binding))
              (reduce r/sum-rel rels)))
          (r/prod-rel (assoc production :tuples []) (empty-rel binding)))]
    (update context :rels collapse-rels new-rel)))

;;; RULES

(def rule-head #{'_ 'or 'or-join 'and 'not 'not-join})

(defn rule?
  [context clause]
  (cond+
    (not (sequential? clause))
    false

    :let [head (if (source? (first clause))
                 (second clause)
                 (first clause))]

    (not (symbol? head))
    false

    (free-var? head)
    false

    (contains? rule-head head)
    false

    (not (contains? (:rules context) head))
    (raise "Unknown rule '" head " in " clause
           {:error :query/where :form clause})

    :else true))

(def rule-seqid (atom 0))

(defn expand-rule
  [clause context]
  (let [[rule & call-args] clause
        seqid              (swap! rule-seqid inc)
        branches           (get (:rules context) rule)]
    (for [branch branches
          :let   [[[_ & rule-args] & clauses] branch
                  replacements (zipmap rule-args call-args)]]
      (walk/postwalk
        #(if (free-var? %)
           (u/some-of
             (replacements %)
             (symbol (str (name %) "__auto__" seqid)))
           %)
        clauses))))

(defn remove-pairs
  [xs ys]
  (let [pairs (->> (map vector xs ys)
                   (remove (fn [[x y]] (= x y))))]
    [(map first pairs)
     (map second pairs)]))

(defn rule-gen-guards
  [rule-clause used-args]
  (let [[rule & call-args] rule-clause
        prev-call-args     (get used-args rule)]
    (for [prev-args prev-call-args
          :let      [[call-args prev-args] (remove-pairs call-args prev-args)]]
      [(concatv ['-differ?] call-args prev-args)])))

(defn collect-vars [clause] (set (u/walk-collect clause free-var?)))

(defn split-guards
  [clauses guards]
  (let [bound-vars (collect-vars clauses)
        pred       (fn [[[_ & vars]]] (every? bound-vars vars))]
    [(filter pred guards)
     (remove pred guards)]))

(defn solve-rule
  [context clause]
  (let [final-attrs     (filter free-var? clause)
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

(defn resolve-pattern-lookup-refs
  [source pattern]
  (if (db/-searchable? source)
    (let [[e a v] pattern]
      [(if (or (lookup-ref? e) (keyword? e)) (db/entid-strict source e) e)
       a
       (if (and v (keyword? a) (db/ref? source a)
                (or (lookup-ref? v) (keyword? v)))
         (db/entid-strict source v)
         v)])
    pattern))

(defn dynamic-lookup-attrs
  [source pattern]
  (let [[e a v] pattern]
    (cond-> #{}
      (free-var? e)         (conj e)
      (and
        (free-var? v)
        (not (free-var? a))
        (db/ref? source a)) (conj v))))

(defn limit-rel
  [rel vars]
  (when-some [attrs' (not-empty (select-keys (:attrs rel) vars))]
    (assoc rel :attrs attrs')))

(defn limit-context
  [context vars]
  (assoc context
         :rels (->> (:rels context)
                    (keep #(limit-rel % vars)))))

(defn bound-vars
  [context]
  (into (sp/new-spillable-set) (mapcat #(keys (:attrs %)) (:rels context))))

(defn check-bound
  [bound vars form]
  (when-not (set/subset? vars bound)
    (let [missing (set/difference (set vars) bound)]
      (raise "Insufficient bindings: " missing " not bound in " form
             {:error :query/where :form form :vars missing}))))

(defn check-free-same
  [bound branches form]
  (let [free (mapv #(set/difference (collect-vars %) bound) branches)]
    (when-not (apply = free)
      (raise "All clauses in 'or' must use same set of free vars, had " free
             " in " form
             {:error :query/where :form form :vars free}))))

(defn check-free-subset
  [bound vars branches]
  (let [free (set (remove bound vars))]
    (doseq [branch branches]
      (when-some [missing (not-empty
                            (set/difference free (collect-vars branch)))]
        (prn branch bound vars free)
        (raise "All clauses in 'or' must use same set of free vars, had "
               missing " not bound in " branch
               {:error :query/where :form branch :vars missing})))))

(defn -resolve-clause
  ([context clause]
   (-resolve-clause context clause clause))
  ([context clause orig-clause]
   (condp looks-like? clause
     [[symbol? '*]] ;; predicate [(pred ?a ?b ?c)]
     (do
       (check-bound (bound-vars context) (filter free-var? (nfirst clause))
                    clause)
       (filter-by-pred context clause))

     [[fn? '*]] ;; predicate [(pred ?a ?b ?c)]
     (do
       (check-bound (bound-vars context) (filter free-var? (nfirst clause))
                    clause)
       (filter-by-pred context clause))

     [[symbol? '*] '_] ;; function [(fn ?a ?b) ?res]
     (do
       (check-bound (bound-vars context) (filter free-var? (nfirst clause))
                    clause)
       (bind-by-fn context clause))

     [[fn? '*] '_] ;; function [(fn ?a ?b) ?res]
     (do
       (check-bound (bound-vars context) (filter free-var? (nfirst clause))
                    clause)
       (bind-by-fn context clause))

     [source? '*] ;; source + anything
     (let [[source-sym & rest] clause]
       (binding [*implicit-source* (get (:sources context) source-sym)]
         (-resolve-clause context rest clause)))

     '[or *] ;; (or ...)
     (let [[_ & branches] clause
           _              (check-free-same (bound-vars context) branches clause)
           contexts       (map #(resolve-clause context %) branches)
           rels           (map #(reduce hash-join (:rels %)) contexts)]
       (assoc (first contexts) :rels [(reduce r/sum-rel rels)]))

     '[or-join [[*] *] *] ;; (or-join [[req-vars] vars] ...)
     (let [[_ [req-vars & vars] & branches] clause
           bound                            (bound-vars context)]
       (check-bound bound req-vars orig-clause)
       (check-free-subset bound vars branches)
       (recur context (list* 'or-join (concat req-vars vars) branches) clause))

     '[or-join [*] *] ;; (or-join [vars] ...)
     (let [[_ vars & branches] clause
           vars                (set vars)
           _                   (check-free-subset (bound-vars context) vars
                                                  branches)
           join-context        (limit-context context vars)
           contexts            (map #(-> join-context (resolve-clause %)
                                         (limit-context vars))
                                    branches)
           rels                (map #(reduce hash-join (:rels %)) contexts)
           sum-rel             (reduce r/sum-rel rels)]
       (update context :rels collapse-rels sum-rel))

     '[and *] ;; (and ...)
     (let [[_ & clauses] clause]
       (reduce resolve-clause context clauses))

     '[not *] ;; (not ...)
     (let [[_ & clauses]    clause
           bound            (bound-vars context)
           negation-vars    (collect-vars clauses)
           _                (when (empty? (set/intersection bound negation-vars))
                              (raise "Insufficient bindings: none of "
                                     negation-vars " is bound in " orig-clause
                                     {:error :query/where :form orig-clause}))
           context'         (assoc context :rels
                                   [(reduce hash-join (:rels context))])
           negation-context (reduce resolve-clause context' clauses)
           negation         (subtract-rel
                              (single (:rels context'))
                              (reduce hash-join (:rels negation-context)))]
       (assoc context' :rels [negation]))

     '[not-join [*] *] ;; (not-join [vars] ...)
     (let [[_ vars & clauses] clause
           bound              (bound-vars context)
           _                  (check-bound bound vars orig-clause)
           context'           (assoc context :rels
                                     [(reduce hash-join (:rels context))])
           join-context       (limit-context context' vars)
           negation-context   (-> (reduce resolve-clause join-context clauses)
                                  (limit-context vars))
           negation           (subtract-rel
                                (single (:rels context'))
                                (reduce hash-join (:rels negation-context)))]
       (assoc context' :rels [negation]))

     '[*] ;; pattern
     (let [source   *implicit-source*
           pattern  (resolve-pattern-lookup-refs source clause)
           relation (lookup-pattern source pattern)]
       (binding [*lookup-attrs* (if (db/-searchable? source)
                                  (dynamic-lookup-attrs source pattern)
                                  *lookup-attrs*)]
         (update context :rels collapse-rels relation))))))

(defn resolve-clause
  [context clause]
  (if (->> (:rels context) (some (comp empty? :tuples)))
    context
    (if (rule? context clause)
      (if (source? (first clause))
        (binding [*implicit-source* (get (:sources context) (first clause))]
          (resolve-clause context (next clause)))
        (update context :rels collapse-rels (solve-rule context clause)))
      (-resolve-clause context clause))))

(defn- or-join-var?
  [clause s]
  (and (list? clause)
       (= 'or-join (first clause))
       (some #(= % s) (tree-seq sequential? seq (second clause)))))

(defn- plugin-inputs*
  [parsed-q inputs]
  (let [qins    (:qin parsed-q)
        finds   (tree-seq sequential? seq (:qorig-find parsed-q))
        owheres (:qorig-where parsed-q)
        to-rm   (keep-indexed
                  (fn [i qin]
                    (let [v (:variable qin)
                          s (:symbol v)]
                      (when (and (instance? BindScalar qin)
                                 (instance? Variable v)
                                 ;; (not (fn? (nth inputs i)))
                                 (not (some #(= s %) finds))
                                 (not (some #(or-join-var? % s) owheres)))
                        [i s])))
                  qins)
        rm-idxs (set (map first to-rm))
        smap    (reduce (fn [m [i s]] (assoc m s (nth inputs i))) {} to-rm)]
    [(assoc parsed-q
            :qwhere (reduce (fn [ws [s v]]
                              (walk/postwalk
                                (fn [e]
                                  (if (and (instance? Variable e)
                                           (= s (:symbol e)))
                                    (Constant. v)
                                    e))
                                ws))
                            (:qwhere parsed-q) smap)
            :qorig-where (walk/postwalk-replace smap owheres)
            :qin (u/remove-idxs rm-idxs qins))
     (u/remove-idxs rm-idxs inputs)]))

(defn- plugin-inputs
  "optimization that plugs simple value inputs into where clauses"
  [parsed-q inputs]
  (let [ins (:qin parsed-q)
        cb  (count ins)
        cv  (count inputs)]
    (cond
      (< cb cv)
      (raise "Extra inputs passed, expected: "
             (mapv #(:source (meta %)) ins) ", got: " cv
             {:error :query/inputs :expected ins :got inputs})
      (> cb cv)
      (raise "Too few inputs passed, expected: "
             (mapv #(:source (meta %)) ins) ", got: " cv
             {:error :query/inputs :expected ins :got inputs})
      :else
      (plugin-inputs* parsed-q inputs))))

(defn- optimizable?
  "only optimize attribute-known patterns referring to Datalevin source"
  [sources resolved clause]
  (when (instance? Pattern clause)
    (let [{:keys [pattern]} clause]
      (when (and (instance? Constant (second pattern))
                 (not-any? resolved (map :symbol pattern)))
        (if-let [s (get-in clause [:source :symbol])]
          (when-let [src (get sources s)] (db/-searchable? src))
          (when-let [src (get sources '$)] (db/-searchable? src)))))))

(defn- split-clauses
  "split clauses into two parts, one part is to be optimized"
  [{:keys [sources parsed-q rels] :as context}]
  (let [resolved (reduce (fn [rs {:keys [attrs]}]
                           (set/union rs (set (keys attrs))))
                         #{} rels)
        ptn-idxs (set (u/idxs-of #(optimizable? sources resolved %)
                                 (:qwhere parsed-q)))
        clauses  (:qorig-where parsed-q)]
    (assoc context :opt-clauses (u/keep-idxs ptn-idxs clauses)
           :late-clauses (u/remove-idxs ptn-idxs clauses))))

(defn- get-v [pattern]
  (when (< 2 (count pattern))
    (let [v (peek pattern)]
      (if (= v '_)
        (gensym "?placeholder")
        v))))

(defn- make-node
  [[e patterns]]
  [e (reduce (fn [m pattern]
               (let [attr (second pattern)]
                 (if-let [v (get-v pattern)]
                   (if (free-var? v)
                     (assoc-in m [:free attr] {:var v})
                     (assoc-in m [:bound attr] {:val v}))
                   (assoc-in m [:free attr] {}))))
             {} patterns)])

(defn- link-refs
  [graph]
  (let [es (set (keys graph))]
    (reduce
      (fn [g [e {:keys [free]}]]
        (reduce
          (fn [g [k {:keys [var]}]]
            (if (es var)
              (-> g
                  (update-in [e :links] conjv {:type :ref :tgt var :attr k})
                  (update-in [var :links] conjv {:type :_ref :tgt e :attr k}))
              g))
          g free))
      graph graph)))

(defn- link-eqs
  [graph]
  (reduce
    (fn [g [v lst]]
      (if (< 1 (count lst))
        (reduce
          (fn [g [[e1 k1] [e2 k2]]]
            (let [attrs {e1 k1 e2 k2}]
              (-> g
                  (update-in [e1 :links] conjv
                             {:type :val-eq :tgt e2 :var v :attrs attrs})
                  (update-in [e2 :links] conjv
                             {:type :val-eq :tgt e1 :var v :attrs attrs}))))
          g (u/combinations lst 2))
        g))
    graph (reduce (fn [m [e {:keys [free]}]]
                    (reduce (fn [m [k {:keys [var]}]]
                              (if var (update m var conjv [e k]) m))
                            m free))
                  {} graph)))

(defn- make-nodes
  [[src patterns]]
  [src (let [graph (into {} (map make-node) (group-by first patterns))]
         (if (< 1 (count graph))
           (-> graph link-refs link-eqs)
           graph))])

(defn- resolve-lookup-refs
  [sources [src patterns]]
  [src (mapv #(resolve-pattern-lookup-refs (sources src) %) patterns)])

(defn- remove-src
  [[src patterns]]
  [src (mapv #(if (= (first %) src) (vec (rest %)) %) patterns)])

(defn- get-src [[f & _]] (if (source? f) f '$))

(defn- init-graph
  "build one graph per Datalevin db"
  [context]
  (let [patterns (:opt-clauses context)
        sources  (:sources context)]
    (assoc context :graph
           (into {}
                 (comp
                   (map remove-src)
                   (map #(resolve-lookup-refs sources %))
                   (map make-nodes))
                 (group-by get-src patterns)))))

(defn- pushdownable
  "predicates that can be pushed down involve only one free variable"
  [where gseq]
  (when (instance? Predicate where)
    (let [vars (filterv #(instance? Variable %) (:args where))]
      (when (= 1 (count vars))
        (let [s (:symbol (first vars))]
          (some #(when (= s (:var %)) s) gseq))))))

(defn- set-range
  [i m args ac o1 o2 o3 switch?]
  (let [ac ^long ac
        i  ^long i]
    (cond
      (= i 0)
      (assoc m :range [o1 (second args)])
      (= i (dec ac))
      (assoc m :range [o2 (nth args (dec i))])
      switch?
      (assoc m :range [o3 (nth args (inc i)) (nth args (dec i))])
      :else
      (assoc m :range [o3 (nth args (dec i)) (nth args (inc i))]))))

(defn- add-pred-clause
  [graph clause v]
  (walk/postwalk
    (fn [m]
      (if (= (:var m) v)
        (let [[f & args :as pred] (first clause)]
          (if (:range m)
            ;; TODO merge multiple range predicates
            (update m :pred conjv pred)
            (let [ac (count args)
                  i  ^long (u/index-of #(= % v) args)]
              (condp = f
                '<  (set-range i m args ac :less-than :greater-than :open false)
                '<= (set-range i m args ac :at-most :at-least :closed false)
                '>  (set-range i m args ac :greater-than :less-than :open true)
                '>= (set-range i m args ac :at-least :at-most :closed true)
                '=  (let [c (some #(when-not (free-var? %) %) args)]
                      (assoc m :range [:closed c c]))
                (update m :pred conjv pred)))))
        m))
    graph))

(defn- pushdown-predicates
  "optimization that pushes predicates down to value scans"
  [{:keys [parsed-q graph] :as context}]
  (let [gseq (tree-seq coll? seq graph)]
    (u/reduce-indexed
      (fn [c where i]
        (if-let [v (pushdownable where gseq)]
          (let [clause (nth (:qorig-where parsed-q) i)]
            (-> c
                (update :late-clauses #(remove #{clause} %))
                (update :opt-clauses conj clause)
                (update :graph #(add-pred-clause % clause v))))
          c))
      context (:qwhere parsed-q))))

(defn- pred-count [^long n]
  (long (Math/ceil (* ^double c/magic-number-pred n))))

(defn- range->start-end
  [[op lv hv]]
  (case op
    (:at-least :greater-than) [lv nil]
    (:at-most :less-than)     [nil lv]
    (:closed :open)           [lv hv]))

(defn- datom-n
  [db k node]
  (reduce
    (fn [{:keys [mcount] :as node} [attr {:keys [val range]}]]
      (let [^long res
            (cond
              (some? val) (db/-count db [nil attr val] mcount)
              range       (let [[lv hv] (range->start-end range)]
                            (db/-index-range-size db attr lv hv mcount))
              :else       (db/-count db [nil attr nil] mcount))]
        (if (< res ^long mcount)
          (-> node
              (assoc-in [k attr :count] res)
              (assoc :mcount res :mpath [k attr]))
          (assoc-in node [k attr :count] (inc res)))))
    node (k node)))

(defn- datom-1
  [k node]
  (let [[attr _] (first (k node))]
    (-> node
        (assoc :mpath [k attr])
        (assoc-in [k attr :count] 1))))

(defn- count-clause-datoms
  [{:keys [sources graph] :as context}]
  (reduce
    (fn [c [src nodes]]
      (let [db (sources src)
            nc (count nodes)]
        (reduce
          (fn [c [e {:keys [free bound]}]]
            (let [fc (count free)
                  bc (count bound)]
              (if (or (< 1 nc) (< 1 (+ fc bc)))
                (update c :graph
                        (fn [g]
                          (cond-> (assoc-in g [src e :mcount] Long/MAX_VALUE)
                            (< 0 bc)
                            (update-in [src e] #(datom-n db :bound %))
                            (< 0 fc)
                            (update-in [src e] #(datom-n db :free %)))))
                ;; don't count for single clause
                (update c :graph
                        (fn [g]
                          (cond-> (assoc-in g [src e :mcount] 1)
                            (< 0 bc)
                            (update-in [src e] #(datom-1 :bound %))
                            (< 0 fc)
                            (update-in [src e] #(datom-1 :free %))))))))
          c nodes)))
    context graph))

(defn- attr-var [[_ {:keys [var]}]] (or var '_))

(defn- build-graph
  "Split clauses, turn the group of clauses to be optimized into a query
  graph that looks like this:
  {$
  {?e  {:links [{:type :ref :tgt ?e0 :attr :friend}
                {:type :val-eq :tgt ?e1 :var ?a :attrs {?e :age ?e1 :age}}]
        :mpath [:bound :name]
        :bound {:name {:val \"Tom\" :count 5}}
        :free  {:age    {:var ?a :range [:less-than 18] :count 1089}
                :school {:var ?s :count 108 :pred [(.startsWith ?s \"New\")]}
                :friend {:var ?e0 :count 2500}}}
   ?e0 {:links [{:type :_ref :tgt ?e :attr :friend}]
        :mpath [:free :age]
        :free  {:age {:var ?a :count 10890}}}
   ...}}

  Remaining clauses will be joined after the graph produces a relation"
  [context]
  (-> context
      split-clauses
      init-graph
      pushdown-predicates
      count-clause-datoms))

(defn- add-pred
  [old-pred new-pred]
  (if old-pred
    (fn [x] (and (old-pred x) (new-pred x)))
    new-pred))

(defn- activate-pred
  [var clause]
  (when clause
    (if (fn? clause)
      clause
      (let [[f & args] clause
            f'         (resolve-pred f nil clause)
            i          (u/index-of #(= var %) args)
            args-arr   (object-array args)]
        (fn [x] (make-call f' (do (aset args-arr i x) args-arr)))))))

(defn- attr-pred
  [[_ {:keys [var pred]}]]
  (reduce add-pred nil (mapv #(activate-pred var %) pred)))

(defn- init-node-plan
  [db [e clauses]]
  (let [{:keys [bound free mpath]} clauses
        {:keys [var val range] mcount :count :as clause}
        (get-in clauses mpath)

        attr    (peek mpath)
        know-e? (int? e)
        schema  (db/-schema db)]
    (if (zero? ^long mcount)
      []
      (let [init (cond-> {:op :init-tuples :attr attr :vars [e] :out #{e}}
                   var     (assoc :pred (attr-pred [attr clause])
                                  :vars (cond-> [e]
                                          (not (placeholder? var))
                                          (conj var))
                                  :range range)
                   val     (assoc :val val)
                   know-e? (assoc :know-e? true)
                   true    (#(assoc % :col (count (:vars %)))))
            col  (:col init)]
        (cond-> [init]
          (< 1 (+ (count bound) (count free)))
          (conj
            (let [bound' (->> (dissoc bound attr)
                              (mapv (fn [[a {:keys [val] :as b}]]
                                      [a (-> b
                                             (update :pred conjv #(= val %))
                                             (assoc :var (gensym "?bound")))])))
                  all    (->> (concatv bound' (dissoc free attr))
                              (sort-by (fn [[a _]] (-> a schema :db/aid))))
                  attrs  (mapv first all)
                  vars   (mapv attr-var all)
                  skips  (remove nil?
                                 (map #(when (or (= %2 '_)
                                                 (placeholder? %2))
                                         %1) attrs vars))]
              {:op    :eav-scan-v
               :index 0
               :attrs attrs
               :vars  vars
               :col   (- ^long (+ ^long col (count vars)) (count skips))
               :in    #{e}
               :out   #{e}
               :preds (mapv attr-pred all)
               :skips skips})))))))

(defn- estimate-cost
  [db steps]
  )

(defn- base-plans
  [db nodes component]
  (into {}
        (map (fn [e]
               [#{e} (let [steps (init-node-plan db (find nodes e))]
                       {:steps steps
                        :cost  (estimate-cost db steps)})]))
        component))

(defn- ref-e-step
  [db prev-plan link-e new-e link]
  )

(defn- r-ref-e-step
  [db prev-plan link-e new-e link]
  )

(defn- val-eq-e-step
  [db prev-plan link-e new-e link]
  )

(defn- e-plan
  [db nodes prev-plan link-e new-e new-key]
  (let [link  (some #(= new-e (:tgt %)) (get-in nodes [link-e :links]))
        steps (conj (case (:type link)
                      :ref    (ref-e-step db prev-plan link-e new-e link)
                      :_ref   (r-ref-e-step db prev-plan link-e new-e link)
                      :val-eq (val-eq-e-step db prev-plan link-e new-e link))
                    {:op :eav-scan-v
                     })]
    {:steps steps
     :cost  (estimate-cost db steps)}))

(defn- h-plan
  [db nodes prev-plan link-e new-key]
  )

(defn- binary-plan
  [db nodes bases prev-plan link-e new-e new-key]
  (let [e-plan (e-plan db nodes prev-plan link-e new-e new-key)
        h-plan (h-plan db nodes bases prev-plan link-e new-key)]
    (if (< ^long (:cost e-plan) ^long (:cost h-plan))
      e-plan
      h-plan)))

(defn- plans
  [db nodes connected bases prev-table]
  (reduce
    (fn [table [prev-key prev-plan]]
      (reduce
        (fn [t pair]
          (if-let [link-e (not-empty (set/intersection prev-key pair))]
            (if (= 1 (count link-e))
              (let [new-key  (set/union prev-key pair)
                    cur-cost (or (:cost (t new-key)) Long/MAX_VALUE)
                    {:keys [cost] :as new-plan}
                    (binary-plan db nodes bases prev-plan
                                 (first link-e)
                                 (first (set/difference pair link-e))
                                 new-key)]
                (if (< ^long cost ^long cur-cost)
                  (assoc t new-key new-plan)
                  t))
              t)
            t))
        table connected))
    {} prev-table))

(defn- connected-pairs
  [nodes component]
  (into #{}
        (comp
          (filter (fn [[e1 e2]]
                    (some #(= % e2) (map :tgt (get-in nodes [e1 :links])))))
          (map set))
        (u/combinations component 2)))

(defn- plan-component
  [db nodes component]
  (let [n (count component)]
    (if (= n 1)
      (init-node-plan db (find nodes (first component)))
      (let [connected (connected-pairs nodes component)
            tables    (FastList. n)
            n-1       (dec n)
            bases     (base-plans db nodes component)]
        (.add tables bases)
        (dotimes [i n-1]
          (.add tables (plans db nodes connected bases (.get tables i))))
        (reduce
          (fn [steps i]
            (concatv (:steps ((.get tables i) (:in (first steps)))) steps))
          (-> (.get tables n-1) first val :steps)
          (range (dec n-1) -1 -1))))))

(defn- dfs
  [graph start]
  (loop [stack [start] visited #{}]
    (if (empty? stack)
      visited
      (let [v     (peek stack)
            stack (pop stack)]
        (if (visited v)
          (recur stack visited)
          (let [neighbors (mapv :tgt (:links (graph v)))]
            (recur (into stack neighbors) (conj visited v))))))))

(defn- connected-components
  [graph]
  (loop [vertices (keys graph) components []]
    (if (empty? vertices)
      components
      (let [start     (first vertices)
            component (dfs graph start)
            vertices  (remove component vertices)]
        (recur vertices (conj components component))))))

(defn- build-plan*
  [db nodes]
  (let [cc (connected-components nodes)]
    (spy cc "connected component")
    (if (= 1 (count cc))
      [(plan-component db nodes (first cc))]
      (pmap #(plan-component db nodes %) cc))))

(defn- build-plan
  "Generate a query plan that looks like this:

  [{:op :init-tuples :attr :name :val \"Tom\" :out #{?e} :vars [?e]
    :col 1}
   {:op :eav-scan-v  :attrs [:age :friend] :preds [(< ?a 20) nil]
    :vars [?a ?f] :in #{?e} :index 0 :out #{?e} :col 3}
   {:op :vae-scan-e :attr :friend :var ?e1 :in #{?e} :index 2
    :out #{?e ?e1} :col 4}
   {:op :eav-scan-v :attrs [:name] :preds [nil] :vars [?n] :index 3
    :in #{?e ?e1} :out #{?e ?e1} :col 5}]"
  [{:keys [graph sources] :as context}]
  (if graph
    (reduce
      (fn [c [src nodes]]
        (let [db (sources src)]
          (assoc-in c [:plan src]
                    (if (< 1 (count nodes))
                      (build-plan* db nodes)
                      [(init-node-plan db (first nodes))]))))
      context graph)
    context))

(defn- init-relation
  [db {:keys [vars val attr range pred know-e?]}]
  (let [get-v? (< 1 (count vars))
        e      (first vars)
        v      (peek vars)]
    (cond
      know-e?
      (r/relation! (cond-> {'_ 0} get-v? (assoc v 1))
                   (let [tuples (doto (FastList.) (.add (object-array [e])))]
                     (if get-v?
                       (db/-eav-scan-v db tuples 0 [attr] [nil] [])
                       tuples)))
      (nil? val)
      (r/relation! (cond-> {e 0} get-v? (assoc v 1))
                   (db/-init-tuples db attr (or range [:all]) pred get-v?))
      :else
      (r/relation! {e 0}
                   (db/-init-tuples db attr [:closed val val] nil false)))))

(defn- update-attrs
  [attrs vars]
  (let [n (count attrs)]
    (into attrs (comp
                  (remove #{'_})
                  (map-indexed (fn [i v] [v (+ n ^long i)]))) vars)))

(defn- eav-scan-v
  [db rel {:keys [attrs preds vars index skips]}]
  (->  rel
       (update :attrs #(update-attrs % vars))
       (assoc :tuples (db/-eav-scan-v
                        db (:tuples rel) index attrs preds skips))))

(defn- vae-scan-e
  [db rel step]
  )

(defn- ave-scan-e
  [db rel step]
  )

(defn- hash-join-step
  [rel step]
  )

(defn- execute-step
  [db {:keys [op] :as step} rel]
  (case op
    :init-tuples (init-relation db step)
    :eav-scan-v  (eav-scan-v db rel step)
    :vae-scan-e  (vae-scan-e db rel step)
    :ave-scan-e  (ave-scan-e db rel step)
    :hash-join   (hash-join-step rel step)))

(defn- execute-steps
  [db [f & r]]
  (reduce (fn [rel step] (execute-step db step rel))
          (execute-step db f nil) r))

(defn- execute-plan
  [{:keys [plan sources] :as context}]
  (let [n (reduce + (map (fn [[_ components]] (count components)) plan))]
    (if (= 1 n)
      (update context :rels collapse-rels
              (let [[src components] (first plan)]
                (execute-steps (sources src) (first components))))
      (let [new-rels (vec (repeatedly n promise))
            i        (atom 0)]
        (doseq [[src components] plan
                :let             [db (sources src)]]
          (doseq [steps components]
            (future
              (let [rel (execute-steps db steps)]
                (swap! i (fn [i]
                           (deliver (nth new-rels i) rel)
                           (u/long-inc i)))))))
        (reduce
          (fn [c j]
            (update c :rels collapse-rels @(nth new-rels j)))
          context (range n))))))

(defn -q
  [context]
  (binding [*implicit-source* (get (:sources context) '$)]
    (as-> context c
      (build-graph c)
      (build-plan c)
      (spy c)
      (execute-plan c)
      (reduce resolve-clause c (:late-clauses c)))))

(defn -collect-tuples
  [acc rel ^long len copy-map]
  (->Eduction
    (comp
      (map (fn [^{:tag "[[Ljava.lang.Object;"} t1]
             (->Eduction
               (map (fn [t2]
                      (let [res (aclone t1)]
                        (if (u/array? t2)
                          (dotimes [i len]
                            (when-some [idx (aget ^objects copy-map i)]
                              (aset res i (aget ^objects t2 idx))))
                          (dotimes [i len]
                            (when-some [idx (aget ^objects copy-map i)]
                              (aset res i (get t2 idx)))))
                        res)))
               (:tuples rel))))
      cat)
    acc))

(defn -collect
  ([context symbols]
   (let [rels (:rels context)]
     (-collect [(make-array Object (count symbols))] rels symbols)))
  ([acc rels symbols]
   (cond+
     :let [rel (first rels)]

     (nil? rel) acc

     ;; one empty rel means final set has to be empty
     (empty? (:tuples rel)) []

     :let [keep-attrs (select-keys (:attrs rel) symbols)]

     (empty? keep-attrs) (recur acc (next rels) symbols)

     :let [copy-map (to-array (map #(get keep-attrs %) symbols))
           len      (count symbols)]

     :else
     (recur (-collect-tuples acc rel len copy-map) (next rels) symbols))))

(defn collect
  [context symbols]
  (into (sp/new-spillable-set) (map vec) (-collect context symbols)))

(defprotocol IContextResolve
  (-context-resolve [var context]))

(extend-protocol IContextResolve
  Variable
  (-context-resolve [var context]
    (context-resolve-val context (.-symbol var)))
  SrcVar
  (-context-resolve [var context]
    (get-in context [:sources (.-symbol var)]))
  PlainSymbol
  (-context-resolve [var _]
    (or (get built-ins/aggregates (.-symbol var))
        (resolve-sym (.-symbol var))))
  Constant
  (-context-resolve [var _]
    (.-value var)))

(defn -aggregate
  [find-elements context tuples]
  (mapv (fn [element fixed-value i]
          (if (dp/aggregate? element)
            (let [f    (-context-resolve (:fn element) context)
                  args (map #(-context-resolve % context)
                            (butlast (:args element)))
                  vals (map #(nth % i) tuples)]
              (apply f (concatv args [vals])))
            fixed-value))
        find-elements
        (first tuples)
        (range)))

(defn aggregate
  [find-elements context resultset]
  (let [group-idxs (u/idxs-of (complement dp/aggregate?) find-elements)
        group-fn   (fn [tuple] (map #(nth tuple %) group-idxs))
        grouped    (group-by group-fn resultset)]
    (for [[_ tuples] grouped]
      (-aggregate find-elements context tuples))))

(defn map* [f xs]
  (persistent! (reduce #(conj! %1 (f %2)) (transient (empty xs)) xs)))

(defn tuples->return-map
  [return-map tuples]
  (let [symbols (:symbols return-map)
        idxs    (range 0 (count symbols))]
    (map* (fn [tuple]
            (persistent!
              (reduce
                (fn [m i] (assoc! m (nth symbols i) (nth tuple i)))
                (transient {}) idxs)))
          tuples)))

(defprotocol IPostProcess
  (-post-process [find return-map tuples]))

(extend-protocol IPostProcess
  FindRel
  (-post-process [_ return-map tuples]
    (if (nil? return-map)
      tuples
      (tuples->return-map return-map tuples)))

  FindColl
  (-post-process [_ _ tuples]
    (into [] (map first) tuples))

  FindScalar
  (-post-process [_ _ tuples]
    (ffirst tuples))

  FindTuple
  (-post-process [_ return-map tuples]
    (if (some? return-map)
      (first (tuples->return-map return-map [(first tuples)]))
      (first tuples))))

(defn- pull
  [find-elements context resultset]
  (let [resolved (for [find find-elements]
                   (when (dp/pull? find)
                     (let [db      (-context-resolve (:source find) context)
                           pattern (-context-resolve (:pattern find) context)]
                       (dpa/parse-opts db pattern))))]
    (for [tuple resultset]
      (mapv
        (fn [parsed-opts el]
          (if parsed-opts (dpa/pull-impl parsed-opts el) el))
        resolved
        tuple))))

(defn- resolve-redudants
  "handle pathological cases of variable is already bound in where clauses"
  [{:keys [parsed-q] :as context}]
  (let [{:keys [qwhere]} parsed-q
        get-v            #(nth (:pattern %) 2)]
    (reduce
      (fn [c [_ patterns]]
        (let [v (some #(let [v (get-v %)]
                         (when (instance? Constant v) (:value v)))
                      patterns)]
          (reduce
            (fn [c pattern]
              (let [idx (u/index-of #(= pattern %) patterns)]
                (-> c
                    (update-in [:parsed-q :qwhere] #(remove #{pattern} %))
                    (update-in [:parsed-q :qorig-where]
                               #(u/remove-idxs #{idx} %))
                    (update :rels conj
                            (r/relation! {(:symbol (get-v pattern)) 0}
                                         (doto (FastList.)
                                           (.add (object-array [v]))))))))
            c (filter #(instance? Variable (get-v %)) patterns))))
      context
      (->> qwhere
           (filter #(instance? Pattern %))
           (group-by (fn [{:keys [source pattern]}]
                       [source (first pattern) (second pattern)]))
           (filter #(< 1 (count (val %))))))))

(defn q
  [q & inputs]
  (let [parsed-q (lru/-get *query-cache* q #(dp/parse-query q))]
    (println "----->" q)
    ;; (println "parsed-q" parsed-q)
    (binding [timeout/*deadline* (timeout/to-deadline (:qtimeout parsed-q))]
      (let [find              (:qfind parsed-q)
            find-elements     (dp/find-elements find)
            result-arity      (count find-elements)
            with              (:qwith parsed-q)
            all-vars          (concatv (dp/find-vars find) (map :symbol with))
            [parsed-q inputs] (plugin-inputs parsed-q inputs)
            context           (-> (Context. parsed-q [] {} {} [] nil nil nil)
                                  (resolve-ins inputs)
                                  (resolve-redudants))
            resultset         (-> (-q context)
                                  (collect all-vars))]
        (cond->> resultset
          with
          (mapv #(vec (subvec % 0 result-arity)))

          (some dp/aggregate? find-elements)
          (aggregate find-elements context)

          (some dp/pull? find-elements)
          (pull find-elements context)

          true
          (-post-process find (:qreturn-map parsed-q)))))))
