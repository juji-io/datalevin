(ns ^:no-doc datalevin.query
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [datalevin.db :as db]
   [datalevin.search :as s]
   [datalevin.built-ins :as bi]
   [datalevin.storage :as st]
   [datalevin.relation :as r]
   [datalevin.util :as u #?(:cljs :refer-macros :clj :refer) [raise]]
   [me.tonsky.persistent-sorted-set.arrays :as da]
   [datalevin.lru]
   [datalevin.entity :as de]
   [datalevin.parser :as dp
    #?@(:cljs [:refer [BindColl BindIgnore BindScalar BindTuple Constant
                       FindColl FindRel FindScalar FindTuple PlainSymbol
                       RulesVar SrcVar Variable]])]
   [datalevin.pull-api :as dpa]
   [datalevin.pull-parser :as dpp])
  #?(:clj (:import [datalevin.parser BindColl BindIgnore BindScalar BindTuple
                    Constant FindColl FindRel FindScalar FindTuple PlainSymbol
                    RulesVar SrcVar Variable]
                   [datalevin.storage Store]
                   [datalevin.relation Relation]
                   [datalevin.search SearchEngine]
                   [datalevin.db DB]
                   [java.lang Long])))

;; ----------------------------------------------------------------------------

(def ^:const lru-cache-size 100)

(declare -collect -resolve-clause resolve-clause)

;; Records

(defrecord Context [rels sources rules])

;; Utilities

(defn single [coll]
  (assert (nil? (next coll)) "Expected single element")
  (first coll))

(defn concatv [& xs]
  (into [] cat xs))

(defn zip
  ([a b] (mapv vector a b))
  ([a b & rest] (apply mapv vector a b rest)))

(defn- looks-like? [pattern form]
  (cond
    (= '_ pattern)
    true
    (= '[*] pattern)
    (sequential? form)
    (symbol? pattern)
    (= form pattern)
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

(defn source? [sym]
  (and (symbol? sym)
       (= \$ (first (name sym)))))

(defn free-var? [sym]
  (and (symbol? sym)
       (= \? (first (name sym)))))

(defn attr? [form]
  (or (keyword? form) (string? form)))

(defn lookup-ref? [form]
  (looks-like? [attr? '_] form))

(defn parse-rules [rules]
  (let [rules (if (string? rules) (edn/read-string rules) rules)]
    (dp/parse-rules rules) ;; validation
    (group-by ffirst rules)))

(defprotocol IBinding
  ^Relation (in->rel [binding value]))

(extend-protocol IBinding
  BindIgnore
  (in->rel [_ _]
    (r/prod-rel))

  BindScalar
  (in->rel [binding value]
    (Relation. {(get-in binding [:variable :symbol]) 0}
               [(into-array Object [value])]))

  BindColl
  (in->rel [binding coll]
    (cond
      (not (u/seqable? coll))
      (raise "Cannot bind value " coll " to collection " (dp/source binding)
             {:error :query/binding, :value coll, :binding (dp/source binding)})
      (empty? coll)
      (r/empty-rel binding)
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
      (raise "Not enough elements in a collection " coll " to bind tuple " (dp/source binding)
             {:error :query/binding, :value coll, :binding (dp/source binding)})
      :else
      (reduce r/prod-rel
              (map #(in->rel %1 %2) (:bindings binding) coll)))))

(defn resolve-in [context [binding value]]
  (cond
    (and (instance? BindScalar binding)
         (instance? SrcVar (:variable binding)))
      (update context :sources assoc (get-in binding [:variable :symbol]) value)
    (and (instance? BindScalar binding)
         (instance? RulesVar (:variable binding)))
      (assoc context :rules (parse-rules value))
    :else
      (update context :rels conj (in->rel binding value))))

(defn resolve-ins [context bindings values]
  (let [cb (count bindings)
        cv (count values)]
    (cond
      (< cb cv)
      (raise "Extra inputs passed, expected: " (mapv #(:source (meta %)) bindings) ", got: " cv
             {:error :query/inputs :expected bindings :got values})

      (> cb cv)
      (raise "Too few inputs passed, expected: " (mapv #(:source (meta %)) bindings) ", got: " cv
             {:error :query/inputs :expected bindings :got values})

      :else
      (reduce resolve-in context (zipmap bindings values)))))

;;

(defn lookup-pattern-db [db pattern]
  ;; TODO optimize with bound attrs min/max values here
  (let [search-pattern (mapv #(if (symbol? %) nil %) pattern)
        datoms         (db/-search db search-pattern)
        attr->prop     (->> (map vector pattern ["e" "a" "v"])
                            (filter (fn [[s _]] (free-var? s)))
                            (into {}))]
    (Relation. attr->prop datoms)))

(defn matches-pattern? [pattern tuple]
  (loop [tuple   tuple
         pattern pattern]
    (if (and tuple pattern)
      (let [t (first tuple)
            p (first pattern)]
        (if (or (symbol? p) (= t p))
          (recur (next tuple) (next pattern))
          false))
      true)))

(defn lookup-pattern-coll [coll pattern]
  (let [data       (filter #(matches-pattern? pattern %) coll)
        attr->idx  (->> (map vector pattern (range))
                        (filter (fn [[s _]] (free-var? s)))
                        (into {}))]
    (Relation. attr->idx (mapv to-array data)))) ;; FIXME to-array

(defn normalize-pattern-clause [clause]
  (if (source? (first clause))
    clause
    (concat ['$] clause)))

(defn lookup-pattern [source pattern]
  (if (db/-searchable? source)
    (lookup-pattern-db source pattern)
    (lookup-pattern-coll source pattern)))

(defn- pattern-size [source pattern]
  (if (db/-searchable? source)
    (let [search-pattern (mapv #(if (symbol? %) nil %) pattern)]
      (db/-count source search-pattern))
    (count (filter #(matches-pattern? pattern %) source))))

(defn- rel-with-attr [context sym]
  (some #(when (contains? (:attrs %) sym) %) (:rels context)))

(defn- context-resolve-val [context sym]
  (when-some [rel (rel-with-attr context sym)]
    (when-some [tuple (first (:tuples rel))]
      (let [tg (if (da/array? tuple) r/typed-aget get)]
        (tg tuple ((:attrs rel) sym))))))

(defn- rel-contains-attrs? [rel attrs]
  (some #(contains? (:attrs rel) %) attrs))

(defn- rel-prod-by-attrs [context attrs]
  (let [rels       (filter #(rel-contains-attrs? % attrs) (:rels context))
        production (reduce r/prod-rel rels)]
    [(update context :rels #(remove (set rels) %)) production]))

(defn- dot-form [f]
  (when (and (symbol? f) (str/starts-with? (name f) "."))
    f))

(defn- dot-call [f args]
  (let [obj   (first args)
        oc    (.getClass ^Object obj)
        fname (subs (name f) 1)
        as    (rest args)
        res   (if (zero? (count as))
                (. (.getDeclaredMethod oc fname nil) (invoke obj nil))
                (. (.getDeclaredMethod oc fname
                                       (into-array Class
                                                   (map #(.getClass ^Object %) as)))
                   (invoke obj (into-array Object as))))]
    (when (not= res false) res)))

(defn- make-call [f args]
  (if (dot-form f)
    (dot-call f args)
    (apply f args)))

(defn -call-fn [context rel f args]
  (let [sources     (:sources context)
        attrs       (:attrs rel)
        len         (count args)
        static-args (da/make-array len)
        tuples-args (da/make-array len)]
    (dotimes [i len]
      (let [arg (nth args i)]
        (if (symbol? arg)
          (if-some [source (get sources arg)]
            (da/aset static-args i source)
            (da/aset tuples-args i (get attrs arg)))
          (da/aset static-args i arg))))
    ;; CLJS `apply` + `vector` will hold onto mutable array of arguments directly
    ;; https://github.com/tonsky/datascript/issues/262
    (if #?(:clj  false
           :cljs (identical? f vector))
      (fn [tuple]
        ;; TODO raise if not all args are bound
        (let [args (da/aclone static-args)]
          (dotimes [i len]
            (when-some [tuple-idx (aget tuples-args i)
                        ]
              (let [tg (if (da/array? tuple) r/typed-aget get)
                    v  (tg tuple tuple-idx)]
                (da/aset args i v))))
          (make-call f args)))
      (fn [tuple]
        ;; TODO raise if not all args are bound
        (dotimes [i len]
          (when-some [tuple-idx (aget tuples-args i)]
            (let [tg (if (da/array? tuple) r/typed-aget get)
                  v  (tg tuple tuple-idx)]
              (da/aset static-args i v))))
        (make-call f static-args)))))

(defn- resolve-sym [sym]
  #?(:cljs nil
     :clj (when-let [v (or (resolve sym)
                           (when (find-ns 'pod.huahaiy.datalevin)
                             (ns-resolve 'pod.huahaiy.datalevin sym)))]
            @v)))

(defn filter-by-pred [context clause]
  (let [[[f & args]]         clause
        pred                 (or (get bi/query-fns f)
                                 (context-resolve-val context f)
                                 (dot-form f)
                                 (resolve-sym f)
                                 (when (nil? (rel-with-attr context f))
                                   (raise "Unknown predicate '" f " in " clause
                                          {:error :query/where, :form clause, :var f})))
        [context production] (rel-prod-by-attrs context (filter symbol? args))
        new-rel              (if pred
                               (let [tuple-pred (-call-fn context production pred args)]
                                 (update production :tuples #(filter tuple-pred %)))
                               (assoc production :tuples []))]
    (update context :rels conj new-rel)))

(defonce pod-fns (atom {}))

(defn bind-by-fn [context clause]
  (let [[[f & args] out] clause
        binding          (dp/parse-binding out)
        fun              (or (get bi/query-fns f)
                             (context-resolve-val context f)
                             (resolve-sym f)
                             (dot-form f)
                             (when (nil? (rel-with-attr context f))
                               (raise "Unknown function '" f " in " clause
                                      {:error :query/where, :form clause, :var f})))
        fun              (if-let [s (:pod.huahaiy.datalevin/inter-fn fun)]
                           (@pod-fns s)
                           fun)

        [context production] (rel-prod-by-attrs context (filter symbol? args))
        new-rel              (if fun
                               (let [tuple-fn (-call-fn context production fun args)
                                     rels     (for [tuple (:tuples production)
                                                    :let  [val (tuple-fn tuple)]
                                                    :when (not (nil? val))]
                                                (r/prod-rel (Relation. (:attrs production) [tuple])
                                                            (in->rel binding val)))]
                                 (if (empty? rels)
                                   (r/prod-rel production (r/empty-rel binding))
                                   (reduce r/sum-rel rels)))
                               (r/prod-rel (assoc production :tuples [])
                                           (r/empty-rel binding)))]
    (update context :rels r/collapse-rels new-rel)))

;;; RULES

(def rule-head #{'_ 'or 'or-join 'and 'not 'not-join})

(defn rule? [context clause]
  (u/cond+
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
           {:error :query/where
            :form  clause})

    :else true))

(def rule-seqid (atom 0))

(defn expand-rule [clause context used-args]
  (let [[rule & call-args] clause
        seqid              (swap! rule-seqid inc)
        branches           (get (:rules context) rule)]
    (for [branch branches
          :let [[[_ & rule-args] & clauses] branch
                replacements (zipmap rule-args call-args)]]
      (walk/postwalk
       #(if (free-var? %)
          (u/some-of
            (replacements %)
            (symbol (str (name %) "__auto__" seqid)))
          %)
        clauses))))

(defn remove-pairs [xs ys]
  (let [pairs (->> (map vector xs ys)
                   (remove (fn [[x y]] (= x y))))]
    [(map first pairs)
     (map second pairs)]))

(defn rule-gen-guards [rule-clause used-args]
  (let [[rule & call-args] rule-clause
        prev-call-args     (get used-args rule)]
    (for [prev-args prev-call-args
          :let [[call-args prev-args] (remove-pairs call-args prev-args)]]
      [(concat ['-differ?] call-args prev-args)])))

(defn walk-collect [form pred]
  (let [res (atom [])]
    (walk/postwalk #(do (when (pred %) (swap! res conj %)) %) form)
    @res))

(defn collect-vars [clause]
  (set (walk-collect clause free-var?)))

(defn split-guards [clauses guards]
  (let [bound-vars (collect-vars clauses)
        pred       (fn [[[_ & vars]]] (every? bound-vars vars))]
    [(filter pred guards)
     (remove pred guards)]))

(defn solve-rule [context clause]
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
           rel   (Relation. final-attrs-map [])]
      (if-some [frame (first stack)]
        (let [[clauses [rule-clause & next-clauses]] (split-with #(not (rule? context %)) (:clauses frame))]
          (if (nil? rule-clause)

            ;; no rules -> expand, collect, sum
            (let [context (solve (:prefix-context frame) clauses)
                  tuples  (-collect context final-attrs)
                  new-rel (Relation. final-attrs-map tuples)]
              (recur (next stack) (r/sum-rel rel new-rel)))

            ;; has rule -> add guards -> check if dead -> expand rule -> push to stack, recur
            (let [[rule & call-args]     rule-clause
                  guards                 (rule-gen-guards rule-clause (:used-args frame))
                  [active-gs pending-gs] (split-guards (concat (:prefix-clauses frame) clauses)
                                                       (concat guards (:pending-guards frame)))]
              (if (some #(= % '[(-differ?)]) active-gs) ;; trivial always false case like [(not= [?a ?b] [?a ?b])]

                ;; this branch has no data, just drop it from stack
                (recur (next stack) rel)

                (let [prefix-clauses (concat clauses active-gs)
                      prefix-context (solve (:prefix-context frame) prefix-clauses)]
                  (if (empty-rels? prefix-context)

                    ;; this branch has no data, just drop it from stack
                    (recur (next stack) rel)

                    ;; need to expand rule to branches
                    (let [used-args (assoc (:used-args frame) rule
                                           (conj (get (:used-args frame) rule []) call-args))
                          branches  (expand-rule rule-clause context used-args)]
                      (recur (concat
                               (for [branch branches]
                                 {:prefix-clauses prefix-clauses
                                  :prefix-context prefix-context
                                  :clauses        (concatv branch next-clauses)
                                  :used-args      used-args
                                  :pending-guards pending-gs})
                               (next stack))
                             rel))))))))
        rel))))

(defn resolve-pattern-lookup-refs [source pattern]
  (if (db/-searchable? source)
    (let [[e a v] pattern]
      (->
        [(if (or (lookup-ref? e) (attr? e)) (db/entid-strict source e) e)
         a
         (if (and v (attr? a) (db/ref? source a) (or (lookup-ref? v) (attr? v)))
           (db/entid-strict source v)
           v)]
        (subvec 0 (count pattern))))
    pattern))

(defn dynamic-lookup-attrs [source pattern]
  (let [[e a v] pattern]
    (cond-> #{}
      (free-var? e)         (conj e)
      (and
        (free-var? v)
        (not (free-var? a))
        (db/ref? source a)) (conj v))))

(defn- clause-size
  [clause]
  (let [source  r/*implicit-source*
        pattern (resolve-pattern-lookup-refs source clause)]
    (pattern-size source pattern)))

(defn limit-rel [rel vars]
  (when-some [attrs' (not-empty (select-keys (:attrs rel) vars))]
    (assoc rel :attrs attrs')))

(defn limit-context [context vars]
  (assoc context
    :rels (->> (:rels context)
               (keep #(limit-rel % vars)))))

(defn bound-vars [context]
  (into #{} (mapcat #(keys (:attrs %)) (:rels context))))

(defn check-bound [bound vars form]
  (when-not (set/subset? vars bound)
    (let [missing (set/difference (set vars) bound)]
      (raise "Insufficient bindings: " missing " not bound in " form
             {:error :query/where
              :form  form
              :vars  missing}))))

(defn check-free-same [bound branches form]
  (let [free (mapv #(set/difference (collect-vars %) bound) branches)]
    (when-not (apply = free)
      (raise "All clauses in 'or' must use same set of free vars, had " free " in " form
             {:error :query/where
              :form  form
              :vars  free}))))

(defn check-free-subset [bound vars branches]
  (let [free (set (remove bound vars))]
    (doseq [branch branches]
      (when-some [missing (not-empty (set/difference free (collect-vars branch)))]
        (prn branch bound vars free)
        (raise "All clauses in 'or' must use same set of free vars, had " missing " not bound in " branch
          {:error :query/where
           :form  branch
           :vars  missing})))))

(defn -resolve-clause
  ([context clause]
   (-resolve-clause context clause clause))
  ([context clause orig-clause]
   (condp looks-like? clause
     [[symbol? '*]] ;; predicate [(pred ?a ?b ?c)]
     (do
       (check-bound (bound-vars context) (filter free-var? (nfirst clause)) clause)
       (filter-by-pred context clause))

     [[symbol? '*] '_] ;; function [(fn ?a ?b) ?res]
     (do
       (check-bound (bound-vars context) (filter free-var? (nfirst clause)) clause)
       (bind-by-fn context clause))

     [source? '*] ;; source + anything
     (let [[source-sym & rest] clause]
       (binding [r/*implicit-source* (get (:sources context) source-sym)]
         (-resolve-clause context rest clause)))

     '[or *] ;; (or ...)
     (let [[_ & branches] clause
           _              (check-free-same (bound-vars context) branches clause)
           contexts       (map #(resolve-clause context %) branches)
           rels           (map #(reduce r/hash-join (:rels %)) contexts)]
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
           _                   (check-free-subset (bound-vars context) vars branches)
           join-context        (limit-context context vars)
           contexts            (map #(-> join-context (resolve-clause %) (limit-context vars)) branches)
           rels                (map #(reduce r/hash-join (:rels %)) contexts)
           sum-rel             (reduce r/sum-rel rels)]
       (update context :rels r/collapse-rels sum-rel))

     '[and *] ;; (and ...)
     (let [[_ & clauses] clause]
       (reduce resolve-clause context clauses))

     '[not *] ;; (not ...)
     (let [[_ & clauses]    clause
           bound            (bound-vars context)
           negation-vars    (collect-vars clauses)
           _                (when (empty? (set/intersection bound negation-vars))
                              (raise "Insufficient bindings: none of " negation-vars " is bound in " orig-clause
                                     {:error :query/where
                                      :form  orig-clause}))
           context'         (assoc context :rels [(reduce r/hash-join (:rels context))])
           negation-context (reduce resolve-clause context' clauses)
           negation         (r/subtract-rel
                              (single (:rels context'))
                              (reduce r/hash-join (:rels negation-context)))]
       (assoc context' :rels [negation]))

     '[not-join [*] *] ;; (not-join [vars] ...)
     (let [[_ vars & clauses] clause
           bound              (bound-vars context)
           _                  (check-bound bound vars orig-clause)
           context'           (assoc context :rels [(reduce r/hash-join (:rels context))])
           join-context       (limit-context context' vars)
           negation-context   (-> (reduce resolve-clause join-context clauses)
                                  (limit-context vars))
           negation           (r/subtract-rel
                                (single (:rels context'))
                                (reduce r/hash-join (:rels negation-context)))]
       (assoc context' :rels [negation]))

     '[*] ;; pattern
     (let [source   r/*implicit-source*
           pattern  (resolve-pattern-lookup-refs source clause)
           relation (lookup-pattern source pattern)]
       (binding [r/*lookup-attrs* (if (db/-searchable? source)
                                    (dynamic-lookup-attrs source pattern)
                                    r/*lookup-attrs*)]
         (update context :rels r/collapse-rels relation))))))

(defn resolve-clause [context clause]
  (if (rule? context clause)
    (if (source? (first clause))
      (binding [r/*implicit-source* (get (:sources context) (first clause))]
        (resolve-clause context (next clause)))
      (update context :rels r/collapse-rels (solve-rule context clause)))
    (-resolve-clause context clause)))

(defn- sort-clauses [context clauses]
  (sort-by (fn [clause]
             (if (rule? context clause)
               Long/MAX_VALUE
               ;; TODO dig into these
               (condp looks-like? clause
                 [[symbol? '*]] ;; predicate [(pred ?a ?b ?c)]
                 Long/MAX_VALUE

                 [[symbol? '*] '_] ;; function [(fn ?a ?b) ?res]
                 Long/MAX_VALUE

                 [source? '*] ;; source + anything
                 Long/MAX_VALUE

                 '[or *] ;; (or ...)
                 Long/MAX_VALUE

                 '[or-join [[*] *] *] ;; (or-join [[req-vars] vars] ...)
                 Long/MAX_VALUE

                 '[or-join [*] *] ;; (or-join [vars] ...)
                 Long/MAX_VALUE

                 '[and *] ;; (and ...)
                 Long/MAX_VALUE

                 '[not *] ;; (not ...)
                 Long/MAX_VALUE

                 '[not-join [*] *] ;; (not-join [vars] ...)
                 Long/MAX_VALUE

                 '[*] ;; pattern
                 (clause-size clause))))
           clauses))

(defn- build-graph [context clauses]
  (let [[node group] (group-by first clauses)]
    ))

(defn- planning [context]
  )

(defn- excecute [context]
  )

(defn -q [context clauses]
  (binding [r/*implicit-source* (get (:sources context) '$)]
    (reduce resolve-clause context (sort-clauses context clauses))
    #_(-> context
          (build-graph clauses)
          planning
          excecute)))

(defn -collect
  ([context symbols]
   (let [rels (:rels context)]
     (-collect [(da/make-array (count symbols))] rels symbols)))
  ([acc rels symbols]
   (if-some [rel (first rels)]
     (let [keep-attrs (select-keys (:attrs rel) symbols)]
       (if (empty? keep-attrs)
         (recur acc (next rels) symbols)
         (let [copy-map (to-array (map #(get keep-attrs %) symbols))
               len      (count symbols)]
           (recur (for [#?(:cljs t1
                           :clj ^{:tag "[[Ljava.lang.Object;"} t1) acc
                        t2                                         (:tuples rel)]
                    (let [res (aclone t1)
                          tg  (if (da/array? t2) r/typed-aget get)]
                      (dotimes [i len]
                        (when-some [idx (aget copy-map i)]
                          (aset res i (tg t2 idx))))
                      res))
                  (next rels)
                  symbols))))
     acc)))

(defn collect [context symbols]
  (->> (-collect context symbols)
       (map vec)
       set))

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
    (or (get bi/aggregates (.-symbol var))
        (resolve-sym (.-symbol var))))
  Constant
  (-context-resolve [var _]
    (.-value var)))

(defn -aggregate [find-elements context tuples]
  (mapv (fn [element fixed-value i]
          (if (dp/aggregate? element)
            (let [f    (-context-resolve (:fn element) context)
                  args (map #(-context-resolve % context) (butlast (:args element)))
                  vals (map #(nth % i) tuples)]
              (apply f (concat args [vals])))
            fixed-value))
    find-elements
    (first tuples)
    (range)))

(defn- idxs-of [pred coll]
  (->> (map #(when (pred %1) %2) coll (range))
       (remove nil?)))

(defn aggregate [find-elements context resultset]
  (let [group-idxs (idxs-of (complement dp/aggregate?) find-elements)
        group-fn   (fn [tuple]
                     (map #(nth tuple %) group-idxs))
        grouped    (group-by group-fn resultset)]
    (for [[_ tuples] grouped]
      (-aggregate find-elements context tuples))))

(defn map* [f xs]
  (reduce #(conj %1 (f %2)) (empty xs) xs))

(defn tuples->return-map [return-map tuples]
  (let [symbols (:symbols return-map)
        idxs    (range 0 (count symbols))]
    (map*
      (fn [tuple]
        (reduce
          (fn [m i] (assoc m (nth symbols i) (nth tuple i)))
          {} idxs))
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
  (-post-process [_ return-map tuples]
    (into [] (map first) tuples))

  FindScalar
  (-post-process [_ return-map tuples]
    (ffirst tuples))

  FindTuple
  (-post-process [_ return-map tuples]
    (if (some? return-map)
      (first (tuples->return-map return-map [(first tuples)]))
      (first tuples))))

(defn- pull [find-elements context resultset]
  (let [resolved (for [find find-elements]
                   (when (dp/pull? find)
                     [(-context-resolve (:source find) context)
                      (dpp/parse-pull
                        (-context-resolve (:pattern find) context))]))]
    (for [tuple resultset]
      (mapv (fn [env el]
              (if env
                (let [[src spec] env]
                  (dpa/pull-spec src spec [el] false))
                el))
            resolved
            tuple))))

(def ^:private query-cache (volatile! (datalevin.lru/lru lru-cache-size
                                                         :constant)))

(defn memoized-parse-query [q]
  (if-some [cached (get @query-cache q nil)]
    cached
    (let [qp (dp/parse-query q)]
      (vswap! query-cache assoc q qp)
      qp)))

(defn q [q & inputs]
  (let [parsed-q      (memoized-parse-query q)
        find          (:qfind parsed-q)
        find-elements (dp/find-elements find)
        find-vars     (dp/find-vars find)
        result-arity  (count find-elements)
        with          (:qwith parsed-q)
        ;; TODO utilize parser
        all-vars      (concat find-vars (map :symbol with))
        q             (cond-> q
                        (sequential? q) dp/query->map)
        wheres        (:where q)
        context       (-> (Context. [] {} {})
                          (resolve-ins (:qin parsed-q) inputs))
        resultset     (-> context
                          (-q wheres)
                          (collect all-vars))]
    (cond->> resultset
      (:with q)
      (mapv #(vec (subvec % 0 result-arity)))
      (some dp/aggregate? find-elements)
      (aggregate find-elements context)
      (some dp/pull? find-elements)
      (pull find-elements context)
      true
      (-post-process find (:qreturn-map parsed-q)))))

(comment

  (require '[datalevin.core :as d])
  (def next-eid (volatile! -1))
  (defn random-man []
    {:db/id      (vswap! next-eid inc)
     :first-name (rand-nth ["James" "John" "Robert" "Michael" "William" "David"
                            "Richard" "Charles" "Joseph" "Thomas"])
     :last-name  (rand-nth ["Smith" "Johnson" "Williams" "Brown" "Jones" "Garcia"
                            "Miller" "Davis" "Rodriguez" "Martinez"])
     :age        (rand-int 100)
     :salary     (rand-int 100000)})
  (def people100 (shuffle (take 100 (repeatedly random-man))))
  (defn rand-str []
    (apply str
           (for [i (range (+ 3 (rand-int 10)))]
             (char (+ (rand 26) 65)))))
  (defn random-school []
    {:db/id      (vswap! next-eid inc)
     :name       (rand-str)
     :ownership  (rand-nth [:private :public])
     :enrollment (rand-int 10000)})
  (def school20 (shuffle (take 20 (repeatedly random-school))))
  (def schema
    {:follows    {:db/valueType   :db.type/ref
                  :db/cardinality :db.cardinality/many }
     :school     {:db/valueType   :db.type/ref
                  :db/cardinality :db.cardinality/many }
     :born       {:db/valueType   :db.type/ref
                  :db/cardinality :db.cardinality/one}
     :location   {:db/valueType   :db.type/ref
                  :db/cardinality :db.cardinality/one}
     :name       {:db/valueType :db.type/string}
     :first-name {:db/valueType :db.type/string}
     :last-name  {:db/valueType :db.type/string}
     :ownership  {:db/valueType :db.type/keyword}
     :enrollment {:db/valueType :db.type/long}
     :age        {:db/valueType :db.type/long}
     :salary     {:db/valueType :db.type/long}})
  (defn random-place []
    {:db/id (vswap! next-eid inc)
     :name  (rand-str)})
  (def place15 (shuffle (take 15 (repeatedly random-place))))
  (def conn (d/create-conn nil schema))
  (d/transact! conn people100)
  (d/transact! conn school20)
  (d/transact! conn place15)
  (dotimes [_ 60]
    (d/transact! conn [{:db/id (rand-int 100) :follows (rand-int 100)}]))
  (dotimes [_ 80]
    (d/transact! conn [{:db/id (rand-int 100) :school (+ 100 (rand-int 20))}]))
  (dotimes [i 70]
    (d/transact! conn [{:db/id i :born (+ 120 (rand-int 15))}]))
  (dotimes [i 15]
    (d/transact! conn [{:db/id (+ 100 i) :location (+ 120 (rand-int 15))}]))

  (def store (.-store (d/db conn)))
  (def attrs (st/attrs store))
  {0 :db/ident, 7 :name, 1 :db/created-at, 4 :last-name, 13 :salary, 6 :school, 3 :enrollment, 12 :location, 2 :db/updated-at, 11 :born, 9 :first-name, 5 :age, 10 :ownership, 8 :follows}
  (def classes (st/classes store))
  (def rclasses (st/rclasses store))
  (def entities (st/entities store))
  (def rentities (st/rentities store))
  (def links (st/links store))
  (def rlinks (st/rlinks store))

  (q '[:find ?e ?e1
       :in $ ?fn
       :where
       [?e :first-name ?fn]
       [?e :follows ?e1]]
     (d/db conn) "Robert")

  )
