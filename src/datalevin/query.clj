(ns ^:no-doc datalevin.query
  "Datalog query engine"
  (:refer-clojure :exclude [update assoc])
  (:require
   [clojure.set :as set]
   [clojure.edn :as edn]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [datalevin.db :as db]
   [datalevin.lmdb :as l]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r]
   [datalevin.storage :as s]
   [datalevin.built-ins :as built-ins]
   [datalevin.util :as u :refer [raise cond+ conjv concatv]]
   [datalevin.inline :refer [update assoc]]
   [datalevin.spill :as sp]
   [datalevin.parser :as dp]
   [datalevin.pull-api :as dpa]
   [datalevin.timeout :as timeout]
   [datalevin.constants :as c]
   [datalevin.bits :as b])
  (:import
   [java.util Arrays List Collection]
   [java.util.concurrent ConcurrentHashMap]
   [clojure.lang ILookup LazilyPersistentVector]
   [datalevin.utl LikeFSM LRUCache]
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.parser BindColl BindIgnore BindScalar BindTuple Constant
    FindColl FindRel FindScalar FindTuple PlainSymbol RulesVar SrcVar
    Variable Pattern Predicate]
   [org.eclipse.collections.impl.map.mutable UnifiedMap]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *query-cache* (LRUCache. 128))

(def ^:dynamic *plan-cache* (LRUCache. 128))

(def ^:dynamic *explain* nil)

(def ^:dynamic *start-time* nil)

(declare -collect -resolve-clause resolve-clause hash-join execute-steps)

;; Records

(defrecord Context [parsed-q rels sources rules opt-clauses late-clauses
                    graph plan intermediates run? result-set])

(defrecord Plan [steps cost size])

(defprotocol IStep
  (-type [step] "return the type of step as a keyword")
  (-execute [step db source] "execute query step and return tuples")
  (-execute-pipe [step db source sink] "execute as part of pipeline")
  (-sample [step db source] "sample the query step")
  (-explain [step context] "explain the query step"))

(defrecord InitStep
    [attr pred val range vars in out know-e? cols mcount result sample]

  IStep
  (-type [_] :init)

  (-execute [_ db _]
    (let [get-v? (< 1 (count vars))
          e      (first vars)]
      (if result
        result
        (cond
          know-e?
          (let [src (doto (FastList.) (.add (object-array [e])))]
            (if get-v?
              (db/-eav-scan-v-list db src 0 [[attr {:skip? false}]])
              src))
          (nil? val)
          (db/-init-tuples-list
            db attr (or range [[[:closed c/v0] [:closed c/vmax]]]) pred get-v?)
          :else
          (db/-init-tuples-list
            db attr [[[:closed val] [:closed val]]] nil false)))))

  (-execute-pipe [_ db _ sink]
    (let [get-v? (< 1 (count vars))
          e      (first vars)]
      (if result
        (.addAll ^Collection sink result)
        (cond
          know-e?
          (let [src (doto ^Collection (s/tuple-pipe)
                      (.add (object-array [e]))
                      (s/finish-output))]
            (if get-v?
              (db/-eav-scan-v db src sink 0 [[attr {:skip? false}]])
              (s/drain-to src sink)))
          (nil? val)
          (db/-init-tuples
            db sink attr
            (or range [[[:closed c/v0] [:closed c/vmax]]]) pred get-v?)
          :else
          (db/-init-tuples
            db sink attr [[[:closed val] [:closed val]]] nil false)))))

  (-sample [_ db _]
    (let [get-v? (< 1 (count vars))]
      (if (nil? val)
        (if range
          (db/-sample-init-tuples-list db attr mcount range pred get-v?)
          (cond-> (db/-e-sample db attr)
            get-v?
            (#(db/-eav-scan-v-list db % 0 [[attr {:skip? false :pred pred}]]))
            (not get-v?)
            (#(db/-eav-scan-v-list db % 0 [[attr {:skip? true :pred pred}]]))))
        (db/-sample-init-tuples-list
          db attr mcount [[[:closed val] [:closed val]]] nil false))))

  (-explain [_ _]
    (str "Initialize " vars " " (cond
                                  know-e? "by a known entity id."

                                  (nil? val)
                                  (if range
                                    (str "by range " range " on " attr ".")
                                    (str "by " attr "."))

                                  (some? val)
                                  (str "by " attr " = " val ".")))))

(defrecord MergeScanStep [index attrs-v vars in out cols result sample]

  IStep
  (-type [_] :merge)

  (-execute [_ db source]
    (if result
      result
      (db/-eav-scan-v-list db source index attrs-v)))

  (-execute-pipe [_ db source sink]
    (if result
      (.addAll ^Collection sink result)
      (db/-eav-scan-v db source sink index attrs-v)))

  (-sample [_ db tuples]
    (if (< 0 (.size ^List tuples))
      (db/-eav-scan-v-list db tuples index attrs-v)
      (FastList.)))

  (-explain [_ _]
    (if (seq vars)
      (str "Merge " (vec vars) " by scanning " (mapv first attrs-v) ".")
      (str "Filter by predicates on " (mapv first attrs-v) "."))))

(defrecord LinkStep [type index attr var fidx in out cols]

  IStep
  (-type [_] :link)

  (-execute [_ db src]
    (cond
      (int? var) (db/-val-eq-scan-e-list db src index attr var)
      fidx       (db/-val-eq-filter-e-list db src index attr fidx)
      :else      (db/-val-eq-scan-e-list db src index attr)))

  (-execute-pipe [_ db src sink]
    (cond
      (int? var) (db/-val-eq-scan-e db src sink index attr var)
      fidx       (db/-val-eq-filter-e db src sink index attr fidx)
      :else      (db/-val-eq-scan-e db src sink index attr)))

  (-explain [_ _]
    (str "Merge " var " by "
         (if (identical? type :_ref) "reverse reference" "equal values")
         " of " attr ".")))

(defrecord Node [links mpath mcount bound free])

(defrecord Link [type tgt var attrs attr])

(defrecord Clause [val var range count pred])

;; rules

(defn parse-rules
  [rules]
  (let [rules (if (string? rules) (edn/read-string rules) rules)]
    (dp/parse-rules rules) ;; validation
    (group-by ffirst rules)))

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
        #(if (qu/free-var? %)
           (u/some-of
             (replacements %)
             (symbol (str (name %) "__auto__" seqid)))
           %)
        clauses))))

(defn remove-pairs
  [xs ys]
  (let [pairs (sequence (comp (map vector)
                           (remove (fn [[x y]] (= x y))))
                        xs ys)]
    [(map first pairs) (map peek pairs)]))

(defn rule-gen-guards
  [rule-clause used-args]
  (let [[rule & call-args] rule-clause
        prev-call-args     (get used-args rule)]
    (for [prev-args prev-call-args
          :let      [[call-args prev-args] (remove-pairs call-args prev-args)]]
      [(concatv ['-differ?] call-args prev-args)])))

(defn split-guards
  [clauses guards]
  (let [bound-vars (qu/collect-vars clauses)
        pred       (fn [[[_ & vars]]] (every? bound-vars vars))]
    [(filter pred guards)
     (remove pred guards)]))

(defn solve-rule
  [context clause]
  (let [final-attrs     (filter qu/free-var? clause)
        final-attrs-map (zipmap final-attrs (range))
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
              (split-with #(not (qu/rule? context %)) (:clauses frame))]
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

;; binding

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
      (r/empty-rel binding)
      :else
      (transduce
        (map #(in->rel (:binding binding) %))
        r/sum-rel
        coll)))

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
      (reduce r/prod-rel (map #(in->rel %1 %2) (:bindings binding) coll)))))

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

(def ^{:dynamic true
       :doc     "List of symbols in current pattern that might potentiall be resolved to refs"}
  *lookup-attrs* nil)

(def ^{:dynamic true
       :doc     "Default pattern source. Lookup refs, patterns, rules will be resolved with it"}
  *implicit-source* nil)

;; hash join

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

(defn hash-tuples [key-fn tuples] (-group-by key-fn '() tuples))

(defn- diff-keys
  "return (- vec2 vec1) elements"
  [vec1 vec2]
  (persistent!
    (reduce
      (fn [d e2]
        (if (some (fn [e1] (= e1 e2)) vec1)
          d
          (conj! d e2)))
      (transient []) vec2)))

(defn- attr-keys
  "attrs are map, preserve order by val"
  [attrs]
  (->> attrs (sort-by val) (mapv key)))

(defn hash-join
  [rel1 rel2]
  (let [tuples1      (:tuples rel1)
        tuples2      (:tuples rel2)
        attrs1       (:attrs rel1)
        attrs2       (:attrs rel2)
        common-attrs (vec (qu/intersect-keys attrs1 attrs2))
        keep-attrs1  (attr-keys attrs1)
        keep-attrs2  (diff-keys keep-attrs1 (attr-keys attrs2))
        keep-idxs1   (to-array (sort (vals attrs1)))
        keep-idxs2   (to-array (->Eduction (map attrs2) keep-attrs2))
        key-fn1      (tuple-key-fn attrs1 common-attrs)
        key-fn2      (tuple-key-fn attrs2 common-attrs)
        attrs        (zipmap (concatv keep-attrs1 keep-attrs2) (range))]
    (if (< (count tuples1) (count tuples2))
      (let [^UnifiedMap hash (hash-tuples key-fn1 tuples1)]
        (r/relation! attrs
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
      (let [^UnifiedMap hash (hash-tuples key-fn2 tuples2)]
        (r/relation! attrs
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

        attrs    (vec (qu/intersect-keys attrs-a attrs-b))
        key-fn-b (tuple-key-fn attrs-b attrs)
        hash     (hash-tuples key-fn-b tuples-b)
        key-fn-a (tuple-key-fn attrs-a attrs)]
    (assoc a :tuples
           (filterv #(nil? (.get ^UnifiedMap hash (key-fn-a %))) tuples-a))))

(defn- rel-with-attr [context sym]
  (some #(when (contains? (:attrs %) sym) %) (:rels context)))

(defn substitute-constant [context pattern-el]
  (when (qu/free-var? pattern-el)
    (when-some [rel (rel-with-attr context pattern-el)]
      (when-some [tuple (first (:tuples rel))]
        (when (nil? (fnext (:tuples rel)))
          (let [idx (get (:attrs rel) pattern-el)]
            (get tuple idx)))))))

(defn substitute-constants [context pattern]
  (mapv #(or (substitute-constant context %) %) pattern))

(defn resolve-pattern-lookup-refs [source pattern]
  (if (db/-searchable? source)
    (let [[e a v] pattern
          e'      (if (or (qu/lookup-ref? e) (keyword? e))
                    (db/entid-strict source e)
                    e)
          v'      (if (and v
                           (keyword? a)
                           (db/ref? source a)
                           (or (qu/lookup-ref? v) (keyword? v)))
                    (db/entid-strict source v)
                    v)]
      (subvec [e' a v'] 0 (count pattern)))
    pattern))

(defn lookup-pattern-db
  [context db pattern]
  (let [search-pattern (->> pattern
                            (substitute-constants context)
                            (resolve-pattern-lookup-refs db)
                            (mapv #(if (or (= % '_) (qu/free-var? %)) nil %)))
        datoms         (db/-search db search-pattern)
        attr->prop     (into {}
                             (filter (fn [[s _]] (qu/free-var? s)))
                             (map vector pattern ["e" "a" "v"]))]
    (r/relation! attr->prop datoms)))

(defn matches-pattern?
  [pattern tuple]
  (loop [tuple   tuple
         pattern pattern]
    (if (and tuple pattern)
      (let [t (first tuple)
            p (first pattern)]
        (if (or (= p '_) (qu/free-var? p) (= t p))
          (recur (next tuple) (next pattern))
          false))
      true)))

(defn lookup-pattern-coll
  [coll pattern]
  (let [data      (filter #(matches-pattern? pattern %) coll)
        attr->idx (into {}
                        (filter (fn [[s _]] (qu/free-var? s)))
                        (map vector pattern (range)))]
    (r/relation! attr->idx (u/map-fl to-array data))))

(defn normalize-pattern-clause
  [clause]
  (if (qu/source? (first clause))
    clause
    (into ['$] clause)))

(defn lookup-pattern
  [context source pattern]
  (if (db/-searchable? source)
    (lookup-pattern-db context source pattern)
    (lookup-pattern-coll source pattern)))

(defn collapse-rels
  [rels new-rel]
  (persistent!
    (loop [rels    rels
           new-rel new-rel
           acc     (transient [])]
      (if-some [rel (first rels)]
        (if (not-empty (qu/intersect-keys (:attrs new-rel) (:attrs rel)))
          (recur (next rels) (hash-join rel new-rel) acc)
          (recur (next rels) new-rel (conj! acc rel)))
        (conj! acc new-rel)))))

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
  [fname ^objects args]
  (let [obj (aget args 0)
        oc  (.getClass ^Object obj)
        as  (rest args)
        res (if (zero? (count as))
              (. (.getDeclaredMethod oc fname nil) (invoke obj nil))
              (. (.getDeclaredMethod
                   oc fname
                   (into-array Class (map #(.getClass ^Object %) as)))
                 (invoke obj (into-array Object as))))]
    (when (not= res false) res)))

(defn- make-call
  [f]
  (if (dot-form f)
    (let [fname (subs (name f) 1)] #(dot-call fname %))
    #(apply f %)))

(defn- resolve-sym
  [sym]
  (when (symbol? sym)
    (when-let [v (or (resolve sym)
                     (when (find-ns 'pod.huahaiy.datalevin)
                       (ns-resolve 'pod.huahaiy.datalevin sym)))]
      @v)))

(defonce pod-fns (atom {}))

(defn- resolve-pred
  [f context]
  (let [fun (if (fn? f)
              f
              (or (get built-ins/query-fns f)
                  (context-resolve-val context f)
                  (dot-form f)
                  (resolve-sym f)
                  (when (nil? (rel-with-attr context f))
                    (raise "Unknown function or predicate '" f
                           {:error :query/where :var f}))))]
    (if-let [s (:pod.huahaiy.datalevin/inter-fn fun)]
      (@pod-fns s)
      fun)))

(defn- -call-fn*
  [context rel f args]
  (let [sources              (:sources context)
        attrs                (:attrs rel)
        len                  (count args)
        ^objects static-args (make-array Object len)
        ^objects tuples-args (make-array Object len)
        call                 (make-call (resolve-pred f context))]
    (dotimes [i len]
      (let [arg (nth args i)]
        (if (symbol? arg)
          (if-some [source (get sources arg)]
            (aset static-args i source)
            (aset tuples-args i (get attrs arg)))
          (aset static-args i arg))))
    (fn call-fn [tuple]
      (dotimes [i len]
        (when-some [tuple-idx (aget tuples-args i)]
          (let [tg (if (u/array? tuple) r/typed-aget get)
                v  (tg tuple tuple-idx)]
            (aset static-args i v))))
      (call static-args))))

(defn -call-fn
  [context rel f args]
  (case f
    (and or not) (let [args' (walk/postwalk
                               (fn [e]
                                 (if (list? e)
                                   (let [[f & args] e]
                                     (-call-fn context rel f args))
                                   e))
                               (vec args))]
                   (fn [tuple]
                     (apply (get built-ins/query-fns f)
                            (walk/postwalk
                              (fn [e] (if (fn? e) (e tuple) e))
                              args'))))
    (-call-fn* context rel f args)))

(defn filter-by-pred
  [context clause]
  (let [[[f & args]]         clause
        attrs                (qu/collect-vars args)
        [context production] (rel-prod-by-attrs context attrs)

        new-rel (let [tuple-pred (-call-fn context production f args)]
                  (update production :tuples #(filter tuple-pred %)))]
    (update context :rels conj new-rel)))

(defn bind-by-fn
  [context clause]
  (let [[[f & args] out]     clause
        binding              (dp/parse-binding out)
        attrs                (qu/collect-vars args)
        [context production] (rel-prod-by-attrs context attrs)
        new-rel
        (let [tuple-fn (-call-fn context production f args)
              rels     (for [tuple (:tuples production)
                             :let  [val (tuple-fn tuple)]
                             :when (not (nil? val))]
                         (r/prod-rel
                           (r/relation! (:attrs production)
                                        (doto (FastList.) (.add tuple)))
                           (in->rel binding val)))]
          (if (empty? rels)
            (r/prod-rel production (r/empty-rel binding))
            (reduce r/sum-rel rels)))]
    (update context :rels collapse-rels new-rel)))

;;; RULES

(def rule-head #{'_ 'or 'or-join 'and 'not 'not-join})

(defn rule?
  [context clause]
  (cond+
    (not (sequential? clause))
    false

    :let [head (if (qu/source? (first clause))
                 (second clause)
                 (first clause))]

    (not (symbol? head))
    false

    (qu/free-var? head)
    false

    (contains? rule-head head)
    false

    (not (contains? (:rules context) head))
    (raise "Unknown rule '" head " in " clause
           {:error :query/where :form clause})

    :else true))

(defn dynamic-lookup-attrs
  [source pattern]
  (let [[e a v] pattern]
    (cond-> #{}
      (qu/free-var? e)      (conj e)
      (and
        (qu/free-var? v)
        (not (qu/free-var? a))
        (db/ref? source a)) (conj v))))

(defn limit-rel
  [rel vars]
  (when-some [attrs (not-empty (select-keys (:attrs rel) vars))]
    (assoc rel :attrs attrs)))

(defn limit-context
  [context vars]
  (assoc context
         :rels (->> (:rels context)
                    (keep #(limit-rel % vars)))))

(defn bound-vars
  [context]
  (into #{} (mapcat #(keys (:attrs %))) (:rels context)))

(defn check-bound
  [bound vars form]
  (when-not (set/subset? vars bound)
    (let [missing (set/difference (set vars) bound)]
      (raise "Insufficient bindings: " missing " not bound in " form
             {:error :query/where :form form :vars missing}))))

(defn check-free-same
  [bound branches form]
  (let [free (mapv #(set/difference (qu/collect-vars %) bound) branches)]
    (when-not (apply = free)
      (raise "All clauses in 'or' must use same set of free vars, had " free
             " in " form
             {:error :query/where :form form :vars free}))))

(defn check-free-subset
  [bound vars branches]
  (let [free (set (remove bound vars))]
    (doseq [branch branches]
      (when-some [missing (not-empty
                            (set/difference free (qu/collect-vars branch)))]
        (prn branch bound vars free)
        (raise "All clauses in 'or' must use same set of free vars, had "
               missing " not bound in " branch
               {:error :query/where :form branch :vars missing})))))

(defn -resolve-clause
  ([context clause]
   (-resolve-clause context clause clause))
  ([context clause orig-clause]
   (condp qu/looks-like? clause
     [[symbol? '*]] ;; predicate [(pred ?a ?b ?c)]
     (do
       (check-bound (bound-vars context) (filter qu/free-var? (nfirst clause))
                    clause)
       (filter-by-pred context clause))

     [[fn? '*]] ;; predicate [(pred ?a ?b ?c)]
     (do
       (check-bound (bound-vars context) (filter qu/free-var? (nfirst clause))
                    clause)
       (filter-by-pred context clause))

     [[symbol? '*] '_] ;; function [(fn ?a ?b) ?res]
     (do
       (check-bound (bound-vars context) (filter qu/free-var? (nfirst clause))
                    clause)
       (bind-by-fn context clause))

     [[fn? '*] '_] ;; function [(fn ?a ?b) ?res]
     (do
       (check-bound (bound-vars context) (filter qu/free-var? (nfirst clause))
                    clause)
       (bind-by-fn context clause))

     [qu/source? '*] ;; source + anything
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
       (recur context (list* 'or-join (concatv req-vars vars) branches) clause))

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
           negation-vars    (qu/collect-vars clauses)
           _                (when (empty? (u/intersection bound negation-vars))
                              (raise "Insufficient bindings: none of "
                                     negation-vars " is bound in " orig-clause
                                     {:error :query/where :form orig-clause}))
           context1         (assoc context :rels
                                   [(reduce hash-join (:rels context))])
           negation-context (reduce resolve-clause context1 clauses)
           negation         (subtract-rel
                              (qu/single (:rels context1))
                              (reduce hash-join (:rels negation-context)))]
       (assoc context1 :rels [negation]))

     '[not-join [*] *] ;; (not-join [vars] ...)
     (let [[_ vars & clauses] clause
           bound              (bound-vars context)
           _                  (check-bound bound vars orig-clause)
           context1           (assoc context :rels
                                     [(reduce hash-join (:rels context))])
           join-context       (limit-context context1 vars)
           negation-context   (-> (reduce resolve-clause join-context clauses)
                                  (limit-context vars))
           negation           (subtract-rel
                                (qu/single (:rels context1))
                                (reduce hash-join (:rels negation-context)))]
       (assoc context1 :rels [negation]))

     '[*] ;; pattern
     (let [source   *implicit-source*
           pattern' (resolve-pattern-lookup-refs source clause)
           relation (lookup-pattern context source pattern')]
       (binding [*lookup-attrs* (if (db/-searchable? source)
                                  (dynamic-lookup-attrs source pattern')
                                  *lookup-attrs*)]
         (update context :rels collapse-rels relation))))))

(defn resolve-clause
  [context clause]
  (if (->> (:rels context) (some (comp empty? :tuples)))
    context
    (if (qu/rule? context clause)
      (if (qu/source? (first clause))
        (binding [*implicit-source* (get (:sources context) (first clause))]
          (resolve-clause context (next clause)))
        (update context :rels collapse-rels (solve-rule context clause)))
      (-resolve-clause context clause))))

;; optimizer

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
                                 (not (some #(= s %) finds))
                                 (not (some #(or-join-var? % s) owheres)))
                        [i s])))
                  qins)
        rm-idxs (set (map first to-rm))
        smap    (reduce (fn [m [i s]] (assoc m s (nth inputs i))) {} to-rm)]
    [(assoc parsed-q
            :qwhere (reduce-kv
                      (fn [ws s v]
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
                   (if (qu/free-var? v)
                     (assoc-in m [:free attr] (map->Clause {:var v}))
                     (assoc-in m [:bound attr] (map->Clause {:val v})))
                   (assoc-in m [:free attr] (map->Clause {})))))
             (map->Node {}) patterns)])

(defn- link-refs
  [graph]
  (let [es (set (keys graph))]
    (reduce-kv
      (fn [g e {:keys [free]}]
        (reduce-kv
          (fn [g k {:keys [var]}]
            (if (es var)
              (-> g
                  (update-in [e :links] conjv (Link. :ref var nil nil k))
                  (update-in [var :links] conjv (Link. :_ref e nil nil k)))
              g))
          g free))
      graph graph)))

(defn- link-eqs
  [graph]
  (reduce-kv
    (fn [g v lst]
      (if (< 1 (count lst))
        (reduce
          (fn [g [[e1 k1] [e2 k2]]]
            (let [attrs {e1 k1 e2 k2}]
              (-> g
                  (update-in
                    [e1 :links] conjv (Link. :val-eq e2 v attrs nil))
                  (update-in
                    [e2 :links] conjv (Link. :val-eq e1 v attrs nil)))))
          g (u/combinations lst 2))
        g))
    graph (reduce-kv (fn [m e {:keys [free]}]
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

(defn- get-src [[f & _]] (if (qu/source? f) f '$))

(defn- init-graph
  "build one graph per Datalevin db"
  [context]
  (let [patterns (:opt-clauses context)
        sources  (:sources context)]
    (assoc context :graph (into {}
                                (comp
                                  (map remove-src)
                                  (map #(resolve-lookup-refs sources %))
                                  (map make-nodes))
                                (group-by get-src patterns)))))

(defn- collect-pred-vars
  [{:keys [fn args]}]
  (case (:symbol fn)
    (and not or) (qu/collect-vars args)
    (into #{}
          (comp (filter #(instance? Variable %))
             (map :symbol))
          args)))

(defn- pushdownable
  "predicates that can be pushed down involve only one free variable"
  [where gseq]
  (when (instance? Predicate where)
    (let [syms (collect-pred-vars where)]
      (when (= (count syms) 1)
        (let [s (first syms)]
          (some #(when (= s (:var %)) s) gseq))))))

(defn- range-compare
  ([r1 r2]
   (range-compare r1 r2 true))
  ([[p i] [q j] from?]
   (case i
     :db.value/sysMin -1
     :db.value/sysMax 1
     (case j
       :db.value/sysMax -1
       :db.value/sysMin 1
       (let [res (compare i j)]
         (if (zero? res)
           (if from?
             (cond
               (identical? p q)       0
               (identical? p :closed) -1
               :else                  1)
             (cond
               (identical? p q)     0
               (identical? p :open) -1
               :else                1))
           res))))))

(def range-compare-to #(range-compare %1 %2 false))

(defn- combine-ranges*
  [ranges]
  (let [orig-from (sort range-compare (map first ranges))]
    (loop [intervals (transient [])
           from      (rest orig-from)
           to        (sort range-compare-to (map peek ranges))
           thread    (transient [(first orig-from)])]
      (if (seq to)
        (let [fc (first from)
              tc (first to)]
          (if (= (count from) (count to))
            (recur (conj! intervals (persistent! thread)) (rest from) to
                   (transient [fc]))
            (if fc
              (if (< ^long (range-compare fc tc) 0)
                (recur intervals (rest from) to (conj! thread fc))
                (recur intervals from (rest to) (conj! thread tc)))
              (recur intervals from (rest to) (conj! thread tc)))))
        (mapv (fn [t] [(first t) (peek t)])
              (persistent! (conj! intervals (persistent! thread))))))))

(defn combine-ranges
  [ranges]
  (reduce
    (fn [vs [[cl l] [cr r] :as n]]
      (let [[[pcl pl] [pcr pr]] (peek vs)]
        (if (and (= pr l) (not (= pcr cl :open)))
          (conj (pop vs) [[pcl pl] [cr r]])
          (conj vs n))))
    [] (combine-ranges* ranges)))

(defn- flip [c] (if (identical? c :open) :closed :open))

(defn flip-ranges
  ([ranges] (flip-ranges ranges c/v0 c/vmax))
  ([ranges v0 vmax]
   (let [vs (reduce
              (fn [vs [[cl l] [cr r]]]
                (-> vs
                    (assoc-in [(dec (count vs)) (count (peek vs))]
                              [(if (= l v0) cl (flip cl)) l])
                    (conj [[(if (= r vmax) cr (flip cr)) r]])))
              [[[:closed v0]]] ranges)]
     (assoc-in vs [(dec (count vs)) (count (peek vs))]
               [:closed vmax]))))

(defn intersect-ranges
  [& ranges]
  (let [n         (count ranges)
        ranges    (apply concatv ranges)
        orig-from (sort range-compare (map first ranges))
        res
        (loop [res  []
               from (rest orig-from)
               fp   (first orig-from)
               to   (sort range-compare-to (map peek ranges))
               i    1
               j    0]
          (let [tc (first to)]
            (if (seq from)
              (let [fc (first from)]
                (if (<= 0 ^long (range-compare fc tc))
                  (if (= i (+ j n))
                    (recur (conj res [fp tc]) (rest from) fc
                           (drop n to) (inc i) i)
                    (recur res (rest from) fc to (inc i) j))
                  (recur res (rest from) fc to (inc i) j)))
              (if (and (<= ^long (range-compare fp tc) 0) (= i (+ j n)))
                (conj res [fp tc])
                res))))]
    (when (seq res) res)))

(defn- add-range [m & rs]
  (let [old-range (:range m)]
    (assoc m :range
           (if old-range
             (if-let [new-range (intersect-ranges old-range rs)]
               new-range
               :empty-range)
             (combine-ranges rs)))))

(defn- prefix-max-string
  [^String prefix]
  (let [n (alength (.getBytes prefix))]
    (if (< n c/+val-bytes-wo-hdr+)
      (let [l  (- c/+val-bytes-wo-hdr+ n)
            ba (byte-array l)]
        (Arrays/fill ba (unchecked-byte 0xFF))
        (str prefix (String. ba)))
      prefix)))

(def ^:const wildm (int \%))
(def ^:const wilds (int \_))
(def ^:const max-string (b/text-ba->str c/max-bytes))

(defn- like-convert-range
  "turn wildcard-free prefix into range"
  [m ^String pattern not?]
  (let [wm-s (.indexOf pattern wildm)
        ws-s (.indexOf pattern wilds)]
    (cond
      (or (zero? wm-s) (zero? ws-s)) m
      ;; not-like w/ a wildcard-free pattern
      (== wm-s ws-s -1)
      (add-range m [[:closed ""] [:open pattern]]
                 [[:open pattern] [:closed max-string]])
      :else
      (let [min-s    (min wm-s ws-s)
            end      (if (== min-s -1) (max wm-s ws-s) min-s)
            prefix-s (subs pattern 0 end)
            prefix-e (prefix-max-string prefix-s)
            range    [[:closed prefix-s] [:closed prefix-e]]]
        (if not?
          (apply add-range m (flip-ranges [range] "" max-string))
          (add-range m range))))))

(defn- like-pattern-as-string
  "Used for plain text matching, e.g. as bounded val or range, not as FSM"
  [^String pattern escape]
  (let [esc (str (or escape \!))]
    (-> pattern
        (str/replace (str esc esc) esc)
        (str/replace (str esc "%") "%")
        (str/replace (str esc "_") "_"))))

(defn- wildcard-free-like-pattern
  [^String pattern {:keys [escape]}]
  (LikeFSM/isValid (.getBytes pattern) (or escape \!))
  (let [pstring (like-pattern-as-string pattern escape)]
    (when (and (not (str/includes? pstring "%"))
               (not (str/includes? pstring "_")))
      pstring)))

(defn- activate-pred
  [var clause]
  (when clause
    (if (fn? clause)
      clause
      (let [[f & args] clause
            fun        (resolve-pred f nil)
            i          (u/index-of #(= var %) args)
            args-arr   (object-array args)
            call       (make-call fun)]
        (fn pred [x]
          (call (do (aset args-arr i x) args-arr)))))))

(defn- add-pred
  ([old-pred new-pred]
   (add-pred old-pred new-pred false))
  ([old-pred new-pred or?]
   (if new-pred
     (if old-pred
       (if or?
         (fn [x] (or (old-pred x) (new-pred x)))
         (fn [x] (and (old-pred x) (new-pred x))))
       new-pred)
     old-pred)))

(defn- optimize-like
  [m pred [_ ^String pattern {:keys [escape]}] v not?]
  (let [pstring (like-pattern-as-string pattern escape)
        m'      (update m :pred add-pred (activate-pred v pred))]
    (like-convert-range m' pstring not?)))

(defn- inequality->range
  [m f args v]
  (let [args (vec args)
        ac-1 (dec (count args))
        i    ^long (u/index-of #(= % v) args)
        fa   (first args)
        pa   (peek args)]
    (case f
      <  (cond
           (zero? i)  (add-range m [[:closed c/v0] [:open pa]])
           (= i ac-1) (add-range m [[:open fa] [:closed c/vmax]])
           :else      (add-range m [[:open fa] [:open pa]]))
      <= (cond
           (zero? i)  (add-range m [[:closed c/v0] [:closed pa]])
           (= i ac-1) (add-range m [[:closed fa] [:closed c/vmax]])
           :else      (add-range m [[:closed fa] [:closed pa]]))
      >  (cond
           (zero? i)  (add-range m [[:open pa] [:closed c/vmax]])
           (= i ac-1) (add-range m [[:closed c/v0] [:open fa]])
           :else      (add-range m [[:open pa] [:open fa]]))
      >= (cond
           (zero? i)  (add-range m [[:closed pa] [:closed c/vmax]])
           (= i ac-1) (add-range m [[:closed c/v0] [:closed fa]])
           :else      (add-range m [[:closed pa] [:closed fa]])))))

(defn- range->inequality
  [v [[so sc :as s] [eo ec :as e]]]
  (cond
    (= s [:closed c/v0])
    (if (identical? eo :open) (list '< v ec) (list '<= v ec))
    (= e [:closed c/vmax])
    (if (identical? so :open) (list '< sc v) (list '<= sc v))
    :else
    (if (identical? so :open) (list '< sc v ec) (list '<= sc v ec))))

(defn- equality->range
  [m args]
  (let [c (some #(when-not (qu/free-var? %) %) args)]
    (add-range m [[:closed c] [:closed c]])))

(defn- in-convert-range
  [m [_ coll] not?]
  (assert (and (coll? coll) (not (map? coll)))
          "function `in` expects a collection")
  (apply add-range m
         (let [ranges (map (fn [v] [[:closed v] [:closed v]]) (sort coll))]
           (if not? (flip-ranges ranges) ranges))))

(defn- logic-pred
  [m f args v]
  (let [logic-f (fn [f args]
                  (fn logic [x]
                    (apply (get built-ins/query-fns f)
                           (walk/postwalk
                             (fn [e] (if (fn? e) (e x) e))
                             args))))
        args'   (walk/postwalk
                  (fn [e]
                    (if (list? e)
                      (let [[f & args] e]
                        (case f
                          (and or not) (logic-f f args)
                          (activate-pred v e)))
                      e))
                  (vec args))]
    (update m :pred add-pred (logic-f f args'))))

(defn- add-pred-clause
  [graph clause v]
  (walk/postwalk
    (fn [m]
      (if (= (:var m) v)
        (let [[f & args :as pred] (first clause)]
          (case f
            (< <= > >=)  (inequality->range m f args v)
            =            (equality->range m args)
            like         (optimize-like m pred args v false)
            not-like     (optimize-like m pred args v true)
            in           (in-convert-range m args false)
            not-in       (in-convert-range m args true)
            (and not or) (logic-pred m f args v)
            (update m :pred add-pred (activate-pred v pred))))
        m))
    graph))

(defn- free->bound
  "cases where free var can be rewritten as bound.
  * like pattern is free of wildcards"
  [graph clause v]
  (walk/postwalk
    (fn [m]
      (if-let [free (:free m)]
        (if-let [[k pstring]
                 (some
                   (fn [[k props]]
                     (when (= v (:var props))
                       (let [[f & args] (first clause)]
                         (when (= f 'like)
                           (let [[_ pattern opts] args]
                             (when-let [ps (wildcard-free-like-pattern
                                             pattern opts)]
                               [k ps]))))))
                   free)]
          (-> m
              (assoc-in [:bound k] {:val pstring})
              (update :free dissoc k))
          m)
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
                (update :graph #(free->bound % clause v))
                (update :graph #(add-pred-clause % clause v))))
          c))
      context (:qwhere parsed-q))))

(defn- estimate-round [x] (long (Math/ceil x)))

(defn- attr-var [[_ {:keys [var]}]] (or var '_))

(defn- build-graph
  "Split clauses, turn the group of clauses to be optimized into a query
  graph that looks like this:
  {$
    {?e  {:links [{:type :ref :tgt ?e0 :attr :friend}
                  {:type :val-eq :tgt ?e1 :var ?a :attrs {?e :age ?e1 :age}}]
          :mpath [:bound :name]
          :bound {:name {:val \"Tom\" :count 5}}
          :free  {:age    {:var ?a :range [[:less-than 18]] :count 1089}
                  :school {:var ?s :count 108 :pred [(.startsWith ?s \"New\")]}
                  :friend {:var ?e0 :count 2500}}}
    ?e0 {:links [{:type :_ref :tgt ?e :attr :friend}]
          :mpath [:free :age]
          :free  {:age {:var ?a :count 10890}}}
    ...
    }}

  Remaining clauses are in :late-clauses.
  "
  [context]
  (-> context
      split-clauses
      init-graph
      pushdown-predicates))

(defn- nillify [v] (if (or (identical? v c/v0) (identical? v c/vmax)) nil v))

(defn- range->start-end [[[_ lv] [_ hv]]] [(nillify lv) (nillify hv)])

(defn- range-count
  [db attr ranges ^long cap]
  (if (identical? ranges :empty-range)
    0
    (reduce
      (fn [^long sum range]
        (let [s (+ sum (let [[lv hv] (range->start-end range)]
                         ^long (db/-index-range-size db attr lv hv)))]
          (if (< s cap) s (reduced cap))))
      0 ranges)))

(defn- count-node-datoms
  [db {:keys [free bound] :as node}]
  (reduce
    (fn [{:keys [mcount] :as node} [k attr {:keys [val range]}]]
      (let [^long c (cond
                      (some? val) (db/-count db [nil attr val] mcount)
                      range       (range-count db attr range mcount)
                      :else       (db/-count db [nil attr nil]))]
        (cond
          (zero? c)          (reduced (assoc node :mcount 0))
          (< c ^long mcount) (-> node
                                 (assoc-in [k attr :count] c)
                                 (assoc :mcount c :mpath [k attr]))
          :else              (assoc-in node [k attr :count] c))))
    (assoc node :mcount Long/MAX_VALUE)
    (let [flat (fn [k m] (mapv (fn [[attr clause]] [k attr clause]) m))]
      (sort-by (fn [[_ attr _]] (db/-count db [nil attr nil]))
               (concat (flat :bound bound) (flat :free free) )))))

(defn- count-known-e-datoms
  [db e {:keys [free] :as node}]
  (reduce-kv
    (fn [{:keys [mcount] :as node} attr _]
      (let [^long c (db/-count db [e attr nil] mcount)]
        (cond
          (zero? c)          (reduced (assoc node :mcount 0))
          (< c ^long mcount) (-> node
                                 (assoc-in [:free attr :count] c)
                                 (assoc :mcount c :mpath [:free attr]))
          :else              (assoc-in node [:free attr :count] c))))
    (assoc node :mcount Long/MAX_VALUE) free))

(defn- count-datoms
  [db e node]
  (if (int? e)
    (count-known-e-datoms db e node)
    (count-node-datoms db node)))

(defn- add-back-range
  [v {:keys [pred range]}]
  (if range
    (reduce
      (fn [p r]
        (if r
          (add-pred p (activate-pred v (range->inequality v r)) true)
          p))
      pred range)
    pred))

(defn- attr-pred [v [_ clause]] (add-back-range v clause))

(defn- attrs-vec
  [attrs preds skips fidxs]
  (mapv (fn [a p f]
          [a (cond-> {:pred p :skip? false :fidx nil}
               (skips a) (assoc :skip? true)
               f         (assoc :fidx f :skip? true))])
        attrs preds fidxs))

(defn- init-steps
  [db e node single?]
  (let [{:keys [bound free mpath mcount]}       node
        {:keys [var val range pred] :as clause} (get-in node mpath)

        schema  (db/-schema db)
        attr    (peek mpath)
        know-e? (int? e)
        no-var? (or (not var) (qu/placeholder? var))
        init    (cond-> (map->InitStep
                          {:attr attr :vars [e] :out [e]
                           :mcount (:count clause)})
                  var     (assoc :pred pred
                                 :vars (cond-> [e]
                                         (not no-var?) (conj var))
                                 :range range)
                  val     (assoc :val val)
                  know-e? (assoc :know-e? true)
                  true    (#(assoc % :cols (if (= 1 (count (:vars %)))
                                             [e]
                                             [e #{attr var}]))))
        init    (if single?
                  init
                  (if (< ^long c/init-exec-size-threshold ^long mcount)
                    (assoc init :sample (-sample init db nil))
                    (assoc init :result (-execute init db nil))))]
    (cond-> [init]
      (< 1 (+ (count bound) (count free)))
      (conj
        (let [bound1  (->> (dissoc bound attr)
                           (mapv (fn [[a {:keys [val] :as b}]]
                                   [a (-> b
                                          (update :pred add-pred #(= val %))
                                          (assoc :var (gensym "?bound")))])))
              all     (->> (concatv bound1 (dissoc free attr))
                           (sort-by (fn [[a _]] ((schema a) :db/aid))))
              attrs   (mapv first all)
              vars    (mapv attr-var all)
              vars-m  (zipmap attrs vars)
              skips   (cond-> (set (sequence
                                     (comp (map (fn [a v]
                                               (when (or (= v '_)
                                                         (qu/placeholder? v))
                                                 a)))
                                        (remove nil?))
                                     attrs vars))
                        no-var? (conj attr))
              preds   (mapv attr-pred vars all)
              attrs-v (attrs-vec attrs preds skips (repeat nil))
              cols    (into (:cols init)
                            (comp (remove skips)
                               (map (fn [attr] #{attr (vars-m attr)})))
                            attrs)
              ires    (:result init)
              isp     (:sample init)
              merge   (MergeScanStep. 0 attrs-v vars [e] [e] cols nil nil)]
          (cond-> merge
            ires (assoc :result (-execute merge db ires))
            isp  (assoc :sample (-sample merge db isp))))))))

(def default-ratio (double (/ 1 (inc ^long c/init-exec-size-threshold))))

(defn- estimate-scan-v-size
  [^long e-size steps]
  (let [{:keys [know-e?] res1 :result sp1 :sample} (first steps)]
    (if know-e?
      1
      (let [{:keys [result sample]} (peek steps)]
        (estimate-round
          (* e-size
             (double (cond
                       result (let [s (.size ^List result)]
                                (if (< 0 s)
                                  (/ s (.size ^List res1))
                                  default-ratio))
                       sample (let [s (.size ^List sample)]
                                (if (< 0 s)
                                  (/ s (.size ^List sp1))
                                  default-ratio))))))))))

(defn- estimate-scan-v-cost
  [{:keys [attrs-v]} ^long size]
  (let [^long n-preds (reduce
                        (fn [^long c [_ {:keys [pred]}]] (if pred (inc c) c))
                        0 attrs-v)
        ^long n-fidx  (reduce
                        (fn [^long c [_ {:keys [fidx]}]] (if fidx (inc c) c))
                        0 attrs-v)
        n-attrs       (count attrs-v)]
    (* size
       ^double c/magic-cost-merge-scan-v
       (if (zero? n-attrs)
         1
         ^long (estimate-round (* ^double c/magic-cost-attr n-attrs)))
       (if (zero? n-preds)
         1
         ^long (estimate-round (* ^double c/magic-cost-pred n-preds)))
       (if (zero? n-fidx)
         1
         ^long (estimate-round (* ^double c/magic-cost-fidx n-fidx))))))

(defn- estimate-base-cost
  [{:keys [mcount]} steps]
  (let [{:keys [vars pred]} (first steps)
        init-cost           (estimate-round
                              (cond-> (* ^double c/magic-cost-init-scan-e
                                         ^long mcount (count vars))
                                pred (* ^double c/magic-cost-pred)))]
    (if (< 1 (count steps))
      (+ ^long init-cost ^long (estimate-scan-v-cost (peek steps) mcount))
      init-cost)))

(defn- base-plan
  ([db nodes e]
   (base-plan db nodes e false))
  ([db nodes e single?]
   (let [node   (get nodes e)
         mcount (:mcount node)]
     (when-not (zero? ^long mcount)
       (let [isteps (init-steps db e node single?)]
         (if single?
           (Plan. isteps nil nil)
           (Plan. isteps
                  (estimate-base-cost node isteps)
                  (estimate-scan-v-size mcount isteps))))))))

(defn- writing? [db] (l/writing? (.-lmdb ^Store (.-store ^DB db))))

;; somehow graal has problem with pmap
(def map+ (if (System/getenv "DTLV_COMPILE_NATIVE") map pmap))

(defn- update-nodes
  [db nodes]
  (if (= 1 (count nodes))
    (let [[e node] (first nodes)] {e (count-datoms db e node)})
    (into {} (map+ (fn [e] [e (count-datoms db e (get nodes e))])
                   (keys nodes)))))

(defn- build-base-plans
  [db nodes component]
  (into {} (map+ (fn [e] [[e] (base-plan db nodes e)])
                 component)))

(defn- find-index
  [a-or-v cols]
  (when a-or-v
    (u/index-of (fn [x] (if (set? x) (x a-or-v) (= x a-or-v))) cols)))

(defn- merge-scan-step
  [db last-step index new-key new-steps skip-attr]
  (let [[s1 s2]  new-steps
        attr1    (:attr s1)
        val1     (:val s1)
        bound?   (some? val1)
        vars1    (:vars s1)
        v1       (when (< 1 (count vars1)) (peek vars1))
        attrs-v2 (:attrs-v s2)
        attrs2   (mapv first attrs-v2)
        vars-m   (cond-> (zipmap attrs2 (:vars s2))
                   v1     (assoc attr1 v1)
                   bound? (assoc attr1 (gensym "?bound")))
        preds-m  (assoc (zipmap attrs2 (mapv (comp :pred peek) attrs-v2))
                        attr1 (cond-> (add-back-range v1 s1)
                                bound? (add-pred #(= % val1))))
        schema   (db/-schema db)
        attrs    (->> (conj attrs2 attr1)
                      (remove #{(:attr last-step)})
                      (sort-by #((schema %) :db/aid)))
        preds    (replace preds-m attrs)
        skips2   (reduce (fn [ss [a m]] (if (m :skip?) (conj ss a) ss))
                         #{} attrs-v2)
        skips    (cond-> (conj skips2 skip-attr)
                   (or bound? (nil? v1)) (conj attr1))
        vars'    (replace vars-m attrs)
        lcols    (:cols last-step)
        fidxs    (mapv #(find-index % lcols) vars')
        attrs-v  (attrs-vec attrs preds skips fidxs)
        skips    (into #{} (keep (fn [[a m]] (when (m :skip?) a))) attrs-v)
        skip-vs  (set (mapv vars-m skips))
        vars     (into [] (comp (remove keyword?) (remove skip-vs)) vars')
        in       (:out last-step)
        out      (if (set? in) (set new-key) new-key)
        cols     (into lcols
                       (comp (remove skips)
                          (map (fn [attr]
                                 (if-let [v (vars-m attr)]
                                   #{attr v}
                                   attr))))
                       attrs)]
    (MergeScanStep. index attrs-v vars in out cols nil nil)))

(defn- index-by-link
  [cols link-e link]
  (case (:type link)
    :ref    (or (find-index (:tgt link) cols)
                (find-index (:attr link) cols))
    :_ref   (find-index link-e cols)
    :val-eq (or (find-index (:var link) cols)
                (find-index ((:attrs link) link-e) cols))))

(defn- enrich-cols
  [cols index attr]
  (let [pa (cols index)]
    (replace {pa (if (set? pa) (conj pa attr) (into #{} [pa attr]))} cols)))

(defn- link-step
  [type last-step index attr tgt new-key]
  (let [in    (:out last-step)
        out   (if (set? in) (set new-key) new-key)
        lcols (:cols last-step)
        fidx  (find-index tgt lcols)
        cols  (cond-> (enrich-cols lcols index attr)
                (nil? fidx) (conj tgt))]
    [(LinkStep. type index attr tgt fidx in out cols)
     (or fidx (dec (count cols)))]))

(defn- rev-ref-plan
  [db last-step index {:keys [type attr tgt]} new-key new-steps]
  (let [[step n-index] (link-step type last-step index attr tgt new-key)]
    (if (= 1 (count new-steps))
      [step]
      [step (merge-scan-step db step n-index new-key new-steps attr)])))

(defn- val-eq-plan
  [db last-step index {:keys [type attrs tgt]} new-key new-steps]
  (let [attr           (attrs tgt)
        [step n-index] (link-step type last-step index attr tgt new-key)]
    (if (= 1 (count new-steps))
      [step]
      [step (merge-scan-step db step n-index new-key new-steps attr)])))

(defn- count-init-follows
  [db tuples attr index]
  (reduce
    (fn [s v] (+ ^long s ^long (db/-count db [nil attr v])))
    1 (r/projection (s/remove-end-scan tuples) index)))

(defn- estimate-link-size
  [db link-e {:keys [attr attrs tgt]} ^ConcurrentHashMap ratios
   prev-size last-step index]
  (let [attr                    (or attr (attrs tgt))
        ratio-key               [link-e tgt]
        {:keys [result sample]} last-step
        ^long ssize             (if sample (.size ^List sample) 0)
        ^long rsize             (if result (.size ^List result) 0)]
    (estimate-round
      (cond
        (< 0 ssize)
        (let [^long size    (count-init-follows db sample attr index)
              ^double ratio (/ size ssize)]
          (.put ratios ratio-key ratio)
          (* ^long prev-size ratio))
        (< 0 rsize)
        (let [^long size (count-init-follows db result attr index)
              ratio      (/ size rsize)]
          (.put ratios ratio-key ratio)
          size)
        :else
        (* ^long prev-size
           ^double (.getOrDefault ratios ratio-key default-ratio))))))

(defn- estimate-join-size
  [db link-e link ratios prev-size last-step index new-base-plan]
  (let [steps (:steps new-base-plan)]
    (if (identical? :ref (:type link))
      (estimate-scan-v-size prev-size steps)
      (let [e-size (estimate-link-size db link-e link ratios prev-size
                                       last-step index)]
        (estimate-scan-v-size e-size steps)))))

(defn- estimate-link-cost
  [{:keys [fidx]} size]
  (estimate-round (* ^long size
                     ^double c/magic-cost-val-eq-scan-e
                     (double (if fidx c/magic-cost-fidx 1.0)))))

(defn- estimate-e-plan-cost
  [prev-size cur-steps]
  (let [step1 (first cur-steps)]
    (if (= 1 (count cur-steps))
      (if (identical? (-type step1) :merge)
        (estimate-scan-v-cost step1 prev-size)
        (estimate-link-cost step1 prev-size))
      (+ ^long (estimate-link-cost step1 prev-size)
         ^long (estimate-scan-v-cost (peek cur-steps) prev-size)))))

(defn- e-plan
  [db {:keys [steps cost size]} index link new-key new-base-plan result-size]
  (let [new-steps (:steps new-base-plan)
        last-step (peek steps)
        cur-steps
        (case (:type link)
          :ref    [(merge-scan-step db last-step index new-key new-steps nil)]
          :_ref   (rev-ref-plan db last-step index link new-key new-steps)
          :val-eq (val-eq-plan db last-step index link new-key new-steps))]
    (Plan. cur-steps
           (+ ^long cost ^long (estimate-e-plan-cost size cur-steps))
           result-size)))

(defn- binary-plan*
  [db base-plans ratios prev-plan last-step link-e new-e link new-key]
  (let [index    (index-by-link (:cols last-step) link-e link)
        new-base (base-plans [new-e])
        size     (estimate-join-size db link-e link ratios (:size prev-plan)
                                     last-step index new-base)]
    (e-plan db prev-plan index link new-key new-base size)))

(defn- binary-plan
  [db nodes base-plans ratios prev-plan link-e new-e new-key]
  (apply min-key :cost
         (into []
               (comp
                 (filter #(= new-e (:tgt %)))
                 (map #(binary-plan*
                         db base-plans ratios prev-plan
                         (peek (:steps prev-plan)) link-e new-e % new-key)))
               (get-in nodes [link-e :links]))))

(defn- plans
  [db nodes pairs base-plans prev-plans ratios]
  (apply u/merge-with
         (fn [p1 p2] (if (< ^long (:cost p2) ^long (:cost p1)) p2 p1))
         (map+
           (fn [[prev-key prev-plan]]
             (let [prev-key-set (set prev-key)]
               (persistent!
                 (reduce
                   (fn [t [link-e new-e]]
                     (if (and (prev-key-set link-e) (not (prev-key-set new-e)))
                       (let [new-key  (conj prev-key new-e)
                             cur-cost (or (:cost (t new-key)) Long/MAX_VALUE)
                             {:keys [cost] :as new-plan}
                             (binary-plan db nodes base-plans ratios prev-plan
                                          link-e new-e new-key)]
                         (if (< ^long cost ^long cur-cost)
                           (assoc! t new-key new-plan)
                           t))
                       t))
                   (transient {}) pairs))))
           prev-plans)))

(defn- connected-pairs
  [nodes component]
  (let [pairs (volatile! #{})]
    (doseq [e    component
            link (get-in nodes [e :links])]
      (vswap! pairs conj [e (:tgt link)]))
    @pairs))

(defn- shrink-space
  [plans]
  (persistent!
    (reduce-kv
      (fn [m k ps]
        (assoc! m k (-> (peek (apply min-key (fn [p] (:cost (peek p))) ps))
                        (update :steps (fn [ss]
                                         (if (= 1 (count ss))
                                           [(update (first ss) :out set)]
                                           [(first ss)
                                            (update (peek ss) :out set)]))))))
      (transient {}) (group-by (fn [p] (set (nth p 0))) plans))))

(defn- trace-steps
  [^List tables ^long n-1]
  (reduce
    (fn [plans i]
      (cons ((.get tables i) (:in (first (:steps (first plans))))) plans))
    [(apply min-key :cost (vals (.get tables n-1)))]
    (range (dec n-1) -1 -1)))

(defn- plan-component
  [db nodes component]
  (let [n (count component)]
    (if (= n 1)
      [(base-plan db nodes (first component) true)]
      (let [base-plans (build-base-plans db nodes component)]
        (if (some nil? (vals base-plans))
          [nil]
          (let [pairs  (connected-pairs nodes component)
                tables (FastList. n)
                ratios (ConcurrentHashMap.)
                n-1    (dec n)]
            (.add tables base-plans)
            (dotimes [i n-1]
              (let [plans (plans
                            db nodes pairs base-plans (.get tables i) ratios)]
                (if (< ^long c/plan-space-reduction-threshold (count plans))
                  (.add tables (shrink-space plans))
                  (.add tables plans))))
            (trace-steps tables n-1)))))))

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
            (recur (concatv stack neighbors) (conj visited v))))))))

(defn- connected-components
  [graph]
  (loop [vertices (keys graph) components []]
    (if (empty? vertices)
      components
      (let [component (dfs graph (first vertices))]
        (recur (remove component vertices)
               (conj components component))))))

(defn- build-plan*
  [db nodes]
  (let [cc (connected-components nodes)]
    (if (= 1 (count cc))
      [(plan-component db nodes (first cc))]
      (map+ #(plan-component db nodes %) cc))))

(defn- strip-result
  [plans]
  (mapv (fn [[f & r]]
          (vec
            (cons (update f :steps
                          (fn [steps]
                            (mapv #(assoc % :result nil :sample nil) steps)))
                  r)))
        plans))

(defn- build-plan
  "Generate a query plan that looks like this:

  [{:op :init :attr :name :val \"Tom\" :out #{?e} :vars [?e]
    :cols [?e]}
   {:op :merge-scan  :attrs [:age :friend] :preds [(< ?a 20) nil]
    :vars [?a ?f] :in #{?e} :index 0 :out #{?e} :cols [?e :age :friend]}
   {:op :link :attr :friend :var ?e1 :in #{?e} :index 2
    :out #{?e ?e1} :cols [?e :age :friend ?e1]}
   {:op :merge-scan :attrs [:name] :preds [nil] :vars [?n] :index 3
    :in #{?e ?e1} :out #{?e ?e1} :cols [?e :age :friend ?e1 :name]}]

  :op here means step type.
  :result-set will be #{} if there is any clause that matches nothing."
  [{:keys [graph sources] :as context}]
  (if graph
    (reduce-kv
      (fn [c src nodes]
        (let [^DB db (sources src)
              k      [(.-store db) nodes]]
          (if-let [cached (.get ^LRUCache *plan-cache* k)]
            (assoc-in c [:plan src] cached)
            (let [nodes (update-nodes db nodes)
                  plans (if (< 1 (count nodes))
                          (build-plan* db nodes)
                          [[(base-plan db nodes (ffirst nodes) true)]])]
              (if (some #(some nil? %) plans)
                (reduced (assoc c :result-set #{}))
                (do
                  (.put ^LRUCache *plan-cache* k (strip-result plans))
                  (assoc-in c [:plan src] plans)))))))
      context graph)
    context))

(defn- save-intermediates
  [context steps ^objects sinks ^List tuples]
  (when-let [res (:intermediates context)]
    (vswap! res merge
            (u/reduce-indexed
              (fn [m step i]
                (assoc m (:out step)
                       {:tuples-count
                        (let [sink (aget sinks i)]
                          (if (s/pipe? sink)
                            (s/total sink)
                            (.size ^Collection (s/remove-end-scan sink))))}))
              {(:out (peek steps)) {:tuples-count (.size tuples)}}
              (butlast steps)))))

(defn- cols->attrs
  [cols]
  (u/reduce-indexed
    (fn [m col i]
      (let [v (if (set? col)
                (some #(when (symbol? %) %) col)
                col)]
        (assoc m v i)))
    {} cols))

(defn- pipelining
  [context db attrs steps n]
  (let [n-1    (dec ^long n)
        tuples (FastList.)
        pipes  (object-array (repeatedly n-1 s/tuple-pipe))
        work   (fn [step ^long i]
                 (if (zero? i)
                   (-execute-pipe step db nil (aget pipes 0))
                   (let [src (aget pipes (dec i))]
                     (if (= i n-1)
                       (-execute-pipe step db src tuples)
                       (-execute-pipe step db src (aget pipes i))))))
        finish #(s/finish-output (if (= % n-1) tuples (aget pipes %)))]
    (dorun ((if (writing? db) map map+)
            (fn [step i] (work step i) (finish i))
            steps (range)))
    (s/remove-end-scan tuples)
    (save-intermediates context steps pipes tuples)
    (r/relation! attrs tuples)))

(defn- step-by-step
  [context db attrs steps]
  (let [[f & r] steps
        fres    (-execute f db nil)
        sinks   (volatile! [fres])
        tuples  (reduce
                  (fn [ts step]
                    (let [res (-execute step db ts)]
                      (vswap! sinks conj res)
                      res))
                  fres r)]
    (save-intermediates context steps (object-array @sinks) tuples)
    (r/relation! attrs tuples)))

(defn- execute-steps
  "execute all steps of a component's plan to obtain a relation"
  [context db steps]
  (let [steps (vec steps)
        n     (count steps)
        attrs (cols->attrs (:cols (peek steps)))]
    (condp = n
      1 (let [tuples (-execute (first steps) db nil)]
           (save-intermediates context steps nil tuples)
           (r/relation! attrs tuples))
      2 (let [src    (-execute (first steps) db nil)
              tuples (-execute (peek steps) db src)]
           (save-intermediates context steps (object-array [src]) tuples)
           (r/relation! attrs tuples))
      (if (u/graal?)
        (step-by-step context db attrs steps)
        (pipelining context db attrs steps n)))))

(defn- execute-plan
  [{:keys [plan sources] :as context}]
  (if (= 1 (transduce (map (fn [[_ components]] (count components))) + plan))
    (update context :rels collapse-rels
            (let [[src components] (first plan)]
              (execute-steps context (sources src)
                             (mapcat :steps (first components)))))
    (reduce
      (fn [c r] (update c :rels collapse-rels r))
      context (->> plan
                   (mapcat (fn [[src components]]
                             (let [db (sources src)]
                               (for [plans components]
                                 [db (mapcat :steps plans)]))))
                   (map+ #(apply execute-steps context %))
                   (sort-by #(count (:tuples %)))))))

(defn- plan-explain
  []
  (when *explain*
    (vswap! *explain* assoc :planning-time
            (- (System/nanoTime) ^long *start-time*))))

(defn -q
  [context run?]
  (binding [*implicit-source* (get (:sources context) '$)]
    (let [{:keys [result-set] :as context} (-> context
                                               build-graph
                                               build-plan)]
      (if (= result-set #{})
        (do (plan-explain) context)
        (as-> context c
          (do (plan-explain) c)
          (if run? (execute-plan c) c)
          (if run? (reduce resolve-clause c (:late-clauses c)) c))))))

(defn -collect-tuples
  [acc rel ^long len copy-map]
  (->Eduction
    (comp
      (map (fn [^objects t1]
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
  [{:keys [result-set] :as context} symbols]
  (if (= result-set #{})
    context
    (assoc context :result-set (into (sp/new-spillable-set) (map vec)
                                     (-collect context symbols)))))

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
                  args (mapv #(-context-resolve % context)
                             (butlast (:args element)))
                  vals (map #(nth % i) tuples)]
              (apply f (conj args vals)))
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
    (map* (fn [^objects tuple]
            (persistent!
              (reduce
                (fn [m i] (assoc! m (nth symbols i) (aget tuple i)))
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
            c (eduction (filter #(instance? Variable (get-v %))) patterns))))
      context
      (->> qwhere
           (eduction (filter #(instance? Pattern %)))
           (group-by (fn [{:keys [source pattern]}]
                       [source (first pattern) (second pattern)]))
           (eduction (filter #(< 1 (count (val %)))))))))

(defn- result-explain
  ([context result]
   (result-explain context)
   (when *explain* (vswap! *explain* assoc :result result)))
  ([{:keys [graph result-set plan opt-clauses late-clauses run?] :as context}]
   (when *explain*
     (let [{:keys [^long planning-time]} @*explain*

           et (double (/ (- ^long (System/nanoTime)
                            (+ ^long *start-time* planning-time))
                         1000000))
           pt (double (/ planning-time 1000000))]
       (vswap! *explain* assoc
               :actual-result-size (count result-set)
               :planning-time (format "%.3f" pt)
               :execution-time (format "%.3f" et)
               :opt-clauses opt-clauses
               :query-graph (walk/postwalk
                              (fn [e]
                                (if (map? e)
                                  (apply dissoc e
                                         (for [[k v] e
                                               :when (nil? v)] k))
                                  e)) graph)
               :plan (walk/postwalk
                       (fn [e]
                         (if (instance? Plan e)
                           (let [{:keys [steps] :as plan} e]
                             (cond->
                                 (assoc plan :steps
                                        (mapv #(-explain % context) steps))
                               run? (assoc :actual-size
                                           (get-in @(:intermediates context)
                                                   [(:out (last steps))
                                                    :tuples-count]))))
                           e)) plan)
               :late-clauses late-clauses)))))

(defn- parsed-q
  [q]
  (or (.get ^LRUCache *query-cache* q)
      (let [res (dp/parse-query q)]
        (.put ^LRUCache *query-cache* q res)
        res)))

(defn q
  [q & inputs]
  (let [parsed-q (parsed-q q)]
    (binding [timeout/*deadline* (timeout/to-deadline (:qtimeout parsed-q))]
      (let [find          (:qfind parsed-q)
            find-elements (dp/find-elements find)
            result-arity  (count find-elements)
            with          (:qwith parsed-q)
            all-vars      (concatv (dp/find-vars find) (map :symbol with))

            [parsed-q inputs] (plugin-inputs parsed-q inputs)

            context
            (-> (Context. parsed-q [] {} {} [] nil nil nil
                          (volatile! {}) true nil)
                (resolve-ins inputs)
                (resolve-redudants)
                (-q true)
                (collect all-vars))
            result
            (cond->> (:result-set context)
              with (mapv #(subvec % 0 result-arity))

              (some dp/aggregate? find-elements)
              (aggregate find-elements context)

              (some dp/pull? find-elements) (pull find-elements context)

              true (-post-process find (:qreturn-map parsed-q)))]
        (result-explain context result)
        result))))

(defn- plan-only
  [q & inputs]
  (let [parsed-q (parsed-q q)]
    (binding [timeout/*deadline* (timeout/to-deadline (:qtimeout parsed-q))]
      (let [[parsed-q inputs] (plugin-inputs parsed-q inputs)]
        (-> (Context. parsed-q [] {} {} [] nil nil nil (volatile! {})
                      false nil)
            (resolve-ins inputs)
            (resolve-redudants)
            (-q false)
            (result-explain))))))

(defn explain
  [{:keys [run?] :or {run? false}} & args]
  (binding [*explain*    (volatile! {})
            *start-time* (System/nanoTime)]
    (if run?
      (do (apply q args) @*explain*)
      (do (apply plan-only args)
          (dissoc @*explain* :actual-result-size :execution-time)))))
