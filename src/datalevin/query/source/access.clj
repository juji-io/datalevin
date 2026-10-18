(ns ^:no-doc datalevin.query.source.access
  "Structural EAV access plans for the shared access abstraction. This bridge
  consumes captured, validated envelopes; it does not negotiate or acquire a
  source, call a provider, sample rows, or install a query execution path."
  (:require [clojure.set :as set]
            [datalevin.parser]
            [datalevin.query.access :as access]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.permissions :as permissions]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]))

(def ^:private kind-rank {:entity-lookup 0 :value-lookup 1 :attribute-enumeration 2})
(def ^:private search-bound 4096)

(defn- ensure! [ok]
  (when-not ok (fail! :query-source/invalid :planning)))

(defn- nonnegative-id? [x]
  (and (integer? x) (<= 0 x Long/MAX_VALUE)))

(defn- variable? [x]
  (and (symbol? x) (.startsWith (name x) "?")))

(defn- source-symbol? [x]
  (and (symbol? x) (.startsWith (name x) "$")))

(defn- alternative-id? [x]
  (or (keyword? x) (and (string? x) (seq x))))

(defn- columns! [columns check!]
  (ensure! (vector? columns))
  (when (seq columns) (contract/binding-columns! columns check!))
  columns)

(defn- column-map [columns]
  (into {} (map (juxt :variable identity)) columns))

(defn- supplies? [from to]
  (and from (= (:type from) (:type to))
       (or (:nullable? to) (not (:nullable? from)))))

(defn- text-compare
  "Compare Unicode code points, including supplementary characters. Java's
  UTF-16 string ordering is different for some valid identifiers."
  [^String a ^String b check!]
  (loop [i 0 j 0 n 0]
    (when (zero? (bit-and n 255)) (check!))
    (cond
      (= i (.length a)) (if (= j (.length b)) 0 -1)
      (= j (.length b)) 1
      :else (let [x (.codePointAt a i) y (.codePointAt b j)]
              (if (= x y)
                (recur (+ i (Character/charCount x)) (+ j (Character/charCount y)) (inc n))
                (compare x y))))))

(defn- key-tag [x]
  (cond (nil? x) 0 (integer? x) 1 (symbol? x) 2 (keyword? x) 3
        (string? x) 4 (vector? x) 5
        :else (fail! :query-source/invalid :planning)))

(defn- key-compare [a b check!]
  (let [tag (key-tag a) other (key-tag b)]
    (if (not= tag other) (compare tag other)
        (case tag
          0 0
          1 (compare a b)
          2 (text-compare (str a) (str b) check!)
          3 (let [n (key-compare (namespace a) (namespace b) check!)]
              (if (zero? n) (text-compare (name a) (name b) check!) n))
          4 (text-compare a b check!)
          5 (loop [i 0]
              (check!)
              (cond (= i (count a)) (if (= i (count b)) 0 -1)
                    (= i (count b)) 1
                    :else (let [c (key-compare (nth a i) (nth b i) check!)]
                            (if (zero? c) (recur (inc i)) c))))))))

(defn- canonical-key [options]
  [(:source-symbol options) (:region-id options)
   (get-in options [:access :anchor-clause])
   (vec (sort (:covers options))) (get-in options [:access :kind])
   (mapv :variable (:requires options)) (:alternative-id options)])

(defn- request-metadata [request]
  (select-keys request [:source-symbol :region-id :scope-id :clauses :bindings
                        :available-inputs :required-vars :demand :max-alternatives]))

(defn- request! [request check!]
  (let [{:keys [source-symbol region-id scope-id clauses bindings available-inputs
                required-vars max-alternatives demand]} request]
    (ensure! (and (source-symbol? source-symbol) (nonnegative-id? region-id)
                  (string? scope-id) (seq scope-id) (vector? clauses)
                  (value/plain-map? bindings) (every? variable? (keys bindings))
                  (set? required-vars) (every? variable? required-vars)
                  (integer? max-alternatives) (<= 1 max-alternatives Long/MAX_VALUE)
                  (= {:quality :exact :ordering [] :offset 0 :limit nil} demand)))
    (columns! available-inputs check!)
    (ensure! (= (count clauses) (count (set (map :id clauses)))))
    (doseq [clause clauses]
      (check!)
      (ensure! (and (value/plain-map? clause) (nonnegative-id? (:id clause))
                    (#{:pattern :predicate} (:kind clause))))
      (when (= :pattern (:kind clause))
        (ensure! (and (= 3 (count (:terms clause)))
                      (= :const (first (second (:terms clause))))
                      (keyword? (second (second (:terms clause))))))))
    request))

(defn candidate!
  "Bridge one validated alternative into an AccessPlan. Request values and the
  opaque handle must already be captured/owned under the invocation's quotas.
  Validate structural boundaries again here before forming dependency edges.
  Advisory estimates are kept separate from native cardinalities and costs."
  [runtime request alternative consumption implementation]
  (let [ctl (:control runtime) check! #(control/check-active! ctl)
        metadata (rt/charge-metadata! runtime (dissoc alternative :plan) :planning 2048)]
    (try
      (request! request check!)
      (let [{:keys [id covers prefilters requires columns properties scope-id completion]} alternative
            {:keys [source-symbol region-id clauses bindings required-vars available-inputs]} request
            by-id (into {} (map (juxt :id identity)) clauses)
            patterns (into #{} (comp (filter #(= :pattern (:kind %))) (map :id)) clauses)
            _ (ensure! (and (set? covers) (set? prefilters)))
            covered-patterns (set/intersection patterns covers)
            access (:access alternative)
            anchor (:anchor-clause access)
            anchor-clause (get by-id anchor)
            required (column-map (columns! requires check!))
            output (column-map (columns! columns check!))
            available (column-map available-inputs)
            provided (into (into (set (keys bindings)) (keys required)) (keys output))
            covered-vars (into #{} (mapcat #(contract/variables (get by-id %))) covers)]
        (ensure! (and (alternative-id? id) (some? (:plan alternative))
                      (= (:scope-id request) scope-id) (= :all-bindings completion)
                      (set? covers) (seq covered-patterns) (set? prefilters)
                      (set/subset? covers (set (keys by-id)))
                      (set/subset? prefilters (set (keys by-id)))
                      (empty? (set/intersection covers prefilters))
                      (every? #(= :predicate (:kind (by-id %))) prefilters)
                      (value/plain-map? access) (every? #(contains? access %) [:kind :anchor-clause])
                      (every? #(or (#{:kind :anchor-clause} %) (qualified-keyword? %)) (keys access))
                      (contains? kind-rank (:kind access)) (contains? covered-patterns anchor)
                      (#{:identical :unstable} (:replay properties))
                      (= [] (:ordering properties)) (= :none (:blocking-state properties))
                      (satisfies? access/IAccessMethod implementation)))
        (doseq [[variable column] required]
          (check!)
          (ensure! (supplies? (get available variable) column)))
        (ensure! (set/subset? (set/intersection required-vars covered-vars) provided))
        (doseq [id (set/union covers prefilters)
                :let [clause (by-id id)] :when (= :predicate (:kind clause))]
          (ensure! (set/subset? (contract/variables clause) provided)))
        (let [position (case (:kind access) :entity-lookup 0 :value-lookup 2 nil)]
          (when position
            (let [[tag x] (nth (:terms anchor-clause) position)]
              (ensure! (or (= :const tag)
                            (and (= :var tag) (or (contains? bindings x) (contains? required x))))))))
        ;; These are structural identifiers/columns only. Do not retain provider
        ;; annotations, labels, handles, estimates, or request values in a key.
        (let [options {:source-symbol source-symbol :region-id region-id :alternative-id id
                       :access (select-keys access [:kind :anchor-clause]) :covers covered-patterns
                       :requires (mapv #(select-keys % [:variable :type :nullable?]) requires)
                       :columns (mapv #(select-keys % [:variable :type :nullable?]) columns)
                       :scope-id scope-id}
              token (rt/charge-metadata! runtime options :planning
                                         (+ 2048 (* 64 (+ (count requires) (count covered-patterns)))))]
          (try
            (let [options (value/diagnostic-copy options check!)
                  options (assoc options :canonical-key (canonical-key options)
                                         :permission (permissions/planning-permission consumption alternative)
                                         :consumption consumption
                                         :capture {:request request :alternative alternative})
                  required-vars (mapv :variable (:requires options))
                  output-vars (mapv :variable (:columns options))
                  expr (access/->AccessExpr :query-source
                         (into #{} (map #(vector (:source-symbol options) region-id %)) covered-patterns)
                         (set required-vars) (set/difference (set output-vars) (set required-vars))
                         (set/intersection (set output-vars) (set required-vars)) output-vars (:source-symbol options))
                  path (access/->AccessPath :query-source implementation (:kind access) [] #{:complete}
                                           options (access/access-policy :none :execution))
                  plan (assoc (access/->AccessPlan expr path (access/source-bounds)
                                                  (access/access-work) nil)
                              :advisory-estimate (:estimate alternative))]
              (rt/own! runtime #(control/release! token))
              plan)
            (catch Throwable e (control/release! token) (throw e)))))
      (finally (control/release! metadata)))))

(defn- options [plan] (get-in plan [:path :options]))
(defn- work-key [plan]
  (let [{:keys [source-symbol region-id covers]} (options plan)]
    [source-symbol region-id (first covers)]))

(defn- ready? [known used plan]
  (let [{:keys [requires permission]} (options plan)]
    (and (every? #(supplies? (get known (:variable %)) %) requires)
         (or (= :many (:opens permission))
             (and (not (:spent? permission)) (not (contains? used (:resource permission))))))))

(defn- extend-known [known columns]
  (reduce (fn [known column]
            (when known
              (let [variable (:variable column) before (get known variable)]
                (when (or (nil? before) (= (:type before) (:type column)))
                  (assoc known variable
                         (assoc column :nullable? (and (:nullable? column)
                                                      (get before :nullable? true))))))))
          known columns))

(defn schedule!
  "Validate complete region-local coverage and schedule typed AccessPlans.
  Inputs are explicit legal exports from the enclosing engine operator. Never
  infer an export from another region or inspect outer rows. Single-pattern
  choices use :eav-structural-v1; fused candidates stay unavailable. Feasibility
  search shares the invocation ledger and the existing 4096-state bound.
  Caller-owned candidate/request reservations must live through execution."
  [runtime region-id input-columns requests plans]
  (let [started (System/nanoTime)
        ctl (:control runtime) check! #(control/check-active! ctl)
        charges (volatile! []) output-token (volatile! nil)
        own-work! (fn [bytes]
                    (let [token (control/reserve! ctl {:retained-bytes bytes})]
                      (vswap! charges conj token)))]
    (try
      (ensure! (and (nonnegative-id? region-id) (vector? requests) (vector? plans)))
      (vswap! charges conj (rt/charge-metadata! runtime input-columns :planning))
      (columns! input-columns check!)
      (own-work! (+ 2048 (* 1024 (count plans)) (* 512 (count requests))))
      (doseq [request requests]
        (vswap! charges conj (rt/charge-copy! runtime (request-metadata request)))
        (request! request check!)
        (ensure! (= region-id (:region-id request))))
      (let [request-map (into {} (map (juxt :source-symbol identity)) requests)
            _ (ensure! (= (count requests) (count request-map)))
            required (into #{} (mapcat (fn [request]
                                        (for [clause (:clauses request) :when (= :pattern (:kind clause))]
                                          [(:source-symbol request) region-id (:id clause)]))) requests)
            live-vars (into #{} (mapcat :required-vars) requests)
            _ (doseq [plan plans]
                (check!)
                (let [o (options plan) request (get request-map (:source-symbol o))]
                  (ensure! (and request (= region-id (:region-id o))
                                (identical? request (get-in o [:capture :request]))
                                (set/subset? (into #{} (map #(vector (:source-symbol o) region-id %)) (:covers o))
                                             required)))))
            _ (doseq [[source-symbol candidates] (group-by #(get-in % [:expr :source]) plans)]
                (ensure! (<= (count candidates) (:max-alternatives (request-map source-symbol))))
                (ensure! (= (count candidates)
                            (count (set (map #(get-in % [:path :options :alternative-id]) candidates))))))
            candidates (->> plans
                            (map (fn [plan]
                                   (let [o (options plan)]
                                     (assoc-in plan [:path :options :permission]
                                               (permissions/planning-permission
                                                 (:consumption o) (get-in o [:capture :alternative]))))))
                            (filter #(= 1 (count (:covers (options %)))))
                            (sort (fn [a b]
                                    (check!)
                                    (let [work (key-compare (work-key a) (work-key b) check!)]
                                      (if (zero? work)
                                        (let [kind (compare (kind-rank (get-in a [:path :options :access :kind]))
                                                            (kind-rank (get-in b [:path :options :access :kind])))]
                                          (if (zero? kind)
                                            (key-compare (:canonical-key (options a)) (:canonical-key (options b)) check!)
                                            kind)) work))))
                            vec)
            failed (volatile! #{}) attempts (volatile! 0)]
        (letfn [(search [known done used]
                  (check!)
                  (let [state [known done used]]
                    (when-not (contains? @failed state)
                      (when (> (vswap! attempts inc) search-bound)
                        (fail! :query-source/unsupported :planning))
                      (if (and (= done required) (set/subset? live-vars (set (keys known)))) []
                          (let [result
                                (some (fn [plan]
                                        (check!)
                                        (let [work (work-key plan) o (options plan)
                                              permission (:permission o)]
                                          (when (and (not (contains? done work)) (ready? known used plan))
                                            (when-let [known (extend-known known (:columns o))]
                                              (let [used (cond-> used (= :once (:opens permission))
                                                           (conj (:resource permission)))]
                                                (when-let [tail (search known (conj done work) used)]
                                                  (cons plan tail))))))) candidates)]
                            (when-not result
                              (own-work! (+ 512 (* 64 (count done)) (* 256 (count known)) (* 128 (count used))))
                              (vswap! failed conj state))
                            result)))))]
          (let [selected (search (column-map input-columns) #{} #{})]
            (when-not selected (fail! :query-source/unsupported :planning))
            ;; Reserve all output containers before building them. Estimates are
            ;; never converted into native scores, cardinalities, or emptiness.
            (vreset! output-token (control/reserve! ctl {:retained-bytes (+ 1024 (* 512 (count selected)))}))
            (let [result {:region-id region-id :policy :eav-structural-v1 :selection :structural
                          :estimated-access-ns nil :fallback-reasons [] :plans selected
                          :planning-time-ms (/ (double (- (System/nanoTime) started)) 1000000.0)}]
              (let [token @output-token] (rt/own! runtime #(control/release! token)))
              (vreset! output-token nil)
              result))))
      (finally
        (when-let [token @output-token] (control/release! token))
        (doseq [token @charges] (control/release! token))))))

(defn validate-query!
  "Audit all selected regions together before execution. Region-local planning
  cannot grant a one-shot resource another open in a different region. This
  guard admits no implicit spool, replay, or duplicate region/work occurrence."
  [runtime schedules]
  (let [ctl (:control runtime)]
    (control/check-active! ctl)
    (ensure! (vector? schedules))
    (let [n (reduce (fn [n schedule] (+' n (count (:plans schedule)))) 0 schedules)
          token (control/reserve! ctl {:retained-bytes (+' 1024 (*' 512 n) (* 256 (count schedules)))})]
      (try
        (ensure! (= (count schedules) (count (set (map :region-id schedules)))))
        (loop [remaining (seq schedules) used #{} work #{}]
          (when-let [schedule (first remaining)]
            (control/check-active! ctl)
            (ensure! (= :eav-structural-v1 (:policy schedule)))
            (let [[used work]
                  (reduce (fn [[used work] plan]
                            (control/check-active! ctl)
                            (let [o (options plan)
                                  permission (permissions/planning-permission
                                               (:consumption o) (get-in o [:capture :alternative]))
                                  key (work-key plan)]
                              (ensure! (and (= (:region-id schedule) (:region-id o))
                                            (= 1 (count (:covers o))) (not (contains? work key))))
                              (when (and (= :once (:opens permission))
                                         (or (:spent? permission) (contains? used (:resource permission))))
                                (fail! :query-source/unsupported :planning))
                              [(cond-> used (= :once (:opens permission)) (conj (:resource permission)))
                               (conj work key)])) [used work] (:plans schedule))]
              (recur (next remaining) used work))))
        nil
        (finally (control/release! token))))))

(defn explain!
  "Copy the structural decision without opaque handles, bindings, annotations,
  session resource keys, or provider labels. Estimates remain advisory."
  [runtime schedule]
  (let [plans (:plans schedule)
        ctl (:control runtime)
        token (control/reserve! ctl {:retained-bytes (+ 1024 (* 1024 (count plans)))})]
    (try
      (let [steps (mapv
                    (fn [plan]
                      (control/check-active! ctl)
                      (let [o (options plan)
                            alternative (get-in o [:capture :alternative])
                            estimate (:advisory-estimate plan)
                            numeric (if (seq (:requires o))
                                      [:startup-ns :per-binding-ns :rows-per-binding :row-bytes
                                       :requests-per-open :requests-per-binding :transfer-bytes-per-binding
                                       :peak-retained-bytes]
                                      [:startup-ns :run-ns :rows :row-bytes :requests-per-open
                                       :transfer-bytes :peak-retained-bytes])]
                        (rt/diagnostic-copy! runtime
                          (cond-> {:source (:source-symbol o) :region-id (:region-id o)
                           :alternative-id (:alternative-id o) :covers (:covers o) :access (:access o)
                           :requires (mapv :variable (:requires o))
                           :replay (get-in alternative [:properties :replay])
                           :constraints (select-keys (:constraints alternative)
                                                     [:max-binding-rows :max-binding-bytes :max-request-binding-rows])
                           :estimate (assoc (select-keys estimate (conj numeric :confidence))
                                            :provenance (select-keys (:provenance estimate)
                                                                     [:kind :id :observed-at-ms :valid-for-ms]))}
                            (contains? (:properties alternative) :consumption-group)
                            (assoc :consumption-group (get-in alternative [:properties :consumption-group]))
                            (seq (:requires o))
                            (assoc :binding-limits
                                   {:rows (min (:binding-rows (control/limits ctl))
                                               (get-in alternative [:constraints :max-binding-rows]))
                                    :bytes (min (:binding-bytes (control/limits ctl))
                                                (get-in alternative [:constraints :max-binding-bytes]))}))))) plans)
            result (assoc (select-keys schedule [:region-id :policy :selection
                                                 :estimated-access-ns :fallback-reasons :planning-time-ms]) :steps steps)]
        (rt/own! runtime #(control/release! token))
        result)
      (catch Throwable e (control/release! token) (throw e)))))
