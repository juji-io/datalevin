(ns ^:no-doc datalevin.query.source.native-plan
  "Controlled execution of existing native attribute plans inside a v2
  region. The native planner and storage scan driver remain authoritative."
  (:require [datalevin.query.optimizer.plan-build :as plan-build]
            [datalevin.query.plan :as plan]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.native :as native]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.relation :as relation])
  (:import [datalevin.db DB]
           [datalevin.parser Constant Placeholder Variable]
           [datalevin.query.plan InitStep MergeScanStep]
           [datalevin.utl QueryKey]
           [java.util HashSet List]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defn- unsupported! [] (fail! :query-source/unsupported :planning))
(defn- ensure! [ok] (when-not ok (fail! :query-source/invalid :native-view)))

(defn- private-variable [prefix variables check!]
  (loop []
    (check!)
    (let [v (gensym prefix)]
      (if (contains? variables v) (recur) v))))

(defn- constant-entity [x]
  (when-not (or (nil? x) (integer? x)
                (and (not= :data (value/inferred-type x))
                     (value/disjoint-constant? :long x (fn [])))) (unsupported!))
  (when (and (integer? x) (<= Long/MIN_VALUE x Long/MAX_VALUE)) (long x)))

(defn spec!
  "Emit the precise interface of a native fixed-attribute access. No
  index is scanned and no payload is decoded while deriving this interface."
  [runtime snapshots database node literal-terms]
  (when-not (instance? DB database) (unsupported!))
  (native/validate! database)
  (let [snapshot (get snapshots (.-store ^DB database))
        _ (native/check-view! (:control runtime) snapshot)
        [entity attribute v :as pattern] (:pattern node)]
    (when-not (and (= 3 (count pattern))
                   (or (instance? Variable entity) (instance? Constant entity) (instance? Placeholder entity))
                   (instance? Constant attribute) (keyword? (:value attribute))
                   (or (instance? Variable v) (instance? Constant v) (instance? Placeholder v))
                   (not (and (instance? Variable entity) (= (:symbol entity) (:symbol v))))) (unsupported!))
    (let [attribute (:value attribute) props (get-in snapshot [:schema attribute])
          _ (when (instance? Constant entity) (constant-entity (:value entity)))
          _ (when (and (:db/noindex props) (instance? Placeholder entity)) (unsupported!))
          type (when-not (instance? Placeholder v) (native/logical-type! snapshot attribute))]
      {:requires (if (and (:db/noindex props) (instance? Variable entity))
                   [{:variable (:symbol entity) :type :data :nullable? true}] [])
       :constant-constraints (mapv (fn [[variable i]]
                                     {:variable variable :type (case i 0 :long 1 :keyword 2 type) :nullable? false})
                                   literal-terms)
       :columns (cond-> []
                  (instance? Variable entity) (conj {:variable (:symbol entity) :type :long :nullable? false})
                  (instance? Variable v) (conj {:variable (:symbol v) :type type :nullable? false}))})))

(defn- controlled! [runtime check! snapshot database step entities accept?]
  (check!)
  (native/check-view! (:control runtime) snapshot)
  ;; Admission covers the output list, visitor closures, and native range/key
  ;; scratch before creating a scan. Each row/value has its own prior admission.
  (let [tokens (volatile! [(control/reserve! (:control runtime) {:retained-bytes 8192})])
        own! #(do (vswap! tokens conj %) %)
        _ (rt/own! runtime #(let [held @tokens]
                             (vreset! tokens [])
                             (doseq [token held] (control/release! token))))
        visit (when (or (and (instance? InitStep step) (< 1 (count (:vars step))))
                        (and (instance? MergeScanStep step) (seq (:vars step))))
                (native/value-visitor (:control runtime) snapshot
                                      (if (instance? InitStep step)
                                        (:attr step)
                                        (ffirst (:attrs-v step)))))
        policy {:schema (:schema snapshot)
                :check! #(do (check!) (native/check-view! (:control runtime) snapshot))
                :unsupported! unsupported!
                :admit-row! #(own! (control/reserve! (:control runtime) {:retained-bytes 1024}))
                :visit-value! (fn [raw eid emit]
                                (visit raw eid
                                       (fn [x]
                                         (when (accept? eid x)
                                           (own! (rt/charge-copy! runtime x))
                                           (emit (value/owned-copy x check!))))))}]
    (plan/-execute-controlled step database entities policy)))

(defn- entity-keys! [runtime check! entity input-cols ^List tuples]
  (let [fixed? (instance? Constant entity)
        n (if fixed? (min 1 (.size tuples)) (.size tuples))
        token (control/reserve! (:control runtime) {:retained-bytes (+' 2048 (*' 256 n))})
        _ (rt/own! runtime #(control/release! token))
        seen (HashSet.) keys (FastList.) index (.indexOf ^List input-cols (:symbol entity))]
    (dotimes [i n]
      (check!)
      (let [e (if fixed? (constant-entity (:value entity)) (aget ^objects (.get tuples i) index))]
        ;; Native IDs are Long keys. Other bound representations cannot join
        ;; these facts under shared key equality; a literal uses constant equality.
        (when (and (instance? Long e) (.add seen e)) (.add keys (object-array [e])))))
    keys))

(defrecord ^:private NativeStep [in cols run]
  plan/IStep
  (-type [_] :native-controlled)
  (-execute [_ _ tuples] (run tuples))
  (-execute-pipe [_ _ _ _] (unsupported!))
  (-sample [_ _ _] (unsupported!))
  (-explain [_ _] "Controlled native attribute access"))

(defrecord ^:private NativeFactory [in cols build]
  plan/IStepFactory
  (-instantiate-step [_ input-cols] (build input-cols)))

(defn factory!
  "Compile a single native pattern with the existing native planner. Native
  InitSteps and entity-bound MergeScanSteps use controlled storage. Other plan
  shapes reject before provider acquisition until their admission paths exist."
  [runtime snapshots database node {:keys [requires columns]} check! join!]
  (let [snapshot (get snapshots (.-store ^DB database))
        _ (native/check-view! (:control runtime) snapshot)
        token (rt/charge! runtime (mapv :variable columns) 16384 1)
        _ (rt/own! runtime #(control/release! token))
        [e attribute v] (:pattern node)
        variables (mapv :variable columns)
        entity (if (instance? Variable e) (:symbol e)
                   (private-variable "?native_entity__" (set variables) check!))
        value-var (when-not (instance? Placeholder v)
                    (if (instance? Variable v) (:symbol v)
                        (private-variable "?native_value__" (conj (set variables) entity) check!)))
        ;; Reuse the native planner's pure single-pattern emitter. Mutable
        ;; Store metadata, global plan caches and installed access methods
        ;; cannot change this plan after its schema was captured in the view.
        init (plan-build/initialization-step entity (:value attribute) value-var)
        lookup (plan-build/entity-lookup-step entity (:value attribute) value-var)
        accept? (if (instance? Constant v) (fn [_ x] (QueryKey/equiv (:value v) x)) (fn [_ _] true))]
    (->NativeFactory (set (map :variable requires)) variables
      (fn [input-cols]
        (let [lookup? (or (instance? Constant e) (and (instance? Variable e) (some #{entity} input-cols)))
              step (if lookup? lookup init)
              output-cols (into input-cols (remove (set input-cols)) variables)
              attrs (select-keys (plan/cols->attrs (:cols step)) variables)]
          (->NativeStep (set input-cols) output-cols
            (fn [^List tuples]
              (check!)
              (native/check-view! (:control runtime) snapshot)
              (let [keys (when lookup? (entity-keys! runtime check! e input-cols tuples))
                    rows (controlled! runtime check! snapshot database step keys accept?)
                    right (relation/relation! attrs rows)
                    left (relation/relation! (zipmap input-cols (range)) tuples)
                    result (join! left right)]
                (ensure! (= output-cols (mapv key (sort-by val (:attrs result)))))
                (:tuples result)))))))))
