(ns ^:no-doc datalevin.query.source.native-plan
  "Controlled execution of existing native initialization plans inside a v2
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
           [datalevin.query.plan InitStep]
           [java.util List]))

(defn- unsupported! [] (fail! :query-source/unsupported :planning))
(defn- ensure! [ok] (when-not ok (fail! :query-source/invalid :native-view)))

(defn columns!
  "Emit the precise interface of a native fixed-attribute initialization. No
  index is scanned and no payload is decoded while deriving this interface."
  [runtime snapshots database node]
  (when-not (instance? DB database) (unsupported!))
  (native/validate! database)
  (let [snapshot (get snapshots (.-store ^DB database))
        _ (native/check-view! (:control runtime) snapshot)
        [entity attribute v :as pattern] (:pattern node)]
    (when-not (and (= 3 (count pattern)) (instance? Variable entity)
                   (instance? Constant attribute) (keyword? (:value attribute))
                   (or (instance? Variable v) (instance? Placeholder v))
                   (not= (:symbol entity) (:symbol v))) (unsupported!))
    (let [attribute (:value attribute) props (get-in snapshot [:schema attribute])]
      (when (:db/noindex props) (unsupported!))
      (cond-> [{:variable (:symbol entity) :type :long :nullable? false}]
        (instance? Variable v)
        (conj {:variable (:symbol v) :type (native/logical-type! snapshot attribute) :nullable? false})))))

(defn- controlled! [runtime check! snapshot database step]
  (check!)
  (native/check-view! (:control runtime) snapshot)
  ;; Admission covers the output list, visitor closures, and native range/key
  ;; scratch before creating a scan. Each row/value has its own prior admission.
  (let [tokens (volatile! [(control/reserve! (:control runtime) {:retained-bytes 8192})])
        own! #(do (vswap! tokens conj %) %)
        _ (rt/own! runtime #(let [held @tokens]
                             (vreset! tokens [])
                             (doseq [token held] (control/release! token))))
        visit (when (< 1 (count (:vars step)))
                (native/value-visitor (:control runtime) snapshot (:attr step)))
        policy {:schema (:schema snapshot)
                :check! #(do (check!) (native/check-view! (:control runtime) snapshot))
                :unsupported! unsupported!
                :admit-row! #(own! (control/reserve! (:control runtime) {:retained-bytes 1024}))
                :visit-value! (fn [raw eid emit]
                                (visit raw eid
                                       (fn [x]
                                         (own! (rt/charge-copy! runtime x))
                                         (emit (value/owned-copy x check!)))))}]
    (plan/-execute-controlled step database nil policy)))

(defrecord ^:private NativeStep [in cols run]
  plan/IStep
  (-type [_] :native-controlled)
  (-execute [_ _ tuples] (run tuples))
  (-execute-pipe [_ _ _ _] (unsupported!))
  (-sample [_ _ _] (unsupported!))
  (-explain [_ _] "Controlled native attribute initialization"))

(defrecord ^:private NativeFactory [in cols build]
  plan/IStepFactory
  (-instantiate-step [_ input-cols] (build input-cols)))

(defn factory!
  "Compile a single native pattern with the existing native planner. Complete
  unfiltered InitSteps use the controlled storage boundary. Other native plan
  shapes reject before provider acquisition until their admission paths exist."
  [runtime snapshots database node columns check! join!]
  (let [snapshot (get snapshots (.-store ^DB database))
        _ (native/check-view! (:control runtime) snapshot)
        token (rt/charge! runtime (mapv :variable columns) 16384 1)
        _ (rt/own! runtime #(control/release! token))
        [e attribute v] (:pattern node)
        variables (mapv :variable columns)
        ;; Reuse the native planner's pure single-pattern emitter. Mutable
        ;; Store metadata, global plan caches and installed access methods
        ;; cannot change this plan after its schema was captured in the view.
        step (plan-build/initialization-step (:symbol e) (:value attribute)
                                             (when (instance? Variable v) (:symbol v)))]
    (when-not (and (instance? InitStep step)
                   (not-any? #(get step %) [:result :sample :know-e? :pred :val :range])
                   (= (set variables) (set (keys (plan/cols->attrs (:cols step))))))
      (unsupported!))
    (->NativeFactory #{} variables
      (fn [input-cols]
        (let [output-cols (into input-cols (remove (set input-cols)) variables)
              attrs (plan/cols->attrs (:cols step))]
          (->NativeStep (set input-cols) output-cols
            (fn [^List tuples]
              (check!)
              (native/check-view! (:control runtime) snapshot)
              (let [rows (controlled! runtime check! snapshot database step)
                    right (relation/relation! attrs rows)
                    left (relation/relation! (zipmap input-cols (range)) tuples)
                    result (join! left right)]
                (ensure! (= output-cols (mapv key (sort-by val (:attrs result)))))
                (:tuples result)))))))))
