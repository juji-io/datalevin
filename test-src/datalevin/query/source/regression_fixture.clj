(ns datalevin.query.source.regression-fixture
  "Explicit EAV row mappings and shared native scan drivers for regressions.
  Test infrastructure only; all Datalog evaluation delegates to public q."
  (:require [datalevin.bits :as bits]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.parser :as parser]
            [datalevin.query.source :as source]
            [datalevin.query.source.fake :as fake]
            [datalevin.query.source.native :as native]
            [datalevin.query.source.native-plan :as native-plan]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value]
            [datalevin.storage.scan :as scans])
  (:import [datalevin.parser Pattern SrcVar]
           [datalevin.utl QueryKey]
           [java.util AbstractCollection]))

(def row-info (fake/row-info))
(defn row-facts [rows] (with-meta (fake/row-facts rows) (meta rows)))
(defn entity-info [info type]
  (-> info
      (assoc-in [:pattern-contract :attributes :row/value :entity-type] type)
      (assoc-in [:logical-schema :domains (keyword "fixture.domain" "0") :entity-type] type)))
(defn events [provider] (mapv first @(:events provider)))
(defn pair-source
  ([info rows] (pair-source info rows {}))
  ([info rows hooks]
   (fake/source (assoc info :source-instance-id (random-uuid)) (row-facts rows)
     {:mode :scan :hooks (into {} (map (fn [[phase f]]
                                        [(case phase :acquisition :acquire :planning :plan :open-cursor :open phase)
                                         (fn [_] (f))])) hooks)})))

(defn raw-source
  "Deliberately bypass constructor validation for malformed provider fixtures."
  [info rows]
  (fake/->EAVSource info (atom {:facts (row-facts rows) :version (random-uuid)})
                    {:mode :scan} (atom [])))

(defn- eager-arguments [query arguments]
  (let [parsed (parser/parse-query query)
        aliases (into #{} (keep (fn [p]
                                 (when (= :row/value (:value (get (:pattern p) 1)))
                                   (or (get-in p [:source :symbol]) '$))))
                      (parser/collect #(instance? Pattern %) (:qwhere parsed)))]
    (mapv (fn [index input]
            (let [binding (get (:qin parsed) index)]
              (if (and (instance? SrcVar (:variable binding))
                     (contains? aliases (get-in binding [:variable :symbol]))
                     (vector? input) (every? #(and (vector? %) (= 2 (count %))) input))
                (row-facts input) input))) (range (count arguments)) arguments)))

(defn q [query & arguments] (apply d/q query (eager-arguments query arguments)))
(defn explain [options query & arguments]
  (apply d/explain options query (eager-arguments query arguments)))

(defn- scan* [ctl snapshot attribute entity bound keys-only? emit]
  (native/check-view! ctl snapshot)
  (let [{:keys [kv schema]} snapshot props (schema attribute)
        aid (:db/aid props) check! #(native/check-view! ctl snapshot)
        stop (ex-info "Completed regression visitor" {})
        failure (volatile! nil)
        token (source/reserve! ctl {:retained-bytes 1024})
        scratch (volatile! nil)]
    (try
      (when (some? bound)
        (vreset! scratch (source/reserve! ctl {:retained-bytes (value/add-charge 4096
                                                              (value/multiply-charge 8 (value/value-charge bound)))})))
      (when aid
        (let [visit (when-not keys-only? (native/value-visitor ctl snapshot attribute))
              physical (:db/valueType props)
              seek? (and (some? bound)
                         (#{:db.type/long :db.type/ref :db.type/string :db.type/keyword :db.type/symbol
                            :db.type/boolean :db.type/uuid :db.type/instant :db.type/float :db.type/double} physical)
                         (value/matches-type? (if (= :db.type/ref physical) :long (keyword (name physical))) bound)
                         (not (and (number? bound) (Double/isNaN (double bound)))))
              policy (cond-> {:check! check! :admit-row! check!
                              :visit-value! (fn [raw eid callback]
                                              (visit raw eid
                                                (fn [x] (when (or (nil? bound) (QueryKey/equiv bound x))
                                                          (callback x)))))}
                       seek? (assoc :index-range [(scans/ave-key-range aid physical
                                                                  [[:closed bound] [:closed bound]]) :avg]))
              out (proxy [AbstractCollection] []
                    (size [] 0)
                    (iterator [] (.iterator ^java.util.List []))
                    (add [tuple]
                      (try
                        (check!)
                        (when (reduced? (emit (cond-> [(aget ^objects tuple 0) attribute]
                                               (not keys-only?) (conj (aget ^objects tuple 1)))))
                          (throw stop))
                        true
                        (catch Throwable e (vreset! failure e) (throw e)))))
              read-entity! #(scans/eav-tuples-scan-controlled kv out aid [(object-array [%])] (not keys-only?) policy)]
          (cond
            (some? entity) (read-entity! entity)
            (:db/noindex props)
            (i/visit-key-range kv c/eav
              (fn [entry]
                (try (check!) (read-entity! (bits/read-buffer entry :id))
                     (catch Throwable e (vreset! failure e) (throw e)))) [:all] :id true)
            :else (scans/ave-tuples-scan-controlled kv out aid (not keys-only?) policy))))
      (catch Throwable e
        (let [error (or @failure e)]
          (if (identical? error stop) (reduced nil)
            (do (source/check-active! ctl) (throw error)))))
      (finally (when-let [held @scratch] (source/release! held)) (source/release! token)))))

(defn scan!
  ([ctl snapshot attribute emit] (scan* ctl snapshot attribute nil nil false emit))
  ([ctl snapshot attribute entity emit] (scan* ctl snapshot attribute entity nil false emit))
  ([ctl snapshot attribute entity bound emit] (scan* ctl snapshot attribute entity bound false emit)))
(defn scan-keys!
  ([ctl snapshot attribute emit] (scan* ctl snapshot attribute nil nil true emit))
  ([ctl snapshot attribute entity emit] (scan* ctl snapshot attribute entity nil true emit)))

(defn with-native-keys
  "Exercise the active native key collector under a scoped workspace."
  [ctl rows variable f]
  (let [runtime {:control ctl :query-id (random-uuid) :cleanup (atom []) :redacted-errors (atom #{})}
        pattern (first (:qwhere (parser/parse-query '[:find ?id :where [?id :key _]])))
        entity (assoc (first (:pattern pattern)) :symbol variable)
        values (mapv #(object-array [(get % variable)]) rows)]
    (rt/with-runtime-context runtime
      (fn [runtime]
        (rt/with-workspace runtime
          (fn [workspace]
            (let [keys (#'native-plan/binding-keys! workspace #(source/check-active! ctl)
                         entity (nth (:pattern pattern) 2) :entity [variable] values)]
              (f (mapv #(aget ^objects % 0) keys)))))))))
