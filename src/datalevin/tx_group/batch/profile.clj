;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.profile
  "Construction-time executor contracts. See doc/wal-executor.md for callback
  arguments, return values, transaction lifetime and thread ownership.")

(def execution-profiles
  "Supported execution strategies. Strategy flags belong to the profile;
  :rmw-opts supplies callbacks only (matching legacy flags are accepted)."
  (let [raw {:prepared-request? false :prepared-owned? false :allow-flags? false
             :retry-frozen? false :abort-returns? false
             :collect-prepared-prefix? false :encode-batch? false
             :shared-datalog-writer? false}
        prepared (assoc raw :prepared-request? true :prepared-owned? true
                            :allow-flags? true :retry-frozen? true :abort-returns? true
                            :collect-prepared-prefix? true)]
    {:blind {:mode :blind}
     :raw-rmw {:mode :rmw :flags raw :required #{:check-row!}}
     :kv-rmw {:mode :rmw :flags prepared :required #{:prepare-rows!}}
     :datalog-rmw {:mode :rmw
                   :flags (assoc prepared :encode-batch? true :shared-datalog-writer? true)
                   :required #{:prepare-rows! :storage-rows! :finish-preparation!}}}))

(def ^:private rmw-callbacks
  #{:check-row! :prepare-rows! :charge-fn :encode-body :body-cost
    :ensure-body-ready! :application-error! :before-batch! :before-body!
    :before-append! :finish-preparation! :storage-rows! :storage-tail! :committed!})
(def ^:private storage-callbacks #{:finish-preparation! :storage-rows! :storage-tail!})
(def ^:private native-callbacks #{:apply-fn :transact! :write-metadata!})
(def ^:private control-callbacks
  #{:check-admission! :throw-if-fatal! :before-append! :before-sync! :mark-fatal! :on-failure!})
(def ^:private factory-keys
  #{:execution-profile :rmw-opts :native-opts :runtime-control :worker-opts
    :schedule-fn :check-batch! :limits :preparation-timeout-ms})

(defn- invalid! [profile option reason]
  (throw (ex-info (str "Invalid executor configuration: " reason)
                  {:error :txlog/invalid-executor-configuration
                   :execution-profile profile :option option :reason reason})))

(defn- known-keys! [profile path options allowed]
  (when-not (or (nil? options) (map? options))
    (invalid! profile path "expected an options map"))
  (doseq [key (keys options)]
    (when-not (contains? allowed key)
      (invalid! profile (conj path key) "unknown or executor-owned option"))))

(defn- callbacks! [profile path options allowed]
  (doseq [key allowed :let [value (get options key)] :when (some? value)]
    (when-not (ifn? value)
      (invalid! profile (conj path key) "expected a callable callback"))))

(defn resolve-options
  "Validate and expand a factory configuration once, before runtime mutation.
  :blind is the default only when :rmw-opts is absent. RMW requires an explicit
  profile. WAL RMW requires :encode-body; storage callbacks require :datalog-rmw.
  Callback arities are documented, never probed by invoking application code."
  [wal? opts]
  (let [profile (or (:execution-profile opts) :blind)
        {:keys [mode flags required] :as definition} (get execution-profiles profile)
        rmw (:rmw-opts opts)
        native (:native-opts opts)
        worker (:worker-opts opts)
        control (:runtime-control opts)]
    (when-not definition (invalid! profile [:execution-profile] "unknown execution profile"))
    (known-keys! profile [] opts factory-keys)
    (known-keys! profile [:native-opts] native native-callbacks)
    (callbacks! profile [:native-opts] native native-callbacks)
    (known-keys! profile [:runtime-control] control control-callbacks)
    (callbacks! profile [:runtime-control] control control-callbacks)
    (known-keys! profile [:worker-opts] worker #{:name :daemon? :close-timeout-ms})
    (callbacks! profile [] opts #{:schedule-fn :check-batch!})
    (when (and (contains? worker :name) (not (string? (:name worker))))
      (invalid! profile [:worker-opts :name] "expected a thread name string"))
    (when (and (contains? worker :daemon?) (not (boolean? (:daemon? worker))))
      (invalid! profile [:worker-opts :daemon?] "expected a boolean"))
    (when (and (contains? worker :close-timeout-ms)
               (not (and (integer? (:close-timeout-ms worker))
                         (<= 0 (:close-timeout-ms worker) Long/MAX_VALUE))))
      (invalid! profile [:worker-opts :close-timeout-ms] "expected nonnegative milliseconds"))
    (when (and (not wal?) (or (seq worker) (seq control)))
      (invalid! profile [] "WAL control and worker options require a WAL runtime"))
    (when (and (not wal?) (:schedule-fn opts))
      (invalid! profile [:schedule-fn] "schedule override requires a WAL runtime"))
    (if (= mode :blind)
      (do (when (some? rmw)
            (invalid! profile [:rmw-opts] "RMW options require an explicit RMW profile"))
          (assoc opts :execution-profile profile))
      (do
        (known-keys! profile [:rmw-opts] rmw (into rmw-callbacks (keys flags)))
        (callbacks! profile [:rmw-opts] rmw rmw-callbacks)
        (doseq [[key value] flags :when (contains? rmw key)]
          (when-not (= value (get rmw key))
            (invalid! profile [:rmw-opts key] "strategy flag conflicts with execution profile")))
        (doseq [key (cond-> required wal? (conj :encode-body))]
          (when-not (ifn? (get rmw key))
            (invalid! profile [:rmw-opts key] "required callback is missing")))
        (when (and (not= profile :datalog-rmw)
                   (some #(some? (get rmw %)) storage-callbacks))
          (invalid! profile [:rmw-opts] "storage callbacks require :datalog-rmw"))
        (when (and (= profile :datalog-rmw) (not wal?))
          (invalid! profile [:execution-profile] ":datalog-rmw requires a WAL runtime"))
        (when (and (not= profile :raw-rmw) (:check-row! rmw))
          (invalid! profile [:rmw-opts :check-row!] "prepared capture validates in :prepare-rows!"))
        (when (or (:apply-fn native) (:transact! native) (:schedule-fn opts))
          (invalid! profile [] "native branch and schedule overrides require :blind"))
        (assoc opts :execution-profile profile :rmw-opts (merge flags rmw))))))
