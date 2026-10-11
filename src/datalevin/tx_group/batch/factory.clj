;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.factory
  "Assemble the new-protocol environment runtime from a private WAL state and a
  raw native application writer.

  Embedded KV and server admission use this wiring seam. It owns neither the
  WAL runtime nor the LMDB writer: the opener opens them, then this namespace
  binds the WAL-only runtime control, builds the selected branch adapters and
  one reusable WAL worker, and returns the collector with a close path that
  stops the worker only after the collector has drained.

  The returned map supplies the collector executor and its close action."
  (:require [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.executor :as executor]
            [datalevin.tx-group.batch.native :as native]
            [datalevin.tx-group.batch.profile :as profile]
            [datalevin.tx-group.batch.rmw :as rmw]
            [datalevin.tx-group.batch.wal :as wal]
            [datalevin.tx-group.batch.worker :as worker]
            [datalevin.txlog :as txlog]))

(defn executor
  "Build the environment executor and worker over a WAL state and native writer.

  `opts`:
  - `:runtime-control` binds the WAL-only control (`txlog/bind-runtime-control!`)
    before anything is built; required for a private WAL runtime.
  - `:native-opts` is forwarded to `native/branch` (`:apply-fn`, `:transact!`,
    `:write-metadata!`).
  - `:execution-profile` selects :blind (default without RMW), :raw-rmw,
    :kv-rmw or :datalog-rmw. Profiles own the strategy flags.
  - `:rmw-opts` supplies callbacks for the selected RMW profile. See
    `doc/wal-executor.md` for required callbacks, signatures and ownership.
    Invalid configurations fail before control binding or worker startup.
  - `:worker-opts` is forwarded to `worker/for-wal` (`:name`, `:daemon?`,
    `:close-timeout-ms`).
  - `:schedule-fn` overrides the fixed schedule selector (tests/diagnostics).

  Returns `{:executor :worker :wal-branch :native-branch :close!}`. `:close!`
  stops the worker and must run only after collector quiescence. `:native-branch`
  is nil when `:rmw-opts` selects direct native write capture."
  ([wal-state native-writer]
   (executor wal-state native-writer nil))
  ([wal-state native-writer
    opts]
   (let [{:keys [runtime-control native-opts rmw-opts worker-opts schedule-fn check-batch!]
          :as opts} (profile/resolve-options (some? wal-state) opts)]
     (when (and wal-state (nil? (or runtime-control (txlog/runtime-control wal-state))))
       (throw (ex-info "WAL executor requires a runtime control"
                       {:error :txlog/invalid-executor-configuration
                        :execution-profile (:execution-profile opts)
                        :option [:runtime-control]})))
     (when (and wal-state runtime-control)
       (txlog/bind-runtime-control! wal-state runtime-control))
     (if-not wal-state
       (let [native-branch (when-not rmw-opts
                             (native/branch native-writer native-opts))
             native-rmw (when rmw-opts
                          (cond-> (assoc (dissoc rmw-opts :encode-body :body-cost) :collect? true)
                            (:write-metadata! native-opts)
                            (assoc :write-metadata! (:write-metadata! native-opts))))
             blind-exec (when-not rmw-opts
                          (executor/create-native native-branch nil check-batch!))]
         {:executor (fn [batch]
                      (if native-rmw
                        (rmw/execute! native-writer nil nil (constantly nil)
                                      check-batch! native-rmw batch)
                        (blind-exec batch)))
          :native-branch native-branch
          :close! (constantly true)})
       (let [wal-branch (wal/branch wal-state)
             next-lsn! #(long @(:next-lsn wal-state))
             native-branch (when-not rmw-opts
                             (native/branch native-writer native-opts))
             w (apply worker/for-wal wal-branch (apply concat worker-opts))
             policy! (when rmw-opts
                       (executor/policy-runner wal-branch (:executor w) (:wake! w)))
             native-rmw (when rmw-opts
                          (cond-> (assoc rmw-opts :collect? true :run-policy! policy!)
                            (:write-metadata! native-opts)
                            (assoc :write-metadata! (:write-metadata! native-opts))))
             exec (when-not rmw-opts
                    (executor/create wal-branch native-branch
                                     next-lsn!
                                     (cond-> {:wal-executor (:executor w)
                                              :wake-maintenance! (:wake! w)}
                                       schedule-fn (assoc :schedule-fn schedule-fn)
                                       check-batch! (assoc :check-batch! check-batch!))))]
         {:executor (fn [batch]
                      (if native-rmw
                        (rmw/execute! native-writer wal-branch
                                      next-lsn! (:wake! w)
                                      check-batch! native-rmw batch)
                        (exec batch)))
          :worker w
          :wal-branch wal-branch
          :native-branch native-branch
          :close! (fn [] ((:close! w)))})))))

(defn collector
  "Build the collector over a factory runtime. Returns
  `{:collector :executor :worker :close!}`."
  ([wal-state native-writer] (collector wal-state native-writer nil))
  ([wal-state native-writer {:as opts}]
   (let [{:keys [executor] :as runtime} (executor wal-state native-writer opts)]
     (assoc runtime
            :collector (batch/create executor
                                      (not-empty (select-keys
                                                  opts
                                                  [:limits
                                                   :preparation-timeout-ms])))))))
