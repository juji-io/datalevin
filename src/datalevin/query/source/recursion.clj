(ns ^:no-doc datalevin.query.source.recursion
  "Resource adapter for the shared recursive stratum kernel. Compiled body
  operators execute serially on the pinned native view's owning thread."
  (:require [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value]
            [datalevin.rules.fixed-point :as fixed-point])
  (:import [java.util IdentityHashMap]))

(defn evaluate!
  "Evaluate one compiled stratum. evaluate-body! receives a short-lived runtime,
  totals, deltas and rule ID, and returns its completed body relation. Body
  operators must admit their own workspace/output storage on that runtime.
  Initial relations remain caller-owned; the returned handle owns all copied
  full-head keys until close or invocation cleanup. Optional initialize!
  produces each initial body inside the same synchronous handoff boundary;
  owners identifies captured operator runtimes that create temporary resources."
  ([runtime initial rule-ids dependencies evaluate-body!]
   (evaluate! runtime initial rule-ids dependencies evaluate-body! nil))
  ([runtime initial rule-ids dependencies evaluate-body! {:keys [initialize! owners] :or {owners []}}]
    (rt/invoke runtime :execution
      (fn []
        (let [ctl (:control runtime) values (IdentityHashMap.)
              closed? (atom false) kernel (volatile! nil)
              check! #(do (source/check-active! ctl)
                          (when @closed? (value/fail! :query-source/invalid :execution {:state :closed})))
              close (rt/own! runtime
                      (fn []
                        (when (compare-and-set! closed? false true)
                          (try (when-let [handle @kernel] ((:close handle)))
                               (finally
                                 (vreset! kernel nil)
                                 (doseq [token (.keySet values)] (source/release! token))
                                 (.clear values))))))
              reserve! (fn [rows bytes]
                         (check!)
                         (when (> rows Integer/MAX_VALUE)
                           (value/fail! :query-source/limit :reserve))
                         (control/reserve! ctl {:retained-bytes bytes}))
              own-row! (fn [scratch]
                         ;; Projection scratch was admitted by the kernel. Charge
                         ;; the complete detached payload before visiting/copying
                         ;; it, and retain it through the full seen-set lifetime.
                         (let [row (vec scratch) token (rt/charge-copy! runtime row)]
                           (.put values token true)
                           (object-array (value/owned-copy row check!))))]
          (try
            (vreset! kernel
              (fixed-point/run-bounded! initial rule-ids dependencies
                (fn [{:keys [totals deltas]} id consume!]
                  (rt/with-workspace runtime owners
                    (fn [workspace]
                      (check!)
                      (consume! (evaluate-body! workspace totals deltas id)))))
                {:check! check! :reserve! reserve! :release! source/release!
                 :validate-row! #(doseq [x %] (value/execution-value! x check!))
                 :initialize! (when initialize!
                                (fn [id consume!]
                                  (rt/with-workspace runtime owners
                                    (fn [workspace] (consume! (initialize! workspace id))))))
                 :own-row! own-row! :invalid! #(value/fail! :query-source/invalid :execution)}))
            (check!)
            {:relations (fn [] (check!) ((:check! @kernel)) (:relations @kernel))
             :close close}
            (catch Throwable e
              (try (close) (catch Throwable secondary (.addSuppressed e secondary)))
              (throw e))))))))
