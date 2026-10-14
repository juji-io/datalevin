(ns ^:no-doc datalevin.query.source.control
  "Invocation-owned F0 controls. Engine deadline/nesting integration is F1."
  (:require [datalevin.query.source.value :refer [fail!]])
  (:import [java.util.concurrent CompletableFuture]))

(def default-limits
  {:batch-rows 4096 :batch-bytes 1048576 :in-flight-batches 2
   :retained-bytes 67108864 :spill-bytes 1073741824 :result-rows 1000000
   :result-bytes 67108864 :external-requests 1024 :concurrent-requests 4})
(def ledger-keys (disj (set (keys default-limits)) :batch-rows :batch-bytes))

(deftype Control [limits deadline state])
(deftype Registration [control callback])
(deftype Reservation [control charges status])

(defn create
  ([] (create {}))
  ([{:keys [limits timeout-ms]}]
   (when-not (and (or (nil? limits) (map? limits))
                 (every? (set (keys default-limits)) (keys limits))
                 (every? (fn [[k v]]
                           (and (integer? v) (<= 0 v Long/MAX_VALUE)
                                (or (not (#{:batch-rows :batch-bytes
                                            :in-flight-batches :concurrent-requests} k))
                                    (pos? v)))) limits)
                 (or (nil? timeout-ms) (and (integer? timeout-ms)
                                           (<= 0 timeout-ms Long/MAX_VALUE))))
     (fail! :query-source/invalid :context))
   (Control. (merge default-limits limits)
             (when timeout-ms (+' (System/nanoTime) (*' timeout-ms 1000000)))
             (atom {:usage (zipmap ledger-keys (repeat 0))
                    :registrations #{} :reservations #{}}))))

(defn limits [^Control control] (.-limits control))
(defn usage [^Control control] (:usage @(.-state control)))
(defn cancelled? [^Control control] (boolean (:reason @(.-state control))))
(defn remaining-time-ms [^Control control]
  (when-let [deadline (.-deadline control)]
    (long (min Long/MAX_VALUE (max 0 (quot (-' deadline (System/nanoTime)) 1000000))))))

(defn- dispatch! [^Registration registration reason]
  (CompletableFuture/runAsync
    ^Runnable (fn [] (try ((.-callback registration) reason)
                         (catch Throwable _ nil)))))

(defn cancel! [^Control control reason]
  (let [state (.-state control)
        reason (select-keys reason [:error :phase])
        registrations
        (locking state
          (when-not (or (:reason @state) (:closed? @state))
            (let [registrations (:registrations @state)]
              (swap! state assoc :reason reason :registrations #{})
              registrations)))]
    (doseq [registration registrations] (dispatch! registration reason)))
  nil)

(defn check-active! [^Control control]
  (let [state (.-state control)]
    (when (:closed? @state) (fail! :query-source/invalid :context {:state :closed}))
    (when (and (some? (.-deadline control))
               (<= (.-deadline control) (System/nanoTime)))
      (cancel! control {:error :query/timeout :phase :execution}))
    (when-let [reason (:reason @state)]
      (fail! (:error reason) (:phase reason))))
  nil)

(defn register-cancel! [^Control control callback]
  (when-not (fn? callback) (fail! :query-source/invalid :context))
  (let [state (.-state control)
        registration (Registration. control callback)
        reason (locking state
                 (when (:closed? @state)
                   (fail! :query-source/invalid :context {:state :closed}))
                 (if-let [reason (:reason @state)] reason
                     (do (swap! state update :registrations conj registration) nil)))]
    (when reason (dispatch! registration reason))
    registration))

(defn unregister-cancel! [^Registration registration]
  (let [state (.-state ^Control (.-control registration))]
    (locking state (swap! state update :registrations disj registration)))
  nil)

(defn reserve! [^Control control charges]
  (when-not (and (map? charges) (every? ledger-keys (keys charges))
                (every? #(and (integer? %) (<= 0 % Long/MAX_VALUE)) (vals charges)))
    (fail! :query-source/invalid :reserve))
  (let [state (.-state control)
        token (Reservation. control charges (atom :owned))]
    (locking state
      (check-active! control)
      (let [usage (merge-with +' (:usage @state) charges)]
        (doseq [[category attempted] usage]
          (when (> attempted (get (.-limits control) category))
            (fail! :query-source/limit :reserve
                   {:category category :maximum (get (.-limits control) category)
                    :attempted attempted})))
        (swap! state assoc :usage usage
               :reservations (conj (:reservations @state) token))))
    token))

(defn release! [^Reservation token]
  (let [state (.-state ^Control (.-control token))]
    (locking state
      (when-not (= :released @(.-status token))
        (reset! (.-status token) :released)
        (swap! state (fn [s]
                       (-> s
                           (update :usage #(merge-with - % (dissoc (.-charges token)
                                                                  :external-requests)))
                           (update :reservations disj token)))))))
  nil)

(defn accept!
  "Validate query ownership and transfer a live batch reservation exactly once."
  [^Control control token bytes]
  (let [state (.-state control)]
    (locking state
      (check-active! control)
      (when-not (and (instance? Reservation token)
                    (identical? control (.-control ^Reservation token))
                    (= :owned @(.-status ^Reservation token))
                    (>= (get (.-charges ^Reservation token) :retained-bytes 0) bytes)
                    (>= (get (.-charges ^Reservation token) :in-flight-batches 0) 1))
        (fail! :query-source/invalid :batch))
      (reset! (.-status ^Reservation token) :transferred)))
  nil)

(defn close! [^Control control]
  (let [state (.-state control)]
    (locking state
      (doseq [token (:reservations @state)] (release! token))
      (swap! state assoc :closed? true :registrations #{})))
  nil)
