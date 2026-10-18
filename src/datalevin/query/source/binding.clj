(ns ^:no-doc datalevin.query.source.binding
  "Invocation-owned dependent keys and fan-out. This is an execution primitive;
   it neither negotiates source descriptors nor opens provider cursors."
  (:require [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]])
  (:import [datalevin.utl QueryKey QueryKey$KeyMap]
           [java.io Closeable]
           [java.util ArrayList]
           [java.util.concurrent.atomic AtomicLong]))

(declare close-keys!)

(deftype Ids [control ^AtomicLong next-id closed token]
  Closeable
  (close [_]
    (reset! closed true)
    (control/release! token)))

(deftype Keys [runtime ids state]
  Closeable
  (close [this] (close-keys! this)))

(defn id-space!
  "Create one ID space per invocation, lazily when its first dependent access
   is needed. Share it across logical accesses, aliases, and binding batches."
  [runtime]
  (let [ctl (:control runtime)
        ids (Ids. ctl (AtomicLong. 0) (atom false)
                  (control/reserve! ctl {:retained-bytes 512}))]
    (rt/own! runtime #(.close ids))
    ids))

(defn- next-id! [^Ids ids]
  (loop []
    (control/check-active! (.-control ids))
    (when @(.-closed ids) (fail! :query-source/invalid :open))
    (let [n (.get (.-next-id ids))]
      (when (neg? n) (fail! :query-source/unsupported :open))
      (if (.compareAndSet (.-next-id ids) n (unchecked-inc n)) n (recur)))))

(defn close-keys!
  "Drop keys, witnesses, response state, and reservations. Closing does not
   replenish IDs and is safe after a partial collection or response failure."
  [^Keys keys]
  (let [state (.-state keys)]
    (locking state
      (when-not (= :closed (:phase @state))
        (when-let [^QueryKey$KeyMap index (:index @state)] (.clear index))
        (doseq [entry (:entries @state)] (.clear ^ArrayList (:outers entry)))
        (doseq [token (:tokens @state)] (control/release! token))
        (reset! state {:phase :closed}))))
  nil)

(defn collect!
  "Create a collector for one chosen logical access. Fully collect outer rows
   before sealing: a key is then evaluated once even with unstable replay.
   Values and outer witnesses added to it are copied and quota-accounted."
  [runtime ^Ids ids requires]
  (when-not (identical? (:control runtime) (.-control ids))
    (fail! :query-source/invalid :open))
  (control/check-active! (:control runtime))
  (when @(.-closed ids) (fail! :query-source/invalid :open))
  (let [token (rt/charge-metadata! runtime requires :open 2048)]
    (try
      (contract/binding-columns! requires #(control/check-active! (:control runtime)))
      (let [requires (value/owned-copy requires #(control/check-active! (:control runtime)))
            keys (Keys. runtime ids
                        (atom {:phase :collecting :requires requires
                               :index (QueryKey$KeyMap.) :entries [] :tokens [token]}))]
        (rt/own! runtime #(.close keys))
        keys)
      (catch Throwable e (control/release! token) (throw e)))))

(defn- keep-token! [state token]
  (swap! state update :tokens conj token)
  token)

(defn add!
  "Associate one outer witness with its ordered required values. Preserve every
   witness, including duplicates; QueryKey equality determines key identity."
  [^Keys keys values outer]
  (let [runtime (.-runtime keys) ctl (:control runtime) state (.-state keys)]
    (locking state
      (control/check-active! ctl)
      (when-not (= :collecting (:phase @state)) (fail! :query-source/invalid :open))
      (try
        (let [requires (:requires @state)]
          (when-not (and (vector? values) (= (count values) (count requires)))
            (fail! :query-source/invalid :open))
          ;; Charge before walking, copying, or hashing keys. The temporary
          ;; owned probe is released when this key was seen on an earlier row.
          (let [key-token (rt/charge-copy! runtime values)
                retained? (volatile! false)
                check! #(control/check-active! ctl)]
            (try
              (let [values (value/owned-copy values check!)]
                (doseq [[column x] (map vector requires values)]
                  (when-not (if (nil? x) (:nullable? column)
                                (value/matches-type? (:type column) x check!))
                    (fail! :query-source/invalid :open)))
                (let [^QueryKey$KeyMap index (:index @state)
                      previous (.get index values)
                      _ (keep-token! state (control/reserve! ctl {:retained-bytes 1024}))
                      _ (keep-token! state (rt/charge-copy! runtime outer))
                      outer (value/owned-copy outer check!)
                      entry (or previous
                                {:binding {:binding-id (next-id! (.-ids keys)) :values values}
                                 :outers (ArrayList.)})]
                  (when-not previous
                    (keep-token! state key-token)
                    (vreset! retained? true)
                    (.put index values entry)
                    (swap! state update :entries conj entry))
                  (.add ^ArrayList (:outers entry) outer)
                  (control/check-active! ctl)))
              (finally (when-not @retained? (control/release! key-token))))))
        nil
        (catch Throwable e (.close keys) (throw e))))))

(defn- constraints! [constraints]
  (when-not (and (value/plain-map? constraints)
                (every? #(contains? constraints %) [:max-binding-rows :max-binding-bytes
                                                    :max-request-binding-rows])
                (every? #(or (#{:max-binding-rows :max-binding-bytes :max-request-binding-rows} %)
                             (qualified-keyword? %)) (clojure.core/keys constraints))
                (every? #(and (integer? %) (<= 1 % Long/MAX_VALUE))
                        (map constraints [:max-binding-rows :max-binding-bytes
                                          :max-request-binding-rows])))
    (fail! :query-source/invalid :open)))

(defn- charge-part! [ctl base maximum size!]
  (let [bytes (volatile! base)
        observe! (fn [n]
                   (control/check-active! ctl)
                   (when (> (vswap! bytes +' n) maximum)
                     (fail! :query-source/limit :open
                            {:category :binding-bytes :maximum maximum :attempted @bytes})))]
    (observe! 0)
    (size! observe!)
    @bytes))

(defn seal!
  "Split complete distinct inputs by both query/provider caps. For :once,
   reject a second batch before any cursor can be opened. Return the batch
   count; an empty collector has zero batches. Provider request splitting is
   separate and does not reduce this cursor-input row cap."
  [^Keys keys constraints opens]
  (let [runtime (.-runtime keys) ctl (:control runtime) state (.-state keys)]
    (locking state
      (control/check-active! ctl)
      (when-not (= :collecting (:phase @state)) (fail! :query-source/invalid :open))
      (try
        (let [metadata (rt/charge-metadata! runtime constraints :open)]
          (try (constraints! constraints) (finally (control/release! metadata))))
        (when-not (#{:once :many} opens) (fail! :query-source/invalid :open))
        (let [{:keys [requires entries]} @state
              limits (control/limits ctl)
              row-cap (min (:binding-rows limits) (:max-binding-rows constraints))
              byte-cap (min (:binding-bytes limits) (:max-binding-bytes constraints))
              scratch (control/reserve! ctl
                        {:retained-bytes (+ 1024 (* 128 (min row-cap (count entries))))})
              finish! (fn [batch batch-entries bytes]
                        (keep-token! state
                          (control/reserve! ctl
                            {:retained-bytes (value/add-charge (+ 1024 (* 256 (count batch))) bytes)}))
                        (swap! state update :batches conj
                               {:bindings batch
                                :entries (into {}
                                               (map (fn [entry]
                                                      [(get-in entry [:binding :binding-id]) entry]))
                                               batch-entries)
                                :phase :pending}))]
          (try
            (swap! state assoc :batches [])
            ;; Generated vectors have no metadata or retained parent. Add the
            ;; public helper's exact header and per-element charges in linear
            ;; work; the collector has already established key/ID uniqueness.
            (when (seq entries)
              (let [header (charge-part! ctl 0 byte-cap
                             #(value/owned-bindings-header-charge requires %))]
                (loop [remaining (seq entries) batch [] pending [] bytes header]
                  (control/check-active! ctl)
                  (if-let [entry (first remaining)]
                    (let [single (charge-part! ctl header byte-cap
                                   #(value/owned-binding-element-charge (:binding entry) %))
                          element (- single header)
                          split? (or (= (count batch) row-cap)
                                     (> (+' bytes element) byte-cap))]
                      (when split?
                        (when (= :once opens) (fail! :query-source/unsupported :open))
                        (finish! batch pending bytes))
                      (recur (next remaining)
                             (conj (if split? [] batch) (:binding entry))
                             (conj (if split? [] pending) entry)
                             (if split? single (+' bytes element))))
                    (when (seq batch) (finish! batch pending bytes))))))
            (swap! state assoc :phase :sealed)
            (count (:batches @state))
            (finally (control/release! scratch))))
        (catch Throwable e (.close keys) (throw e))))))

(defn batch-inputs
  "Borrow one admitted nonempty input vector until the collector closes. The
   cursor adapter must keep the collector alive through cursor close."
  [^Keys keys index]
  (let [state (.-state keys)]
    (locking state
      (control/check-active! (:control (.-runtime keys)))
      (when-not (and (= :sealed (:phase @state)) (integer? index)
                     (<= 0 index) (< index (count (:batches @state))))
        (fail! :query-source/invalid :open))
      (:bindings (nth (:batches @state) index)))))

(defn consume!
  "Associate an already admitted cursor batch and synchronously fan each row
   to every owned outer witness. The cursor adapter validates/reserves the
   batch envelope and keeps its reservation live through this call. emit!
   must separately own/account anything it retains. Successful final batches
   complete all requested keys, including keys with no returned rows."
  [^Keys keys index columns rows binding-ids exhausted? emit!]
  (let [ctl (:control (.-runtime keys)) state (.-state keys)]
    (locking state
      (control/check-active! ctl)
      (when-not (and (= :sealed (:phase @state)) (integer? index)
                     (<= 0 index) (< index (count (:batches @state))))
        (fail! :query-source/invalid :batch))
      (try
        (let [batch (nth (:batches @state) index)
              requires (:requires @state)
              check! #(control/check-active! ctl)]
          (when-not (and (#{:pending :reading} (:phase batch))
                        (vector? columns) (vector? rows) (vector? binding-ids) (boolean? exhausted?)
                        (or (seq rows) exhausted?))
            (fail! :query-source/invalid :batch))
          (when (seq columns) (contract/binding-columns! columns check!))
          (value/dependent-batch-charge-bytes columns rows binding-ids (fn [_] (check!)))
          ;; Validate the whole batch before delivering its first row. Membership
          ;; is per open, so an ID from a different batch/access cannot match.
          (doseq [[row id] (map vector rows binding-ids)]
            (check!)
            (let [entry (get (:entries batch) id)]
              (when-not entry (fail! :query-source/invalid :batch))
              (doseq [[i column] (map-indexed vector columns)
                      [j required] (map-indexed vector requires)
                      :when (= (:variable column) (:variable required))]
                (when-not (and (= (:type column) (:type required))
                               (QueryKey/equals (nth row i) (get-in entry [:binding :values j])))
                  (fail! :query-source/invalid :batch)))))
          (swap! state assoc-in [:batches index :phase] :delivering)
          (doseq [[row id] (map vector rows binding-ids)
                  outer (:outers (get (:entries batch) id))]
            (check!)
            (emit! outer row)
            (check!)
            (when-not (= :sealed (:phase @state)) (fail! :query-source/invalid :batch)))
          (swap! state assoc-in [:batches index :phase] (if exhausted? :complete :reading)))
        nil
        (catch Throwable e (.close keys) (throw e))))))

(defn complete?
  "True only after every admitted input batch was successfully exhausted."
  [^Keys keys]
  (let [state (.-state keys)]
    (locking state
      (control/check-active! (:control (.-runtime keys)))
      (and (= :sealed (:phase @state))
           (every? #(= :complete (:phase %)) (:batches @state))))))
