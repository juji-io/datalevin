(ns ^:no-doc datalevin.query.source.cursor
  "Exhaustive execution of captured v2 access plans. Provider batches are
  borrowed only during delivery; a retaining operator must own its output."
  (:require [datalevin.parser]
            [datalevin.query.access :as access]
            [datalevin.query.source :as source]
            [datalevin.query.source.binding :as binding]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.permissions :as permissions]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]])
  (:import [datalevin.utl QueryKey]
           [java.io Closeable]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private common-keys
  [:spi-version :features :query-id :authorization :consistency :control :limits])

(def ^:private object-array-class (Class/forName "[Ljava.lang.Object;"))

(defn- ensure! [ok phase]
  (when-not ok (fail! :query-source/invalid phase)))

(defn- nonnegative? [x]
  (and (integer? x) (<= 0 x Long/MAX_VALUE)))

(defn- captured!
  "Execution identity comes from the captured request, never caller overrides.
  The caller has already validated all descriptors, alternatives and regions."
  [runtime group plan dependent?]
  (let [options (get-in plan [:path :options])
        {:keys [request alternative]} (:capture options)
        entry (some #(when (= (:symbol %) (:source-symbol options)) %) (:entries group))
        descriptor (:descriptor entry)
        columns #(mapv (fn [c] (select-keys c [:variable :type :nullable?])) %)]
    (control/check-active! (:control runtime))
    (ensure! (and (= :query-source (get-in plan [:path :method]))
                  (identical? (:control runtime) (get-in group [:context :control]))
                  (= (:query-id runtime) (get-in group [:context :query-id]))
                  (= 2 (get-in group [:context :spi-version]))
                  (= (:context group) (select-keys request common-keys))
                  (satisfies? source/IQuerySourceSession (:session group))
                  entry (identical? descriptor (:source-descriptor request))
                  (= (:source-symbol options) (:source-symbol request))
                  (= (:region-id options) (:region-id request))
                  (nonnegative? (:region-id options))
                  (= (:scope-id options) (:scope-id request) (:scope-id alternative)
                     (get-in descriptor [:scope :id]))
                  (= :all-bindings (:completion alternative))
                  (= (:alternative-id options) (:id alternative))
                  (some? (:plan alternative))
                  (= (:requires options) (columns (:requires alternative)))
                  (= (:columns options) (columns (:columns alternative)))
                  (vector? (:requires alternative))
                  (= dependent? (boolean (seq (:requires alternative))))
                  (identical? (:consumption options) (:consumption group))) :open)
    {:alternative alternative
     :request (cond-> (assoc (:context group)
                            :alternative-id (:id alternative) :plan (:plan alternative)
                            :source-symbol (:symbol entry)
                            :definition-fingerprint (:definition-fingerprint descriptor)
                            :scope-id (:scope-id options) :region-id (:region-id options))
                (contains? (:properties alternative) :consumption-group)
                (assoc :consumption-group (get-in alternative [:properties :consumption-group])))}))

(defn- limit! [category actual maximum]
  (when-not (value/at-most? actual (long maximum))
    (fail! :query-source/limit :batch
           {:category category :maximum maximum :attempted actual})))

(defn- deliver-batch!
  [runtime columns dependent? request batch deliver!]
  (let [ctl (:control runtime) tokens (volatile! []) accepted? (volatile! false)
        keep! #(vswap! tokens conj %)]
    (try
      (keep! (control/reserve! ctl {:retained-bytes 512}))
      (when (nil? batch) (fail! :query-source/incomplete :batch))
      (ensure! (value/plain-map? batch) :batch)
      (let [required (cond-> #{:columns :rows :row-count :byte-count :exhausted?}
                       dependent? (conj :binding-ids))]
        (ensure! (and (every? #(contains? batch %) required)
                      (every? #(do (control/check-active! ctl)
                                   (or (required %) (= :reservation %) (qualified-keyword? %)))
                              (keys batch))
                      (vector? (:columns batch)) (= columns (:columns batch)) (vector? (:rows batch))
                      (nonnegative? (:row-count batch)) (nonnegative? (:byte-count batch))
                      (boolean? (:exhausted? batch))
                      (= (:row-count batch) (count (:rows batch)))
                      (or (seq (:rows batch)) (:exhausted? batch))) :batch)
        ;; Reject oversized envelopes before walking any rows or annotations.
        (limit! :batch-rows (:row-count batch) (:max-rows request))
        (limit! :batch-bytes (:byte-count batch) (:max-bytes request))
        (keep! (rt/charge-metadata! runtime (meta batch) :batch))
        ;; The empty input-free charge is zero; its columns can still carry
        ;; metadata/backing, so admit those separately even for an empty final.
        (keep! (rt/charge-metadata! runtime (:columns batch) :batch))
        (doseq [[k x] batch :when (qualified-keyword? k)]
          (keep! (rt/charge-metadata! runtime [k x] :batch)))
        (let [charged (volatile! 0)
              observe! (fn [n]
                         (control/check-active! ctl)
                         (limit! :batch-bytes (vswap! charged value/add-charge n) (:max-bytes request)))
              bytes (if dependent?
                      (value/dependent-batch-charge-bytes (:columns batch) (:rows batch) (:binding-ids batch) observe!)
                      (value/batch-charge-bytes (:columns batch) (:rows batch) observe!))]
          (ensure! (= bytes (:byte-count batch)) :batch)
          (when (or (pos? bytes) (:reservation batch))
            (control/accept! ctl (:reservation batch) bytes)
            (vreset! accepted? true)))
        (deliver! batch)
        (control/check-active! ctl))
      (finally
        (when @accepted? (control/release! (:reservation batch)))
        (doseq [token @tokens] (control/release! token))))))

(defn- finish! [runtime primary close]
  (try (close)
       (catch Throwable e
         (if primary
           (.addSuppressed ^Throwable primary (rt/redacted-error runtime :close e))
           (let [error (rt/redacted-error runtime :close e)]
             (control/cancel! (:control runtime) (select-keys (ex-data error) [:error :phase]))
             (throw error))))))

(defn- read-open!
  [runtime group alternative request dependent? deliver!]
  (let [ctl (:control runtime)
        permission (permissions/reserve-open! (:consumption group) alternative request)
        close-permission (rt/own! runtime #(.close ^Closeable permission))
        close (volatile! close-permission)
        primary (volatile! nil)]
    (try
      (let [cursor (rt/invoke runtime :open #(source/-open-cursor (:session group) request))]
        (ensure! (satisfies? source/IQuerySourceCursor cursor) :open)
        ;; Own the returned resource before checking a cancellation that won
        ;; the handoff race. Keep concurrency reserved through provider close.
        (vreset! close (rt/own! runtime
                       #(try (source/-close-cursor! cursor) (finally (close-permission)))))
        (control/check-active! ctl)
        (let [limits (control/limits ctl)
              fetch {:control ctl :max-rows (:batch-rows limits) :max-bytes (:batch-bytes limits)}]
          (loop []
            (let [batch (rt/invoke runtime :fetch #(source/-next-batch! cursor fetch))]
              (deliver-batch! runtime (:columns alternative) dependent? fetch batch deliver!)
              (when-not (:exhausted? batch) (recur))))))
      (catch Throwable e
        (let [error (rt/redacted-error runtime (or (:phase (ex-data e)) :execution) e)]
          (vreset! primary error)
          (control/cancel! ctl (select-keys (ex-data error) [:error :phase]))
          (throw error)))
      (finally (finish! runtime @primary @close)))))

(defn- emit-all! [runtime emit! & args]
  (control/check-active! (:control runtime))
  (when (reduced? (apply emit! args)) (fail! :query-source/incomplete :execution))
  (control/check-active! (:control runtime)))

(defn scan!
  "Exhaust one input-free access. emit! borrows one row until it returns.
  This path constructs no binding IDs, input vectors, or association indexes."
  [runtime group plan emit!]
  (let [{:keys [alternative request]} (captured! runtime group plan false)]
    (rt/with-context runtime (select-keys request [:source-symbol :region-id :alternative-id])
      (fn [runtime]
        (read-open! runtime group alternative request false
          (fn [batch]
            (doseq [row (:rows batch)] (emit-all! runtime emit! row))))))))

(defn lookup!
  "Collect all outer bindings before any lookup, then exhaust each legal
  distinct-key batch once. ids is the invocation's shared lazy ID space.
  emit! borrows [outer row]; retaining operators must copy/account separately."
  [runtime group plan ids outer-bindings emit!]
  (let [{:keys [alternative request]} (captured! runtime group plan true)]
    (rt/with-context runtime (select-keys request [:source-symbol :region-id :alternative-id])
      (fn [runtime]
        (with-open [keys (binding/collect! runtime @ids (:requires alternative))]
          (doseq [outer outer-bindings]
            (control/check-active! (:control runtime))
            (ensure! (and (value/plain-map? outer)
                          (every? #(contains? outer (:variable %)) (:requires alternative))) :open)
            (binding/add! keys (mapv #(get outer (:variable %)) (:requires alternative)) outer))
          (let [opens (:opens (permissions/planning-permission (:consumption group) alternative))
                n (binding/seal! keys (:constraints alternative) opens)]
            (dotimes [i n]
              (read-open! runtime group alternative (assoc request :bindings (binding/batch-inputs keys i)) true
                (fn [batch]
                  (binding/consume! keys i (:columns alternative) (:rows batch) (:binding-ids batch)
                                    (:exhausted? batch) #(emit-all! runtime emit! %1 %2)))))
            (when-not (binding/complete? keys) (fail! :query-source/incomplete :execution))))))))

(defn- tuple-bindings! [runtime columns tuple]
  (let [array? (instance? object-array-class tuple)
        n (if array? (alength ^objects tuple) (when (vector? tuple) (count tuple)))]
    (ensure! (= (count columns) n) :execution)
    ;; Reserve before allocating the map, then detach mutable leaves before an
    ;; upstream iterator can advance/reuse its tuple or value buffers.
    (let [token (control/reserve! (:control runtime) {:retained-bytes (+ 256 (* 64 n))})]
      (try
        (let [check! #(control/check-active! (:control runtime))
              bindings (persistent!
                         (reduce-kv (fn [m i column]
                                      (check!)
                                      (assoc! m column (if array? (aget ^objects tuple (long i)) (nth tuple i))))
                                    (transient {}) columns))
              owned-token (rt/charge-copy! runtime bindings)]
          (try {:token owned-token :bindings (value/owned-copy bindings check!)}
               (catch Throwable e (control/release! owned-token) (throw e))))
        (finally (control/release! token))))))

(defn- execute-shared!
  [runtime group path demand bounds work ids input-cols output-cols tuples]
  (let [ctl (:control runtime) plan {:path path}
        dependent? (boolean (seq (get-in path [:options :requires])))
        columns (get-in path [:options :columns])
        tokens (volatile! []) success? (volatile! false)
        own! #(do (vswap! tokens conj %) %)
        check! #(control/check-active! ctl)]
    (try
      ;; This adapter implements complete regions only. Final windows belong
      ;; downstream, and cannot authorize a prefix of a dependent access.
      (ensure! (and (= :exact (:quality demand)) (empty? (:ordering demand))
                    (zero? (or (:offset bounds) 0)) (nil? (:limit bounds))
                    (nil? (:required-count bounds)) (nil? (:max-candidates work))) :execution)
      (captured! runtime group plan dependent?)
      (own! (control/reserve! ctl {:retained-bytes 2048}))
      (let [output (FastList.)
            input-tokens (volatile! [])
            ;; Realize maps once under admission, including a streaming/reused
            ;; input tuple. Collector deduplication never reads a mutable outer
            ;; tuple again. Input-free cross products also keep owned witnesses.
            outers (when (seq input-cols)
                     (eduction (map (fn [tuple]
                                      (let [{:keys [bindings token]} (tuple-bindings! runtime input-cols tuple)]
                                        (vswap! input-tokens conj token)
                                        bindings))) tuples))
            save! (fn [outer row]
                    (check!)
                    (let [scratch (control/reserve! ctl
                                    {:retained-bytes (+ 256 (* 96 (+ (count input-cols) (count columns))))})]
                      (try
                        (let [returned (zipmap (map :variable columns) row)]
                          (when (every? (fn [[k x]]
                                          (check!)
                                          (or (not (contains? outer k)) (QueryKey/equals (get outer k) x)))
                                        returned)
                            (let [values (mapv (merge outer returned) output-cols)
                                  token (rt/charge-copy! runtime values)]
                              (own! token)
                              ;; Includes the object array, list slot and the
                              ;; transient owned vector in the conservative copy.
                              (.add output (object-array (value/owned-copy values check!))))))
                        (finally (control/release! scratch)))))]
        (try
          (if dependent?
            (lookup! runtime group plan ids outers save!)
            (if (seq input-cols)
              (let [owned (into [] outers)]
                (when (seq owned)
                  (scan! runtime group plan
                         (fn [row] (doseq [outer owned] (save! outer row))))))
              (scan! runtime group plan #(save! {} %))))
          (check!)
          (let [held @tokens]
            ;; Shared operators may retain these tuples after cursor/session
            ;; close. Their independent reservations last through invocation.
            (rt/own! runtime #(doseq [token held] (control/release! token))))
          (vreset! success? true)
          output
          (finally (doseq [token @input-tokens] (control/release! token)))))
      (finally (when-not @success? (doseq [token @tokens] (control/release! token)))))))

(defn method
  "Shared AccessStep implementation for already acquired/captured v2 paths.
  Pass the SAME delay of binding/id-space! to all methods in one invocation.
  No source detection, planning callback, or ID space is created by this factory."
  [runtime group ids]
  (reify
    access/IAccessMethod
    (-access-plans [_ _] (fail! :query-source/unsupported :planning))
    (-open-access [_ _ _ _ _ _] (fail! :query-source/unsupported :open))
    (-frontier-satisfies? [_ _ _ _ _] false)
    access/IBatchedAccessMethod
    (-execute-batched-access [_ path demand bounds work _ input-cols output-cols tuples]
      (execute-shared! runtime group path demand bounds work ids input-cols output-cols tuples))))
