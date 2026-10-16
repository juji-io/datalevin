(ns datalevin.query.source.fake
  "Deterministic, deliberately eager F0 fake. Never a production adapter."
  (:require [datalevin.datom :as datom]
            [datalevin.query.source :as source]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.value :as value])
  (:import [java.util Objects]))

(defn- open! [state]
  (when (:closed? @state)
    (value/fail! :query-source/invalid :execution {:state :closed})))

(defn- fault! [faults phase]
  (when-let [f (get faults phase)] (f)))

(defn- match-row [bindings terms row]
  (reduce (fn [bindings [term value]]
            (when bindings
              (case (first term)
                :ignore bindings
                :const (when (= (second term) value) bindings)
                :var (let [variable (second term)]
                       (if (contains? bindings variable)
                         (when (= (get bindings variable) value) bindings)
                         (assoc bindings variable value))))))
          bindings (map vector terms row)))

(defn evaluate
  "Small eager oracle for positive patterns and scalar predicate envelopes."
  [descriptor rows clauses bindings]
  (let [rows (if (= :eav (get-in descriptor [:pattern-contract :kind]))
               (filterv #(and (some? (nth % 0)) (some? (nth % 2))) rows) rows)
        patterns (filter #(= :pattern (:kind %)) clauses)
        predicates (filter #(= :predicate (:kind %)) clauses)
        matches (reduce (fn [matches pattern]
                          (vec (for [binding matches, row rows
                                     :let [match (match-row bindings (:terms pattern) row)]
                                     :when (and match
                                                (every? (fn [[k v]]
                                                          (or (not (contains? binding k))
                                                              (Objects/equals (get binding k) v))) match))]
                                 (merge binding match)))) [bindings] patterns)]
    (filterv (fn [binding]
               (every? (fn [{:keys [operator args]}]
                         (let [[a b] (map #(if (= :const (first %)) (second %)
                                              (get binding (second %))) args)]
                           (case operator
                             := (= a b)
                             :not= (not= a b)
                             :< (neg? (long (datom/compare-with-type a b)))
                             :<= (not (pos? (long (datom/compare-with-type a b))))
                             :> (pos? (long (datom/compare-with-type a b)))
                             :>= (not (neg? (long (datom/compare-with-type a b))))))) predicates)) matches)))

(defrecord Cursor [columns rows position state metrics request events faults]
  source/IQuerySourceCursor
  (-next-batch! [_ {:keys [control max-rows max-bytes]}]
    (open! state)
    (source/check-active! control)
    (fault! faults :fetch)
    (swap! events conj :fetch)
    (let [rows @rows
          start (long @position)
          batch (loop [index start, batch []]
                  (if (or (= index (count rows)) (= max-rows (count batch))) batch
                      (let [candidate (conj batch (nth rows index))
                            charge (source/batch-charge-bytes columns candidate)]
                        (if-not (value/at-most? charge (long max-bytes))
                          (if (seq batch) batch
                              (value/fail! :query-source/limit :batch
                                           {:category :batch-bytes :maximum max-bytes
                                            :attempted charge}))
                          (recur (inc index) candidate)))))
          bytes (source/batch-charge-bytes columns batch)
          reservation (when (seq batch)
                        (source/reserve! control {:retained-bytes bytes :in-flight-batches 1}))]
      ;; The fake owns its immutable snapshot; copy mutable leaves for each handoff.
      (try
        (let [owned (value/owned-copy batch)]
          (swap! position + (count batch))
          (swap! metrics #(-> % (update :batches inc) (update :rows + (count batch))
                              (update :charged-bytes + bytes)))
          {:columns columns :rows owned :row-count (count owned) :byte-count bytes
           :reservation reservation :exhausted? (= @position (count rows))})
        (catch Throwable e
          (when reservation (source/release! reservation))
          (throw e)))))
  (-cursor-metrics [_] @metrics)
  (-close-cursor! [_]
    (when-not (:closed? @state)
      (swap! state assoc :closed? true)
      (reset! rows [])
      (swap! events conj :close-cursor)
      (fault! faults :close-cursor))
    nil))

(defrecord Session [info descriptor snapshot request state cursors plans events faults]
  source/IQuerySourceSession
  (-session-info [_] info)
  (-open-cursor [_ cursor-request]
    (open! state)
    (source/check-active! (:control request))
    (when-not (= (:definition-fingerprint descriptor) (:definition-fingerprint cursor-request))
      (value/fail! :query-source/stale-definition :open))
    (let [plan (get @plans (:plan cursor-request))]
      (when-not (and plan (= (:alternative-id cursor-request) (:id plan))
                     (= (:source-symbol cursor-request) (:source-symbol plan)))
        (value/fail! :query-source/invalid :open))
      (fault! faults :open-cursor)
      (swap! events conj :open-cursor)
      (let [matches (evaluate descriptor @snapshot (:clauses plan) (:bindings plan))
            rows (mapv #(mapv % (map :variable (:columns plan))) matches)
            cursor (->Cursor (:columns plan) (atom rows) (atom 0) (atom {})
                             (atom {:batches 0 :rows 0 :charged-bytes 0
                                    :read-rows nil :read-bytes nil :external-requests 0
                                    :elapsed-ns 0})
                             request events faults)]
        (swap! cursors conj cursor)
        cursor)))
  (-cancel-session! [_ _]
    (when-not (:cancelled? @state)
      (swap! state assoc :cancelled? true)
      (swap! events conj :cancel-session))
    nil)
  (-close-session! [_]
    (when-not (:closed? @state)
      (swap! state assoc :closed? true)
      (let [error (atom nil)]
        (doseq [cursor (reverse @cursors)]
          (try (source/-close-cursor! cursor)
               (catch Throwable e
                 (if-let [primary @error] (.addSuppressed ^Throwable primary e)
                         (reset! error e)))))
        (reset! cursors [])
        (reset! plans {})
        (reset! snapshot [])
        (swap! events conj :close-session)
        (fault! faults :close-session)
        (when @error (throw @error))))
    nil))

(defrecord FakeSource [descriptor rows events faults]
  source/IQuerySource
  (-source-info [_] descriptor)
  (-open-session [_ request]
    (source/check-active! (:control request))
    (when-not (= :snapshot (:consistency request))
      (value/fail! :query-source/unsupported :acquisition))
    (fault! faults :acquisition)
    (swap! events conj :open-session)
    (->Session (merge (select-keys descriptor [:source-instance-id :compute-context-id])
                     (select-keys request [:spi-version :features :consistency])
                     {:session-id :fake-session :authorization-token :fake-policy
                      :acquired-at-ms 0 :data-version nil})
               descriptor (atom (value/owned-copy @rows)) request (atom {}) (atom []) (atom {})
               events faults))
  (-plan-source [_ session request]
    (open! (:state session))
    (source/check-active! (:control request))
    (contract/planning-request! request)
    (fault! faults :planning)
    (swap! events conj :plan)
    (let [columns (->> (:clauses request)
                       (filter #(= :pattern (:kind %)))
                       (mapcat (fn [clause]
                                 (keep (fn [[term column]]
                                         (when (= :var (first term))
                                           (assoc column :variable (second term))))
                                       (map vector (:terms clause)
                                            (contract/pattern-columns descriptor clause)))))
                       (reduce (fn [columns column]
                                 (let [v (:variable column)]
                                   (assoc columns v
                                          (update column :nullable?
                                                  #(and % (get-in columns [v :nullable?] true)))))) {})
                       vals vec)
          handle (Object.)
          plan {:id :scan :source-symbol (:source-symbol request) :columns columns
                :clauses (:clauses request) :bindings (:bindings request)}]
      (swap! (:plans session) assoc handle plan)
      {:alternatives [{:id :scan :plan handle
                       :covers (set (map :id (:clauses request))) :prefilters #{}
                       :requires [] :columns columns
                       :properties {:replay :reopen :ordering [] :blocking-state :none}
                       :estimate {:rows nil :row-bytes nil :startup-ms nil :per-row-ns nil
                                  :requests nil :confidence :unknown}
                       :explain {:label "F0 fake"}}]})))

(defn source
  ([descriptor rows] (source descriptor rows {}))
  ([descriptor rows faults]
   (->FakeSource (source/validate-source-info! descriptor) (atom rows) (atom []) faults)))
