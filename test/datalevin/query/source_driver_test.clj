(ns datalevin.query.source-driver-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.driver :as driver]
            [datalevin.query.source.program :as program]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value]
            [datalevin.query.source-planning-test :as fixture]
            [datalevin.timeout :as timeout])
  (:import [datalevin.utl QueryKey QueryKey$Budget QueryKey$Workspace]
           [java.util Iterator]
           [java.util.concurrent CountDownLatch TimeUnit]))

(def ^:private pairs '{:find [?id ?name] :in [$s] :where [[$s ?id :row/value ?name]]})
(def ^:private secret "private query or provider detail")
(defn- caught [f] (try (f) nil (catch clojure.lang.ExceptionInfo e e)))
(defn- error [f] (ex-data (caught f)))
(defn- events-of [events kind] (filterv #(= kind (first %)) @events))

(defn- observed [events observations config]
  (let [delegate (fixture/provider events config)
        record! (fn [phase]
                  (let [runtime rt/*runtime*
                        entry {:phase phase :control (:control runtime) :limits (control/limits (:control runtime))
                               :deadline timeout/*deadline*}]
                    (swap! observations conj entry)
                    (when-let [hook (:observe config)] (hook phase entry))))]
    (reify source/IQuerySource
      (-source-info [_] (record! :descriptor) (source/-source-info delegate))
      (-open-session [_ request]
        (record! :acquire)
        (let [session (source/-open-session delegate request)]
          (reify source/IQuerySourceSession
            (-session-info [_] (source/-session-info session))
            (-open-cursor [_ request] (record! :open) (source/-open-cursor session request))
            (-cancel-session! [_ reason] (source/-cancel-session! session reason))
            (-close-session! [_] (record! :close) (source/-close-session! session)))))
      (-plan-source [_ session request] (record! :plan) (source/-plan-source delegate session request)))))

(deftest query-and-explain-use-the-same-cache-free-v2-invocation
  (let [events (atom []) p (fixture/provider events {})]
    (is (= #{[1 "one"] [2 "two"]} (driver/run pairs [p] nil)))
    (let [plan (driver/run pairs [p] {})]
      (is (= 2 (:spi-version plan)))
      (is (false? (:executed? plan)))
      (is (= [] (:cursors plan))))
    (let [executed (driver/run pairs [p] {:run? true :intermediate-counts? false})]
      (is (= 2 (:result-rows executed)))
      (is (= 2 (get-in executed [:cursors 0 :metrics :rows]))))
    (is (= 3 (count (events-of events :acquire))))
    (is (= 2 (count (events-of events :open))))
    (is (= 3 (count (events-of events :session-close))))
    (let [changed (fixture/provider (atom []) {:facts [[3 :row/value "fresh"]]})]
      (is (= #{[3 "fresh"]} (driver/run pairs [changed] nil))))))

(deftest embedded-map-and-vector-contexts-limit-complete-binding-batches
  (doseq [query
          ['{:find [?name] :in [$s [?id ...]] :where [[$s ?id :row/value ?name]]
             :query-source {:limits {:binding-rows 1 :binding-bytes 4096}}}
           '[:find ?name :in $s [?id ...] :query-source {:limits {:binding-rows 1 :binding-bytes 4096}}
             :where [$s ?id :row/value ?name]]]]
    (let [events (atom []) p (fixture/provider events {:lookup-only? true})]
      (is (= #{["one"] ["two"]} (driver/run query [p [1 2 2 3]] nil)))
      (is (= 3 (count (events-of events :open))))
      (is (every? #(= 1 (count (:bindings (second %)))) (events-of events :open)))
      (is (every? #(= 4096 (get-in (second %) [:limits :binding-bytes])) (events-of events :open))))))

(deftest context-validation-precedes-input-consumption-and-provider-callbacks
  (doseq [options [{:sources {'$unknown {:consistency :snapshot}}}
                   {:sources {'$s {:extra true :consistency :snapshot}}}
                   {:planning {:max-alternatives 0}} {:limits {:binding-rows 0}}
                   {:unknown true} {:authorization (java.util.Date.)} []]]
    (let [events (atom []) reads (atom 0)
          arguments (reify Iterable
                      (iterator [_]
                        (reify Iterator
                          (hasNext [_] true)
                          (next [_] (swap! reads inc) (fixture/provider events {})))))
          failure (error #(driver/run (assoc pairs :query-source options) arguments nil))]
      (is (= :query-source/invalid (:error failure)))
      (is (= :context (:phase failure)))
      (is (zero? @reads))
      (is (empty? @events)))))

(deftest tiny-embedded-caps-reject-before-copying-other-query-values
  (let [events (atom []) visits (atom 0)
        huge (reify value/IQuerySourceValue
               (-logical-type [_] (swap! visits inc) {:kind :custom :name :test/large :version 1 :fingerprint "v1"})
               (-retained-bytes [_] (swap! visits inc) Long/MAX_VALUE))
        query {:find ['?id] :in ['$s] :where [['$s '?id :row/value huge]]
               :query-source {:limits {:retained-bytes 0}}}
        failure (error #(driver/run query [(fixture/provider events {})] nil))]
    (is (= :query-source/limit (:error failure)))
    (is (= :retained-bytes (:category failure)))
    (is (zero? (:maximum failure)))
    (is (zero? @visits))
    (is (empty? @events))))

(deftest explicit-internal-options-override-embedded-context-without-extending-parent-caps
  (let [events (atom []) query (assoc pairs :query-source {:limits {:result-rows 0}})]
    (rt/with-runtime {:limits {:binding-rows 2}}
      (fn [runtime]
        (is (= #{[1 "one"] [2 "two"]}
               (program/query! runtime query [(fixture/provider events {})]
                 {:limits {:result-rows 2 :binding-rows 100}})))
        (is (every? zero? (vals (control/usage (:control runtime)))))))
    (is (= 2 (get-in (second (first (events-of events :acquire))) [:limits :binding-rows])))))

(deftest nested-invocations-and-cleanup-inherit-the-effective-control
  (let [events (atom []) observations (atom []) inner-events (atom [])
        query (assoc pairs :timeout 10000 :query-source {:limits {:binding-rows 1}})
        p (observed events observations
            {:observe (fn [phase _]
                        (when (#{:plan :close} phase)
                          (is (= #{[1 "one"] [2 "two"]}
                                 (driver/run (assoc pairs :timeout 20000 :query-source {:limits {:binding-rows 100}})
                                   [(fixture/provider inner-events {})] nil)))))})]
    (is (= #{[1 "one"] [2 "two"]} (driver/run query [p] nil)))
    (is (= #{:descriptor :acquire :plan :open :close} (set (map :phase @observations))))
    (is (every? #(= 1 (get-in % [:limits :binding-rows])) @observations))
    (is (= 1 (count (set (map :control @observations)))))
    (is (every? #(some? (:deadline %)) @observations))
    (is (every? #(<= (:deadline %) (:deadline (first @observations))) @observations))
    (is (= 2 (count (events-of inner-events :acquire))))
    (is (every? #(= 1 (get-in (second %) [:limits :binding-rows])) (events-of inner-events :acquire)))
    (is (every? #(every? zero? (vals (control/usage %))) (map :control @observations)))))

(deftest query-key-workspaces-use-and-restore-the-child-budget
  (let [events (atom []) observations (atom []) before (atom nil)
        p (observed events observations
            {:observe (fn [phase {:keys [control]}]
                        (when (= phase :plan)
                          (let [other (QueryKey/setBudget nil)]
                            (try
                              (is (= :query-source/limit
                                     (:error (error #(let [workspace (.reserve ^QueryKey$Budget other 2000001)]
                                                      (.close ^QueryKey$Workspace workspace))))))
                              (finally (QueryKey/setBudget other))))
                          (reset! before (control/usage control))))})
        sentinel (reify QueryKey$Budget
                   (check [_])
                   (reserve [_ _] (reify QueryKey$Workspace (close [_]))))
        previous (QueryKey/setBudget sentinel)]
    (try
      (is (= #{[1 "one"] [2 "two"]}
             (driver/run (assoc pairs :query-source {:limits {:retained-bytes 2000000}}) [p] nil)))
      (let [restored (QueryKey/setBudget sentinel)] (is (identical? sentinel restored)))
      (is (some? @before))
      (is (= :query-source/invalid (:error (error #(driver/run [:find] [] nil)))))
      (let [restored (QueryKey/setBudget sentinel)] (is (identical? sentinel restored)))
      (finally (QueryKey/setBudget previous)))))

(deftest a-native-query-in-a-provider-callback-inherits-the-bound-deadline
  (let [events (atom []) observations (atom []) seen (atom nil)
        conn (d/create-conn nil {} {:kv-opts {:inmemory? true} :background-sampling? false})
        native-query '{:find [?value .] :in [$] :where [[?e :value ?value]] :timeout 20000}
        p (observed events observations
            {:observe (fn [phase {:keys [deadline]}]
                        (when (= phase :plan)
                          (reset! seen deadline)
                          (is (= 1 (d/q native-query (d/db conn))))))})
        outer (+ (System/currentTimeMillis) 10000)]
    (try
      (d/transact! conn [{:value 1}])
      (binding [timeout/*deadline* outer]
        (is (= #{[1 "one"] [2 "two"]} (driver/run (assoc pairs :timeout 20000) [p] nil))))
      (is (some? @seen))
      (is (<= @seen outer))
      (is (nil? timeout/*deadline*))
      (finally (d/close conn)))))

(deftest timeout-and-cancellation-precede-a-new-input-producer-error
  (doseq [cancel? [false true]]
    (let [events (atom []) captured (atom nil)
          arguments (reify Iterable
                      (iterator [_]
                        (let [i (atom 0)]
                          (reify Iterator
                            (hasNext [_] (< @i 2))
                            (next [_]
                              (if (= 1 (swap! i inc)) (fixture/provider events {})
                                  (let [ctl (:control rt/*runtime*) ready (CountDownLatch. 1)
                                        registration (source/register-cancel! ctl (fn [_] (.countDown ready)))]
                                    (reset! captured ctl)
                                    (try
                                      (when cancel? (control/cancel! ctl {:error :query-source/cancelled :phase :inputs}))
                                      (is (.await ready 2 TimeUnit/SECONDS))
                                      (throw (Exception. secret))
                                      (finally (source/unregister-cancel! registration))))))))))
          query '{:find [?name] :in [$s ?id] :where [[$s ?id :row/value ?name]]}
          failure (caught #(driver/run (cond-> query (not cancel?) (assoc :timeout 100)) arguments nil))]
      (is (= (if cancel? :query-source/cancelled :query/timeout) (:error (ex-data failure))))
      (is (not (.contains (pr-str failure) secret)))
      (is (empty? @events))
      (is (every? zero? (vals (control/usage @captured)))))))

(deftest unbounded-and-malformed-query-headers-stop-without-input-or-provider-work
  (let [reads (atom 0) captured (atom nil)
        query (map (fn [x] (swap! reads inc) x) (cycle [:find '?value]))
        failure (error #(rt/with-runtime {:limits {:retained-bytes 8192}}
                          (fn [runtime]
                            (reset! captured (:control runtime))
                            (driver/run query [] nil))))]
    (is (= :query-source/limit (:error failure)))
    (is (< @reads 100))
    (is (every? zero? (vals (control/usage @captured)))))
  (let [events (atom [])
        failure (error #(driver/run '[:find ?id :in $s :where [$s ?id :row/value _]
                                     :query-source {} {}] [(fixture/provider events {})] nil))]
    (is (= :query-source/invalid (:error failure)))
    (is (= :context (:phase failure)))
    (is (empty? @events))))

(deftest maximum-query-timeout-does-not-overflow-a-wall-clock-deadline
  (is (= #{[1]} (driver/run (assoc '{:find [?value] :in [?value]} :timeout Long/MAX_VALUE) [1] nil))))
