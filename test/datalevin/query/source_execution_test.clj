(ns datalevin.query.source-execution-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.fake :as fake]
            [datalevin.query.source.value :as value]
            [datalevin.query.source-test :refer [tuple-info category error-data]]
            [datalevin.util :as u])
  (:import [datalevin.query.source.control Control]
           [java.util.concurrent CountDownLatch ScheduledFuture TimeUnit]))

(def pairs-query '[:find ?id ?name :in $s :where [$s ?id ?name]])

(deftest query-entry-and-fresh-invocations
  (let [rows [[1 "Ada"] [2 nil] [3 "Ada"] [3 "Ada"]]
        s (fake/source tuple-info rows)]
    (is (= (set rows) (d/q pairs-query s)))
    (is (= [1 2 3] (sort (d/q '[:find [?id ...] :in $s :where [$s ?id _]] s))))
    (is (= 3 (d/q '[:find (count ?id) . :in $s :where [$s ?id _]] s)))
    (is (= 2 (d/q '[:find (count ?name) . :in $s :where [$s _ ?name]] s)))
    (is (= 3 (d/q '[:find (count ?name) . :with ?id :in $s :where [$s ?id ?name]] s)))
    (is (= #{[2 nil]} (d/q '[:find ?id ?name :in $s ?id :where [$s ?id ?name]] s 2)))
    (reset! (:rows s) [[4 "fresh"]])
    (is (= #{[4 "fresh"]} (d/q pairs-query s)))
    (is (= 7 (count (filter #{:open-session} @(:events s)))))))

(deftest aliases-share-a-view
  (let [s (fake/source tuple-info [[1 "a"] [2 "b"]])
        query '[:find ?id ?name :in $a $b :where [$a ?id ?name] [$b ?id ?name]]]
    (is (= #{[1 "a"] [2 "b"]} (d/q query s s)))
    (is (= 1 (count (filter #{:open-session} @(:events s)))))
    (is (= 2 (count (filter #{:plan} @(:events s)))))
    (is (= [:close-cursor :close-session] (take-last 2 @(:events s))))))

(deftest flat-input-bindings-match-eager-relations
  (let [rows [[1 "a"] [2 "b"] [3 nil]]
        s (fake/source tuple-info rows)]
    (doseq [[query args]
            [['[:find ?id ?name :in $s [?id ...] :where [$s ?id ?name]] [[1 3 3]]]
             ['[:find ?id :in $s [?id ?name] :where [$s ?id ?name]] [[2 "b"]]]
             ['[:find ?id ?name :in $s [[?id ?name]] :where [$s ?id ?name]]
              [[[1 "a"] [3 nil]]]]
             ['[:find ?id ?label :in $s [[?id ?label]] :where [$s ?id _]]
              [[[1 "first"] [1 "second"]]]]]]
      (is (= (apply d/q query rows args) (apply d/q query s args))))
    (reset! (:events s) [])
    (is (= #{} (d/q '[:find ?id :in $s [?id ...] :where [$s ?id _]] s [])))
    (is (not-any? #{:open-cursor :fetch} @(:events s)))))

(deftest malformed-input-rows-reject-before-source-acquisition
  (doseq [[binding input category]
          [['[?id ?name] [1] :query-source/invalid]
           ['[[?id ?name]] [[1 "a"] [2]] :query-source/invalid]
           ['[?id ...] [1 "wrong-type"] :query-source/invalid]
           ['[?id ...] "not-a-collection" :query-source/invalid]
           ['[[?id ...] ...] [[1 2]] :query-source/unsupported]]]
    (let [s (fake/source tuple-info [[1 "a"]])
          query {:find ['?id] :in ['$s binding] :where [['$s '?id '_]]}]
      (is (= category (:error (error-data #(d/q query s input)))))
      (is (not-any? #{:open-session :plan :open-cursor} @(:events s))))))

(deftest fused-fake-matches-native-key-and-predicate-semantics
  (let [info (assoc-in tuple-info [:pattern-contract :columns 1] {:type :bigdec :nullable? false})
        rows [[1 1.0M] [2 1.00M] [3 2M]]
        s (fake/source info rows)
        query '[:find ?a ?b :in $s :where [$s ?a ?n] [$s ?b ?n] [(= ?n 1M)]]]
    (is (= (d/q query rows) (d/q query s))))
  (let [rows [[1 "a"] [2 "b"]]
        s (fake/source tuple-info rows)
        query '[:find ?id :in $s :where [$s ?id ?name] [(> ?name "a")]]]
    (is (= (d/q query rows) (d/q query s)))))

(deftest scalar-and-tuple-inputs-preserve-decimal-key-equality
  (let [rows [[1 1.0M] [2 1.00M]]
        info (assoc-in tuple-info [:pattern-contract :columns 1 :type] :bigdec)
        s (fake/source info rows)]
    (doseq [[binding input] [['?value 1.0M] ['[?value] [1.0M]]]]
      (let [q {:find '[?id ?value] :in ['$s binding] :where '[[$s ?id ?value]]}]
        (is (= #{[1 1.0M]} (d/q q s input)))
        (is (= (d/q q rows input) (d/q q s input)))))))

(deftest decimal-projection-and-aggregation-match-eager-results
  (let [rows [[1 1.0M] [2 1.00M] [3 1.0M]]
        s (fake/source (assoc-in tuple-info [:pattern-contract :columns 1 :type] :bigdec) rows)]
    (doseq [input [rows s]]
      (is (= ["1.0" "1.00"]
             (sort (map str (d/q '[:find [?v ...] :in $s :where [$s _ ?v]] input)))))
      (is (= [2 2.00M 1.0]
             (d/q '[:find [(count ?v) (sum ?v) (avg ?v)]
                    :in $s :where [$s _ ?v]] input))))))

(deftest unsupported-shapes-reject-before-acquisition
  (doseq [query ['[:find ?id :in $s :where [$s ?id _] (not [$s ?id "a"])]
                 '[:find ?x :in $s :where [$s ?id _] [(inc ?id) ?x]]
                 '{:find [?id] :in [$s] :where [[$s ?id _]] :order-by [?id :asc]}]]
    (let [s (fake/source tuple-info [[1 "a"]])]
      (is (= :query-source/unsupported (category #(d/q query s))))
      (is (empty? @(:events s)))))
  (let [s (fake/source tuple-info [[1 "a"]])]
    (is (= :query-source/invalid
           (category #(d/q '[:find ?n :in $s :where [$s "bad" ?n]] s))))
    (is (empty? @(:events s)))))

(deftest native-natural-key-join
  (let [dir (u/tmp-dir (str "source-native-" (random-uuid)))
        conn (d/get-conn dir {:person/id {:db/valueType :db.type/long}
                             :person/name {:db/valueType :db.type/string}})
        rows [[1 "external"] [2 "other"]]
        s (fake/source tuple-info rows)
        query '[:find ?name ?external :in $ $s
                :where [?e :person/id ?id] [?e :person/name ?name] [$s ?id ?external]]]
    (try
      (d/transact! conn [{:person/id 1 :person/name "native"}])
      (is (= (d/q query @conn rows) (d/q query @conn s)))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest native-value-wildcards-do-not-decode-byte-payloads
  (let [dir (u/tmp-dir (str "source-byte-existence-" (random-uuid)))
        conn (d/get-conn dir {:item/blob {:db/valueType :db.type/bytes}})
        rows [[1 "one"] [2 "two"] [3 "absent"]]
        s (fake/source tuple-info rows)
        query '{:find [?label] :in [$ $s]
                :where [[?e :item/blob _] [$s ?e ?label]]
                :query-source {:limits {:retained-bytes 32768}}}]
    (try
      (d/transact! conn [{:db/id 1 :item/blob (byte-array 131072)}
                         {:db/id 2 :item/blob (byte-array [1 2])}])
      (is (= #{["one"] ["two"]} (d/q query @conn s)))
      (is (= (d/q (dissoc query :query-source) @conn rows) (d/q query @conn s)))
      (reset! (:events s) [])
      (is (= :query-source/unsupported
             (category #(d/q (assoc query :where '[[?e :item/blob ?blob] [$s ?e ?label]]) @conn s))))
      (is (empty? @(:events s)))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest bound-native-decimal-joins-match-source-key-equality
  (let [dir (u/tmp-dir (str "source-decimal-join-" (random-uuid)))
        conn (d/get-conn dir {:item/amount {:db/valueType :db.type/bigdec}})
        rows [[1 1.0M] [1 1.00M] [2 2M]]
        s (fake/source (assoc-in tuple-info [:pattern-contract :columns 1 :type] :bigdec) rows)
        query '[:find ?tag :in $ $s [[?id ?amount ?tag]]
                :where [?id :item/amount ?amount] [$s ?id ?amount]]
        input [[1 1.0M :exact] [1 1.00M :wrong-scale] [2 2M :other]]]
    (try
      (d/transact! conn [{:db/id 1 :item/amount 1.0M} {:db/id 2 :item/amount 2M}])
      (is (= #{[:exact] [:other]} (d/q query @conn rows input) (d/q query @conn s input)))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest semantic-limit-and-overflow
  (let [s (fake/source tuple-info [[1 "a"] [2 "b"]])]
    (is (= 1 (d/q '{:find [?id .] :in [$s] :where [[$s ?id _]]
                     :query-source {:limits {:result-rows 1 :batch-rows 1}}} s)))
    (is (= 1 (count (filter #{:fetch} @(:events s))))))
  (let [s (fake/source tuple-info (mapv #(vector % (str %)) (range 20)))
        query '{:find [?id] :in [$s] :where [[$s ?id _] [(> ?id 5)]]
                :limit 2 :query-source {:limits {:batch-rows 1}}}]
    (is (= #{[6] [7]} (d/q query s)))
    (is (= 2 (count (filter #{:fetch} @(:events s)))))
    (is (= :close-session (last @(:events s))))
    (reset! (:events s) [])
    (is (= :query-source/limit
           (category #(d/q (-> query (dissoc :limit)
                                (assoc-in [:query-source :limits :result-rows] 1)) s))))
    (is (= :close-session (last (remove #{:cancel-session} @(:events s)))))))

(deftest large-result-windows-do-not-wrap
  (let [s (fake/source tuple-info [[1 "a"] [2 "b"]])
        query '{:find [?id] :in [$s] :where [[$s ?id _]]}]
    (is (= #{} (d/q (assoc query :offset Long/MAX_VALUE :limit Long/MAX_VALUE) s)))
    (is (= #{[2]} (d/q (assoc query :offset 1 :limit Long/MAX_VALUE) s)))))

(deftest oversized-result-charge-does-not-wrap
  (let [type {:kind :custom :name :test/large :version 1 :fingerprint "large:v1"}
        x (reify value/IQuerySourceValue
            (-logical-type [_] type)
            (-retained-bytes [_] (quot Long/MAX_VALUE 3)))
        info (assoc-in tuple-info [:pattern-contract :columns 1 :type] type)
        s (fake/source info [[1 x]])
        error (error-data
                #(d/q '{:find [?id ?value] :in [$s] :where [[$s ?id ?value]]
                        :query-source {:limits {:batch-bytes 9223372036854775807
                                                :retained-bytes 9223372036854775807}}} s))]
    ;; The batch and row scratch fit, but result deduplication charges three
    ;; copies. Its exact charge exceeds a long and must reject before retaining.
    (is (= :query-source/invalid (:error error)))
    (is (= :reserve (:phase error)))
    (is (= :close-session (last (remove #{:cancel-session} @(:events s)))))))

(deftest redacted-explanations-and-failure
  (let [s (fake/source tuple-info [[1 "secret-value"]])
        plan (d/explain {:run? false} pairs-query s)]
    (is (false? (:executed? plan)))
    (is (not-any? #{:open-cursor :fetch} @(:events s)))
    (is (not (.contains (pr-str plan) "secret-value")))
    (let [executed (d/explain {:run? true} pairs-query s)]
      (is (= 1 (:result-rows executed)))
      (is (= 1 (:streamed-rows executed)))
      (is (= 1 (get-in executed [:cursors 0 :metrics :rows]))))
    (let [executed (d/explain {:run? true :intermediate-counts? false} pairs-query s)]
      (is (= 1 (:result-rows executed)))
      (is (not (contains? executed :streamed-rows)))
      (is (= 1 (get-in executed [:cursors 0 :metrics :rows])))))
  (let [s (fake/source tuple-info [[1 "secret"]]
                       {:fetch #(throw (ex-info "secret-primary" {}))
                        :close-cursor #(throw (Exception. "secret-cleanup"))})
        error (try (d/q pairs-query s) (catch Exception e e))]
    (is (= :query-source/provider (:error (ex-data error))))
    (is (= :fetch (:phase (ex-data error))))
    (is (= {:source-symbol '$s :clause-ids [0] :alternative-id :scan}
           (select-keys (ex-data error) [:source-symbol :clause-ids :alternative-id])))
    (is (= 1 (count (.getSuppressed ^Throwable error))))
    (is (= (select-keys (ex-data error) [:query-id :source-symbol :clause-ids :alternative-id])
           (select-keys (ex-data (first (.getSuppressed ^Throwable error)))
                        [:query-id :source-symbol :clause-ids :alternative-id])))
    (is (not (.contains (str error) "secret")))
    (is (= :close-session (last (remove #{:cancel-session} @(:events s)))))))

(deftest deadline-unblocks-acquisition
  (let [released (CountDownLatch. 1)
        captured (atom nil)
        s (reify source/IQuerySource
            (-source-info [_] tuple-info)
            (-open-session [_ request]
              (reset! captured (:control request))
              (let [registration (source/register-cancel! (:control request) (fn [_] (.countDown released)))]
                (try
                  (when-not (.await released 3 TimeUnit/SECONDS) (throw (Exception. "not cancelled")))
                  (source/check-active! (:control request))
                  (finally (source/unregister-cancel! registration)))))
            (-plan-source [_ _ _] (throw (Exception. "unreachable"))))]
    (is (= :query/timeout
           (category #(d/q '{:find [?id] :in [$s] :where [[$s ?id _]] :timeout 200} s))))
    (is (source/cancelled? @captured))
    (is (= 0 (:retained-bytes (control/usage @captured))))))

(deftest nested-control-budgets
  (let [parent (control/create {:limits {:retained-bytes 100 :external-requests 2}})
        token (source/reserve! parent {:retained-bytes 30 :external-requests 1})
        child (control/create {:parent parent :limits {:retained-bytes 40 :external-requests 100}})]
    (is (= 2 (:external-requests (control/limits child))))
    (is (= :query-source/limit (category #(source/reserve! child {:retained-bytes 11}))))
    (source/reserve! child {:retained-bytes 10 :external-requests 1})
    (control/close! child)
    (is (= 30 (:retained-bytes (control/usage parent))))
    (is (= 2 (:external-requests (control/usage parent))))
    (is (= :closed (:state (error-data #(source/check-active! child)))))
    (is (nil? (source/check-active! parent)))
    (source/release! token)
    (control/close! parent)))

(deftest closing-a-control-releases-its-descendants
  (let [parent (control/create)
        child (control/create {:parent parent :timeout-ms 60000})
        grandchild (control/create {:parent child :timeout-ms 60000})]
    (try
      (source/reserve! parent {:retained-bytes 30 :external-requests 1})
      (source/reserve! child {:retained-bytes 20 :external-requests 1})
      (source/reserve! grandchild {:retained-bytes 10 :external-requests 1})
      (control/close! child)
      (control/close! child)
      (is (= 30 (:retained-bytes (control/usage parent))))
      (is (= 3 (:external-requests (control/usage parent))))
      (is (nil? (source/check-active! parent)))
      (doseq [^Control closed [child grandchild]]
        (is (= :closed (:state (error-data #(source/check-active! closed)))))
        (is (.isCancelled ^ScheduledFuture @(.-timer closed))))
      (is (= :closed (:state (error-data #(control/create {:parent child :timeout-ms 60000})))))
      (finally (control/close! grandchild) (control/close! child) (control/close! parent)))))

(deftest completion-and-cancellation-have-one-outcome
  (dotimes [_ 20]
    (let [ctl (control/create)
          start (CountDownLatch. 1)
          finish (future (.await start)
                         (try (control/complete! ctl) :completed
                              (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))
          cancel (future (.await start)
                         (control/cancel! ctl {:error :query-source/cancelled :phase :execution}))]
      (try
        (source/reserve! ctl {:retained-bytes 10})
        (.countDown start)
        (let [outcome (deref finish 2000 :hung)]
          (is (nil? (deref cancel 2000 :hung)))
          (is (#{:completed :query-source/cancelled} outcome))
          (is (= (= :query-source/cancelled outcome) (source/cancelled? ctl)))
          (is (zero? (:retained-bytes (control/usage ctl)))))
        (finally (control/close! ctl) (future-cancel finish) (future-cancel cancel))))))

(deftest blocked-fetch-observes-cancellation
  (let [latch (CountDownLatch. 1)
        blocked (fake/source tuple-info [[1 "a"]]
                             {:fetch #(when-not (.await latch 3 TimeUnit/SECONDS)
                                        (throw (Exception. "not cancelled")))})
        provider (reify source/IQuerySource
                   (-source-info [_] tuple-info)
                   (-open-session [_ request]
                     (source/register-cancel! (:control request) (fn [_] (.countDown latch)))
                     (source/-open-session blocked request))
                   (-plan-source [_ session request] (source/-plan-source blocked session request)))]
    (is (= :query/timeout
           (category #(d/q '{:find [?id] :in [$s] :where [[$s ?id _]] :timeout 200} provider))))
    (is (= [:close-cursor :close-session]
           (take-last 2 (remove #{:cancel-session} @(:events blocked)))))))

(deftest aggregate-reduction-observes-cancellation-between-values
  (let [ctl (atom nil) comparisons (atom 0)
        type {:kind :custom :name :test/cancellable-order :version 1 :fingerprint "order:v1"}
        rows (mapv (fn [id]
                     [id (reify value/IQuerySourceValue
                           (-logical-type [_] type)
                           (-retained-bytes [_] 1024)
                           Comparable
                           (compareTo [_ _]
                             (when (= 5 (swap! comparisons inc))
                               (control/cancel! @ctl {:error :query-source/cancelled :phase :execution}))
                             0))]) (range 100))
        inner (fake/source (assoc-in tuple-info [:pattern-contract :columns 1 :type] type) rows)
        provider (reify source/IQuerySource
                   (-source-info [_] (source/-source-info inner))
                   (-open-session [_ request]
                     (reset! ctl (:control request))
                     (source/-open-session inner request))
                   (-plan-source [_ session request]
                     (source/-plan-source inner session request)))]
    (is (= :query-source/cancelled
           (category #(d/q '[:find (min ?value) . :in $s :where [$s _ ?value]] provider))))
    (is (= 5 @comparisons))
    (is (every? zero? (vals (control/usage @ctl))))
    (is (= :close-session (last (remove #{:cancel-session} @(:events inner)))))))
