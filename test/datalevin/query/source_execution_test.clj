(ns datalevin.query.source-execution-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.interpret :as inter]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.fake :as fake]
            [datalevin.query.source.value :as value]
            [datalevin.query.source-test :refer [row-info category error-data]]
            [datalevin.util :as u])
  (:import [datalevin.query.source.control Control]
           [java.util.concurrent CountDownLatch ScheduledFuture TimeUnit]
           [java.math BigDecimal BigInteger]))

(defn- events [provider] (mapv first @(:events provider)))
(defn- pair-source
  ([info rows] (pair-source info rows {}))
  ([info rows faults]
   (let [phases {:acquisition :acquire :planning :plan :open-cursor :open
                 :fetch :fetch :close-cursor :close-cursor :close-session :close-session}]
     (fake/source info (fake/row-facts rows)
       {:mode :scan :hooks (into {} (map (fn [[phase f]] [(phases phase phase) (fn [_] (f))])) faults)}))))

(def pairs-query '[:find ?id ?name :in $s :where [$s ?id :row/value ?name]])

(deftest query-entry-and-fresh-invocations
  (let [rows [[1 "Ada"] [2 nil] [3 "Ada"] [3 "Ada"]]
        s (pair-source row-info rows)]
    (is (= #{[1 "Ada"] [3 "Ada"]} (d/q pairs-query s)))
    (is (= [1 3] (sort (d/q '[:find [?id ...] :in $s :where [$s ?id :row/value _]] s))))
    (is (= 2 (d/q '[:find (count ?id) . :in $s :where [$s ?id :row/value _]] s)))
    (is (= 1 (d/q '[:find (count ?name) . :in $s :where [$s _ :row/value ?name]] s)))
    (is (= 2 (d/q '[:find (count ?name) . :with ?id :in $s :where [$s ?id :row/value ?name]] s)))
    (is (= #{} (d/q '[:find ?id ?name :in $s ?id :where [$s ?id :row/value ?name]] s 2)))
    (fake/replace-facts! s (fake/row-facts [[4 "fresh"]]))
    (is (= #{[4 "fresh"]} (d/q pairs-query s)))
    (is (= 7 (count (filter #{:acquire} (events s)))))))

(deftest aliases-share-a-view
  (let [s (pair-source row-info [[1 "a"] [2 "b"]])
        query '[:find ?id ?name :in $a $b :where [$a ?id :row/value ?name] [$b ?id :row/value ?name]]]
    (is (= #{[1 "a"] [2 "b"]} (d/q query s s)))
    (is (= 1 (count (filter #{:acquire} (events s)))))
    (is (= 2 (count (filter #{:plan} (events s)))))
    (is (= [:close-cursor :close-session] (take-last 2 (events s))))))

(deftest obsolete-positional-descriptors-reject-before-acquisition
  (let [events (atom [])
        provider (reify source/IQuerySource
                   (-source-info [_] (assoc row-info :spi-versions #{1}))
                   (-open-session [_ _] (swap! events conj :acquire))
                   (-plan-source [_ _ _] (swap! events conj :plan)))]
    (is (= :query-source/version (category #(d/q pairs-query provider))))
    (is (empty? @events))))

(deftest eav-reference-joins-and-missing-values
  (let [info (assoc row-info :pattern-contract
                    {:kind :eav :attributes {:order/customer {:entity-type :long :value-type :long}
                                             :customer/name {:entity-type :long :value-type :string}}})
        rows [[1 :order/customer 10] [2 :order/customer 20] [3 :order/customer nil]
              [10 :customer/name "Ada"] [20 :customer/name nil]]
        s (fake/source (fake/info (get-in info [:pattern-contract :attributes])) rows)
        facts (filterv #(some? (nth % 2)) rows)]
    (doseq [q ['[:find ?order ?name :in $s
                 :where [$s ?order :order/customer ?customer] [$s ?customer :customer/name ?name]]
               '[:find ?order :in $s :where [$s ?order :order/customer _]]
               '[:find ?order :in $s :where [$s ?order :order/customer ?customer]]]]
      (is (= (d/q q facts) (d/q q s))))
    (is (= #{[1 "Ada"]}
           (d/q '[:find ?order ?name :in $s
                  :where [$s ?order :order/customer ?customer] [$s ?customer :customer/name ?name]] s)))))

(deftest flat-input-bindings-match-eager-relations
  (let [rows [[1 "a"] [2 "b"] [3 nil]]
        s (pair-source row-info rows)]
    (doseq [[query args]
            [['[:find ?id ?name :in $s [?id ...] :where [$s ?id :row/value ?name]] [[1 3 3]]]
             ['[:find ?id :in $s [?id ?name] :where [$s ?id :row/value ?name]] [[2 "b"]]]
             ['[:find ?id ?name :in $s [[?id ?name]] :where [$s ?id :row/value ?name]]
              [[[1 "a"] [3 nil]]]]
             ['[:find ?id ?label :in $s [[?id ?label]] :where [$s ?id :row/value _]]
              [[[1 "first"] [1 "second"]]]]]]
      (is (= (apply d/q query (fake/row-facts rows) args) (apply d/q query s args))))
    (reset! (:events s) [])
    (is (= #{} (d/q '[:find ?id :in $s [?id ...] :where [$s ?id :row/value _]] s [])))
    (is (not-any? #{:open :batch} (events s)))))

(deftest known-empty-results-skip-unneeded-source-reads
  (let [a (pair-source (assoc row-info :source-instance-id :a) [])
        b (pair-source (assoc row-info :source-instance-id :b) [[1 "b"]]
                        {:open-cursor #(throw (Exception. "unneeded cursor"))})
        c (pair-source (assoc row-info :source-instance-id :c) [[1 "c"]])
        q '{:find [?id] :in [$a $b $c]
            :where [[$a ?id :row/value _] [$b ?id :row/value _] [$c ?id :row/value _]]}]
    (is (= #{} (d/q q a b c)))
    (is (= 1 (count (filter #{:open} (events a)))))
    (doseq [s [a b c]]
      (is (= :close-session (last (events s)))))
    (is (not-any? #{:open :batch} (events c)))))

(deftest dynamic-windows-require-native-scalar-domains
  (doseq [[option binding input] [[:limit '?window 0] [:limit '?window -1]
                                 [:limit '?window nil] [:offset '?window -1]
                                 [:offset '?window nil] [:limit '[?window] [1]]]]
    (let [s (pair-source row-info [[1 "a"]])
          q (assoc {:find '[?id] :in ['$s binding] :where '[[$s ?id :row/value _]]} option '?window)
          error (error-data #(d/q q s input))]
      (is (= :query-source/invalid (:error error)))
      (is (= :window (:phase error)))
      (is (empty? (events s))))))

(deftest malformed-input-rows-reject-before-source-acquisition
  (doseq [[binding input category]
          [['[?id ?name] [1] :query-source/invalid]
           ['[[?id ?name]] [[1 "a"] [2]] :query-source/invalid]
           ['[?id ...] "not-a-collection" :query-source/invalid]]]
    (let [s (pair-source row-info [[1 "a"]])
          query {:find ['?id] :in ['$s binding] :where [['$s '?id :row/value '_]]}]
      (is (= category (:error (error-data #(d/q query s input)))))
      (is (not-any? #{:acquire :plan :open} (events s))))))

(deftest fused-fake-matches-native-key-and-predicate-semantics
  (let [info (fake/row-info :bigdec)
        rows [[1 1.0M] [2 1.00M] [3 2M]]
        s (pair-source info rows)
        query '[:find ?a ?b :in $s :where [$s ?a :row/value ?n] [$s ?b :row/value ?n] [(= ?n 1M)]]]
    (is (= (d/q query (fake/row-facts rows)) (d/q query s))))
  (let [rows [[1 "a"] [2 "b"]]
        s (pair-source row-info rows)
        query '[:find ?id :in $s :where [$s ?id :row/value ?name] [(> ?name "a")]]]
    (is (= (d/q query (fake/row-facts rows)) (d/q query s)))))

(deftest scalar-and-tuple-inputs-preserve-decimal-key-equality
  (let [rows [[1 1.0M] [2 1.00M]]
        info (assoc-in row-info [:pattern-contract :attributes :row/value :value-type] :bigdec)
        s (pair-source info rows)]
    (doseq [[binding input] [['?value 1.0M] ['[?value] [1.0M]]]]
      (let [q {:find '[?id ?value] :in ['$s binding] :where '[[$s ?id :row/value ?value]]}]
        (is (= #{[1 1.0M]} (d/q q s input)))
        (is (= (d/q q (fake/row-facts rows) input) (d/q q s input)))))))

(deftest query-constants-are-owned-before-source-acquisition
  (let [instant (java.util.Date. 1000)
        rows [[1 (java.util.Date. 1000)] [2 (java.util.Date. 2000)]]
        s (pair-source (assoc-in row-info [:pattern-contract :attributes :row/value :value-type] :instant)
                       rows {:acquisition #(.setTime instant 2000)})
        q {:find '[?id] :in '[$s] :where [['$s '?id :row/value instant]]}]
    (is (= #{[1]} (d/q q s)))
    ;; A later invocation captures the caller's current constant afresh.
    (is (= #{[2]} (d/q q s)))
    (is (= :close-session (last (events s))))))

(deftest decimal-projection-and-aggregation-match-eager-results
  (let [rows [[1 1.0M] [2 1.00M] [3 1.0M]]
        s (pair-source (assoc-in row-info [:pattern-contract :attributes :row/value :value-type] :bigdec) rows)]
    (doseq [input [(fake/row-facts rows) s]]
      (is (= ["1.0" "1.00"]
             (sort (map str (d/q '[:find [?v ...] :in $s :where [$s _ :row/value ?v]] input)))))
      (is (= [2 2.00M 1.0]
             (d/q '[:find [(count ?v) (sum ?v) (avg ?v)]
                    :in $s :where [$s _ :row/value ?v]] input))))))

(deftest unsupported-shapes-reject-before-acquisition
  (doseq [query ['[:find ?id :in $s :where [$s ?id :row/value _] (not [$s ?id :row/value "a"])]
                 '[:find ?x :in $s :where [$s ?id :row/value _] [(inc ?id) ?x]]
                 '{:find [?id] :in [$s] :where [[$s ?id :row/value _]] :order-by [?id :asc]}]]
    (let [s (pair-source row-info [[1 "a"]])]
      (is (= :query-source/unsupported (category #(d/q query s))))
      (is (not-any? #{:acquire :open} (events s)))))
  (let [s (pair-source row-info [[1 "a"]])]
    (is (= #{} (d/q '[:find ?n :in $s :where [$s "bad" :row/value ?n]] s)))
    (is (= 1 (count (filter #{:acquire} (events s)))))
    (is (not-any? #{:open} (events s)))))

(deftest native-natural-key-join
  (let [dir (u/tmp-dir (str "source-native-" (random-uuid)))
        conn (d/get-conn dir {:person/id {:db/valueType :db.type/long}
                             :person/name {:db/valueType :db.type/string}})
        rows [[1 "external"] [2 "other"]]
        s (pair-source row-info rows)
        query '[:find ?name ?external :in $ $s
                :where [?e :person/id ?id] [?e :person/name ?name] [$s ?id :row/value ?external]]]
    (try
      (d/transact! conn [{:person/id 1 :person/name "native"}])
      (is (= (d/q query @conn (fake/row-facts rows)) (d/q query @conn s)))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest native-value-wildcards-do-not-decode-byte-payloads
  (let [dir (u/tmp-dir (str "source-byte-existence-" (random-uuid)))
        conn (d/get-conn dir {:item/blob {:db/valueType :db.type/bytes}})
        rows [[1 "one"] [2 "two"] [3 "absent"]]
        s (pair-source row-info rows)
        query '{:find [?label] :in [$ $s]
                :where [[?e :item/blob _] [$s ?e :row/value ?label]]
                :query-source {:limits {:retained-bytes 524288}}}]
    (try
      (d/transact! conn [{:db/id 1 :item/blob (byte-array 2097152)}
                         {:db/id 2 :item/blob (byte-array [1 2])}])
      (is (= #{["one"] ["two"]} (d/q query @conn s)))
      (is (= (d/q (dissoc query :query-source) @conn (fake/row-facts rows)) (d/q query @conn s)))
      (reset! (:events s) [])
      (let [error (error-data #(d/q (assoc query :where '[[?e :item/blob ?blob] [$s ?e :row/value ?label]]) @conn s))]
        (is (= :query-source/limit (:error error)))
        (is (= :retained-bytes (:category error)))
)
      (is (= 1 (count (filter #{:close-session} (events s)))))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest bound-native-entity-skips-unrelated-giant-values
  (let [dir (u/tmp-dir (str "source-bound-entity-" (random-uuid)))
        conn (d/create-conn dir {:item/body {:db/valueType :db.type/string}}
                            {:wal? false :background-sampling? false})
        s (pair-source row-info [[1 "source"] [2 "source"]])
        q '{:find [?body] :in [$ $s ?id]
            :where [[?id :item/body ?body] [$s ?id :row/value _]]
            :query-source {:limits {:retained-bytes 524288}}}]
    (try
      (d/transact! conn [{:db/id 1 :item/body (.repeat "x" 2097152)}
                         {:db/id 2 :item/body "wanted"}])
      (is (= #{["wanted"]} (d/q q @conn s 2)))
      (is (= #{} (d/q q @conn s 3)))
      (doseq [[binding ids] [['[?id ...] [2 2]] ['[[?id ?label]] [[2 :a] [2 :b]]]]]
        (is (= #{["wanted"]} (d/q (assoc q :in ['$ '$s binding]) @conn s ids))))
      (let [missing '[:find ?value :where [2 :item/absent ?value]]]
        (is (= #{} (d/q missing @conn)))
        (d/transact! conn [[:db/add 2 :item/absent "fresh"]])
        (is (= #{["fresh"]} (d/q missing @conn))))
      ;; A selected giant still enforces the decoding budget.
      (is (= :query-source/limit (category #(d/q q @conn s 1))))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest bound-native-decimal-joins-match-source-key-equality
  (let [dir (u/tmp-dir (str "source-decimal-join-" (random-uuid)))
        conn (d/get-conn dir {:item/amount {:db/valueType :db.type/bigdec}})
        rows [[1 1.0M] [1 1.00M] [2 2M]]
        s (pair-source (assoc-in row-info [:pattern-contract :attributes :row/value :value-type] :bigdec) rows)
        query '[:find ?tag :in $ $s [[?id ?amount ?tag]]
                :where [?id :item/amount ?amount] [$s ?id :row/value ?amount]]
        input [[1 1.0M :exact] [1 1.00M :wrong-scale] [2 2M :other]]]
    (try
      (d/transact! conn [{:db/id 1 :item/amount 1.0M} {:db/id 2 :item/amount 2M}])
      (is (= #{[:exact] [:other]} (d/q query @conn (fake/row-facts rows) input) (d/q query @conn s input)))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest semantic-limit-and-overflow
  (let [s (pair-source row-info [[1 "a"] [2 "b"]])]
    (is (= 1 (d/q '{:find [?id .] :in [$s] :where [[$s ?id :row/value _]]
                     :query-source {:limits {:result-rows 1 :batch-rows 1}}} s)))
    (is (= 3 (count (filter #{:batch} (events s))))))
  (let [s (pair-source row-info (mapv #(vector % (str %)) (range 20)))
        query '{:find [?id] :in [$s] :where [[$s ?id :row/value _] [(> ?id 5)]]
                :limit 2 :query-source {:limits {:batch-rows 1}}}]
    (let [result (d/q query s)]
      (is (vector? result))
      (is (= 2 (count result)))
      (is (every? #(> (first %) 5) result)))
    (is (= 21 (count (filter #{:batch} (events s)))))
    (is (= :close-session (last (events s))))
    (reset! (:events s) [])
    (is (= :query-source/limit
           (category #(d/q (-> query (dissoc :limit)
                                (assoc-in [:query-source :limits :result-rows] 1)) s))))
    (is (= :close-session (last (remove #{:cancel} (events s)))))))

(deftest large-result-windows-do-not-wrap
  (let [s (pair-source row-info [[1 "a"] [2 "b"]])
        query '{:find [?id] :in [$s] :where [[$s ?id :row/value _]]}]
    (is (= [] (d/q (assoc query :offset Long/MAX_VALUE :limit Long/MAX_VALUE) s)))
    (is (= [[2]] (d/q (assoc query :offset 1 :limit Long/MAX_VALUE) s)))))

(deftest oversized-result-charge-does-not-wrap
  (let [type {:kind :custom :name :test/large :version 1 :fingerprint "large:v1"}
        x (reify value/IQuerySourceValue
            (-logical-type [_] type)
            (-retained-bytes [_] (quot Long/MAX_VALUE 3)))
        info (assoc-in row-info [:pattern-contract :attributes :row/value :value-type] type)
        s (pair-source info [[1 x]])
        error (error-data
                #(d/q '{:find [?id ?value] :in [$s] :where [[$s ?id :row/value ?value]]
                        :query-source {:limits {:batch-bytes 9223372036854775807
                                                :retained-bytes 9223372036854775807}}} s))]
    ;; The batch and row scratch fit, but result deduplication charges three
    ;; copies. Report the exact overflow as a quota failure, before narrowing
    ;; the charge to the ledger's long representation or retaining the result.
    (is (= :query-source/limit (:error error)))
    (is (= :reserve (:phase error)))
    (is (= :retained-bytes (:category error)))
    (is (= Long/MAX_VALUE (:maximum error)))
    (is (> (:attempted error) Long/MAX_VALUE))
    (is (= :close-session (last (remove #{:cancel} (events s)))))))

(deftest redacted-explanations-and-failure
  (let [s (pair-source row-info [[1 "secret-value"]])
        plan (d/explain {:run? false} pairs-query s)]
    (is (false? (:executed? plan)))
    (is (not-any? #{:open :batch} (events s)))
    (is (not (.contains (pr-str plan) "secret-value")))
    (let [executed (d/explain {:run? true} pairs-query s)]
      (is (= 1 (:result-rows executed)))
      (is (= 1 (get-in executed [:cursors 0 :actual :rows])))
      (is (= 1 (get-in executed [:cursors 0 :metrics :rows]))))
    (let [executed (d/explain {:run? true :intermediate-counts? false} pairs-query s)]
      (is (= 1 (:result-rows executed)))
      (is (not (contains? executed :streamed-rows)))
      (is (= 1 (get-in executed [:cursors 0 :metrics :rows])))))
  (let [s (pair-source row-info [[1 "secret"]]
                       {:fetch #(throw (ex-info "secret-primary" {}))
                        :close-cursor #(throw (Exception. "secret-cleanup"))})
        error (try (d/q pairs-query s) (catch Exception e e))]
    (is (= :query-source/provider (:error (ex-data error))))
    (is (= :fetch (:phase (ex-data error))))
    (is (= {:source-symbol '$s :region-id 0 :alternative-id (keyword "attribute-enumeration" "0")}
           (select-keys (ex-data error) [:source-symbol :region-id :alternative-id])))
    (is (= 1 (count (.getSuppressed ^Throwable error))))
    (is (= (select-keys (ex-data error) [:query-id :source-symbol :region-id :alternative-id])
           (select-keys (ex-data (first (.getSuppressed ^Throwable error)))
                        [:query-id :source-symbol :region-id :alternative-id])))
    (is (not (.contains (str error) "secret")))
    (is (= :close-session (last (remove #{:cancel} (events s)))))))

(deftest deadline-unblocks-acquisition
  (let [released (CountDownLatch. 1)
        captured (atom nil)
        s (reify source/IQuerySource
            (-source-info [_] row-info)
            (-open-session [_ request]
              (reset! captured (:control request))
              (let [registration (source/register-cancel! (:control request) (fn [_] (.countDown released)))]
                (try
                  (when-not (.await released 3 TimeUnit/SECONDS) (throw (Exception. "not cancelled")))
                  (source/check-active! (:control request))
                  (finally (source/unregister-cancel! registration)))))
            (-plan-source [_ _ _] (throw (Exception. "unreachable"))))]
    (is (= :query/timeout
           (category #(d/q '{:find [?id] :in [$s] :where [[$s ?id :row/value _]] :timeout 200} s))))
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
        blocked (pair-source row-info [[1 "a"]]
                             {:fetch #(when-not (.await latch 3 TimeUnit/SECONDS)
                                        (throw (Exception. "not cancelled")))})
        provider (reify source/IQuerySource
                   (-source-info [_] row-info)
                   (-open-session [_ request]
                     (source/register-cancel! (:control request) (fn [_] (.countDown latch)))
                     (source/-open-session blocked request))
                   (-plan-source [_ session request] (source/-plan-source blocked session request)))]
    (is (= :query/timeout
           (category #(d/q '{:find [?id] :in [$s] :where [[$s ?id :row/value _]] :timeout 200} provider))))
    (is (= [:close-cursor :close-session]
           (take-last 2 (remove #{:cancel} (events blocked)))))))

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
        inner (pair-source (assoc-in row-info [:pattern-contract :attributes :row/value :value-type] type) rows)
        provider (reify source/IQuerySource
                   (-source-info [_] (source/-source-info inner))
                   (-open-session [_ request]
                     (reset! ctl (:control request))
                     (source/-open-session inner request))
                   (-plan-source [_ session request]
                     (source/-plan-source inner session request)))]
    (is (= :query-source/cancelled
           (category #(d/q '[:find (min ?value) . :in $s :where [$s _ :row/value ?value]] provider))))
    (is (= 5 @comparisons))
    (is (every? zero? (vals (control/usage @ctl))))
    (is (= :close-session (last (remove #{:cancel} (events inner)))))))

(deftest native-nippy-custom-data-joins-under-quota
  (let [dir (u/tmp-dir (str "source-custom-smoke-" (random-uuid)))
        conn (d/create-conn dir {} {:wal? false :background-sampling? false})
        small {:rank 1 :tags #{:a :b}}
        large {:rank 2 :body (.repeat "large" 419431)}
        rows [[1 small] [2 large]]
        s (pair-source row-info [[1 "anchor"] [2 "anchor"]])
        q '{:find [?e ?value] :in [$ $s ?e]
            :where [[?e :item/custom ?value] [$s ?e :row/value _]]}]
    (try
      (d/register-type conn :app/ranked {:index {:type :long :order-fn (inter/inter-fn [v] (:rank v))}})
      (d/update-schema conn {:item/custom {:db/valueType :app/ranked}})
      (d/transact! conn [{:db/id 1 :item/custom small} {:db/id 2 :item/custom large}])
      (is (= #{[1 small]} (d/q q @conn (fake/row-facts rows) 1) (d/q q @conn s 1)))
      (is (= #{[2 large]} (d/q q @conn (fake/row-facts rows) 2) (d/q q @conn s 2)))
      (let [bounded (assoc q :query-source {:limits {:retained-bytes 524288}})]
        (is (= :query-source/limit (category #(d/q bounded @conn s 2))))
        (is (= #{[1 small]} (d/q bounded @conn s 1)))
        (is (= #{[1 small]}
               (d/q (assoc bounded :in '[$ $s [?e ...]]) @conn
                    (pair-source row-info [[1 "anchor"]]) [1 1])))
        ;; Without an upstream entity key, the native read is a complete scan
        ;; and must reject the selected giant before decoding it.
        (is (= :query-source/limit
               (category #(d/q (assoc bounded :in '[$ $s]) @conn
                                (pair-source row-info [[1 "anchor"]]))))))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest decimal-aggregate-alignment-reserves-before-arithmetic
  (let [info (assoc-in row-info [:pattern-contract :attributes :row/value :value-type] :bigdec)
        rows [[1 (BigDecimal. BigInteger/ONE 100000)] [2 1M]]
        s (pair-source info rows)
        q '{:find [(avg ?v) .] :in [$s] :where [[$s ?id :row/value ?v]] :with [?id]}]
    (let [error (error-data #(d/q (assoc q :query-source {:limits {:retained-bytes 32768}}) s))]
      (is (= :query-source/limit (:error error)))
      (is (= :retained-bytes (:category error))))
    (is (= 0.5 (d/q q (fake/row-facts rows)) (d/q q s)))))


(deftest bound-native-string-avoids-unrelated-giant-decoding
  (let [dir (u/tmp-dir (str "source-value-seek-" (random-uuid)))
        conn (d/create-conn dir {:item/body {:db/valueType :db.type/string}}
                            {:wal? false :background-sampling? false})
        s (pair-source row-info [[2 "selected"]])
        q '{:find [?e] :in [$ $s ?body] :where [[?e :item/body ?body] [$s _ :row/value _]]
            :query-source {:limits {:retained-bytes 524288}}}]
    (try
      (d/transact! conn [{:db/id 1 :item/body (.repeat "large" 419431)}
                        {:db/id 2 :item/body "wanted"}])
      (is (= #{[2]} (d/q q @conn s "wanted")))
      (is (= #{} (d/q q @conn s "absent")))
      (finally (d/close conn) (u/delete-files dir)))))
