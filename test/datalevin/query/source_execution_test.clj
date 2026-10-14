(ns datalevin.query.source-execution-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.fake :as fake]
            [datalevin.query.source-test :refer [tuple-info category error-data]]
            [datalevin.util :as u])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

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

(deftest redacted-explanations-and-failure
  (let [s (fake/source tuple-info [[1 "secret-value"]])
        plan (d/explain {:run? false} pairs-query s)]
    (is (false? (:executed? plan)))
    (is (not-any? #{:open-cursor :fetch} @(:events s)))
    (is (not (.contains (pr-str plan) "secret-value")))
    (let [executed (d/explain {:run? true} pairs-query s)]
      (is (= 1 (:result-rows executed)))
      (is (= 1 (get-in executed [:cursors 0 :metrics :rows])))))
  (let [s (fake/source tuple-info [[1 "secret"]]
                       {:fetch #(throw (ex-info "secret-primary" {}))
                        :close-cursor #(throw (Exception. "secret-cleanup"))})
        error (try (d/q pairs-query s) (catch Exception e e))]
    (is (= :query-source/provider (:error (ex-data error))))
    (is (= :fetch (:phase (ex-data error))))
    (is (= 1 (count (.getSuppressed ^Throwable error))))
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
