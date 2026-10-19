(ns datalevin.query.source.mapped-conformance
  "Shared v2 checks through public q/explain and the real compiler.
  Constructors receive a v2 EAV descriptor, facts and explicit fixture options."
  (:require [clojure.test :refer [is testing]]
            [datalevin.core :as d]
            [datalevin.parser :as parser]
            [datalevin.query.source :as spi]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.fake :as fixture])
  (:import [datalevin.parser FindScalar FindTuple]
           [datalevin.utl QueryKey QueryKey$KeyMap]
           [java.time Duration Instant]
           [java.util Date UUID]))

(defn- error-data [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(defn- invoke! [query arguments options]
  (if (nil? options) (apply d/q query arguments)
      (apply d/explain options query arguments)))

(defn- events-of [observed kind]
  (filterv #(= kind (first %)) @(:events observed)))

(defrecord ObservedSession [delegate events transforms]
  spi/IQuerySourceSession
  (-session-info [_]
    ((get transforms :session identity) (spi/-session-info delegate)))
  (-open-cursor [_ request]
    (let [cursor (spi/-open-cursor delegate request)]
      (swap! events conj [:open request])
      (reify spi/IQuerySourceCursor
        (-next-batch! [_ fetch]
          (let [batch ((get transforms :batch identity) (spi/-next-batch! cursor fetch))]
            (swap! events conj [:batch batch]) batch))
        (-cursor-metrics [_]
          (swap! events conj [:metrics])
          (spi/-cursor-metrics cursor))
        (-close-cursor! [_]
          (swap! events conj [:close-cursor])
          (spi/-close-cursor! cursor)))))
  (-cancel-session! [_ reason] (spi/-cancel-session! delegate reason))
  (-close-session! [_]
    (swap! events conj [:close-session])
    (spi/-close-session! delegate)))

(defn- observe
  ([provider] (observe provider {}))
  ([provider transforms]
   (let [events (atom [])]
     {:events events
      :source
      (reify spi/IQuerySource
        (-source-info [_]
          (swap! events conj [:descriptor])
          ((get transforms :descriptor identity) (spi/-source-info provider)))
        (-open-session [_ request]
          (let [session (spi/-open-session provider request)]
            (swap! events conj [:acquire request])
            (->ObservedSession session events transforms)))
        (-plan-source [_ session request]
          (swap! events conj [:plan request])
          ((get transforms :plan identity) (spi/-plan-source provider (:delegate session) request))))})))

(defn- closed! [observed]
  (is (= (count (events-of observed :acquire)) (count (events-of observed :close-session))))
  (is (= (count (events-of observed :open)) (count (events-of observed :close-cursor))))
  (doseq [[_ request] (events-of observed :acquire)]
    (let [ctl (:control request)]
      (is (= :closed (:state (error-data #(spi/check-active! ctl)))))
      (is (every? zero? (vals (dissoc (control/usage ctl) :external-requests)))))))

(defn- bag [rows]
  (let [result (QueryKey$KeyMap.)]
    (doseq [row rows] (.put result row (inc (long (or (.get result row) 0)))))
    result))

(defn- same-result? [query expected actual]
  (let [find (:qfind (parser/parse-query query))]
    (if (or (instance? FindScalar find) (instance? FindTuple find))
      (QueryKey/equals expected actual)
      (.equals ^QueryKey$KeyMap (bag expected) (bag actual)))))

(defn- compare! [query eager arguments observed]
  (try
    (is (same-result? query (apply d/q (dissoc query :query-source) eager) (invoke! query arguments nil))
        (pr-str query))
    (finally (run! closed! observed))))

(def ^:private families
  {:boolean [false true nil]
   :long [-1 0 1 Long/MIN_VALUE Long/MAX_VALUE nil]
   :bigint [-1N 0N (biginteger 1) 18446744073709551616N nil]
   :string ["" "a" "雪" nil]
   :keyword [:a :test/a :测试/值 nil]
   :symbol ['a 'test/a (with-meta 'test/b {:note "owned"}) nil]
   :uuid [(UUID. 0 0) (UUID. -1 -1) nil]
   :instant [(Date. -1) (Date. 0) (Date. 1) nil]
   :bigdec [1.0M 1.00M -2M nil]
   :double [0.0 -0.0 1.0 ##Inf ##NaN nil]
   :float [(float 0) (float -0.0) Float/NaN nil]
   :bytes [(byte-array []) (byte-array [0 31]) (byte-array [1 0]) nil]
   :data [[1 nil] (list 1 nil) {:nested (byte-array [1 2])} #{:a :b}
          (Instant/ofEpochSecond -1 123) (Duration/ofSeconds -2 3) nil]})

(defn values!
  "Compare scan and dependent paths with eager EAV evaluation. Null source
  values contribute no fact; nullable ordinary inputs remain ordinary inputs."
  [make-source]
  (doseq [[type values] families mode [:scan :lookup :value :both] batch-size [1 4]]
    (testing (str type " " mode " batch=" batch-size)
      (let [info (fixture/info {:row/value {:entity-type :long :value-type type}})
            facts (mapv (fn [id v] [id :row/value v]) (range 1 (inc (count values))) values)
            eager (filterv #(some? (nth % 2)) facts)
            ;; Duplicate facts and complete duplicate keys must preserve every
            ;; outer witness while rule/find distinct removes fact duplicates.
            facts (into facts (take 2 facts)) eager (into eager (take 2 eager))
            input (into (mapv (fn [[id _ v]] [id v :first]) eager)
                        (mapv (fn [[id _ v]] [id v :second]) eager))
            provider (make-source info facts {:mode mode})
            base {:in '[$s [[?id ?wanted ?tag]]]
                  :where '[[$s ?id :row/value ?wanted]]
                  :query-source {:limits {:batch-rows batch-size :binding-rows 2}}}]
        (doseq [find '[[?id ?wanted ?tag] [[?wanted ...]] [(count ?id) .] [?tag (count ?id)]]]
          (let [o (observe provider) query (assoc base :find find)]
            (compare! query [eager input] [(:source o) input] [o])))
        (let [o (observe provider)
              query (assoc base :find '[?wanted] :with '[?id ?tag])]
          (compare! query [eager input] [(:source o) input] [o]))
        (let [o (observe provider) input (vec (take 1 input))
              query (assoc base :find '[[?id ?wanted ?tag]])]
          (compare! query [eager input] [(:source o) input] [o]))
        (let [o (observe provider)
              query (assoc base :find '[?id ?wanted ?tag] :keys '[id value tag])]
          (compare! query [eager input] [(:source o) input] [o]))
        (let [o (observe provider)
              query (assoc base :find '[?id])]
          (compare! query [eager []] [(:source o) []] [o])
          (is (empty? (events-of o :open))))))))

(defn dependencies!
  "Required inputs keep complete correlations and ID association through
  splitting, repeated outer keys, empty matches and final empty batches."
  [make-source]
  (let [info (fixture/info {:row/value {:entity-type :long :value-type :string}})
        facts [[1 :row/value "a"] [1 :row/value "b"] [2 :row/value "a"]]
        input [[2 :two] [1 :one] [2 :again] [99 :missing] [1 :more]]
        query '{:find [?tag ?value] :in [$s [[?id ?tag]]]
                :where [[$s ?id :row/value ?value]]
                :query-source {:limits {:binding-rows 2 :batch-rows 1}}}]
    (doseq [request-cap [1 2] replay [:identical :unstable]]
      (let [o (observe (make-source info facts {:mode :lookup :replay replay
                                               :constraints {:max-binding-rows 2 :max-binding-bytes 262144
                                                             :max-request-binding-rows request-cap}}))]
        (compare! query [facts input] [(:source o) input] [o])
        (let [opens (map second (events-of o :open))
              keys (mapcat :bindings opens) ids (map :binding-id keys)]
          (is (= 3 (count keys)))
          (is (= 3 (count (set ids))))
          (is (= #{[1] [2] [99]} (set (map :values keys))))
          ;; Backend request splitting is provider-owned. It does not change
          ;; the independent cursor-input cap advertised as max-binding-rows.
          (is (every? #(<= 1 (count (:bindings %)) 2) opens))
          (is (every? #(contains? (second %) :binding-ids) (events-of o :batch)))
          (is (some #(and (:exhausted? (second %)) (zero? (:row-count (second %)))
                          (= [] (:binding-ids (second %)))) (events-of o :batch))))))
    (let [o (observe (make-source info facts {:mode :scan}))]
      (compare! query [facts input] [(:source o) input] [o])
      (is (every? #(not (contains? (second %) :bindings)) (events-of o :open)))
      (is (every? #(not (contains? (second %) :binding-ids)) (events-of o :batch))))
    (doseq [mode [:lookup :value]]
      (let [o (observe (make-source info facts {:mode mode}))
            failure (error-data #(invoke! '{:find [?id] :in [$s] :where [[$s ?id :row/value _]]}
                                  [(:source o)] nil))]
        (is (= :query-source/unsupported (:error failure)))
        (is (empty? (events-of o :open)))
        (closed! o)))))

(defn regions!
  "Many-valued products, references, positive mapped rules, repeated calls
  and branch-local reads use the same EAV schema and eager oracle."
  [make-source]
  (let [info (fixture/info {:order/customer {:entity-type :long :value-type :long}
                            :customer/name {:entity-type :long :value-type :string}
                            :customer/nick {:entity-type :long :value-type :string}
                            :customer/tag {:entity-type :long :value-type :string}})
        facts [[1 :order/customer 10] [2 :order/customer 20] [3 :order/customer nil]
               [10 :customer/name "Ada"] [10 :customer/nick "A"]
               [10 :customer/tag "x"] [10 :customer/tag "y"] [10 :customer/tag "x"]
               [20 :customer/name "Bea"] [20 :customer/tag "z"] [20 :customer/nick nil]]
        eager (filterv #(some? (nth % 2)) facts)
        rules '[[(named ?order ?name) [?order :order/customer ?customer] [?customer :customer/name ?name]]
                [(tagged ?order ?name ?tag) (named ?order ?name) [?order :order/customer ?customer]
                 [?customer :customer/tag ?tag]]]
        queries
        ['{:find [?order ?name ?tag] :in [$s % [?order ...]]
           :where [($s tagged ?order ?name ?tag)]}
         '{:find [?order ?left ?right] :in [$s % [?order ...]]
           :where [($s named ?order ?left) ($s named ?order ?right)]}
         '{:find [?order ?name] :in [$s % [?order ...]]
           :where [(or-join [?order ?name]
                     (and [$s ?order :order/customer ?c] [$s ?c :customer/name ?name])
                     (and [$s ?order :order/customer ?c] [$s ?c :customer/nick ?name]))]}
         '{:find [?name] :with [?order] :in [$s % [?order ...]] :where [($s named ?order ?name)]}]]
    (doseq [mode [:scan :lookup :both] query queries]
      (let [o (observe (make-source info facts {:mode mode}))]
        (compare! query [eager rules [1 2 2 3]] [(:source o) rules [1 2 2 3]] [o])))))

(defn explanations!
  "Plans disclose structural selection and authorized read guarantees without
  executing cursors; actual metrics stay separate from optional core counts."
  [make-source]
  (let [info (fixture/info {:row/value {:entity-type :long :value-type :string}})
        facts [[1 :row/value "private payload"] [2 :row/value "two"]]
        query '{:find [?id ?value] :in [$a $b [?id ...]]
                :where [[$a ?id :row/value ?value] [$b ?id :row/value ?value]]
                :query-source {:consistency :best-effort :sources {$b {:consistency :statement}}}}
        choices (atom [])]
    (doseq [reverse? [false true] execute? [false true] counts? [false true]]
      (let [provider (make-source info facts {:mode :both :actual-consistency :snapshot})
            o (observe provider {:plan (fn [response] (cond-> response reverse? (update :alternatives #(vec (reverse %)))))})
            result (invoke! query [(:source o) (:source o) [1 2]] {:run? execute? :intermediate-counts? counts?})
            steps (mapcat :steps (:regions result))]
        (is (= 2 (:spi-version result)))
        (is (= execute? (:executed? result)))
        (is (= 1 (count (events-of o :acquire))))
        (is (= #{:entity-lookup} (set (map #(get-in % [:access :kind]) steps))))
        (is (every? #(= :structural (:selection %)) (:regions result)))
        (is (every? #(nil? (:estimated-access-ns %)) (:regions result)))
        (is (every? #(= :snapshot (:consistency %)) (:sources result)))
        (is (every? #(= :statement (:effective-consistency %)) (:sources result)))
        (is (= [:best-effort :statement] (mapv :requested-consistency (:sources result))))
        (is (every? #(some? (:data-version %)) (:sources result)))
        (swap! choices conj (mapv #(select-keys % [:region-id :alternative-id :covers]) steps))
        (if execute?
          (do (is (= 2 (:result-rows result)))
              (is (seq (:cursors result)))
              (is (= (count (:cursors result)) (count (events-of o :metrics))))
              (is (every? #(= counts? (contains? % :actual)) (:cursors result))))
          (do (is (empty? (events-of o :open)))
              (is (empty? (events-of o :batch)))
              (is (empty? (events-of o :metrics)))))
        (is (not (.contains (pr-str result) "private payload")))
        (closed! o)))
    (is (apply = @choices))))

(defn native!
  "Exercise a real pinned native DB as both producer and dependent consumer
  alongside mapped facts and native/positive mapped rule bodies."
  [make-source]
  (let [conn (d/create-conn nil {:native/key {:db/valueType :db.type/long}
                               :native/label {:db/valueType :db.type/string}}
                          {:kv-opts {:inmemory? true} :background-sampling? false})
        info (fixture/info {:row/value {:entity-type :long :value-type :string}})
        facts [[1 :row/value "one"] [2 :row/value "two"]]
        rules '[[(native-row ?key) [?e :native/key ?key]]
                [(mapped-row ?key ?value) [?key :row/value ?value]]]]
    (try
      (d/transact! conn [{:db/id 10 :native/key 1 :native/label "a"}
                         {:db/id 20 :native/key 2 :native/label "b"}
                         {:db/id 30 :native/key 99 :native/label "missing"}])
      (doseq [mode [:scan :lookup :both]
              query ['{:find [?key ?value] :in [$ $s %] :where [(native-row ?key) [$s ?key :row/value ?value]]}
                     '{:find [?key ?value ?label] :in [$ $s %]
                       :where [(native-row ?key) ($s mapped-row ?key ?value) [?e :native/key ?key] [?e :native/label ?label]]}]]
        (let [o (observe (make-source info facts {:mode mode}))]
          (compare! query [@conn facts rules] [@conn (:source o) rules] [o])))
      (finally (d/close conn)))))

(defn caller-seeds!
  "Independent caller producers seed mapped rule/union inputs even when
  written later. Private scope outputs and cyclic producers cannot do so."
  [make-source]
  (let [info (fixture/info {:row/value {:entity-type :long :value-type :string}})
        facts [[1 :row/value "one"] [2 :row/value "two"]]
        rules '[[(named ?id ?value) [?id :row/value ?value]]]
        inputs [[1 :a] [2 :b] [2 :again] [99 :missing]]
        base '{:find [?tag ?value] :in [$seed $s %]}
        clauses '[[(identity ?raw) ?id] ($s named ?id ?value) [$seed ?raw ?tag]]]
    (doseq [mode [:scan :lookup :both]
            where [clauses (vec (reverse clauses))
                   '[(or-join [?id ?value] ($s named ?id ?value) ($s named ?id ?value))
                     [(identity ?raw) ?id] [$seed ?raw ?tag]]]]
      (let [o (observe (make-source info facts {:mode mode}))
            query (assoc base :where where)]
        (compare! query [inputs facts rules] [inputs (:source o) rules] [o])))
    (let [o (observe (make-source info facts {:mode :lookup}))]
      (compare! (assoc base :where clauses) [[] facts rules] [[] (:source o) rules] [o])
      (is (empty? (events-of o :open))))
    (doseq [where
            ['[($s named ?id ?left) ($s named ?id ?value) [(identity ?id) ?copy]]
             '[(or-join [?tag]
                 (and [$seed ?id _] [(ground :a) ?tag])
                 (and [$seed ?id _] [(ground :b) ?tag]))
               ($s named ?id ?value)]]]
      (let [o (observe (make-source info facts {:mode :lookup}))
            query (assoc base :find '[?value] :where where)
            failure (error-data #(invoke! query [inputs (:source o) rules] nil))]
        (is (= :query-source/unsupported (:error failure)))
        (is (empty? (events-of o :open)))
        (closed! o)))))

(defn lifecycle!
  "Each acquisition pins one owned fact version across aliases. Planning can
  publish a new version without changing the acquired query or cached results."
  [make-source replace-facts!]
  (let [info (fixture/info {:row/value {:entity-type :long :value-type :string}})
        old [[1 :row/value "old"]] fresh [[2 :row/value "fresh"]]
        provider (make-source info old {:mode :scan})
        changed (atom false)
        o (observe provider {:plan (fn [response]
                                    (when (compare-and-set! changed false true) (replace-facts! provider fresh))
                                    response)})
        query '{:find [?id ?value] :in [$a $b]
                :where [[$a ?id :row/value ?value] [$b ?id :row/value ?value]]}]
    (is (= #{[1 "old"]} (invoke! query [(:source o) (:source o)] nil)))
    (closed! o)
    (is (= 1 (count (events-of o :acquire))))
    (let [first (invoke! query [provider provider] {:run? true})]
      (is (= 1 (:result-rows first)))
      (replace-facts! provider [[3 :row/value "newer"]])
      (let [second (invoke! query [provider provider] {:run? true})]
        (is (= 1 (:result-rows second)))
        (is (not= (get-in first [:sources 0 :data-version]) (get-in second [:sources 0 :data-version])))))
    (is (= #{[3 "newer"]} (invoke! query [provider provider] nil)))))

(defn ownership!
  "Caller data, batches and returned mutable leaves have independent ownership
  after cleanup. Later writes to either borrowed input or a batch cannot alter
  a returned result or a later invocation's pinned facts."
  [make-source]
  (let [bytes (byte-array [1 2]) date (Date. 42)
        info (fixture/info {:row/value {:entity-type :long :value-type :data}})
        provider (make-source info [[1 :row/value {:bytes bytes :date date}]] {:mode :scan})
        o (observe provider)
        query '{:find [?value] :in [$s] :where [[$s _ :row/value ?value]]}]
    (aset-byte bytes 0 (byte 99))
    (.setTime date 999)
    (let [result (invoke! query [(:source o)] nil)
          returned (ffirst result)
          batch-value (get-in (second (first (events-of o :batch))) [:rows 0 0])]
      (is (= [1 2] (vec (:bytes returned))))
      (is (= 42 (.getTime ^Date (:date returned))))
      (aset-byte ^bytes (:bytes batch-value) 0 (byte 88))
      (.setTime ^Date (:date batch-value) 888)
      (is (= [1 2] (vec (:bytes returned))))
      (is (= 42 (.getTime ^Date (:date returned))))
      (aset-byte ^bytes (:bytes returned) 0 (byte 77))
      (.setTime ^Date (:date returned) 777)
      (let [again (ffirst (invoke! query [provider] nil))]
        (is (= [1 2] (vec (:bytes again))))
        (is (= 42 (.getTime ^Date (:date again))))))
    (closed! o)))

(defn failures!
  "Malformed completion/association fails the whole invocation, and cleanup
  releases shared charges even when the final batch or provider close fails."
  [make-source]
  (let [info (fixture/info {:row/value {:entity-type :long :value-type :string}})
        facts [[1 :row/value "one"]]
        query '{:find [?value] :in [$s [?id ...]] :where [[$s ?id :row/value ?value]]}]
    (doseq [[transform expected]
            [[(constantly nil) :query-source/incomplete]
             [#(dissoc % :binding-ids) :query-source/invalid]
             [#(assoc % :binding-ids [999999]) :query-source/invalid]
             [#(assoc % :exhausted? nil) :query-source/invalid]]]
      (let [o (observe (make-source info facts {:mode :lookup}) {:batch transform})
            failure (error-data #(invoke! query [(:source o) [1]] nil))]
        (is (= expected (:error failure)))
        (is (not (.contains (pr-str failure) "one")))
        (closed! o)))
    (doseq [mode [:scan :lookup] limits [{:result-rows 0} {:batch-bytes 1} {:retained-bytes 1}]]
      (let [o (observe (make-source info facts {:mode mode}))
            failure (error-data #(invoke! (assoc query :query-source {:limits limits}) [(:source o) [1]] nil))]
        (is (= :query-source/limit (:error failure)))
        (closed! o)))
    (let [o (observe (make-source info facts {:mode :value}))
          query '{:find [?id] :in [$s [?value ...]] :where [[$s ?id :row/value ?value]]
                  :query-source {:limits {:binding-bytes 2048}}}
          failure (error-data #(invoke! query [(:source o) [(.repeat "x" 8192)]] nil))]
      (is (= :query-source/limit (:error failure)))
      (is (= :binding-bytes (:category failure)))
      (is (empty? (events-of o :open)))
      (closed! o))
    (doseq [execute? [false true]]
      (let [o (observe (make-source info facts {:mode :scan :replay :unstable
                                               :consumption {:opens :once :max-open-cursors 1}}))
            failure (error-data #(invoke! '{:find [?id] :in [$s]
                                              :where [[$s ?id :row/value _] [$s ?other :row/value _]]}
                                  [(:source o)] {:run? execute?}))]
        (is (= :query-source/unsupported (:error failure)))
        (is (empty? (events-of o :open)))
        (closed! o)))))
