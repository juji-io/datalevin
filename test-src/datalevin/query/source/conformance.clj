(ns datalevin.query.source.conformance
  "Reusable embedded execution checks. Adapters supply constructors, never
  engine dependencies or an alternate evaluator. Not part of the runtime SPI."
  (:require [clojure.test :refer [is testing]]
            [datalevin.core :as d]
            [datalevin.parser :as parser]
            [datalevin.query :as query]
            [datalevin.query.source :as spi]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.value :as value]
            [datalevin.util :as u])
  (:import [datalevin.parser FindScalar FindTuple]
           [datalevin.utl QueryKey QueryKey$KeyMap]
           [java.time Duration Instant]
           [java.util Date Random]
           [java.util.concurrent CountDownLatch TimeUnit]))

(defn- descriptor [contract]
  {:spi-versions #{1} :source-instance-id (random-uuid) :compute-context-id :conformance
   :definition-id :fixture :definition-fingerprint "conformance:v1"
   :features #{} :required-features #{} :pattern-contract contract})

(defn- tuple-descriptor [type]
  (descriptor {:kind :tuple :columns [{:type :long :nullable? false}
                                      {:type type :nullable? true}]}))

(defn- error-data [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(defn- bag [rows]
  ;; Preserve multiplicities, decimal scale, signed zero and byte content. An
  ;; ordinary Clojure set/frequency map would erase some of these distinctions.
  (let [result (QueryKey$KeyMap.)]
    (doseq [row rows]
      (.put result row (inc (long (or (.get result row) 0)))))
    result))

(defn- same-result? [q expected actual]
  (let [shape (:qfind (parser/parse-query q))]
    (if (or (instance? FindScalar shape) (instance? FindTuple shape))
      (QueryKey/equals expected actual)
      (.equals ^QueryKey$KeyMap (bag expected) (bag actual)))))

(defrecord ObservedSession [delegate events hook]
  spi/IQuerySourceSession
  (-session-info [_] (spi/-session-info delegate))
  (-open-cursor [_ request]
    (hook :open request)
    (let [cursor (spi/-open-cursor delegate request)]
      (swap! events conj [:open-cursor])
      (reify spi/IQuerySourceCursor
        (-next-batch! [_ batch-request]
          (hook :fetch batch-request)
          (let [batch (spi/-next-batch! cursor batch-request)]
            (swap! events conj [:batch (:row-count batch)])
            batch))
        (-cursor-metrics [_] (spi/-cursor-metrics cursor))
        (-close-cursor! [_]
          (swap! events conj [:close-cursor])
          (spi/-close-cursor! cursor)))))
  (-cancel-session! [_ reason] (spi/-cancel-session! delegate reason))
  (-close-session! [_]
    (swap! events conj [:close-session])
    (spi/-close-session! delegate)))

(defn- observe
  ([source] (observe source (fn [_ _])))
  ([source hook]
   (let [events (atom []) controls (atom [])]
     {:events events :controls controls
      :source
      (reify spi/IQuerySource
        (-source-info [_] (spi/-source-info source))
        (-open-session [_ request]
          (swap! controls conj (:control request))
          (hook :acquisition request)
          (let [session (spi/-open-session source request)]
            (swap! events conj [:open-session])
            (->ObservedSession session events hook)))
        (-plan-source [_ session request]
          (swap! events conj [:plan (:source-symbol request)])
          (hook :planning request)
          (spi/-plan-source source (:delegate session) request)))})))

(defn- closed! [{:keys [events controls]}]
  (let [counts (frequencies (map first @events))]
    (is (= (get counts :open-session 0) (get counts :close-session 0)))
    (is (= (get counts :open-cursor 0) (get counts :close-cursor 0))))
  (doseq [ctl @controls]
    (is (= :closed (:state (error-data #(spi/check-active! ctl)))))
    (is (every? zero? (vals (dissoc (control/usage ctl) :external-requests))))))

(defn- compare! [q eager-inputs source-inputs observed]
  (try
    (binding [query/*cache?* false]
      (is (same-result? q (apply d/q q eager-inputs) (apply d/q q source-inputs))
          (pr-str q)))
    (finally (run! closed! observed))))

(def ^:private families
  {:long [nil -1 0 1 2]
   :string [nil "" "a" "雪"]
   :bigdec [nil 1.0M 1.00M -2M 3M]
   :double [nil 0.0 -0.0 1.0 ##Inf ##NaN]
   :data [nil [1 nil] (list 1 nil) {:nested [1 "x"]} #{:a :b}
          (Date. 42) (Instant/ofEpochSecond -1 123) (Duration/ofSeconds -2 3)]
   :bytes [nil (byte-array []) (byte-array [0 31]) (byte-array [1 0])
           (byte-array [0 31])]})

(defn- generated-rows [seed values]
  (if (zero? (long seed)) []
      (let [random (Random. (long seed))
            rows (mapv (fn [i] [(long (inc (long i)))
                                (value/owned-copy (nth values (.nextInt random (count values))))])
                       (range 9))]
        (into rows (value/owned-copy (subvec rows 0 3))))))

(def ^:private tuple-queries
  ['{:find [?id ?v] :in [$s] :where [[$s ?id ?v]]}
   '{:find [?v] :in [$s] :where [[$s _ ?v]]}
   '{:find [[?v ...]] :in [$s] :where [[$s _ ?v]]}
   '{:find [?v] :keys [value] :in [$s] :where [[$s _ ?v]]}
   '{:find [?v] :with [?id] :in [$s] :where [[$s ?id ?v]]}
   '{:find [?v (count ?id)] :in [$s] :where [[$s ?id ?v]]}
   '{:find [(count ?v) .] :in [$s] :where [[$s _ ?v]]}
   '{:find [(count ?v) .] :with [?id] :in [$s] :where [[$s ?id ?v]]}
   '{:find [?id] :in [$s] :where [[$s ?id ?v] [(not= ?v nil)]]}
   '{:find [?a ?b] :in [$s] :where [[$s ?a ?v] [$s ?b ?v] [(< ?a ?b)]]}])

(defn tuple-semantics!
  "Compare fused/unfused source plans to eager d/q with deterministic seeds.
  Failure contexts name the seed, logical type and batch size."
  [make-source]
  (doseq [[type values] families, seed [0 1 42], batch-size [1 4]]
    (testing (str "type=" type " seed=" seed " batch=" batch-size)
      (let [rows (generated-rows seed values)
            source (make-source (tuple-descriptor type) rows)
            options {:limits {:batch-rows batch-size :spill-bytes 0}}]
        (doseq [q tuple-queries]
          (let [observed (observe source)]
            (compare! (assoc q :query-source options) [rows] [(:source observed)] [observed])))
        (doseq [[binding input] [['?v (second values)] ['[?v] [(second values)]]
                                 ['[?v ...] [(second values) (second values) nil]]
                                 ['[[?v]] [[(second values)] [nil] [(second values)]]]]]
          (let [observed (observe source)
                q {:find '[?id] :in ['$s binding] :where '[[$s ?id ?v]] :query-source options}]
            (compare! q [rows (value/owned-copy input)]
                      [(:source observed) (value/owned-copy input)] [observed])))
        (let [observed (observe source)
              q '{:find [?a ?b] :in [$a $b]
                  :where [[$a ?a ?v] [$b ?b ?v] [(< ?a ?b)]]}]
          (compare! (assoc q :query-source options) [rows (value/owned-copy rows)]
                    [(:source observed) (:source observed)] [observed])
          (is (= 1 (count (filter #(= :open-session (first %)) @(:events observed)))))
          (is (= {'$a 1 '$b 1} (frequencies (keep #(when (= :plan (first %)) (second %)) @(:events observed))))))))))

(defn eav-semantics! [make-source]
  (let [info (descriptor {:kind :eav :attributes
                           {:item/key {:entity-type :long :value-type :long}
                            :item/value {:entity-type :long :value-type :string}}})
        facts [[1 :item/key 1] [2 :item/key 2] [3 :item/key 3]
               [1 :item/value "a"] [1 :item/value "a"] [2 :item/value nil]
               [3 :item/value "a"] [nil :item/value "missing entity"]]
        eager (filterv #(and (some? (first %)) (some? (nth % 2))) facts)
        source (make-source info facts)]
    (doseq [q ['{:find [?e] :in [$s] :where [[$s ?e :item/value _]]}
               '{:find [?v] :with [?e] :in [$s] :where [[$s ?e :item/value ?v]]}
               '{:find [?e ?key] :in [$s] :where [[$s ?e :item/key ?key] [$s ?e :item/value _]]}
               '{:find [?a ?b] :in [$s]
                 :where [[$s ?a :item/value ?v] [$s ?b :item/value ?v] [(< ?a ?b)]]}
               '{:find [(count ?e) .] :in [$s] :where [[$s ?e :item/value _]]}]]
      (let [observed (observe source)]
        (compare! (assoc q :query-source {:limits {:batch-rows 1}})
                  [eager] [(:source observed)] [observed])))))

(defn native-semantics! [make-source]
  (let [dir (u/tmp-dir (str "source-conformance-" (random-uuid)))
        conn (d/create-conn dir {:native/id {:db/valueType :db.type/long}
                                :native/value {} :native/private {:db/noindex true}}
                            {:wal? false :background-sampling? false})]
    (try
      (let [rows [[1 {:v (byte-array [1])}] [2 {:v (byte-array [2])}]
                  [3 {:v (byte-array [1])}] [4 nil]]
            source (make-source (tuple-descriptor :data) (value/owned-copy rows))]
        (d/transact! conn
          (vec (concat (for [[id _] rows] [:db/add id :native/id id])
                       (for [[id v] rows :when (some? v) a [:native/value :native/private]]
                         [:db/add id a v]))))
        (doseq [q ['{:find [?e ?v] :in [$ $s] :where [[$s ?id ?v] [?e :native/id ?id]]}
                   '{:find [?id] :in [$ $s] :where [[$s ?id ?v] [?id :native/value ?v]]}
                   '{:find [?id] :in [$ $s] :where [[$s ?id ?v] [?id :native/private ?v]]}
                   '{:find [?a ?b] :in [$ $s] :where [[?a :native/value ?v] [$s ?b ?v]]}
                   '{:find [?v (count ?id)] :in [$ $s] :where [[$s ?id ?v] [?id :native/id _]]}]]
          (let [observed (observe source)]
            (compare! q [@conn (value/owned-copy rows)] [@conn (:source observed)] [observed]))))
      (finally (d/close conn) (u/delete-files dir)))))

(defn- acquisition-boundary! [make-source replace-rows!]
  ;; More than one lazy-sequence chunk: small source groups can accidentally
  ;; acquire every view through lookahead even without an explicit boundary.
  (let [n 40
        symbols (mapv #(symbol (str "$s" %)) (range n))
        q {:find '[?id ?v] :in symbols
           :where (mapv #(vector % '?id '?v) symbols)}]
    (testing "all views are pinned before the first planning callback"
      (let [providers (mapv (fn [_] (make-source (tuple-descriptor :string) [[1 "old"]])) symbols)
            acquired (atom 0)
            at-plan (atom nil)
            hook (fn [phase _]
                   (case phase
                     :acquisition (swap! acquired inc)
                     :planning (when (compare-and-set! at-plan nil @acquired)
                                 (doseq [provider providers]
                                   (replace-rows! provider [[2 "new"]])))
                     nil))
            observed (mapv #(observe % hook) providers)]
        (try
          (is (= #{[1 "old"]} (apply d/q q (mapv :source observed))))
          (is (= n @at-plan))
          (is (= #{[2 "new"]} (apply d/q q (mapv :source observed))))
          (finally (run! closed! observed)))))
    (testing "a late acquisition failure prevents every planning callback"
      (let [attempts (atom 0)
            hook (fn [phase _]
                   (when (and (= :acquisition phase) (= n (swap! attempts inc)))
                     (throw (ex-info "secret acquisition failure" {:secret true}))))
            observed (mapv (fn [_] (observe (make-source (tuple-descriptor :string) [[1 "old"]])
                                           hook)) symbols)]
        (try
          (is (= :query-source/provider
                 (:error (error-data #(apply d/q q (mapv :source observed))))))
          (is (= n @attempts))
          (is (not-any? #(#{:plan :open-cursor} (first %))
                        (mapcat #(deref (:events %)) observed)))
          (finally (run! closed! observed)))))))

(defn- concurrent-views! [make-source replace-rows!]
  (doseq [phase [:planning :fetch], cancel? [false true]]
    (testing (str "independent invocations at " phase ", cancel first=" cancel?)
      (let [provider (make-source (tuple-descriptor :string) [[1 "old"]])
            q '[:find ?id ?v :in $a $b :where [$a ?id ?v] [$b ?id ?v]]
            pending (mapv
                      (fn [_]
                        (let [entered (promise) release (CountDownLatch. 1)
                              first? (atom true)]
                          {:entered entered :release release
                           :observed (observe provider
                                       (fn [at request]
                                         (when (and (= at phase) (compare-and-set! first? true false))
                                           (deliver entered request)
                                           (when-not (.await release 10 TimeUnit/SECONDS)
                                             (throw (ex-info "conformance wait expired" {}))))))}))
                      (range 2))
            start (fn [{:keys [observed]}]
                    (future
                      (try {:result (d/q q (:source observed) (:source observed))}
                           (catch clojure.lang.ExceptionInfo e {:error (ex-data e)}))))
            [a b] pending
            workers (atom [])]
        (try
          (let [first-query (start a)
                _ (swap! workers conj first-query)
                first-request (deref (:entered a) 5000 ::timeout)]
            (is (map? first-request))
            (when (map? first-request)
              (replace-rows! provider [[2 "new"]])
              (let [second-query (start b)
                    _ (swap! workers conj second-query)
                    second-request (deref (:entered b) 5000 ::timeout)]
                (is (map? second-request))
                (when (map? second-request)
                  (is (not (identical? (:control first-request) (:control second-request))))
                  (when cancel?
                    (control/cancel! (:control first-request)
                                     {:error :query-source/cancelled :phase :execution}))
                  (.countDown ^CountDownLatch (:release a))
                  (let [result (deref first-query 5000 ::timeout)]
                    (if cancel?
                      (is (= :query-source/cancelled (get-in result [:error :error])))
                      (is (= {:result #{[1 "old"]}} result))))
                  (closed! (:observed a))
                  (is (not (realized? second-query)))
                  (is (nil? (spi/check-active! (:control second-request))))
                  (.countDown ^CountDownLatch (:release b))
                  (is (= {:result #{[2 "new"]}} (deref second-query 5000 ::timeout)))
                  (closed! (:observed b))))))
          (finally
            (doseq [{:keys [release]} pending] (.countDown ^CountDownLatch release))
            (doseq [worker @workers] (future-cancel worker))))))))

(defn lifecycle!
  "Exercise public SPI lifetime boundaries without inspecting adapter internals."
  [make-source replace-rows!]
  (acquisition-boundary! make-source replace-rows!)
  (concurrent-views! make-source replace-rows!)
  (let [info (tuple-descriptor :string)
        rows [[1 "old"] [2 "two"] [3 "three"]]
        source (make-source info rows)
        changed? (atom false)
        observed (observe source (fn [phase _]
                                   (when (and (= phase :planning)
                                              (compare-and-set! changed? false true))
                                     (replace-rows! source [[4 "new"]]))))
        q '{:find [?id ?v] :in [$a $b] :where [[$a ?id ?v] [$b ?id ?v]]}]
    (compare! q [rows rows] [(:source observed) (:source observed)] [observed])
    (compare! q [[[4 "new"]] [[4 "new"]]] [(:source observed) (:source observed)] [observed]))
  (doseq [phase [:acquisition :planning :open :fetch]]
    (testing (str "failure at " phase)
      (let [observed (observe (make-source (tuple-descriptor :string) [[1 "x"]])
                             (fn [at _] (when (= at phase)
                                          (throw (ex-info "secret provider detail" {:secret true})))))
            error (error-data #(d/q '[:find ?id :in $s :where [$s ?id _]] (:source observed)))]
        (is (= :query-source/provider (:error error)))
        (is (= '$s (:source-symbol error)))
        (is (not (contains? error :secret)))
        (closed! observed))))
  (let [observed (observe (make-source (tuple-descriptor :string) [[1 "x"]]))]
    (d/explain {:run? false} '[:find ?id :in $s :where [$s ?id _]] (:source observed))
    (is (= 1 (count (filter #(= :plan (first %)) @(:events observed)))))
    (is (not-any? #(= :open-cursor (first %)) @(:events observed)))
    (closed! observed))
  (let [observed (observe (make-source (tuple-descriptor :string) [[1 "x"]])
                         (fn [phase request]
                           (when (= :fetch phase)
                             (control/cancel! (:control request)
                                              {:error :query-source/cancelled
                                               :phase :execution}))))]
    (is (= :query-source/cancelled
           (:error (error-data #(d/q '[:find ?id :in $s :where [$s ?id _]]
                                     (:source observed))))))
    (closed! observed))
  (doseq [[options category] [[{:limits {:result-rows 1}} :query-source/limit]
                              [{:limits {:batch-bytes 1}} :query-source/limit]]]
    (let [observed (observe (make-source (tuple-descriptor :string) [[1 "x"] [2 "y"]]))
          q {:find '[?id] :in '[$s] :where '[[$s ?id _]] :query-source options}]
      (is (= category (:error (error-data #(d/q q (:source observed))))))
      (closed! observed)))
  (let [observed (observe (make-source (tuple-descriptor :string)
                                       (mapv #(vector % "x") (range 20))))
        q '{:find [?id] :in [$s] :where [[$s ?id _]] :limit 1
            :query-source {:limits {:batch-rows 1 :in-flight-batches 1}}}]
    (is (= 1 (count (d/q q (:source observed)))))
    (is (= [[:batch 1]] (filter #(= :batch (first %)) @(:events observed))))
    (closed! observed)))

(defn key-semantics!
  "Pin expected key behavior independently of the eager comparison oracle."
  [make-source]
  (let [dir (u/tmp-dir (str "source-key-conformance-" (random-uuid)))
        ;; Native storage canonicalizes floating zero. Native expectations use
        ;; the stored values, while eager/source fixtures retain both signs.
        conn (d/create-conn dir {:native/value {}
                                :native/zero {:db/valueType :db.type/double}}
                            {:wal? false :background-sampling? false})
        rows [[1 :native/value 1.0M] [2 :native/value 1.00M]
              [3 :native/zero 0.0] [4 :native/zero -0.0]
              [5 :native/value (long 1)] [6 :native/value (int 1)]]
        info (descriptor {:kind :tuple
                          :columns [{:type :long :nullable? false}
                                    {:type :keyword :nullable? false}
                                    {:type :data :nullable? false}]})
        provider (make-source info rows)]
    (try
      (d/transact! conn (mapv (fn [[id a v]] [:db/add id a v]) rows))
      (let [stored (mapv (juxt :e :a :v) (d/datoms @conn :eav))]
        (doseq [[id attr v] rows
                [binding input] [['?v v] ['[?v] [v]]
                                 ['[?v ...] [v]] ['[[?v]] [[v]]]
                                 ['[?v ...] [v v]] ['[[?v]] [[v] [v]]]]]
          (testing (str "exact input " id " " binding " " (pr-str input))
            (let [observed (observe provider)
                  q {:find '[?id] :in ['$s binding]
                     :where [['$s '?id attr '?v]]}
                  native-result
                  (set (for [[e a value] stored
                              :when (and (= a attr) (QueryKey/equals v value))]
                         [e]))]
              (doseq [[label source expected]
                      [[:eager rows #{[id]}]
                       [:native @conn native-result]
                       [:source (:source observed) #{[id]}]]]
                (testing (name label)
                  (is (= expected (d/q q source input)))))
              ;; The same native input join through source-backed execution.
              (let [mixed (-> q
                              (update :in conj '$anchor)
                              (update :where conj '[$anchor 1 _ _]))]
                (is (= native-result (d/q mixed @conn input (:source observed)))))
              (closed! observed))))
        (doseq [shape [{:keys '[value]} {:strs '[value]} {:syms '[value]}]]
          (let [q (merge '{:find [?v] :in [$s] :where [[$s _ _ ?v]]} shape)
                observed (observe provider)]
            (doseq [[label source] [[:eager rows] [:native @conn]
                                    [:source (:source observed)]]]
              (testing (name label)
                (is (= (if (= label :native) 5 6) (count (d/q q source))))))
            (closed! observed)))
        (doseq [input [[[3 -0.0 :wrong] [4 -0.0 :wrong-too]]
                       [[3 0.0 :a] [3 -0.0 :wrong] [4 0.0 :b]]]]
          (let [observed (observe provider)
                q '[:find ?tag :in $s [[?e ?v ?tag]]
                    :where [$s ?e :native/zero ?v]]
                expected (set (for [[e v tag] input, [id a value] stored
                                    :when (and (= e id) (= a :native/zero)
                                               (QueryKey/equals v value))]
                                [tag]))]
            (is (= expected (d/q q stored input) (d/q q @conn input)))
            (let [mixed {:find '[?tag] :in '[$s [[?e ?v ?tag]] $anchor]
                         :where '[[$s ?e :native/zero ?v] [$anchor 1 _ _]]}]
              (is (= expected (d/q mixed @conn input (:source observed)))))
            (closed! observed))))
      (finally (d/close conn) (u/delete-files dir))))
  (let [rows [[1 1] [1 2] [2 2] [3 4]]
        info (descriptor {:kind :tuple
                          :columns [{:type :long :nullable? false}
                                    {:type :long :nullable? false}]})
        observed (observe (make-source info rows))]
    (doseq [source [rows (:source observed)]]
      (is (= #{[1] [2]} (d/q '[:find ?v :in $s :where [$s ?v ?v]] source))))
    (closed! observed)))
