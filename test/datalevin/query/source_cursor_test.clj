(ns datalevin.query.source-cursor-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.query.source.cursor :as cursor]
            [datalevin.query.access :as access]
            [datalevin.query.plan :as plan]
            [datalevin.query.source :as source]
            [datalevin.query.source.access :as source-access]
            [datalevin.query.source.binding :as binding]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.session :as session])
  (:import [java.util ArrayList Date Iterator Random]
           [java.util.concurrent CountDownLatch TimeUnit]))

(def ^:private descriptor
  {:spi-versions #{2} :source-instance-id :cursor/store :compute-context-id :local
   :definition-id :rows :definition-fingerprint "rows:v2" :features #{} :required-features #{}
   :pattern-contract {:kind :eav :attributes {:row/value {:entity-type :long :value-type :string}}}
   :logical-schema {:domains {:row {:entity-type :long}}
                    :attributes {:row/value {:entity-domain :row :cardinality :many
                                              :absence :no-fact :reference-domain nil}}}
   :scope {:id "scope:rows" :kind :full :parameters {}}
   :completion {:domain :finite :extent :all-matches} :read-capabilities #{:snapshot}})

(def ^:private requires [{:variable '?e :type :long :nullable? false}])
(def ^:private columns (conj requires {:variable '?v :type :string :nullable? false}))

(defn- caught [f]
  (try (f) nil (catch Throwable e e)))

(defn- error [f] (ex-data (caught f)))

(defn- batch! [request columns rows ids exhausted?]
  (let [bytes (if (some? ids) (source/batch-charge-bytes columns rows ids)
                  (source/batch-charge-bytes columns rows))]
    (cond-> {:columns columns :rows rows :row-count (count rows) :byte-count bytes :exhausted? exhausted?}
      (some? ids) (assoc :binding-ids ids)
      (pos? bytes) (assoc :reservation (source/reserve! (:control request)
                                         {:retained-bytes bytes :in-flight-batches 1})))))

(defn- fixture [runtime events config]
  (let [descriptor (get config :descriptor descriptor)
        requires (get config :requires requires)
        columns (get config :columns columns)
        provider
        (reify source/IQuerySource
          (-source-info [_] (throw (Exception. "already captured")))
          (-plan-source [_ _ _] (throw (Exception. "already planned")))
          (-open-session [_ _]
            (swap! events conj [:session])
            (reify source/IQuerySourceSession
              (-session-info [_]
                (cond-> (merge {:spi-version 2 :features #{} :session-id :session
                        :source-instance-id (:source-instance-id descriptor) :compute-context-id :local
                        :authorization-token :policy :consistency :snapshot :acquired-at-ms 0 :data-version :v1
                        :consumption {:opens (get config :opens :many) :max-open-cursors 1}
                        :request-constraints {:max-concurrent-requests 1 :rate-limit nil}}
                               (:info config))
                  (contains? (:info config) :consumption-groups) (dissoc :consumption)))
              (-open-cursor [_ open]
                (swap! events conj [:open open])
                (when-let [f (:open config)] (f open))
                (let [n (atom -1) tokens (atom [])]
                  (reify source/IQuerySourceCursor
                    (-next-batch! [_ fetch]
                      (swap! events conj [:fetch open])
                      (let [batch (if-let [f (:fetch config)] (f open fetch (swap! n inc))
                                      (let [bindings (:bindings open)
                                            rows (if bindings
                                                   (mapv (fn [{:keys [values]}] [(first values) (str (first values))])
                                                         bindings)
                                                   [[1 "one"] [2 "two"]])]
                                        (batch! fetch columns rows (when bindings (mapv :binding-id bindings)) true)))]
                        (when-let [token (:reservation batch)] (swap! tokens conj token))
                        batch))
                    (-cursor-metrics [_] (throw (Exception. "metrics are optional to this execution primitive")))
                    (-close-cursor! [_]
                      (swap! events conj [:close open])
                      (doseq [token @tokens] (source/release! token))
                      (when-let [f (:close config)] (f open))))))
              (-cancel-session! [_ reason]
                (swap! events conj [:cancel reason])
                (when-let [f (:cancel config)] (f reason)))
              (-close-session! [_] (swap! events conj [:session-close])))))
        prepared (session/prepare! runtime [{:symbol '$s :source provider :descriptor descriptor}] nil)
        [group] (session/acquire! runtime prepared)
        ids (delay (binding/id-space! runtime))]
    {:group group :ids ids
     :plan (fn [dependent? & [overrides]]
             (let [request (assoc (:context group) :source-symbol '$s :region-id 3 :scope-id "scope:rows"
                                  :source-descriptor descriptor
                                  :clauses [{:id 0 :kind :pattern
                                             :terms [[:var '?e] [:const :row/value] [:var '?v]]}]
                                  :available-inputs (if dependent? requires []) :bindings {}
                                  :required-vars #{'?e '?v} :max-alternatives 32
                                  :demand {:quality :exact :ordering [] :offset 0 :limit nil})
                   alternative (merge {:id :lookup :plan (Object.) :covers #{0} :prefilters #{}
                                       :requires (if dependent? requires []) :columns columns
                                       :scope-id "scope:rows" :completion :all-bindings
                                       :access {:kind (if dependent? :entity-lookup :attribute-enumeration) :anchor-clause 0}
                                       :properties {:replay :unstable :ordering [] :blocking-state :none}
                                       :constraints {:max-binding-rows 100 :max-binding-bytes 65536
                                                     :max-request-binding-rows 1}
                                       :estimate nil :explain {:label "private"}} overrides)]
               (source-access/candidate! runtime request alternative (:consumption group)
                                        (cursor/method runtime group ids))))}))

(defn- events-of [events kind] (filterv #(= kind (first %)) @events))
(defn- step [p input-cols]
  (plan/access-step (:expr p) (:path p) (access/complete-demand :exact nil) (:bounds p) (:work p) input-cols))

(deftest scheduled-native-seed-and-dependent-access-preserve-all-outer-witnesses
  (let [events (atom [])]
    (rt/with-runtime {:limits {:binding-rows 2}}
      (fn [runtime]
        (let [{:keys [plan]} (fixture runtime events {})
              p (plan true)
              outers [[1 :a] [2 :b] [3 :c] [1 :d] [1 :a] [4 :e] [3 :f]]
              native (plan/map->InitStep {:in #{} :cols ['?e '?tag]
                                         :result (mapv object-array outers)})
              operator (source-access/core-operator! runtime 3 0 []
                         [{:variable '?e :type :long :nullable? false}
                          {:variable '?tag :type :keyword :nullable? false}] #{} native)
              request (get-in p [:path :options :capture :request])
              schedule (source-access/schedule! runtime 3 [] [request] [p] [operator])]
          (is (nil? (source-access/validate-query! runtime [schedule])))
          (is (empty? (events-of events :open)))
          (let [[cols output]
                (reduce (fn [[cols tuples] {:keys [kind value]}]
                          (let [s (if (= :core kind) (:step value) (step value cols))]
                            [(:cols s) (plan/-execute s nil tuples)]))
                        [[] nil] (:operators schedule))]
            (is (= ['?e '?tag '?v] cols))
            (is (= (frequencies (map (fn [[e tag]] [e tag (str e)]) outers))
                   (frequencies (map vec output)))))
          (is (= [[1] [2] [3] [4]]
                 (mapv :values (mapcat :bindings (map second (events-of events :open))))))
          (is (= 2 (count (events-of events :open))))
          (is (= 2 (count (events-of events :close)))))))))

(deftest input-free-shared-step-uses-the-exact-open-shape-without-id-state
  (let [events (atom []) saved (atom nil)]
    (rt/with-runtime {}
      (fn [runtime]
        (reset! saved runtime)
        (let [{:keys [plan ids group]} (fixture runtime events {})
              p (plan false) output (plan/-execute (step p []) nil nil)
              open (second (first (events-of events :open)))]
          (is (= [[1 "one"] [2 "two"]] (mapv vec output)))
          (is (false? (realized? ids)))
          (is (= (into (set (keys (:context group)))
                       [:alternative-id :plan :source-symbol :definition-fingerprint :scope-id :region-id])
                 (set (keys open))))
          (is (identical? (get-in p [:path :options :capture :alternative :plan]) (:plan open)))
          (is (= [3 "scope:rows" "rows:v2"] ((juxt :region-id :scope-id :definition-fingerprint) open)))
          (is (= 1 (count (events-of events :close))))
          (is (pos? (:retained-bytes (control/usage (:control runtime))))))))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest shared-dependent-step-collects-all-keys-before-opening-and-preserves-multiplicity
  (let [events (atom [])]
    (rt/with-runtime {:limits {:binding-rows 2}}
      (fn [runtime]
        (let [{:keys [plan]} (fixture runtime events {})
              outers [[1 :a] [2 :b] [3 :c] [1 :d] [1 :a] [4 :e] [3 :f]]
              output (plan/-execute (step (plan true) ['?e '?tag]) nil outers)
              opens (map second (events-of events :open))
              bindings (mapcat :bindings opens)]
          (is (= (frequencies (map (fn [[e tag]] [e tag (str e)]) outers))
                 (frequencies (map vec output))))
          (is (= [[1] [2] [3] [4]] (mapv :values bindings)))
          (is (= [0 1 2 3] (mapv :binding-id bindings)))
          (is (= 2 (count opens)))
          (is (= 2 (count (events-of events :close))))
          (is (zero? (:in-flight-batches (control/usage (:control runtime)))))
          ;; Another occurrence in the same invocation must really read again
          ;; and must not recycle the first occurrence's IDs.
          (is (= [[1 "1"]] (mapv vec (plan/-execute (step (plan true) ['?e]) nil [[1]]))))
          (is (= [4] (mapv :binding-id (:bindings (second (last (events-of events :open))))))))))))

(deftest generated-shared-access-results-match-an-eager-multiplicity-oracle
  (let [random (Random. 734)]
    (dotimes [_ 40]
      (let [events (atom [])
            outers (vec (repeatedly (+ 1 (.nextInt random 20)) #(vector (long (.nextInt random 9)) (.nextInt random 3))))
            matches (into {} (for [e (range 9)] [e (vec (repeat (.nextInt random 4) (str e)))]))]
        (rt/with-runtime {:limits {:binding-rows 3}}
          (fn [runtime]
            (let [{:keys [plan]} (fixture runtime events
                                   {:fetch (fn [open fetch _]
                                             (let [pairs (for [{:keys [binding-id values]} (reverse (:bindings open))
                                                               v (matches (first values))]
                                                           [binding-id [(first values) v]])]
                                               (batch! fetch columns (mapv second pairs) (mapv first pairs) true)))})
                  result (plan/-execute (step (plan true) ['?e '?tag]) nil outers)]
              (is (= (frequencies (for [[e tag] outers v (matches e)] [e tag v]))
                     (frequencies (map vec result)))))
            (is (= (count (distinct (map first outers)))
                   (count (mapcat (comp :bindings second) (events-of events :open)))))))))))

(deftest dependent-final-batches-complete-zero-match-keys-and-stop-advancing
  (let [events (atom []) result (atom [])]
    (rt/with-runtime {}
      (fn [runtime]
        (let [{:keys [group ids plan]} (fixture runtime events
                                       {:fetch (fn [_ fetch n]
                                                 (is (zero? n)) (batch! fetch columns [] [] true))})]
          (cursor/lookup! runtime group (plan true) ids [{'?e 1} {'?e 2}]
                          #(swap! result conj [%1 %2]))
          (is (empty? @result))
          (is (= 1 (count (events-of events :fetch))))
          (is (zero? (:in-flight-batches (control/usage (:control runtime))))))))))

(deftest empty-dependent-input-opens-nothing
  (let [events (atom [])]
    (rt/with-runtime {}
      (fn [runtime]
        (let [{:keys [plan]} (fixture runtime events {})]
          (is (empty? (plan/-execute (step (plan true) ['?e]) nil [])))
          (is (empty? (events-of events :open))))))))

(deftest one-shot-splits-and-oversized-keys-fail-before-opening
  (doseq [[limits expected] [[{:binding-rows 1} :query-source/unsupported]
                             [{:binding-bytes 1} :query-source/limit]]]
    (let [events (atom [])
          result (error #(rt/with-runtime {:limits limits}
                           (fn [runtime]
                             (let [{:keys [plan]} (fixture runtime events {:opens :once})]
                               (plan/-execute (step (plan true) ['?e]) nil [[1] [2]])))))]
      (is (= expected (:error result)))
      (is (empty? (events-of events :open))))))

(deftest once-attempt-is-spent-even-when-provider-open-fails
  (let [events (atom [])]
    (is (= :query-source/provider
           (:error (error #(rt/with-runtime {}
                             (fn [runtime]
                               (let [{:keys [group plan]} (fixture runtime events
                                                          {:opens :once :open (fn [_] (throw (Exception. "secret")))})
                                     p (plan false)
                                     e (caught (fn [] (cursor/scan! runtime group p identity)))]
                                 (is (= :query-source/provider (:error (ex-data e))))
                                 ;; Cancellation must not hide that the failed
                                 ;; one-shot attempt was spent permanently.
                                 (is (= 1 (count (events-of events :open))))
                                 (is (control/cancelled? (:control runtime)))
                                 (throw e))))))))))

(deftest malformed-batches-fail-before-first-emission-and-release-provider-state
  (doseq [dependent? [false true]
          change [:wrong-columns :count :bytes :empty :nil-final :missing-id :foreign-id :echo :nil-batch]]
    (when (or dependent? (not (#{:foreign-id :echo} change)))
      (let [events (atom []) emitted (atom []) saved (atom nil)
            result (error #(rt/with-runtime {}
                             (fn [runtime]
                               (reset! saved runtime)
                               (let [{:keys [plan group ids]} (fixture runtime events
                                                               {:fetch
                                                                (fn [open fetch _]
                                                                  (let [id (get-in open [:bindings 0 :binding-id])
                                                                        batch (batch! fetch columns [[1 "one"]]
                                                                                      (when dependent? [id]) true)]
                                                                    (case change
                                                                      :wrong-columns (assoc batch :columns [])
                                                                      :count (assoc batch :row-count 9)
                                                                      :bytes (update batch :byte-count inc)
                                                                      :empty (assoc batch :rows [] :row-count 0 :exhausted? false)
                                                                      :nil-final (assoc batch :exhausted? nil)
                                                                      :missing-id (if dependent? (dissoc batch :binding-ids)
                                                                                      (assoc batch :binding-ids [0]))
                                                                      :foreign-id (assoc batch :binding-ids [(inc id)])
                                                                      :echo (assoc batch :rows [[2 "one"]])
                                                                      :nil-batch nil)))})]
                                 (if dependent?
                                   (cursor/lookup! runtime group (plan true) ids [{'?e 1}]
                                                   (fn [outer row] (swap! emitted conj [outer row])))
                                   (cursor/scan! runtime group (plan false) (fn [row] (swap! emitted conj row))))))))]
        (is (= (if (= :nil-batch change) :query-source/incomplete :query-source/invalid) (:error result))
            (str [dependent? change]))
        (is (empty? @emitted))
        (is (= 1 (count (events-of events :close))))
        (is (every? zero? (vals (control/usage (:control @saved)))))))))

(deftest premature-end-and-known-truncation-never-become-successful-prefixes
  (doseq [ending [:eof :provider-error :reduced]]
    (let [events (atom []) saved (atom nil)
          result (caught #(rt/with-runtime {}
                            (fn [runtime]
                              (reset! saved runtime)
                              (let [{:keys [group plan]} (fixture runtime events
                                                         {:fetch (fn [_ fetch n]
                                                                   (if (zero? n)
                                                                     (batch! fetch columns [[1 "one"]] nil false)
                                                                     (if (= :eof ending) nil
                                                                         (throw (ex-info "private truncation"
                                                                                         {:error :query-source/incomplete
                                                                                          :region-id 999 :secret :private})))) )})]
                                (cursor/scan! runtime group (plan false)
                                              (if (= :reduced ending) (fn [_] (reduced :stop)) identity))))))]
      (is (= :query-source/incomplete (:error (ex-data result))))
      (is (= 3 (:region-id (ex-data result))))
      (is (not (.contains (str result) "private truncation")))
      (is (= 1 (count (events-of events :close))))
      (is (every? zero? (vals (control/usage (:control @saved))))))))

(deftest late-cancellation-handoff-still-owns-and-closes-the-returned-cursor
  (let [events (atom []) result (error #(rt/with-runtime {}
                                        (fn [runtime]
                                          (let [{:keys [group plan]} (fixture runtime events
                                                                     {:open (fn [_]
                                                                              (control/cancel! (:control runtime)
                                                                                {:error :query-source/cancelled
                                                                                 :phase :open}))})]
                                            (cursor/scan! runtime group (plan false) identity)))))]
    (is (= :query-source/cancelled (:error result)))
    (is (empty? (events-of events :fetch)))
    (is (= 1 (count (events-of events :close))))))

(deftest blocked-fetch-is-cancelled-through-the-acquired-session
  (let [events (atom []) fetching (CountDownLatch. 1) cancelled (CountDownLatch. 1)
        ctl (promise)
        worker (future
                 (error #(rt/with-runtime {}
                           (fn [runtime]
                             (deliver ctl (:control runtime))
                             (let [{:keys [group plan]} (fixture runtime events
                                                        {:cancel (fn [_] (.countDown cancelled))
                                                         :fetch (fn [_ _ _]
                                                                  (.countDown fetching)
                                                                  (is (.await cancelled 5 TimeUnit/SECONDS))
                                                                  nil)})]
                               (cursor/scan! runtime group (plan false) identity))))))]
    (is (.await fetching 5 TimeUnit/SECONDS))
    (control/cancel! @ctl {:error :query-source/cancelled :phase :fetch})
    (is (= :query-source/cancelled (:error (deref worker 5000 {:error :hung}))))
    (is (= 1 (count (events-of events :close))))))

(deftest invalid-captured-identities-reject-before-provider-open
  (doseq [alter [(fn [p] (assoc-in p [:path :options :source-symbol] '$foreign))
                 (fn [p] (assoc-in p [:path :options :region-id] 4))
                 (fn [p] (assoc-in p [:path :options :scope-id] "scope:foreign"))
                 (fn [p] (assoc-in p [:path :options :alternative-id] :foreign))
                 (fn [p] (assoc-in p [:path :options :capture :request :source-descriptor]
                                    (assoc descriptor :definition-fingerprint "foreign")))
                 (fn [p] (assoc-in p [:path :options :capture :request :authorization] :foreign))
                 (fn [p] (assoc-in p [:path :options :consumption] nil))]]
    (let [events (atom [])]
      (rt/with-runtime {}
        (fn [runtime]
          (let [{:keys [plan group]} (fixture runtime events {})]
            (is (= :query-source/invalid
                   (:error (error #(cursor/scan! runtime group (alter (plan false)) identity)))))
            (is (empty? (events-of events :open)))))))))

(deftest cleanup-preserves-primary-errors-and-redacts-suppressed-close-errors
  (let [events (atom [])
        e (caught #(rt/with-runtime {}
                     (fn [runtime]
                       (let [{:keys [plan group]} (fixture runtime events
                                                  {:close (fn [_] (throw (Exception. "private close")))})]
                         (cursor/scan! runtime group (plan false)
                                       (fn [_] (throw (ex-info "private emitter" {:error :query-source/limit}))))))))]
    (is (= :query-source/limit (:error (ex-data e))))
    (is (= 1 (count (.getSuppressed ^Throwable e))))
    (is (= :close (:phase (ex-data (first (.getSuppressed ^Throwable e))))))
    (is (not (.contains (str e) "private emitter")))
    (is (not (.contains (str (first (.getSuppressed ^Throwable e))) "private close")))
    (is (= 1 (count (events-of events :close))))))

(deftest borrowed-batches-stay-reserved-during-delivery-and-final-rows-are-not-lost
  (let [events (atom []) rows (atom [])]
    (rt/with-runtime {}
      (fn [runtime]
        (let [{:keys [group plan]} (fixture runtime events
                                   {:fetch (fn [_ fetch n]
                                             (batch! fetch columns [[(long n) (str n)]] nil (= 2 n)))})]
          (cursor/scan! runtime group (plan false)
                        (fn [row]
                          (is (= 1 (:in-flight-batches (control/usage (:control runtime)))))
                          (swap! rows conj row)))
          (is (= [[0 "0"] [1 "1"] [2 "2"]] @rows))
          (is (= 3 (count (events-of events :fetch))))
          (is (zero? (:in-flight-batches (control/usage (:control runtime))))))))))

(deftest shared-output-owns-mutable-row-values-outer-witnesses-and-their-metadata
  (let [events (atom []) bytes (byte-array [1 2]) date (Date. 100)
        meta-bytes (byte-array [3]) outer-bytes (byte-array [4])
        data (with-meta {:bytes bytes :date date} {:owned meta-bytes})
        data-columns (assoc-in columns [1 :type] :data)
        data-descriptor (assoc-in descriptor [:pattern-contract :attributes :row/value :value-type] :data)]
    (rt/with-runtime {}
      (fn [runtime]
        (let [{:keys [plan]} (fixture runtime events
                             {:columns data-columns :descriptor data-descriptor
                              :fetch (fn [open fetch _]
                                       (batch! fetch data-columns [[1 data]]
                                               [(get-in open [:bindings 0 :binding-id])] true))
                              :close (fn [_]
                                       (aset-byte bytes 0 (byte 9))
                                       (.setTime date 999)
                                       (aset-byte meta-bytes 0 (byte 9))
                                       (aset-byte outer-bytes 0 (byte 9)))})
              output (plan/-execute (step (plan true) ['?e '?outer]) nil [[1 outer-bytes]])
              [e outer value] (vec (first output))]
          (is (= 1 e))
          (is (= [4] (vec outer)))
          (is (= [1 2] (vec (:bytes value))))
          (is (= 100 (.getTime ^Date (:date value))))
          (is (= [3] (vec (:owned (meta value)))))
          (is (zero? (:in-flight-batches (control/usage (:control runtime))))))))))

(deftest shared-merge-compares-returned-overlapping-bytes-with-query-key-equality
  (let [events (atom []) byte-columns (assoc-in columns [1 :type] :bytes)
        d (assoc-in descriptor [:pattern-contract :attributes :row/value :value-type] :bytes)]
    (rt/with-runtime {}
      (fn [runtime]
        (let [{:keys [plan]} (fixture runtime events
                             {:columns byte-columns :descriptor d
                              :fetch (fn [open fetch _]
                                       (batch! fetch byte-columns [[1 (byte-array [1])]]
                                               [(get-in open [:bindings 0 :binding-id])] true))})
              output (plan/-execute (step (plan true) ['?e '?v '?tag]) nil
                                    [[1 (byte-array [1]) :match] [1 (byte-array [2]) :miss]])]
          (is (= [[1 [1] :match]] (mapv (fn [tuple] [(aget ^objects tuple 0)
                                                    (vec (aget ^objects tuple 1))
                                                    (aget ^objects tuple 2)]) output)))
          (is (= 1 (count (events-of events :open)))))))))

(deftest input-free-with-upstream-tuples-scans-once-and-keeps-all-witnesses
  (let [events (atom [])]
    (rt/with-runtime {}
      (fn [runtime]
        (let [{:keys [plan ids]} (fixture runtime events {})
              output (plan/-execute (step (plan false) ['?tag]) nil [[:a] [:b] [:a]])]
          (is (= (frequencies [[:a 1 "one"] [:b 1 "one"] [:a 1 "one"]
                               [:a 2 "two"] [:b 2 "two"] [:a 2 "two"]])
                 (frequencies (map vec output))))
          (is (= 1 (count (events-of events :open))))
          (is (false? (realized? ids))))))))

(deftest input-free-keeps-zero-column-outer-multiplicity-and-still-exhausts-on-empty-input
  (doseq [outers [nil [] [[]] [[] [] []]]]
    (let [events (atom [])]
      (rt/with-runtime {}
        (fn [runtime]
          (let [{:keys [plan ids]} (fixture runtime events {})
                output (plan/-execute (step (plan false) []) nil outers)
                witnesses (if (nil? outers) 1 (count outers))]
            (is (= (frequencies (mapcat (fn [_] [[1 "one"] [2 "two"]]) (range witnesses)))
                   (frequencies (map vec output))))
            (is (= 1 (count (events-of events :open))))
            (is (= 1 (count (events-of events :fetch))))
            (is (false? (realized? ids)))))))))

(deftest shared-pipe-execution-uses-the-same-batched-path
  (let [events (atom []) sink (ArrayList.)]
    (rt/with-runtime {}
      (fn [runtime]
        (let [{:keys [plan]} (fixture runtime events {})]
          (plan/-execute-pipe (step (plan true) ['?e]) nil [[1] [2] [1]] sink)
          (is (= (frequencies [[1 "1"] [2 "2"] [1 "1"]]) (frequencies (map vec sink))))
          (is (= 1 (count (events-of events :open)))))))))

(deftest provider-explicit-groups-are-carried-only-as-captured
  (let [events (atom [])]
    (rt/with-runtime {:limits {:binding-rows 2}}
      (fn [runtime]
        (let [{:keys [plan group ids]} (fixture runtime events
                                   {:info {:consumption-groups [{:id :scan :opens :once :max-open-cursors 1}
                                                                {:id :lookups :opens :many :max-open-cursors 1}]}})]
          (cursor/scan! runtime group
                        (plan false {:properties {:replay :identical :ordering [] :blocking-state :none
                                                  :consumption-group :scan}}) identity)
          (cursor/lookup! runtime group
                          (plan true {:properties {:replay :unstable :ordering [] :blocking-state :none
                                                   :consumption-group :lookups}})
                          ids [{'?e 1} {'?e 2} {'?e 3}] (fn [_ _]))
          (is (= [:scan :lookups :lookups]
                 (mapv (comp :consumption-group second) (events-of events :open))))
          (is (= 3 (count (events-of events :close)))))))))

(deftest bounded-shared-output-fails-without-returning-a-prefix-and-rolls-back
  (let [events (atom []) saved (atom nil)
        result (error #(rt/with-runtime {}
                         (fn [runtime]
                           (reset! saved runtime)
                           (let [{:keys [plan]} (fixture runtime events
                                                {:fetch (fn [_ fetch n]
                                                          (batch! fetch columns
                                                                  [[(long n) (apply str (repeat 512 \x))]] nil false))})
                                 p (plan false)
                                 ;; Leave enough room for several rows. Exhaustion
                                 ;; is deliberately never promised by this source.
                                 fill (source/reserve! (:control runtime)
                                        {:retained-bytes (- (:retained-bytes (control/limits (:control runtime)))
                                                            (:retained-bytes (control/usage (:control runtime))) 20000)})
                                 before (control/usage (:control runtime))]
                             (try (let [failure (caught (fn [] (plan/-execute (step p []) nil nil)))]
                                    ;; Prefix output must be released now, even
                                    ;; before the invocation's final cleanup.
                                    (is (= before (control/usage (:control runtime))))
                                    (throw failure))
                                  (finally (source/release! fill)))))))]
    (is (= :query-source/limit (:error result)))
    (is (= :retained-bytes (:category result)))
    (is (<= 1 (count (events-of events :fetch)) 20))
    (is (= 1 (count (events-of events :close))))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest provider-column-metadata-uses-the-actual-public-batch-charge
  (doseq [dependent? [false true]]
    (let [events (atom [])
          annotated (with-meta columns {:provider/annotation (apply str (repeat 500 \x))})]
      (rt/with-runtime {}
        (fn [runtime]
          (let [{:keys [group plan ids]} (fixture runtime events
                                         {:fetch (fn [open fetch _]
                                                   (batch! fetch annotated [[1 "one"]]
                                                           (when dependent? [(get-in open [:bindings 0 :binding-id])]) true))})
                emitted (atom [])]
            (if dependent?
              (cursor/lookup! runtime group (plan true) ids [{'?e 1}]
                              (fn [_ row] (swap! emitted conj row)))
              (cursor/scan! runtime group (plan false) #(swap! emitted conj %)))
            (is (= [[1 "one"]] @emitted))
            (is (zero? (:in-flight-batches (control/usage (:control runtime)))))))))))

(deftest oversized-row-envelopes-reject-before-walking-values-or-provider-annotations
  (let [events (atom [])
        result (error #(rt/with-runtime {:limits {:batch-rows 1}}
                         (fn [runtime]
                           (let [{:keys [group plan]} (fixture runtime events
                                                      {:fetch (fn [_ _ _]
                                                                {:columns columns :rows [[(Object.)] [(Object.)]]
                                                                 :row-count 2 :byte-count 0 :exhausted? true
                                                                 :provider/private (Object.)})})]
                             (cursor/scan! runtime group (plan false) identity)))))]
    (is (= :query-source/limit (:error result)))
    (is (= :batch-rows (:category result)))
    (is (= 1 (count (events-of events :close))))))

(deftest batch-envelope-and-empty-column-metadata-remain-bounded
  (doseq [where [:map :columns :annotation]]
    (let [events (atom []) saved (atom nil)
          result (error #(rt/with-runtime {}
                           (fn [runtime]
                             (reset! saved runtime)
                             (let [{:keys [group plan]} (fixture runtime events
                                                        {:fetch
                                                         (fn [_ fetch _]
                                                           (let [batch (batch! fetch columns [] nil true)
                                                                 payload {:provider/large (apply str (repeat 30000 \x))
                                                                          :provider/unsupported (Object.)}]
                                                             (case where
                                                               :map (with-meta batch payload)
                                                               :columns (assoc batch :columns (with-meta columns payload))
                                                               :annotation (assoc batch :provider/metadata payload))))})
                                   p (plan false)
                                   fill (source/reserve! (:control runtime)
                                          {:retained-bytes (- (:retained-bytes (control/limits (:control runtime)))
                                                              (:retained-bytes (control/usage (:control runtime))) 15000)})]
                               (try (cursor/scan! runtime group p identity)
                                    (finally (source/release! fill)))))))]
      (is (= :query-source/limit (:error result)) (str where))
      (is (= :retained-bytes (:category result)))
      (is (= 1 (count (events-of events :close))))
      (is (every? zero? (vals (control/usage (:control @saved))))))))

(deftest reservation-must-be-live-and-transferred-once
  (doseq [mode [:missing :reuse]]
    (let [events (atom []) previous (atom nil)
          result (error #(rt/with-runtime {}
                           (fn [runtime]
                             (let [{:keys [group plan]} (fixture runtime events
                                                        {:fetch (fn [_ fetch n]
                                                                  (if (zero? n)
                                                                    (let [batch (batch! fetch columns [[1 "one"]] nil false)]
                                                                      (reset! previous batch)
                                                                      (if (= :missing mode) (dissoc batch :reservation) batch))
                                                                    (assoc @previous :exhausted? true)))})]
                               (cursor/scan! runtime group (plan false) identity)))))]
      (is (= :query-source/invalid (:error result)))
      (is (= 1 (count (events-of events :close)))))))

(deftest reused-upstream-tuple-and-value-buffers-are-owned-before-iteration-advances
  (doseq [dependent? [false true]]
    (let [events (atom []) buffer (byte-array [0]) tuple (object-array [1 buffer])
          input (reify Iterable
                  (iterator [_]
                    (let [n (atom 0)]
                      (reify Iterator
                        (hasNext [_] (< @n 40))
                        (next [_]
                          (aset-byte buffer 0 (byte (swap! n inc)))
                          tuple)))))]
      (rt/with-runtime {}
        (fn [runtime]
          (let [{:keys [plan]} (fixture runtime events {})
                output (plan/-execute (step (plan dependent?) ['?e '?tag]) nil input)]
            (is (= (set (range 1 41)) (set (map #(aget ^bytes (aget ^objects % 1) 0) output))))
            (is (= 40 (count output)))
            (is (= 1 (count (events-of events :open))))))))))
