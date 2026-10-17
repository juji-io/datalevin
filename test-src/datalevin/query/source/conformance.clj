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
           [java.util Date HashSet Objects Random UUID]
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

(defrecord ObservedSession [delegate events hook request envelopes]
  spi/IQuerySourceSession
  (-session-info [_]
    ((get envelopes :session-info identity) (spi/-session-info delegate)))
  (-open-cursor [_ request]
    (hook :open request)
    (let [cursor (spi/-open-cursor delegate request)
          wrapped
          (reify spi/IQuerySourceCursor
            (-next-batch! [_ batch-request]
              (hook :fetch batch-request)
              (let [batch (spi/-next-batch! cursor batch-request)]
                (swap! events conj [:batch (:row-count batch)])
                (hook :fetched batch-request)
                batch))
            (-cursor-metrics [_] (spi/-cursor-metrics cursor))
            (-close-cursor! [_]
              (hook :close-cursor request)
              (swap! events conj [:close-cursor])
              (spi/-close-cursor! cursor)))]
      (swap! events conj [:open-cursor])
      (hook :opened request)
      wrapped))
  (-cancel-session! [_ reason]
    (hook :cancel-session reason)
    (spi/-cancel-session! delegate reason))
  (-close-session! [_]
    (hook :close-session request)
    (swap! events conj [:close-session])
    (spi/-close-session! delegate)))

(defn- observe
  ([source] (observe source (fn [_ _])))
  ([source hook] (observe source hook {}))
  ([source hook envelopes]
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
            (let [session (->ObservedSession session events hook request envelopes)]
              (hook :acquired request)
              session)))
        (-plan-source [_ session request]
          (swap! events conj [:plan (:source-symbol request)])
          (hook :planning request)
          (let [response ((get envelopes :planning identity)
                          (spi/-plan-source source (:delegate session) request))]
            (hook :planned {:request request :response response})
            response)))})))

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
  {:boolean [nil false true]
   :long [nil -1 0 1 2 Long/MIN_VALUE Long/MAX_VALUE]
   :bigint [nil -1N 0N 1N (biginteger 1) 18446744073709551616N]
   :string [nil "" "a" "雪"]
   :keyword [nil :a :test/a :test/b :测试/值]
   :symbol [nil 'a 'test/a 'test/b (with-meta 'test/a {:note ["metadata"]})]
   :uuid [nil (UUID. 0 0) (UUID. 0 1) (UUID. -1 -1)]
   :instant [nil (Date. -1) (Date. 0) (Date. 1) (Date. Long/MAX_VALUE)]
   :bigdec [nil 1.0M 1.00M -2M 3M]
   :double [nil 0.0 -0.0 1.0 ##Inf ##NaN]
   :float [nil (float 0) (float -0.0) (float 1) Float/POSITIVE_INFINITY Float/NaN]
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

(defn- repeated-variable-columns! [make-source]
  (doseq [width [2 3], mask (range (bit-shift-left 1 width)), batch-size [1 4]]
    (let [nullable (mapv #(bit-test mask %) (range width))
          info (descriptor {:kind :tuple
                            :columns (mapv #(hash-map :type :long :nullable? %) nullable)})
          rows (into [(vec (repeat width 1)) (vec (repeat width 2))
                      (vec (repeat width 1))]
                     (concat (for [i (range width) :when (nullable i)]
                               (assoc (vec (repeat width 1)) i nil))
                             (when (every? true? nullable) [(vec (repeat width nil))])))
          provider (make-source info rows)
          pattern (into ['$s] (repeat width '?v))
          q {:find '[?v] :in '[$s] :where [pattern]
             :query-source {:limits {:batch-rows batch-size}}}]
      (testing (str "repeated variable, nullable=" nullable ", batch=" batch-size)
        (doseq [query [q (assoc q :find '[(count ?v) .])]]
          (let [observed (observe provider)]
            (compare! query [rows] [(:source observed)] [observed])))))))

(defn tuple-semantics!
  "Compare fused/unfused source plans to eager d/q with deterministic seeds.
  Failure contexts name the seed, logical type and batch size."
  [make-source]
  (is (= value/scalar-types (set (keys families))))
  (repeated-variable-columns! make-source)
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

(deftype NominalRef [type key]
  value/IQuerySourceValue
  (-logical-type [_] type)
  (-retained-bytes [_] (+ 128 (long (value/value-charge type)) (long (value/value-charge key))))
  Object
  ;; Descriptor equality is Clojure equality; keys retain Java key equality.
  (hashCode [_] (unchecked-int (bit-xor (hash type) (Objects/hashCode key))))
  (equals [_ other]
    (and (instance? NominalRef other)
         (= type (.-type ^NominalRef other))
         (Objects/equals key (.-key ^NominalRef other)))))

(defn- reference-type [domain epoch key-type]
  {:kind :custom :name :conformance/entity-ref :version 1 :fingerprint "conformance-ref:v1"
   :parameters {:domain domain :identity-epoch epoch :key-type key-type}})

(defn- reference-type-variants [type]
  [[:same type true]
   [:map-order (into (array-map) (reverse (seq type))) true]
   [:metadata (with-meta type {:conformance/note "annotation"}) true]
   [:name (assoc type :name :conformance/another-ref) false]
   [:version (update type :version inc) false]
   [:fingerprint (assoc type :fingerprint "conformance-ref:v2") false]
   [:namespaced-extra (assoc type :conformance/extra {:meaning "v2"}) false]
   [:namespaced-extra-value (assoc type :conformance/extra {:meaning "v3"}) false]
   [:parameter-extra (assoc-in type [:parameters :refinement] :v2) false]
   [:domain (assoc-in type [:parameters :domain] :suppliers) false]
   [:epoch (update-in type [:parameters :identity-epoch] inc) false]])

(defn- reference-identity! [left right same?]
  (is (= same? (.equals ^Object left right)))
  (is (= same? (.equals ^Object right left)))
  (is (= same? (= left right)))
  (when same?
    (is (= (.hashCode ^Object left) (.hashCode ^Object right)))
    (is (= (hash left) (hash right))))
  ;; Distinct identities may collide; only equal identities must hash alike.
  (is (= (if same? 1 2) (count (HashSet. [left right]))))
  (is (= (if same? 1 2) (count (set [left right])))))

(defn- nominal-descriptor-validation! [make-source]
  (let [type (reference-type :customers 0 :long)
        ref (NominalRef. type 7)
        info (descriptor {:kind :eav :attributes {:customer/id {:entity-type type :value-type :long}}})
        valid (make-source info [[ref :customer/id 7]])]
    (doseq [[label invalid] [[:kind (assoc type :kind :entity-ref)]
                             [:name (assoc type :name :entity-ref)]
                             [:version (assoc type :version 0)]
                             [:fingerprint (assoc type :fingerprint "")]
                             [:unknown-field (assoc type :extra :v2)]]]
      (testing (str "invalid nominal descriptor " label)
        (let [provider (reify spi/IQuerySource
                         (-source-info [_] (assoc-in info [:pattern-contract :attributes :customer/id :entity-type]
                                                    invalid))
                         (-open-session [_ request] (spi/-open-session valid request))
                         (-plan-source [_ session request] (spi/-plan-source valid session request)))
              observed (observe provider)]
          (is (= :query-source/invalid
                 (:error (error-data #(d/q '[:find ?ref :in $s :where [$s ?ref :customer/id _]]
                                          (:source observed))))))
          (is (empty? @(:controls observed)))
          (closed! observed))))
    (let [refs (mapv #(NominalRef. (second %) 7) (reference-type-variants type))
          observed (observe (make-source (tuple-descriptor :data) (mapv vector (range) refs)))]
      ;; Three equivalent descriptors plus eight distinct nominal variants.
      ;; Grouping must preserve changed extra values as well as their presence.
      (is (= 9 (count (HashSet. refs))))
      (is (= 9 (d/q '[:find (count ?ref) . :in $s :where [$s _ ?ref]] (:source observed))))
      (is (= {1 8, 3 1}
             (frequencies
               (map second
                    (d/q '[:find ?ref (count ?id) :in $s :where [$s ?id ?ref]] (:source observed))))))
      (closed! observed))
    ;; Descriptor equality is Clojure map equality, including inside extras;
    ;; hashing must agree even when equal metadata has different JVM hashes.
    (let [a (NominalRef. (assoc type :conformance/extra {:n 1.0M}) 7)
          b (NominalRef. (assoc type :conformance/extra {:n 1.00M}) 7)
          rows [[1 a] [2 b]]
          provider (make-source (tuple-descriptor :data) rows)
          q '[:find (count ?ref) . :in $s :where [$s _ ?ref]]
          observed (observe provider)]
      (reference-identity! a b true)
      (is (= 1 (d/q q (:source observed))))
      (closed! observed))))

(defn- nominal-fixture [composite?]
  (let [key-type (if composite? {:kind :tuple :elements [:string :long]} :long)
        key (fn [id] (if composite? ["tenant" id] id))
        order-type (reference-type :orders 0 key-type)
        customer-type (reference-type :customers 0 key-type)
        order #(NominalRef. order-type (key %))
        customer #(NominalRef. customer-type (key %))
        info (descriptor {:kind :eav :attributes
                          {:order/id {:entity-type order-type :value-type key-type}
                           :order/customer {:entity-type order-type :value-type customer-type}
                           :order/amount {:entity-type order-type :value-type :long}
                           :customer/id {:entity-type customer-type :value-type key-type}
                           :customer/name {:entity-type customer-type :value-type :string}
                           :customer/friend {:entity-type customer-type :value-type customer-type}}})
        rows [[(order 1) :order/id (key 1)] [(order 2) :order/id (key 2)]
              [(order 1) :order/customer (customer 7)] [(order 2) :order/customer (customer 7)]
              [(order 3) :order/customer (customer 8)] [(order 4) :order/customer nil]
              [(order 1) :order/amount 10] [(order 1) :order/amount 10] [(order 2) :order/amount 20]
              [(order 3) :order/amount nil]
              [(customer 7) :customer/id (key 7)] [(customer 8) :customer/id (key 8)]
              [(customer 7) :customer/name "Ada"] [(customer 8) :customer/name nil]
              [(customer 7) :customer/friend (customer 7)] [(customer 8) :customer/friend (customer 7)]
              [nil :customer/name "missing entity"]]]
    {:info info :rows rows :key key :key-type key-type :order order :customer customer
     :eager (filterv #(and (some? (first %)) (some? (nth % 2))) rows)}))

(defn- nominal-eav! [make-source]
  (doseq [composite? [false true], batch-size [1 4]]
    (testing (str "nominal references, composite=" composite? ", batch=" batch-size)
      (let [{:keys [info rows eager order customer key key-type]} (nominal-fixture composite?)
            provider (make-source info rows)
            options {:limits {:batch-rows batch-size}}
            queries ['{:find [?order ?name] :in [$s]
                       :where [[$s ?order :order/customer ?customer] [$s ?customer :customer/name ?name]]}
                     '{:find [?name] :with [?order] :in [$s]
                       :where [[$s ?order :order/customer ?customer] [$s ?customer :customer/name ?name]]}
                     '{:find [?name (count ?order) (sum ?amount)] :in [$s]
                       :where [[$s ?order :order/customer ?customer] [$s ?customer :customer/name ?name]
                               [$s ?order :order/amount ?amount]]}
                     '{:find [?order] :in [$s] :where [[$s ?order :order/amount _]]}
                     '{:find [?order] :in [$s] :where [[$s ?order :order/customer _]]}
                     '{:find [?customer] :in [$s] :where [[$s ?customer :customer/friend ?customer]]}
                     '{:find [?order ?name] :in [$a $b]
                       :where [[$a ?order :order/customer ?customer] [$b ?customer :customer/name ?name]]}]]
        (doseq [q queries]
          (let [observed (observe provider)
                aliases (count (:in q))]
            (compare! (assoc q :query-source options) (repeat aliases eager)
                      (repeat aliases (:source observed)) [observed])))
        (doseq [[binding input] [['?customer (customer 7)]
                                 ['[?customer] [(customer 7)]]
                                 ['[?customer ...] [(customer 7) (customer 7) (customer 8)]]
                                 ['[[?customer ?tag]] [[(customer 7) :a] [(customer 7) :b]]]]]
          (let [observed (observe provider)
                q {:find '[?order] :in ['$s binding]
                   :where '[[$s ?order :order/customer ?customer]] :query-source options}]
            (compare! q [eager input] [(:source observed) input] [observed])))
        (let [observed (observe provider)
              q {:find '[?order] :in '[$s] :where [['$s '?order :order/customer (customer 7)]]
                 :query-source options}]
          (compare! q [eager] [(:source observed)] [observed]))
        (doseq [input [(order 7) (key 7) (NominalRef. (reference-type :customers 1 key-type) (key 7))]]
          (let [observed (observe provider)
                q '[:find ?order :in $s ?customer :where [$s ?order :order/customer ?customer]]]
            (is (= :query-source/invalid (:error (error-data #(d/q q (:source observed) input)))))
            (is (empty? @(:controls observed)))
            (closed! observed)))
        (doseq [[label type same?] (reference-type-variants (reference-type :customers 0 key-type))]
          (testing (str "nominal type field " label ", composite=" composite?)
            (let [ref (NominalRef. type (key 7))
                  other-info (descriptor {:kind :eav :attributes
                                          {:customer/id {:entity-type type :value-type key-type}}})
                  other-rows [[ref :customer/id (key 7)]]
                  a (observe provider) b (observe (make-source other-info other-rows))
                  natural '[:find ?left ?right :in $a $b
                            :where [$a ?left :customer/id ?key] [$b ?right :customer/id ?key]]
                  nominal '[:find ?ref :in $a $b
                            :where [$a ?ref :customer/id _] [$b ?ref :customer/id _]]]
              (compare! natural [eager other-rows] [(:source a) (:source b)] [a b])
              (reference-identity! (customer 7) ref same?)
              (if same?
                (compare! nominal [eager other-rows] [(:source a) (:source b)] [a b])
                (let [opens (mapv #(count @(:controls %)) [a b])]
                  (is (= :query-source/unsupported
                         (:error (error-data #(d/q nominal (:source a) (:source b))))))
                  (is (= opens (mapv #(count @(:controls %)) [a b])))
                  (run! closed! [a b])))
              (let [bound (observe provider)
                    q '[:find ?order :in $s ?customer :where [$s ?order :order/customer ?customer]]]
                (if same?
                  (compare! q [eager ref] [(:source bound) ref] [bound])
                  (do
                    (is (= :query-source/invalid
                           (:error (error-data #(d/q q (:source bound) ref)))))
                    (is (empty? @(:controls bound)))
                    (closed! bound)))))))))))

(defn eav-semantics! [make-source]
  (nominal-descriptor-validation! make-source)
  (nominal-eav! make-source)
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

(defn- native-nominal! [make-source]
  (doseq [composite? [false true]]
    (let [{:keys [info rows eager key customer]} (nominal-fixture composite?)
          dir (u/tmp-dir (str "source-nominal-conformance-" (random-uuid)))
          conn (d/create-conn dir
                 {:native/key (if composite?
                                {:db/valueType :db.type/tuple
                                 :db/tupleTypes [:db.type/string :db.type/long]}
                                {:db/valueType :db.type/long})}
                 {:wal? false :background-sampling? false})
          provider (make-source info rows)]
      (try
        (d/transact! conn [{:db/id 1 :native/key (key 7)} {:db/id 2 :native/key (key 9)}])
        (doseq [batch-size [1 4]]
          (let [observed (observe provider)
                q {:find '[?native ?ref ?name] :in '[$ $s]
                   :where '[[?native :native/key ?key] [$s ?ref :customer/id ?key]
                            [$s ?ref :customer/name ?name]]
                   :query-source {:limits {:batch-rows batch-size}}}]
            (compare! q [@conn eager] [@conn (:source observed)] [observed])
            (is (= #{[1 (customer 7) "Ada"]} (d/q q @conn (:source observed))))
            (closed! observed)))
        (let [observed (observe provider)]
          (is (= :query-source/unsupported
                 (:error (error-data #(d/q '[:find ?ref :in $ $s
                                             :where [?ref :native/key _] [$s ?ref :customer/name _]]
                                           @conn (:source observed))))))
          (is (empty? @(:controls observed)))
          (closed! observed))
        (finally (d/close conn) (u/delete-files dir))))))

(defn native-semantics! [make-source]
  (native-nominal! make-source)
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

(defn- blocking-cancellation! [make-source]
  (doseq [phase [:acquisition :acquired :open :opened :fetch :fetched
                :close-cursor :close-session]
          cause [:cancel :deadline]
          aliases? [false true]
          mode (if (#{:acquisition :acquired :close-session} phase)
                 [:query :explain :plan] [:query :explain])]
    (testing (str "blocked " phase ", " cause ", aliases=" aliases? ", " mode)
      (let [entered (promise)
            callback (promise)
            released (CountDownLatch. 1)
            visits (atom 0)
            signal! (fn [reason]
                      (deliver callback {:thread (Thread/currentThread) :reason reason})
                      (.countDown released))
            hook (fn [at request]
                   (when (= :cancel-session at) (signal! request))
                   (when (and (= phase at)
                              (= (if (#{:fetch :fetched} phase) 2 1) (swap! visits inc)))
                     ;; Acquisition has no session callback yet. All later
                     ;; operations must be released by core's session callback.
                     ;; After-return hooks deliberately hand the acquired
                     ;; resource to core even when cancellation won the race.
                     (let [ctl (:control request)
                           registration (when (#{:acquisition :acquired} phase)
                                          (spi/register-cancel! ctl signal!))]
                       (try
                         (deliver entered {:control ctl :thread (Thread/currentThread)})
                         (when-not (.await released 10 TimeUnit/SECONDS)
                           (throw (ex-info "conformance wait expired" {})))
                         (finally
                           (when registration (spi/unregister-cancel! registration)))))))
            rows [[1 "one"] [2 "two"] [3 "three"]]
            provider (make-source (tuple-descriptor :string) rows)
            observed (observe provider hook)
            q (cond-> '{:find [?id ?v] :in [$a] :where [[$a ?id ?v]]
                        :query-source {:limits {:batch-rows 1 :in-flight-batches 1}}}
                aliases? (assoc :in '[$a $b] :where '[[$a ?id ?v] [$b ?id ?v]]))
            inputs (repeat (if aliases? 2 1) (:source observed))
            worker (future
                     (try
                       {:result (case mode
                                  :query (apply d/q q inputs)
                                  :explain (apply d/explain {:run? true} q inputs)
                                  :plan (apply d/explain {:run? false} q inputs))}
                       (catch clojure.lang.ExceptionInfo e {:error (ex-data e)})))
            nested (atom nil)]
        (try
          (let [{:keys [control thread] :as waiting} (deref entered 5000 ::timeout)]
            (is (map? waiting))
            (when control
              (is (not (realized? worker)))
              (if (= :cancel cause)
                (control/cancel! control {:error :query-source/cancelled :phase :execution})
                ;; Start a real nested deadline only after the operation is
                ;; blocked, avoiding compilation/scheduling-dependent timeouts.
                (reset! nested (control/create {:parent control :timeout-ms 25})))
              (let [outcome (deref worker 5000 ::timeout)
                    category (if (= :cancel cause) :query-source/cancelled :query/timeout)
                    signalled (deref callback 1000 ::timeout)]
                (is (= category (get-in outcome [:error :error])))
                (is (not (contains? outcome :result)))
                (is (= category (get-in signalled [:reason :error])))
                (is (not (identical? thread (:thread signalled))))
                (when (#{:acquisition :acquired :open :opened :fetch :fetched} phase)
                  (is (= '$a (get-in outcome [:error :source-symbol])))
                  (is (= [0] (get-in outcome [:error :clause-ids]))))
                (when (#{:fetch :fetched} phase)
                  (is (some #(= [:batch 1] %) @(:events observed))))
                (let [events (mapv first @(:events observed))]
                  (is (= (if (= :acquisition phase) 0 1)
                         (count (filter #{:open-session} events))))
                  (when (some #{:open-cursor} events)
                    (is (< (.lastIndexOf events :close-cursor)
                           (.lastIndexOf events :close-session)))))
                (closed! observed)
                ;; A cancelled session must not poison the source object or a
                ;; later invocation's captured view and reservations.
                (let [fresh (observe provider)]
                  (compare! q (repeat (count inputs) rows)
                            (repeat (count inputs) (:source fresh)) [fresh])))))
          (finally
            (.countDown released)
            (when-let [ctl @nested] (control/close! ctl))
            (future-cancel worker)))))))

(defn- cursor-request-contract! [make-source]
  (doseq [aliases? [false true], mode [:query :explain :plan]]
    (testing (str "cursor request envelope, aliases=" aliases? ", " mode)
      (let [info (tuple-descriptor :string)
            acquired (atom nil)
            plans (atom {})
            opened (atom [])
            common [:spi-version :features :query-id :authorization :consistency :control :limits]
            observed
            (observe (make-source info [[1 "a"] [2 "b"] [3 "b"]])
              (fn [phase request]
                (case phase
                  :acquisition (reset! acquired request)
                  :planned (swap! plans assoc (get-in request [:request :source-symbol]) request)
                  :open
                  (let [{:keys [alternative-id plan source-symbol definition-fingerprint]} request
                        planned (get @plans source-symbol)
                        selected (some #(when (= alternative-id (:id %)) %)
                                       (get-in planned [:response :alternatives]))]
                    (swap! opened conj request)
                    (is (= (into (set common) [:alternative-id :plan :source-symbol :definition-fingerprint])
                           (set (keys request))))
                    (is (= (select-keys @acquired common) (select-keys request common)))
                    (is (identical? (:control @acquired) (:control request)))
                    (is (= (:definition-fingerprint info) definition-fingerprint))
                    (is (some? selected))
                    (is (identical? (:plan selected) plan))
                    (is (= {'?name "b"} (get-in planned [:request :bindings]))))
                  nil)))
            q (cond-> '{:find [?id] :in [$a ?name]
                        :where [[$a ?id ?name] [(not= ?id 1)]]
                        :query-source {:authorization {:principal-id :cursor-test :policy-token "policy"}
                                       :limits {:batch-rows 1 :in-flight-batches 1}}}
                aliases? (assoc :in '[$a $b ?name]
                               :where '[[$a ?id ?name] [$b ?id ?name] [(not= ?id 1)]]))
            inputs (conj (vec (repeat (if aliases? 2 1) (:source observed))) "b")]
        (try
          (case mode
            :query (is (= #{[2] [3]} (apply d/q q inputs)))
            :explain (is (= 2 (:result-rows (apply d/explain {:run? true} q inputs))))
            :plan (is (false? (:executed? (apply d/explain {:run? false} q inputs)))))
          (is (= (if aliases? #{'$a '$b} #{'$a}) (set (keys @plans))))
          (is (= (if (= :plan mode) 0 (if aliases? 2 1)) (count @opened)))
          (finally (closed! observed)))))))

(defn explanation!
  "Check estimates, session consistency, and their diagnostic boundary through
  an adapter's real sessions, plans, cursors, and reservations."
  [make-source]
  (let [secret "private explanation annotation"
        unknown {:rows nil :row-bytes nil :startup-ms nil :per-row-ns nil
                 :requests nil :confidence :unknown}
        zero-estimate {:rows 0 :row-bytes 0 :startup-ms 0 :per-row-ns 0
                       :requests 0 :confidence :high}
        known {:rows 200 :row-bytes 32 :startup-ms 1.5M :per-row-ns 100
               :requests 2 :confidence :low}
        q '{:find [?id] :in [$a]
            :where [[$a ?id ?value] [$a ?id ?other]]}
        annotate (fn [x]
                   (with-meta (if (map? x) (assoc x :test/private secret) x)
                     {:private secret}))
        estimates (fn [estimate response]
                    (update response :alternatives
                      #(mapv (fn [a]
                               (-> a
                                   (assoc :estimate (annotate estimate)
                                          :explain {:label secret})
                                   (update :covers annotate)
                                   (update :prefilters annotate)
                                   (update :properties annotate)
                                   annotate)) %)))
        make #(make-source (tuple-descriptor :string) [[1 "a"] [2 "b"] [3 "c"]])]
    (doseq [estimate [unknown (assoc unknown :rows 200) zero-estimate
                     known (assoc known :confidence :medium)]
            aliases? [false true], run? [false true]]
      (testing (str "explain estimate " estimate ", aliases=" aliases? ", run=" run?)
        (let [planned (atom {}) session-reads (atom 0)
              observed (observe (make)
                         (fn [phase payload]
                           (when (= :planned phase)
                             (swap! planned assoc (get-in payload [:request :source-symbol])
                                    (get-in payload [:response :alternatives]))))
                         {:planning (partial estimates estimate)
                          :session-info #(do (swap! session-reads inc) (annotate %))})
              query (cond-> q aliases? (assoc :in '[$a $b]
                                              :where (conj (:where q) '[$b ?id ?value])))
              inputs (repeat (if aliases? 2 1) (:source observed))
              result (apply d/explain {:run? run? :intermediate-counts? false} query inputs)]
          (is (= (if aliases? #{'$a '$b} #{'$a}) (set (map :source (:sources result)))))
          (is (= 1 @session-reads))
          (doseq [{:keys [source consistency selected]} (:sources result)]
            (is (= :snapshot consistency))
            (is (= (mapv :covers (@planned source)) (mapv :covers selected)))
            (is (= (repeat (count selected) estimate) (map :estimate selected))))
          (if run?
            (do (is (= 3 (:result-rows result)))
                (is (seq (:cursors result)))
                (is (every? #(= 3 (get-in % [:metrics :rows])) (:cursors result))))
            (do (is (not (contains? result :result-rows)))
                (is (not (contains? result :cursors)))
                (is (not-any? #(= :open-cursor (first %)) @(:events observed)))))
          (is (not (.contains (binding [*print-meta* true] (pr-str result)) secret)))
          (closed! observed))))
    ;; A high-confidence zero is advisory: it neither short-circuits the rows
    ;; above nor permits execution to overrun a configured quota below.
    (doseq [limits [{:result-rows 1} {:batch-bytes 1}]]
      (let [observed (observe (make) (fn [_ _]) {:planning (partial estimates zero-estimate)})
            error (error-data #(d/explain {:run? true} (assoc q :query-source {:limits limits})
                                         (:source observed)))
            [category maximum] (first limits)]
        (is (= :query-source/limit (:error error)))
        (is (= category (:category error)))
        (is (= maximum (:maximum error)))
        (is (< maximum (:attempted error)))
        (is (not (.contains (pr-str error) secret)))
        (closed! observed)))
    (doseq [estimate [(assoc known :rows -1) (assoc known :row-bytes ##NaN)
                     (assoc known :requests nil) (assoc known :confidence :test/invalid)]
            run? [false true]]
      (let [observed (observe (make) (fn [_ _]) {:planning (partial estimates estimate)})
            error (error-data #(d/explain {:run? run?} q (:source observed)))]
        (is (= {:error :query-source/invalid :phase :planning :source-symbol '$a}
               (select-keys error [:error :phase :source-symbol])))
        (is (not-any? #(= :open-cursor (first %)) @(:events observed)))
        (is (not (.contains (binding [*print-meta* true] (pr-str error)) secret)))
        (closed! observed)))
    (doseq [run? [false true], requested [:statement :best-effort]]
      (let [observed (observe (make))
            error (error-data #(d/explain {:run? run?}
                                         (assoc q :query-source {:consistency requested})
                                         (:source observed)))]
        (is (= :query-source/unsupported (:error error)))
        (is (not-any? #(#{:plan :open-cursor} (first %)) @(:events observed)))
        (closed! observed)))
    (doseq [run? [false true]]
      (let [observed (observe (make) (fn [_ _])
                             {:session-info #(assoc % :consistency :best-effort)})
            error (error-data #(d/explain {:run? run?} q (:source observed)))]
        (is (= {:error :query-source/unsupported :phase :session :source-symbol '$a}
               (select-keys error [:error :phase :source-symbol])))
        (is (not-any? #(#{:plan :open-cursor} (first %)) @(:events observed)))
        (closed! observed)))
    (let [provider (make)
          observed (observe provider
                     (fn [phase _]
                       (when (= phase :fetch)
                         (throw (ex-info secret {:error :query-source/snapshot-lost
                                                 :private secret})))))
          query (assoc q :in '[$a $b] :where (conj (:where q) '[$b ?id ?value]))
          error (error-data #(d/explain {:run? true} query (:source observed) (:source observed)))]
      (is (= :query-source/snapshot-lost (:error error)))
      (is (= :fetch (:phase error)))
      (is (#{'$a '$b} (:source-symbol error)))
      (is (seq (:clause-ids error)))
      (is (not (.contains (pr-str error) secret)))
      (closed! observed)
      (let [fresh (observe provider)
            result (d/explain {:run? true} query (:source fresh) (:source fresh))]
        (is (= 3 (:result-rows result)))
        (is (= [:snapshot :snapshot] (mapv :consistency (:sources result))))
        (closed! fresh)))))

(defn lifecycle!
  "Exercise public SPI lifetime boundaries without inspecting adapter internals."
  [make-source replace-rows!]
  (explanation! make-source)
  (cursor-request-contract! make-source)
  (acquisition-boundary! make-source replace-rows!)
  (concurrent-views! make-source replace-rows!)
  (blocking-cancellation! make-source)
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
