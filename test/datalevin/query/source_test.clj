(ns datalevin.query.source-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.query.source :as source]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.fake :as fake]
            [datalevin.query.source.harness :as harness]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value]))

(def row-info (fake/row-info))
(def patterns [{:id 0 :kind :pattern :terms [[:var '?id] [:const :row/value] [:var '?name]]}])
(defn error-data [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))
(defn category [f] (:error (error-data f)))

(deftest nested-type-validation-preserves-cancellation
  (doseq [matches? [true false]
          category [:query-source/cancelled :query/timeout]]
    (let [ctl (control/create)
          descriptor {:kind :custom :name :test/checked :version 1 :fingerprint "v1"}
          visited (atom [])
          head (reify value/IQuerySourceValue
                 (-logical-type [_]
                   (swap! visited conj :head)
                   (control/cancel! ctl {:error category :phase :execution})
                   (if matches? descriptor (assoc descriptor :fingerprint "v2")))
                 (-retained-bytes [_] 64))
          tail (reify value/IQuerySourceValue
                 (-logical-type [_] (swap! visited conj :tail) descriptor)
                 (-retained-bytes [_] 64))
          type {:kind :tuple :elements [{:kind :tuple :elements [descriptor]} descriptor]}]
      (try
        (is (= category (:error (error-data #(value/matches-type? type [[head] tail]
                                                                (fn [] (source/check-active! ctl)))))))
        (is (= [:head] @visited))
        (finally (control/close! ctl))))))

(deftest retained-value-preflight-preserves-charges-and-rejects-overflow
  (let [ctl (control/create {:limits {:retained-bytes 32768}})
        x (with-meta [{:date (java.util.Date. 1)} '(1 2)] {:bytes (byte-array 16)})]
    (try
      (let [token (rt/charge! {:control ctl} x)]
        (is (= (+ 256 (value/value-charge x)) (:retained-bytes (control/usage ctl))))
        (source/release! token)
        (is (every? zero? (vals (control/usage ctl)))))
      (finally (control/close! ctl))))
  (let [ctl (control/create {:limits {:retained-bytes Long/MAX_VALUE}})
        huge (reify value/IQuerySourceValue
               (-logical-type [_] {:kind :custom :name :test/huge :version 1
                                   :fingerprint "huge:v1" :parameters {}})
               (-retained-bytes [_] Long/MAX_VALUE))]
    (try
      (let [error (error-data #(rt/charge! {:control ctl} huge))]
        (is (= :query-source/limit (:error error)))
        (is (= :retained-bytes (:category error)))
        (is (> (:attempted error) Long/MAX_VALUE))
        (is (every? zero? (vals (control/usage ctl)))))
      (finally (control/close! ctl)))))

(deftest immutable-time-values-use-the-data-contract
  (doseq [x [(java.time.Instant/ofEpochSecond 1 123456789)
             (java.time.Duration/ofSeconds -1 123456789)
             (java.time.Period/of 1 -2 3)]]
    (is (value/matches-type? :data x))
    (is (identical? x (value/owned-copy x)))
    (is (not (value/matches-type? :instant x)))
    ;; Descriptor/session metadata still uses the immutable EDN subset.
    (is (false? (value/portable? x))))
  (is (value/matches-type? :instant (java.util.Date. 1))))

(deftest portable-envelopes-include-clojure-metadata
  (doseq [x ['id [] {} #{} '() (sorted-map) (sorted-set)]
          mutable [(java.util.Date.) (byte-array [1]) (atom :value) (Object.)]]
    (is (false? (value/portable? (with-meta x {:hidden mutable}))))
    (is (false? (value/portable? {:nested [(with-meta x {:hidden mutable})]}))))
  (let [metadata {:source (with-meta 'declared {:revision [1 :stable]})}
        descriptor (with-meta row-info metadata)]
    (is (true? (value/portable? descriptor)))
    (is (identical? descriptor (source/validate-source-info! descriptor))))
  (is (= :query-source/invalid
         (category #(source/validate-source-info!
                      (with-meta row-info {:hidden (atom :mutable)}))))))

(deftest diagnostic-identifiers-drop-metadata-and-detach-backing
  (let [private {:private "private identifier annotation"}
        sym (with-meta 'version private)
        identifier (with-meta
                     {:key sym :vector (subvec ["private hidden parent" sym] 1)
                      :map (with-meta {sym (with-meta '(1 2) private)} private)
                      :set (with-meta #{sym} private)} private)
        copied (value/diagnostic-copy identifier (fn []))]
    (is (= identifier copied))
    (is (not (instance? clojure.lang.APersistentVector$SubVector (:vector copied))))
    (is (every? #(nil? (meta %)) (tree-seq coll? seq copied)))
    (is (not (.contains (binding [*print-meta* true] (pr-str copied)) "private"))))
  (let [checks (atom 0)
        active! #(when (= 5 (swap! checks inc))
                   (throw (ex-info "cancelled" {:error :query-source/cancelled})))]
    (is (= :query-source/cancelled
           (category #(value/diagnostic-copy (vec (range 100)) active!))))
    (is (= 5 @checks))))

(deftest resource-reservations-are-atomic-query-scoped-and-transfer-once
  (let [c (control/create {:limits {:retained-bytes 10 :external-requests 2}})
        other (control/create)
        token (source/reserve! c {:retained-bytes 10 :external-requests 1 :in-flight-batches 1})]
    (try
      (is (= :query-source/limit (category #(source/reserve! c {:retained-bytes 1 :external-requests 1}))))
      (is (= 1 (:external-requests (control/usage c))))
      (is (= :query-source/invalid (category #(control/accept! other token 10))))
      (is (nil? (control/accept! c token 10)))
      (is (= :query-source/invalid (category #(control/accept! c token 10))))
      (source/release! token)
      (source/release! token)
      (is (= 0 (:retained-bytes (control/usage c))))
      (is (= 1 (:external-requests (control/usage c))))
      (is (= :query-source/invalid (category #(control/accept! c token 0))))
      (finally (control/close! c) (control/close! other)))))

(deftest cancellation-signals-registration-races-and-timeouts
  (let [c (control/create) before (promise) after (promise) removed (atom false)
        registration (source/register-cancel! c #(reset! removed %))]
    (source/unregister-cancel! registration)
    (source/register-cancel! c #(deliver before %))
    (control/cancel! c {:error :query-source/cancelled :phase :fetch :secret "hidden"})
    (source/register-cancel! c #(deliver after %))
    (is (= {:error :query-source/cancelled :phase :fetch} (deref before 2000 :timeout)))
    (is (= @before (deref after 2000 :timeout)))
    (is (false? @removed))
    (is (source/cancelled? c))
    (is (= :query-source/cancelled (category #(source/check-active! c))))
    (control/close! c))
  (let [c (control/create {:timeout-ms 0})]
    (is (zero? (source/remaining-time-ms c)))
    (is (= :query/timeout (category #(source/check-active! c))))
    (control/close! c)))

(deftest resource-limits-and-deadlines-preserve-large-integers
  (let [c (control/create {:limits {:retained-bytes Long/MAX_VALUE}
                          :timeout-ms Long/MAX_VALUE})
        token (source/reserve! c {:retained-bytes (dec Long/MAX_VALUE) :in-flight-batches 1})]
    (try
      (is (nil? (source/check-active! c)))
      (let [error (error-data #(source/reserve! c {:retained-bytes 2}))]
        (is (= :query-source/limit (:error error)))
        (is (= (inc' Long/MAX_VALUE) (:attempted error))))
      (is (= (dec Long/MAX_VALUE) (:retained-bytes (control/usage c))))
      (is (= :query-source/invalid
             (category #(control/accept! c token (inc' Long/MAX_VALUE)))))
      (is (= :query-source/invalid
             (category #(control/accept! c token Double/NaN))))
      (is (nil? (control/accept! c token (biginteger (dec Long/MAX_VALUE)))))
      (source/release! token)
      (is (zero? (:retained-bytes (control/usage c))))
      (finally (control/close! c)))))

(deftest concurrent-reservations-and-cancellation-registration
  (let [c (control/create {:limits {:retained-bytes 16}})
        start (promise)
        workers (mapv (fn [_] (future @start
                                     (try (source/reserve! c {:retained-bytes 8})
                                          (catch clojure.lang.ExceptionInfo _ nil)))) (range 8))]
    (deliver start true)
    (let [tokens (keep deref workers)]
      (is (= 2 (count tokens)))
      (is (= 16 (:retained-bytes (control/usage c))))
      (run! source/release! tokens)
      (is (zero? (:retained-bytes (control/usage c)))))
    (control/close! c))
  (dotimes [_ 20]
    (let [c (control/create) start (promise) signal (promise)
          registration (future @start (source/register-cancel! c #(deliver signal %)))
          cancellation (future @start (control/cancel! c {:error :query-source/cancelled :phase :test}))]
      (deliver start true)
      @registration
      @cancellation
      (is (= :query-source/cancelled (:error (deref signal 2000 {}))))
      (control/close! c))))

(deftest owned-byte-values-survive-advance-and-close
  (let [bytes (byte-array [1 2 3])
        info (fake/row-info :bytes)
        clauses [{:id 0 :kind :pattern :terms [[:ignore] [:const :row/value] [:var '?bytes]]}]
        fake (fake/source info (vec (repeat 5 [1 :row/value bytes])))
        batches (atom [])
        result (harness/run! fake clauses {:limits {:batch-rows 2}
                                           :consume (fn [batch]
                                                      (swap! batches conj (:row-count batch))
                                                      (aset-byte bytes 0 (byte 9)))})
        values (mapv first (:rows result))]
    (is (= [2 2 1] @batches))
    (is (= (repeat 5 [1 2 3]) (mapv vec values)))
    (is (= 5 (count (set values))))
    ;; Ownership holds between rows in one batch as well as across advances.
    (aset-byte ^bytes (first values) 0 (byte 7))
    (is (= (repeat 4 [1 2 3]) (mapv vec (next values))))
    (is (= :close-session (first (last @(:events fake)))))))

(deftest owned-metadata-survives-advance-and-close
  (doseq [x [[1] {:a 1} #{1} '(1) 'value (sorted-map :a 1) (sorted-set 1 2)]]
    (let [bytes (byte-array [1 2])
          date (java.util.Date. 1000)
          x (with-meta x {:bytes bytes :nested (with-meta 'leaf {:date date})})
          info (fake/row-info :data)
          clauses [{:id 0 :kind :pattern :terms [[:ignore] [:const :row/value] [:var '?value]]}]
          s (fake/source info (vec (repeat 5 [1 :row/value x])))
          result (harness/run! s clauses
                              {:limits {:batch-rows 2}
                               :consume (fn [_] (aset-byte bytes 0 (byte 9)) (.setTime date 9000))})
          values (mapv first (:rows result))
          stamp #(-> % meta :nested meta :date)]
      (is (= (repeat 5 x) values))
      (doseq [v values]
        (is (= (class x) (class v)))
        (is (= [1 2] (vec (:bytes (meta v)))))
        (is (= 1000 (.getTime ^java.util.Date (stamp v)))))
      (aset-byte ^bytes (:bytes (meta (first values))) 0 (byte 8))
      (.setTime ^java.util.Date (stamp (first values)) 8000)
      (doseq [v (next values)]
        (is (= [1 2] (vec (:bytes (meta v)))))
        (is (= 1000 (.getTime ^java.util.Date (stamp v)))))
      (is (= :close-session (first (last @(:events s))))))))

(deftest batch-accounting-validates-each-custom-value-once
  (let [calls (atom 0)
        type {:kind :custom :name :test/charged :version 1 :fingerprint "v1"}
        x (reify value/IQuerySourceValue
            (-logical-type [_] type)
            (-retained-bytes [_] (swap! calls inc) 64))]
    (doseq [[t row] [[:data [x]] [type [x]]
                    [{:kind :tuple :elements [:long :data]} [[1 x]]]]]
      (let [columns [{:type t :nullable? false}]
            rows [row]
            expected (+' 64 (value/value-charge columns) (value/value-charge rows))]
        (reset! calls 0)
        (is (= expected (source/batch-charge-bytes columns rows)))
        (is (= 1 @calls))))))

(deftest incremental-batch-charges-include-metadata-and-overflow
  (let [huge (reify value/IQuerySourceValue
               (-logical-type [_] {:kind :custom :name :test/huge :version 1 :fingerprint "v1"})
               (-retained-bytes [_] Long/MAX_VALUE))]
    (doseq [x [nil (java.util.Date. 1) (byte-array [1 2])
               (with-meta 'item {:when (java.time.Instant/ofEpochSecond 1 123)})
               (with-meta [1N 2/3 1.23M] {:hidden (sorted-map :a (sorted-set 1 2))})
               {:values (list #{1 2} [huge huge])}]]
      (let [columns [{:type :data :nullable? true}]
            rows (with-meta [(with-meta [x] {:row :metadata})] {:batch :metadata})
            observed (volatile! 0)
            bytes (value/batch-charge-bytes columns rows #(vswap! observed +' %))]
        (is (= (+' 64 (value/value-charge columns) (value/value-charge rows)) bytes))
        (is (= bytes @observed)))))
  (is (zero? (source/batch-charge-bytes [] []))))

(deftest batch-rows-accept-all-persistent-vector-representations
  (let [columns [{:type :long :nullable? false} {:type :data :nullable? false}]
        expected (source/batch-charge-bytes columns [[1 :ok]])]
    (doseq [row [[1 :ok] (subvec [:prefix 1 :ok :suffix] 1 3) (first {1 :ok})]]
      (is (vector? row))
      (is (<= expected (source/batch-charge-bytes columns [row]))))))

(deftest vector-views-charge-their-parent-and-detached-copies-do-not
  (let [parent (with-meta [(.repeat "x" 8192) :visible (byte-array 8192)]
                         {:hidden (byte-array 8192)})
        view (with-meta (subvec parent 1 2) {:visible (java.util.Date. 1)})
        copied (value/owned-copy view)
        observed (volatile! 0)
        retained (value/value-charge view #(vswap! observed +' %))]
    (is (= view copied))
    (is (> retained (value/value-charge parent)))
    (is (= retained @observed))
    (is (= (value/value-charge copied) (value/copy-charge view)))
    (is (< (value/copy-charge view) 1024))
    (is (not (identical? (:visible (meta view)) (:visible (meta copied))))))
  (let [view (subvec [(Object.) :visible] 1 2)
        columns [{:type :data :nullable? false}]]
    (is (value/matches-type? :data view))
    (is (= [[view]] (value/rows! columns [[view]])))
    (is (= :query-source/unsupported
           (category #(source/batch-charge-bytes columns [[view]]))))
    (is (pos? (source/batch-charge-bytes columns (value/owned-copy [[view]]))))))

(deftest empty-batches-with-retained-state-need-a-reservation
  (let [rows (with-meta [] {:payload (byte-array 1024)})
        ctl (control/create)
        bytes (source/batch-charge-bytes [] rows)
        request {:control ctl :max-rows 1 :max-bytes 8192}
        batch {:columns [] :rows rows :row-count 0 :byte-count bytes :exhausted? true}]
    (try
      (is (pos? bytes))
      (is (= :query-source/invalid (category #(contract/batch! [] request batch))))
      (let [token (source/reserve! ctl {:retained-bytes bytes :in-flight-batches 1})
            batch (assoc batch :reservation token)]
        (is (= batch (contract/batch! [] request batch)))
        (source/release! token)
        (is (every? zero? (vals (control/usage ctl)))))
      (finally (control/close! ctl)))))
