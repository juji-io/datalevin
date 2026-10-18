(ns datalevin.query.source-binding-execution-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.query.source :as source]
            [datalevin.query.source.binding :as binding]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value])
  (:import [datalevin.utl QueryKey]
           [java.io Closeable]
           [java.util Date Random]))

(def requires [{:variable '?id :type :long :nullable? false}])
(def columns [{:variable '?value :type :string :nullable? false}])
(def constraints {:max-binding-rows 100 :max-binding-bytes 65536
                  :max-request-binding-rows 20})

(defn- error [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(defn- close! [^Closeable x] (.close x))

(defn- collector [runtime ids inputs]
  (let [keys (binding/collect! runtime ids requires)]
    (doseq [[values outer] inputs] (binding/add! keys values outer))
    keys))

(defn- empty-state? [keys]
  (= {:phase :closed} @(.-state ^datalevin.query.source.binding.Keys keys)))

(deftest distinct-keys-fan-out-to-every-outer-row-across-input-and-output-batches
  (rt/with-runtime {:limits {:binding-rows 2}}
    (fn [runtime]
      (let [ids (binding/id-space! runtime)
            keys (binding/collect! runtime ids requires)
            ;; Keys recur after enough other keys to cross several opens.
            outer-batches [[[1 :a] [2 :b] [3 :c]] [[4 :d] [1 :e]]
                          [[3 :f] [5 :g] [1 :a] [4 :h]]]
            result (atom []) evaluated (atom [])]
        (doseq [rows outer-batches [key tag] rows]
          (binding/add! keys [key] tag))
        (is (= 3 (binding/seal! keys constraints :many)))
        (is (false? (binding/complete? keys)))
        (dotimes [i 3]
          (let [inputs (binding/batch-inputs keys i)
                _ (swap! evaluated into (map #(first (:values %)) inputs))
                ;; Key 2 has no matches; keys 1 and 4 have multiple rows.
                matches {1 ["a" "b"] 3 ["c"] 4 ["d" "e"] 5 ["f"]}
                rows (vec (for [{:keys [binding-id values]} (reverse inputs)
                                v (get matches (first values))]
                            [binding-id [v]]))
                emit! #(swap! result conj [%1 (first %2)])]
            (doseq [[id row] rows]
              (binding/consume! keys i columns [row] [id] false emit!))
            (binding/consume! keys i columns [] [] true emit!)))
        (is (= [1 2 3 4 5] @evaluated))
        (is (= (frequencies [[:a "a"] [:a "b"] [:e "a"] [:e "b"]
                            [:a "a"] [:a "b"] [:c "c"] [:f "c"]
                            [:d "d"] [:d "e"] [:h "d"] [:h "e"] [:g "f"]])
               (frequencies @result)))
        (is (binding/complete? keys))
        (close! keys)
        (is (empty-state? keys))))))

(deftest typed-composite-keys-preserve-decimal-scale-bytes-and-nulls
  (rt/with-runtime {}
    (fn [runtime]
      (let [requires [{:variable '?key :type {:kind :tuple :elements [:bigdec :bytes]} :nullable? true}]
            keys (binding/collect! runtime (binding/id-space! runtime) requires)
            rows (atom [])]
        (binding/add! keys [[1M (byte-array [1 2])]] :first)
        (binding/add! keys [nil] :null)
        (binding/add! keys [[1M (byte-array [1 2])]] :duplicate)
        (binding/add! keys [[1.00M (byte-array [1 2])]] :scale)
        (binding/add! keys [[1M (byte-array [1 3])]] :bytes)
        (is (= 1 (binding/seal! keys constraints :many)))
        (let [inputs (binding/batch-inputs keys 0)]
          (is (= 4 (count inputs)))
          (doseq [{:keys [binding-id values]} inputs]
            (binding/consume! keys 0 requires [values] [binding-id] false
                              #(swap! rows conj [%1 %2])))
          (binding/consume! keys 0 requires [] [] true (fn [_ _])))
        (is (= [:first :duplicate :null :scale :bytes] (mapv first @rows)))))))

(deftest collected-keys-and-outer-witnesses-own-mutable-leaves-and-metadata
  (rt/with-runtime {}
    (fn [runtime]
      (let [date (Date. 42) data (byte-array [1 2]) annotation (byte-array [7])
            requires [{:variable '?key :type :data :nullable? false}]
            keys (binding/collect! runtime (binding/id-space! runtime) requires)
            outer (with-meta [date data] {:tag annotation})
            seen (atom nil)]
        (binding/add! keys [[date data]] outer)
        (.setTime date 99)
        (aset-byte data 0 (byte 9))
        (aset-byte annotation 0 (byte 8))
        (binding/add! keys [[(Date. 42) (byte-array [1 2])]] :duplicate)
        (binding/seal! keys constraints :many)
        (let [[{:keys [binding-id values]}] (binding/batch-inputs keys 0)]
          (is (= 42 (.getTime ^Date (ffirst values))))
          (is (= [1 2] (vec (second (first values)))))
          (binding/consume! keys 0 [] [[]] [binding-id] true
                            (fn [witness _] (when (vector? witness) (reset! seen witness)))))
        (is (= 42 (.getTime ^Date (first @seen))))
        (is (= [1 2] (vec (second @seen))))
        (is (= [7] (vec (:tag (meta @seen)))))))))

(deftest ids-are-shared-across-accesses-and-are-not-recycled-on-close
  (rt/with-runtime {}
    (fn [runtime]
      (let [ids (binding/id-space! runtime)
            first-keys (collector runtime ids [[[1] :a] [[2] :b] [[1] :duplicate]])]
        (binding/seal! first-keys constraints :many)
        (is (= [0 1] (mapv :binding-id (binding/batch-inputs first-keys 0))))
        (close! first-keys)
        (let [second-keys (collector runtime ids [[[1] :another-access]])]
          (binding/seal! second-keys constraints :many)
          (is (= [2] (mapv :binding-id (binding/batch-inputs second-keys 0)))))))))

(deftest row-caps-and-request-splitting-are-independent
  (doseq [[query-cap provider-cap expected] [[2 4 [2 2 1]] [4 2 [2 2 1]] [4 4 [4 1]]]]
    (rt/with-runtime {:limits {:binding-rows query-cap}}
      (fn [runtime]
        (let [keys (collector runtime (binding/id-space! runtime)
                              (mapv (fn [n] [[n] n]) (range 5)))
              n (binding/seal! keys (assoc constraints :max-binding-rows provider-cap
                                                      :max-request-binding-rows 1) :many)]
          (is (= expected (mapv #(count (binding/batch-inputs keys %)) (range n)))))))))

(deftest byte-caps-use-the-complete-binding-envelope
  (let [one (source/binding-charge-bytes requires [{:binding-id 0 :values [1]}])
        two (source/binding-charge-bytes requires [{:binding-id 0 :values [1]}
                                                   {:binding-id 1 :values [2]}])]
    (doseq [[query-cap provider-cap expected] [[one two [1 1]] [two one [1 1]] [two two [2]]]]
      (rt/with-runtime {:limits {:binding-bytes query-cap}}
        (fn [runtime]
          (let [keys (collector runtime (binding/id-space! runtime) [[[1] :a] [[2] :b]])
                n (binding/seal! keys (assoc constraints :max-binding-bytes provider-cap) :many)]
            (is (= expected (mapv #(count (binding/batch-inputs keys %)) (range n))))))))))

(deftest one-shot-keys-must-fit-in-a-single-open-before-provider-work
  (rt/with-runtime {}
    (fn [runtime]
      (let [keys (collector runtime (binding/id-space! runtime) [[[1] :a] [[2] :b] [[1] :c]])]
        (is (= 1 (binding/seal! keys (assoc constraints :max-binding-rows 2
                                                       :max-request-binding-rows 1) :once)))
        (is (= 2 (count (binding/batch-inputs keys 0)))))))
  (doseq [constraint [(assoc constraints :max-binding-rows 1)
                      (assoc constraints :max-binding-bytes
                             (source/binding-charge-bytes requires [{:binding-id 0 :values [1]}]))]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [keys (collector runtime (binding/id-space! runtime) [[[1] :a] [[2] :b]])]
          (is (= :query-source/unsupported (:error (error #(binding/seal! keys constraint :once)))))
          (is (empty-state? keys)))))))

(deftest a-single-oversized-key-fails-instead-of-truncating-or-reopening
  (doseq [opens [:once :many]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [requires [{:variable '?key :type :string :nullable? false}]
              keys (binding/collect! runtime (binding/id-space! runtime) requires)]
          (binding/add! keys ["small"] :a)
          (binding/add! keys [(apply str (repeat 10000 "x"))] :b)
          (let [e (error #(binding/seal! keys (assoc constraints :max-binding-bytes 4096) opens))]
            (is (= :query-source/limit (:error e)))
            (is (= :binding-bytes (:category e))))
          (is (empty-state? keys)))))))

(deftest empty-inputs-open-no-cursors-and-allocate-no-binding-ids
  (rt/with-runtime {}
    (fn [runtime]
      (let [ids (binding/id-space! runtime)
            keys (binding/collect! runtime ids requires)]
        (is (zero? (binding/seal! keys constraints :once)))
        (is (binding/complete? keys))
        (is (= :query-source/invalid (:error (error #(binding/batch-inputs keys 0)))))
        (let [next-keys (collector runtime ids [[[42] :only]])]
          (binding/seal! next-keys constraints :once)
          (is (= 0 (:binding-id (first (binding/batch-inputs next-keys 0))))))))))

(deftest final-batches-complete-all-keys-including-zero-match-bindings
  (doseq [final-rows? [true false]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [keys (collector runtime (binding/id-space! runtime) [[[1] :hit] [[2] :miss]])
              seen (atom [])]
          (binding/seal! keys constraints :many)
          (is (false? (binding/complete? keys)))
          (when-not final-rows?
            (binding/consume! keys 0 columns [["hit"]] [0] false #(swap! seen conj [%1 %2]))
            (is (false? (binding/complete? keys))))
          (binding/consume! keys 0 columns (if final-rows? [["hit"]] [])
                            (if final-rows? [0] []) true #(swap! seen conj [%1 %2]))
          (is (= [[:hit ["hit"]]] @seen))
          (is (binding/complete? keys))
          (is (= :query-source/invalid
                 (:error (error #(binding/consume! keys 0 columns [] [] true (fn [_ _])))))))))))

(deftest responses-cannot-cross-opens-and-are-validated-before-delivery
  (doseq [[rows ids exhausted?] [[[["a"] ["b"]] [0 1] true]
                               [[["a"]] [9] true]
                               [[["a"]] [] true]
                               [[["a"]] [-1] true]
                               [[["a"]] [0.0] true]
                               [[["a"]] nil true]
                               [[["a"]] [0] nil]
                               [[] [] false]]]
    (rt/with-runtime {:limits {:binding-rows 1}}
      (fn [runtime]
        (let [keys (collector runtime (binding/id-space! runtime) [[[1] :a] [[2] :b]])
              delivered (atom 0)]
          (binding/seal! keys constraints :many)
          (is (= :query-source/invalid
                 (:error (error #(binding/consume! keys 0 columns rows ids exhausted?
                                                  (fn [_ _] (swap! delivered inc)))))))
          (is (zero? @delivered))
          (is (empty-state? keys)))))))

(deftest returning-required-variables-must-agree-with-associated-inputs
  (doseq [[out-columns row] [[requires [2]]
                             [[{:variable '?id :type :data :nullable? false}] [1]]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [keys (collector runtime (binding/id-space! runtime) [[[1] :a]])]
          (binding/seal! keys constraints :many)
          (is (= :query-source/invalid
                 (:error (error #(binding/consume! keys 0 out-columns [row] [0] true
                                                  (fn [_ _] (throw (Exception. "should not emit")))))))))))))

(deftest collection-rejects-wrong-types-arity-and-closed-or-sealed-state
  (doseq [values [[1.0] [nil] [] [1 2] '(1)]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [keys (binding/collect! runtime (binding/id-space! runtime) requires)]
          (is (= :query-source/invalid (:error (error #(binding/add! keys values :bad)))))
          (is (empty-state? keys))))))
  (rt/with-runtime {}
    (fn [runtime]
      (let [keys (collector runtime (binding/id-space! runtime) [[[1] :a]])]
        (binding/seal! keys constraints :many)
        (is (= :query-source/invalid (:error (error #(binding/add! keys [2] :late)))))
        (is (= :query-source/invalid (:error (error #(binding/seal! keys constraints :many)))))
        (close! keys)
        (close! keys)
        (is (empty-state? keys))))))

(deftest declarations-and-foreign-id-spaces-are-rejected
  (doseq [constraints [nil {} (assoc constraints :max-binding-rows 0)
                       (assoc constraints :max-binding-bytes 1.5)
                       (assoc constraints :max-request-binding-rows (inc' Long/MAX_VALUE))
                       (assoc constraints :unknown true)]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [keys (collector runtime (binding/id-space! runtime) [[[1] :a]])]
          (is (= :query-source/invalid (:error (error #(binding/seal! keys constraints :many)))))
          (is (empty-state? keys))))))
  (rt/with-runtime {}
    (fn [runtime]
      (let [ids (binding/id-space! runtime)
            ctl (control/create)]
        (try
          (is (= :query-source/invalid
                 (:error (error #(binding/collect! (assoc runtime :control ctl) ids requires)))))
          (close! ids)
          (is (= :query-source/invalid (:error (error #(binding/collect! runtime ids requires)))))
          (finally (control/close! ctl)))))))

(deftest quota-rejection-precedes-unneeded-custom-value-hooks
  (let [visited (atom false) captured (atom nil)
        opaque (reify value/IQuerySourceValue
                 (-logical-type [_] {:kind :custom :name :test/key :version 1 :fingerprint "key"})
                 (-retained-bytes [_] (reset! visited true) 64))]
    (is (= :query-source/limit
           (:error (error #(rt/with-runtime {:limits {:retained-bytes 16000}}
                             (fn [runtime]
                               (reset! captured runtime)
                               (let [keys (binding/collect! runtime (binding/id-space! runtime)
                                                            [{:variable '?key :type :data :nullable? false}])]
                                 (binding/add! keys [[(apply str (repeat 20000 "x")) opaque]] :a))))))))
    (is (false? @visited))
    (is (every? zero? (vals (control/usage (:control @captured)))))))

(deftest cancellation-during-copy-and-response-fan-out-releases-all-state
  (testing "collection cancellation inside value sizing"
    (let [captured (atom nil) saved (atom nil)]
      (is (= :query-source/cancelled
             (:error (error #(rt/with-runtime {}
                               (fn [runtime]
                                 (reset! captured runtime)
                                 (let [keys (binding/collect! runtime (binding/id-space! runtime)
                                                              [{:variable '?key :type :data :nullable? false}])
                                       cancel (reify value/IQuerySourceValue
                                                (-logical-type [_] {:kind :custom :name :test/key
                                                                    :version 1 :fingerprint "key"})
                                                (-retained-bytes [_]
                                                  (control/cancel! (:control runtime)
                                                    {:error :query-source/cancelled :phase :execution}) 64))]
                                   (reset! saved keys)
                                   (binding/add! keys [[cancel "unvisited"]] :a))))))))
      (is (empty-state? @saved))
      (is (every? zero? (vals (control/usage (:control @captured)))))))
  (testing "fan-out cancellation keeps no partial successful completion"
    (let [captured (atom nil) saved (atom nil) emitted (atom 0)]
      (is (= :query-source/cancelled
             (:error (error #(rt/with-runtime {}
                               (fn [runtime]
                                 (reset! captured runtime)
                                 (let [keys (collector runtime (binding/id-space! runtime)
                                                       (vec (repeat 100 [[1] :duplicate])))]
                                   (reset! saved keys)
                                   (binding/seal! keys constraints :many)
                                   (binding/consume! keys 0 columns [["match"]] [0] true
                                     (fn [_ _]
                                       (when (= 3 (swap! emitted inc))
                                         (control/cancel! (:control runtime)
                                           {:error :query-source/cancelled :phase :execution})))))))))))
      (is (= 3 @emitted))
      (is (empty-state? @saved))
      (is (every? zero? (vals (control/usage (:control @captured))))))))

(deftest duplicate-outer-witnesses-remain-quota-accounted-and-cleanup-is-idempotent
  (rt/with-runtime {:limits {:retained-bytes 30000}}
    (fn [runtime]
      (let [ids (binding/id-space! runtime)
            baseline (:retained-bytes (control/usage (:control runtime)))
            keys (collector runtime ids [[[1] :first]])]
        (is (= :query-source/limit
               (:error (error #(dotimes [_ 100]
                                 (binding/add! keys [1] (apply str (repeat 1000 "x"))))))))
        (is (empty-state? keys))
        (close! keys)
        (is (= baseline (:retained-bytes (control/usage (:control runtime)))))))))

(deftest generated-batch-sizing-is-linear-and-equals-the-public-charge
  (rt/with-runtime {:limits {:binding-rows 1000}}
    (fn [runtime]
      (let [visits (atom 0)
            logical-type {:kind :custom :name :test/key :version 1 :fingerprint "key"}
            requires [{:variable '?key :type logical-type :nullable? false}]
            keys (binding/collect! runtime (binding/id-space! runtime) requires)]
        (dotimes [_ 300]
          (binding/add! keys [(reify value/IQuerySourceValue
                               (-logical-type [_] logical-type)
                               (-retained-bytes [_] (swap! visits inc) 64))] :outer))
        (reset! visits 0)
        (is (= 1 (binding/seal! keys (assoc constraints :max-binding-rows 1000
                                                       :max-binding-bytes 1000000) :many)))
        (is (= 300 @visits))
        (let [inputs (binding/batch-inputs keys 0)
              charges (:batches @(.-state ^datalevin.query.source.binding.Keys keys))
              incremental (+' (value/owned-bindings-header-charge requires nil)
                              (reduce +' (map #(value/owned-binding-element-charge % nil) inputs)))]
          (is (= incremental (source/binding-charge-bytes requires inputs)))
          (is (= 1 (count charges))))))))

(deftest small-dependent-fixtures-match-eager-typed-evaluation
  (let [random (Random. 981281)
        keys [nil [1M (byte-array [1])] [1.00M (byte-array [1])]
              [1M (byte-array [2])] "a" :b {:nested [(byte-array [3])]}]
        facts (let [rows (mapv (fn [n] [(nth keys (mod n (count keys))) n]) (range 19))]
                (into rows (take 3 rows)))
        requires [{:variable '?key :type :data :nullable? true}]
        columns [{:variable '?answer :type :long :nullable? false}]]
    (dotimes [trial 80]
      (let [row-cap (inc (.nextInt random 8))
            outer (mapv (fn [n] [(value/owned-copy (nth keys (.nextInt random (count keys))))
                                (mod n 13)])
                        (range (.nextInt random 80)))
            expected (frequencies (for [[key tag] outer [fact-key answer] facts
                                        :when (QueryKey/equals key fact-key)]
                                    [tag answer]))]
        (rt/with-runtime {:limits {:binding-rows row-cap}}
          (fn [runtime]
            (let [keys (binding/collect! runtime (binding/id-space! runtime) requires)
                  actual (atom [])]
              (doseq [[key tag] outer] (binding/add! keys [key] tag))
              (let [n (binding/seal! keys (assoc constraints :max-binding-rows 5) :many)]
                (dotimes [i n]
                  (let [inputs (binding/batch-inputs keys i)
                        response (vec (for [{:keys [binding-id values]} (reverse inputs)
                                            [fact-key answer] facts
                                            :when (QueryKey/equals (first values) fact-key)]
                                        [binding-id [answer]]))
                        parts (mapv vec (partition-all (inc (mod trial 4)) response))
                        final-rows? (even? trial)
                        emit! #(swap! actual conj [%1 (first %2)])]
                    (is (<= (count inputs) (min row-cap 5)))
                    (doseq [j (range (count parts))]
                      (let [part (nth parts j)]
                        (binding/consume! keys i columns (mapv second part) (mapv first part)
                                          (and final-rows? (= j (dec (count parts)))) emit!)))
                    (when (or (empty? response) (not final-rows?))
                      (binding/consume! keys i columns [] [] true emit!)))))
              (is (= expected (frequencies @actual)))
              (is (binding/complete? keys)))))))))

(deftest batch-workspace-failure-and-emitter-failure-release-keys-and-associations
  (rt/with-runtime {}
    (fn [runtime]
      (let [ids (binding/id-space! runtime)
            baseline (:retained-bytes (control/usage (:control runtime)))
            keys (collector runtime ids (mapv (fn [n] [[n] n]) (range 20)))
            ctl (:control runtime)
            remaining (- (:retained-bytes (control/limits ctl))
                         (:retained-bytes (control/usage ctl)))
            held (control/reserve! ctl {:retained-bytes (- remaining 100)})]
        (try
          (is (= :query-source/limit (:error (error #(binding/seal! keys constraints :many)))))
          (is (empty-state? keys))
          (finally (control/release! held)))
        (is (= baseline (:retained-bytes (control/usage ctl)))))))
  (let [saved (atom nil) captured (atom nil)
        result (error #(rt/with-runtime {}
                         (fn [runtime]
                           (reset! captured runtime)
                           (let [keys (collector runtime (binding/id-space! runtime) [[[1] :a]])]
                             (reset! saved keys)
                             (binding/seal! keys constraints :many)
                             (binding/consume! keys 0 columns [["a"]] [0] true
                               (fn [_ _] (throw (ex-info "private callback" {:secret "private"}))))))))]
    (is (= :query-source/provider (:error result)))
    (is (not (.contains (pr-str result) "private")))
    (is (empty-state? @saved))
    (is (every? zero? (vals (control/usage (:control @captured)))))))

(deftest reentrant-close-during-delivery-does-not-resurrect-state
  (rt/with-runtime {}
    (fn [runtime]
      (let [keys (collector runtime (binding/id-space! runtime) [[[1] :a]])]
        (binding/seal! keys constraints :many)
        (is (= :query-source/invalid
               (:error (error #(binding/consume! keys 0 columns [["a"]] [0] true
                                                (fn [_ _] (close! keys)))))))
        (is (empty-state? keys))))))
