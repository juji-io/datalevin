(ns datalevin.query.source-recursion-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.join :as join]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.recursion :as recursion]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value]
            [datalevin.relation :as relation]
            [datalevin.rules.fixed-point :as fixed-point])
  (:import [java.util AbstractList Date Random]))

(defn- rel [variables rows]
  (relation/relation! (zipmap variables (range)) (mapv object-array rows)))

(deftest initial-storage-estimates-preserve-overflow-and-the-primitive-fast-path
  (let [estimate @#'fixed-point/initial-allocation-bytes]
    (is (= 4096 (estimate 0 0)))
    (is (= 5376 (estimate 1 1)))
    (is (instance? Long (estimate 1 1)))
    (let [product-overflow (estimate 0 36028797018963968)
          sum-overflow (estimate 3 36028797018963967)]
      (is (integer? product-overflow))
      (is (= 9223372036854779904N product-overflow))
      (is (integer? sum-overflow))
      (is (= 9223372036854782720N sum-overflow)))))

(defn- rows [handle id]
  (mapv vec (:tuples (get ((:relations handle)) id))))

(defn- joined! [runtime a b]
  (join/hash-join-bounded a b
    {:check! #(source/check-active! (:control runtime))
     :reserve! (fn [n bytes]
                 (when (> n Integer/MAX_VALUE)
                   (throw (ex-info "capacity" {:error :query-source/limit})))
                 (source/reserve! (:control runtime) {:retained-bytes bytes}))
     :release! source/release!
     :retain! #(rt/own! runtime (fn [] (source/release! %)))}))

(defn- reachable [edges]
  (loop [paths (set edges)]
    (let [next (into paths (for [[a b] paths [c d] edges :when (= b c)] [a d]))]
      (if (= paths next) paths (recur next)))))

(deftest compiled-shared-joins-match-generated-transitive-closures
  (let [random (Random. 314159)]
    (dotimes [_ 40]
      (let [edges (vec (repeatedly (.nextInt random 25)
                                  #(vector (.nextInt random 8) (.nextInt random 8))))]
        (rt/with-runtime {}
          (fn [runtime]
            (let [edge-rel (rel ['?via '?to] edges)
                  handle (recursion/evaluate! runtime {:path (rel ['?from '?to] edges)}
                           [:path] {:path #{:path}}
                           (fn [workspace _ deltas _]
                             (joined! workspace
                               (assoc (:path deltas) :attrs {'?from 0 '?via 1}) edge-rel)))]
              (is (= (reachable edges) (set (rows handle :path)))))))))))

(deftest complete-head-keys-precede-projection-and-body-private-columns
  (rt/with-runtime {}
    (fn [runtime]
      (let [calls (atom 0)
            handle (recursion/evaluate! runtime {:path (rel ['?a '?b] [[1 2] [1 2]])}
                     [:path] {:path #{:path}}
                     (fn [_ _ _ _]
                       (if (= 1 (swap! calls inc))
                         (rel ['?private '?b '?a] [[:one 3 1] [:two 3 1] [:three 4 1]])
                         (rel ['?a '?b] [[1 3] [1 4]]))))]
        (is (= #{[1 2] [1 3] [1 4]} (set (rows handle :path))))
        (is (= 2 @calls))
        ((:close handle))
        ((:close handle))
        (is (every? zero? (vals (control/usage (:control runtime)))))
        (is (= :query-source/invalid
               (:error (ex-data (try ((:relations handle)) (catch Throwable e e))))))))))

(deftest mutually-recursive-rules-schedule-only-live-delta-dependencies
  (rt/with-runtime {}
    (fn [runtime]
      (let [calls (atom [])
            handle (recursion/evaluate! runtime
                     {:even (rel ['?n] [[0]]) :odd (rel ['?n] []) :inactive (rel ['?n] [])}
                     [:even :odd :inactive]
                     {:even #{:odd} :odd #{:even} :inactive #{:inactive}}
                     (fn [_ _ deltas id]
                       (swap! calls conj id)
                       (rel ['?n] (for [tuple (:tuples (get deltas (if (= id :even) :odd :even)))
                                       :let [n (inc (aget ^objects tuple 0))] :when (< n 20)] [n]))))]
        (is (= (set (map vector (range 0 20 2))) (set (rows handle :even))))
        (is (= (set (map vector (range 1 20 2))) (set (rows handle :odd))))
        (is (= 20 (count @calls)))
        (is (not-any? #{:inactive} @calls))))))

(deftest empty-base-and-zero-column-heads-terminate
  (doseq [initial [(rel [] []) (rel [] [[] []])]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [calls (atom 0)
              handle (recursion/evaluate! runtime {:exists initial} [:exists] {:exists #{:exists}}
                       (fn [_ _ _ _] (swap! calls inc) (rel ['?private] [[1] [2]])))
              exists? (seq (:tuples initial))]
          (is (= (if exists? [[]] []) (rows handle :exists)))
          (is (= (if exists? 1 0) @calls)))))))

(deftest borrowed-rows-and-mutable-leaves-are-owned-before-next-read
  (rt/with-runtime {}
    (fn [runtime]
      (let [bytes (byte-array 1) date (Date. 0) tuple (object-array [bytes date])
            borrowed (relation/relation! {'?bytes 0 '?date 1}
                       (proxy [AbstractList] []
                         (size [] 4)
                         (get [i] (aset-byte bytes 0 (byte i)) (.setTime date i) tuple)))
            handle (recursion/evaluate! runtime {:item borrowed} [:item] {:item #{:item}}
                     (fn [_ _ _ _] (rel ['?bytes '?date] [])))
            actual (rows handle :item)]
        (aset-byte bytes 0 (byte 99))
        (.setTime date 99)
        (is (= [[0 0] [1 1] [2 2] [3 3]]
               (mapv (fn [[b d]] [(aget ^bytes b 0) (.getTime ^Date d)]) actual)))
        (is (every? #(not (identical? bytes (first %))) actual))))))

(deftest seen-keys-use-shared-byte-and-numeric-equivalence
  (rt/with-runtime {}
    (fn [runtime]
      (let [values [(byte-array [1]) (byte-array [1]) 1 1N 1.0 nil nil]
            base (rel ['?x] (map vector values))
            handle (recursion/evaluate! runtime {:item base} [:item] {:item #{:item}}
                     (fn [_ _ _ _] base))
            expected (count (set (map #(relation/wrap-array (object-array [%])) values)))]
        (is (= expected (count (rows handle :item))))))))

(deftest iteration-lists-and-body-workspaces-release-under-a-fixed-budget
  (let [n 600]
    (rt/with-runtime {:limits {:retained-bytes 1000000}}
      (fn [runtime]
        (let [iterations (atom 0) peak (atom 0)
              handle (recursion/evaluate! runtime {:chain (rel ['?n] [[0]])}
                       [:chain] {:chain #{:chain}}
                       (fn [workspace _ deltas _]
                         (swap! iterations inc)
                         (is (= 1 (count @(:cleanup runtime))))
                         (is (empty? @(:cleanup workspace)))
                         (let [token (source/reserve! (:control workspace) {:retained-bytes 4096})]
                           (rt/own! workspace #(source/release! token)))
                         (swap! peak max (:retained-bytes (control/usage (:control workspace))))
                         (rel ['?n] (for [tuple (:tuples (:chain deltas))
                                         :let [next (inc (aget ^objects tuple 0))] :when (< next n)] [next]))))]
          (is (= n @iterations (count (rows handle :chain))))
          (is (< @peak 900000))
          ((:close handle))
          (is (every? zero? (vals (control/usage (:control runtime))))))))))

(deftest quota-and-cancellation-unwind-seen-values-and-body-storage
  (doseq [cancel? [false true]]
    (let [captured (atom nil) closed (atom 0)
          error (try
                  (rt/with-runtime {:limits {:retained-bytes 100000}}
                    (fn [runtime]
                      (reset! captured (:control runtime))
                      (recursion/evaluate! runtime {:grow (rel ['?n] [[0]])} [:grow] {:grow #{:grow}}
                        (fn [workspace _ deltas _]
                          (rt/own! workspace #(swap! closed inc))
                          (when cancel?
                            (control/cancel! (:control workspace) {:error :query-source/cancelled}))
                          (rel ['?n] (map #(vector (inc (aget ^objects % 0))) (:tuples (:grow deltas))))))))
                  (catch Throwable e e))]
      (is (= (if cancel? :query-source/cancelled :query-source/limit) (:error (ex-data error))))
      (is (pos? @closed))
      (is (every? zero? (vals (control/usage @captured)))))))

(deftest body-failure-redacts-details-and-keeps-cleanup-error-secondary
  (let [captured (atom nil)
        error (try
                (rt/with-runtime {}
                  (fn [runtime]
                    (reset! captured (:control runtime))
                    (recursion/evaluate! runtime {:r (rel ['?x] [[1]])} [:r] {:r #{:r}}
                      (fn [workspace _ _ _]
                        (rt/own! workspace #(throw (Exception. "private cleanup")))
                        (throw (ex-info "private body" {:error :query-source/snapshot-lost
                                                        :secret "hidden"}))))))
                (catch Throwable e e))]
    (is (= :query-source/snapshot-lost (:error (ex-data error))))
    (is (= 1 (count (.getSuppressed ^Throwable error))))
    (is (not (.contains (pr-str error) "private")))
    (is (nil? (:secret (ex-data error))))
    (is (every? zero? (vals (control/usage @captured))))))

(deftest deep-borrowed-keys-reject-before-shared-hashing
  (let [deep (nth (iterate vector 0) 5000) captured (atom nil)
        error (try
                (rt/with-runtime {}
                  (fn [runtime]
                    (reset! captured (:control runtime))
                    (recursion/evaluate! runtime {:r (rel ['?x] [[deep]])} [:r] {:r #{:r}}
                      (fn [_ _ _ _] (rel ['?x] [])))))
                (catch Throwable e e))]
    (is (= :query-source/limit (:error (ex-data error))))
    (is (every? zero? (vals (control/usage @captured))))))

(deftest seen-container-growth-is-admitted-before-owning-a-new-payload
  (let [visits (atom 0) captured (atom nil)
        item (reify value/IQuerySourceValue
               (-logical-type [_] {:kind :custom :name :test/payload :version 1 :fingerprint "v1"})
               (-retained-bytes [_] (swap! visits inc) 1024))
        error (try
                (rt/with-runtime {:limits {:retained-bytes 13500}}
                  (fn [runtime]
                    (reset! captured (:control runtime))
                    (recursion/evaluate! runtime {:r (rel ['?x] [[item]])} [:r] {:r #{:r}}
                      (fn [_ _ _ _] (rel ['?x] [])))))
                (catch Throwable e e))]
    (is (= :query-source/limit (:error (ex-data error))))
    (is (zero? @visits))
    (is (every? zero? (vals (control/usage @captured))))))

(defn- kernel-hooks [active]
  {:check! (fn []) :reserve! (fn [_ _] (let [token (Object.)] (swap! active conj token) token))
   :release! #(swap! active disj %) :validate-row! (fn [_]) :own-row! aclone
   :invalid! #(throw (ex-info "invalid" {:error :query-source/invalid}))})

(deftest kernel-rejects-missing-double-and-late-consumption
  (doseq [mode [:missing :double :late]]
    (let [active (atom #{}) saved (atom nil) base (rel ['?x] [[1]])
          outcome (try
                    (fixed-point/run-bounded! {:r base} [:r] {:r #{:r}}
                      (fn [_ _ consume!]
                        (case mode
                          :missing nil
                          :double (do (consume! base) (consume! base))
                          :late (do (reset! saved consume!) (consume! base))))
                      (kernel-hooks active))
                    (catch Throwable e e))]
      (if (= mode :late)
        (do (is (= :query-source/invalid
                   (:error (ex-data (try (@saved base) (catch Throwable e e))))))
            ((:close outcome)))
        (is (= :query-source/invalid (:error (ex-data outcome)))))
      (is (empty? @active)))))

(deftest kernel-validation-unwinds-all-reservations
  (doseq [[base ids dependencies body]
          [[(rel ['?x] [[1]]) [:r :r] {:r #{:r}} (rel ['?x] [])]
           [(rel ['?x] [[1]]) [:r] {:r #{:foreign}} (rel ['?x] [])]
           [(rel ['?x] [[1]]) [:r] {:r #{:r}} (rel ['?other] [])]
           [(rel ['?x] [[1]]) [:r] {:r #{:r}} (relation/relation! {'?x -1} [(object-array [1])])]
           [(relation/relation! {'?x 100} [(object-array [1])]) [:r] {:r #{:r}} (rel ['?x] [])]
           [(rel ['?x] [[1]]) [:r] {:r #{:r}} (relation/relation! {'?x 0} [[1]])]]]
    (let [active (atom #{})
          error (try (fixed-point/run-bounded! {:r base} ids dependencies
                       (fn [_ _ consume!] (consume! body)) (kernel-hooks active))
                     (catch Throwable e e))]
      (is (= :query-source/invalid (:error (ex-data error))))
      (is (empty? @active)))))
