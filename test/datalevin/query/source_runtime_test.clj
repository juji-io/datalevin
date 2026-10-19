(ns datalevin.query.source-runtime-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value])
  (:import [datalevin.query.source.runtime Cleanup]
           [java.util.concurrent.atomic AtomicReference]))

(defn- detached? [^Cleanup cleanup]
  ;; Check the owning slot directly instead of depending on GC scheduling.
  (nil? (.get ^AtomicReference (.-action cleanup))))

(deftest explicit-close-detaches-its-resource-and-runtime
  (doseq [fail? [false true]]
    (let [calls (atom 0)]
      (rt/with-runtime {}
        (fn [runtime]
          (let [close (rt/with-context runtime {:source-symbol '$s :clause-ids [2]}
                        #(rt/own! % (fn []
                                      (swap! calls inc)
                                      (when fail? (throw (Exception. "private close detail")))
                                      :closed)))
                cleanup (peek @(:cleanup runtime))]
            (is (fn? close))
            (is (not (detached? cleanup)))
            (if fail?
              (let [error (try (close) (catch Throwable e e))]
                (is (= {:error :query-source/provider :phase :close
                        :source-symbol '$s :clause-ids [2]}
                       (select-keys (ex-data error) [:error :phase :source-symbol :clause-ids])))
                (is (nil? (.getCause ^Throwable error)))
                (is (not (.contains (str error) "private close detail"))))
              (is (= :closed (close))))
            (is (detached? cleanup))
            (is (nil? (close)))
            (is (= 1 @calls)))))
      (is (= 1 @calls)))))

(deftest concurrent-and-reentrant-close-execute-once
  (doseq [fail? [false true]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [calls (atom 0) self (atom nil) start (promise)
              close (rt/own! runtime #(do (swap! calls inc)
                                         (@self)
                                         (when fail? (throw (Exception. "private close detail")))
                                         :closed))
              _ (reset! self close)
              workers (mapv (fn [_] (future @start (try (close) (catch Throwable e e)))) (range 8))]
          (try
            (deliver start true)
            (let [outcomes (mapv #(deref % 3000 :hung) workers)]
              (is (= 1 @calls))
              (is (= 7 (count (filter nil? outcomes))))
              (is (not-any? #{:hung} outcomes))
              (if fail?
                (is (= [:query-source/provider]
                       (mapv #(-> % ex-data :error) (remove nil? outcomes))))
                (is (= [:closed] (vec (remove nil? outcomes)))))
              (is (every? detached? @(:cleanup runtime))))
            (finally (deliver start true) (run! future-cancel workers))))))))

(deftest cleanup-keeps-order-primary-errors-and-ledger-release
  (doseq [primary? [false true]]
    (let [events (atom []) captured (atom nil) entries (atom nil)
          error (try
                  (rt/with-runtime {}
                    (fn [runtime]
                      (reset! captured runtime)
                      (source/reserve! (:control runtime) {:retained-bytes 1024})
                      (rt/own! runtime #(swap! events conj :first))
                      (rt/own! runtime #(do (swap! events conj :second)
                                           (throw (Exception. "private cleanup"))))
                      (let [close (rt/own! runtime #(swap! events conj :last))]
                        (reset! entries @(:cleanup runtime))
                        (close))
                      (when primary?
                        (throw (ex-info "private primary" {:error :query-source/snapshot-lost
                                                          :phase :fetch})))))
                  (catch Throwable e e))]
      (is (= [:last :second :first] @events))
      (is (= (if primary? :query-source/snapshot-lost :query-source/provider)
             (:error (ex-data error))))
      (is (= (if primary? 1 0) (count (.getSuppressed ^Throwable error))))
      (is (every? detached? @entries))
      (is (empty? @(:cleanup @captured)))
      (is (every? zero? (vals (control/usage (:control @captured)))))
      (is (not (.contains (pr-str error) "private"))))))

(defn- measured [bytes on-size]
  (reify value/IQuerySourceValue
    (-logical-type [_] {:kind :custom :name :test/measured :version 1 :fingerprint "v1"})
    (-retained-bytes [_] (on-size) bytes)))

(deftest metadata-admission-rejects-custom-values-without-calling-hooks
  (let [calls (atom 0)
        custom (measured 64 #(swap! calls inc))]
    (doseq [phase [:context :descriptor :session :planning]]
      (let [error (try
                    (rt/with-runtime {}
                      #(rt/charge-metadata! % {:extension custom} phase))
                    (catch clojure.lang.ExceptionInfo e (ex-data e)))]
        (is (= :query-source/invalid (:error error)))
        (is (= phase (:phase error)))))
    (is (zero? @calls))))

(deftest metadata-validation-and-sizing-observe-nested-cancellation
  (doseq [size? [false true]]
    (let [ctl (control/create {})
          visits (atom 0)
          check! #(do (when (= 12 (swap! visits inc))
                        (control/cancel! ctl {:error :query-source/cancelled :phase :execution}))
                      (source/check-active! ctl))
          metadata {:extension (with-meta (vec (range 1000)) {:nested (vec (range 1000))})}
          error (try
                  (if size?
                    (value/metadata-charge metadata :planning (fn [_] (check!)))
                    (value/portable? metadata check!))
                  (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (try
        (is (= :query-source/cancelled (:error error)))
        (is (= 12 @visits))
        (finally (control/close! ctl))))))

(deftest scaled-and-output-charges-preserve-ledger-amounts
  (let [row [(with-meta [1 (byte-array [1 2 3])] {:source "fixture"})]
        size (value/value-charge row)]
    (rt/with-runtime {}
      (fn [{:keys [control] :as runtime}]
        (let [token (rt/charge! runtime row 512 3)]
          (is (= (+ 512 (* 3 size)) (:retained-bytes (control/usage control))))
          (is (zero? (:result-rows (control/usage control))))
          (source/release! token))
        (let [token (rt/charge-result! runtime row)
              usage (control/usage control)]
          (is (= 1 (:result-rows usage)))
          (is (= (+ 256 size) (:retained-bytes usage) (:result-bytes usage)))
          (source/release! token))
        (is (every? zero? (vals (control/usage control))))))))

(deftest output-quotas-reject-before-visiting-values
  (doseq [category [:retained-bytes :result-bytes :result-rows]]
    (let [visits (atom 0) captured (atom nil)
          row [(measured 64 #(swap! visits inc))]
          error (try
                  (rt/with-runtime {:limits {category 1}}
                    (fn [{:keys [control] :as runtime}]
                      (reset! captured control)
                      (source/reserve! control {category 1})
                      (rt/charge-result! runtime row)))
                  (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (= :query-source/limit (:error error)))
      (is (= category (:category error)))
      (is (= 1 (:maximum error)))
      (is (> (:attempted error) 1))
      (is (zero? @visits))
      (is (every? zero? (vals (control/usage @captured)))))))

(deftest scaled-and-result-byte-overflows-stop-before-later-values
  (doseq [[size maximum result?]
          [[4096 8192 false] [Long/MAX_VALUE Long/MAX_VALUE false] [4096 1024 true]]]
    (let [visits (atom []) captured (atom nil)
          row [(measured size #(swap! visits conj :head))
               (measured 64 #(swap! visits conj :tail))]
          category (if result? :result-bytes :retained-bytes)
          error (try
                  (rt/with-runtime {:limits {category maximum}}
                    (fn [{:keys [control] :as runtime}]
                      (reset! captured control)
                      (if result? (rt/charge-result! runtime row)
                          (rt/charge! runtime row 512 3))))
                  (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (= :query-source/limit (:error error)))
      (is (= category (:category error)))
      (is (= maximum (:maximum error)))
      (is (> (:attempted error) maximum))
      (is (= [:head] @visits))
      (is (every? zero? (vals (control/usage @captured)))))))

(deftest synchronous-workspaces-release-without-growing-parent-cleanup
  (rt/with-runtime {}
    (fn [runtime]
      (let [events (atom [])]
        (dotimes [i 100]
          (rt/with-workspace runtime
            (fn [workspace]
              (is (identical? workspace rt/*runtime*))
              (is (identical? (:control runtime) (:control workspace)))
              (let [token (source/reserve! (:control workspace) {:retained-bytes 4096})]
                (rt/own! workspace #(do (swap! events conj [i :first]) (source/release! token)))
                (rt/own! workspace #(swap! events conj [i :last])))
              (rt/with-workspace workspace
                #(rt/own! % (fn [] (swap! events conj [i :nested]))))))
          (is (empty? @(:cleanup runtime)))
          (is (every? zero? (vals (control/usage (:control runtime))))))
        (is (= (vec (mapcat (fn [i] [[i :nested] [i :last] [i :first]]) (range 100))) @events))))))

(deftest workspace-cleanup-observes-cancellation-before-close-errors
  (let [captured (atom nil)
        error (try
                (rt/with-runtime {}
                  (fn [runtime]
                    (reset! captured (:control runtime))
                    (rt/with-workspace runtime
                      (fn [workspace]
                        (rt/own! workspace #(throw (Exception. "private close")))
                        (rt/own! workspace #(control/cancel! (:control workspace)
                                             {:error :query-source/cancelled}))))))
                (catch Throwable e e))]
    (is (= :query-source/cancelled (:error (ex-data error))))
    (is (= [:query-source/provider] (mapv #(-> % ex-data :error) (.getSuppressed ^Throwable error))))
    (is (not (.contains (pr-str error) "private")))
    (is (every? zero? (vals (control/usage @captured))))))

(deftest workspace-owns-new-resources-from-captured-operator-runtimes
  (rt/with-runtime {}
    (fn [runtime]
      (let [owner (assoc runtime :cleanup (atom [])) closed (atom 0)]
        (dotimes [_ 100]
          (rt/with-workspace runtime [owner]
            (fn [_]
              (let [token (source/reserve! (:control owner) {:retained-bytes 4096})]
                (rt/own! owner #(do (swap! closed inc) (source/release! token))))))
          (is (empty? @(:cleanup owner)))
          (is (empty? @(:cleanup runtime)))
          (is (every? zero? (vals (control/usage (:control runtime))))))
        (is (= 100 @closed))
        (let [foreign (assoc owner :query-id :foreign)
              error (try (rt/with-workspace runtime [foreign] (fn [_])) (catch Throwable e e))]
          (is (= :query-source/invalid (:error (ex-data error))))
          (is (every? zero? (vals (control/usage (:control runtime))))))))))
