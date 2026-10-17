(ns datalevin.query.source-runtime-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt])
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
