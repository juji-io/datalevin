(ns datalevin.copy-out-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.client :as client]
            [datalevin.constants :as c]
            [datalevin.datom :as dd]
            [datalevin.server.handlers :as handlers]))

(defn- receive-copy
  [header items done]
  (let [messages (atom (seq (concat (partition-all 3 items) [done])))
        conn (reify client/IConnection
               (receive [_]
                 (let [message (first @messages)]
                   (swap! messages next)
                   message))
               (close [_] nil))]
    (#'client/copy-out conn {:type :q} header)))

(deftest copy-out-preserves-collection-types
  (let [entries (mapv (fn [i] [i {:value i}]) (range 17))]
    (doseq [[kind items expected pred]
            [[:map entries (into {} entries) map?]
             [:set (range 17) (set (range 17)) set?]
             [nil entries entries vector?]
             [:map [] {} map?]
             [:set [] #{} set?]]]
      (testing (str "copy result type " kind)
        (let [header (cond-> {:type :copy-out-response}
                       kind (assoc :copy-result-type kind))
              response (receive-copy header items {:type :copy-done})]
          (is (= :command-complete (:type response)))
          (is (pred (:result response)))
          (is (= expected (:result response)))
          (is (not (contains? response :copy-result-type))))))))

(deftest copy-out-retains-response-metadata
  (let [header {:type :copy-out-response :copy-result-type :map
                :db-info {:max-eid 17 :max-tx 2}
                :copy-meta {:db-name "copy-test"}}
        done {:type :copy-done :new-attributes [:item/id]}
        result {:item/id 17}]
    (is (= {:type :command-complete :result result
            :db-info (:db-info header) :copy-meta (:copy-meta header)
            :new-attributes (:new-attributes done)}
           (receive-copy header result done)))))

(deftest transaction-copy-out-preserves-datom-order-for-sequential-reports
  (let [tempids {-1 1 "-1" 2 :db/current-tx 3}
        boundary (- c/+wire-datom-batch-size+ (inc (count tempids)))]
    (doseq [n [(dec boundary) boundary (inc boundary)]
            [kind tx-data-fn] [[:vector identity]
                              [:list #(apply list %)]
                              [:lazy-seq #(map identity %)]]]
      (testing (str kind " report with " n " datoms")
        (let [tx-data (mapv #(dd/datom (inc %) :item/id % 3 (even? %))
                            (range n))
              expected {:tx-data tx-data :tempids (assoc tempids :max-eid n)
                        :db-info {:max-eid n :max-tx 3 :last-modified 123}
                        :new-attributes [:item/id]}
              report (assoc expected :tx-data (tx-data-fn tx-data))
              response (atom nil)
              copied? (atom false)
              deps {:write-message (fn [_ message]
                                     (reset! response message))
                    :copy-out (fn [_ data batch-size copy-meta response-meta]
                                (reset! copied? true)
                                (is (= c/+wire-datom-batch-size+ batch-size))
                                (is (nil? copy-meta))
                                (reset! response
                                        (receive-copy
                                          (assoc response-meta
                                                 :type :copy-out-response)
                                          data {:type :copy-done})))}]
          (#'handlers/write-tx-response! deps nil report)
          (is (= (<= boundary n) @copied?))
          (let [decoded (client/decode-tx-response @response)]
            (is (= expected decoded))
            (is (= (mapv (juxt dd/datom-e dd/datom-tx dd/datom-added) tx-data)
                   (mapv (juxt dd/datom-e dd/datom-tx dd/datom-added)
                         (:tx-data decoded))))))))))

(deftest binary-copy-still-returns-vector-of-chunks
  (let [chunk (byte-array [1 2 3])
        header {:type :copy-out-response :copy-format :binary-chunks}
        done {:type :copy-done :checksum "checksum" :bytes 3 :chunks 1}
        response (receive-copy header [chunk] done)]
    (is (vector? (:result response)))
    (is (= [chunk] (:result response)))
    (is (= :binary-chunks (:copy-format response)))
    (is (= "checksum" (:checksum response)))
    (is (= 3 (:bytes response)))
    (is (= 1 (:chunks response)))))
