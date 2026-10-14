(ns datalevin.copy-out-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.client :as client]))

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
