(ns datalevin.remote-tx-retry-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.client :as cl]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.remote :as remote]
            [datalevin.test.core :refer [server-fixture]]))

(use-fixtures :each server-fixture)

(deftest remote-tx-result-finds-max-eid-by-key
  (let [tx-data [(d/datom 1 :item/id 0)]
        entries [[-1 1] ["second" 2] [:db/current-tx 3]]]
    (doseq [position (range (inc (count entries)))]
      (testing (str ":max-eid at position " position)
        (let [pairs (concat (take position entries)
                            [[:max-eid 2]]
                            (drop position entries))
              result (into tx-data pairs)]
          (is (= [tx-data (into {} entries) 2 nil]
                 (#'db/remote-tx-result result))))))
    (testing "an older response without :max-eid retains every tempid"
      (is (= [tx-data (into {} entries) nil nil]
             (#'db/remote-tx-result (into tx-data entries)))))))

(deftest remote-tx-response-normalizes-both-wire-shapes
  (let [tx-data [(d/datom 1 :item/id 0 2 true)
                 (d/datom 2 :item/id 1 2 false)]
        tempids {-1 1 "second" 2 :db/current-tx 2 :max-eid 2}
        info {:max-eid 2 :max-tx 2 :last-modified 123}
        attrs [:item/id]
        expected {:tx-data tx-data :tempids tempids
                  :db-info info :new-attributes attrs}]
    (doseq [result [{:tx-data [[1 :item/id 0 2 true]
                             [2 :item/id 1 2 false]]
                    :tempids tempids}
                   (into tx-data tempids)]]
      (let [decoded (cl/decode-tx-response
                     {:type :command-complete :result result
                      :db-info info :new-attributes attrs})]
        (is (= expected decoded))
        (is (every? datom/datom? (:tx-data decoded)))
        (is (= [2 2] (mapv datom/datom-tx (:tx-data decoded))))
        (is (= [true false] (mapv datom/datom-added (:tx-data decoded))))))
    (testing "metadata within an inline result is retained"
      (is (= expected
             (cl/decode-tx-response
              {:type :command-complete :result expected}))))
    (testing "acknowledgements retain their result and database info"
      (let [ack {:result :transacted :db-info info}]
        (is (= ack (cl/decode-tx-response
                    {:type :command-complete :result ack})))))))

(deftest remote-tx-retries-preserve-tempids-and-metadata
  (doseq [wal? [false true]
          failure [:rejection :transport]
          n [100 (quot c/+wire-datom-batch-size+ 2)
             c/+wire-datom-batch-size+]]
    (testing (str failure ", " n " entities, WAL " wal?)
      (let [db-name (str "tx-retry-" wal? "-" (name failure) "-" n)
            conn (d/create-conn
                   (str "dtlv://datalevin:datalevin@localhost/" db-name)
                   nil {:wal? wal?})
            base (cl/->Client "datalevin" "datalevin" "127.0.0.1" 1
                              1 5000 nil nil)
            endpoint (str "127.0.0.1:" cl/*default-port*)
            source {:ha-members [{:endpoint endpoint}]}
            tempid (fn [i] (if (even? i) (- (inc i)) (str "entity-" i)))
            tx (mapv (fn [i] {:db/id (tempid i) :item/id i}) (range n))
            expected (into {} (map (fn [i] [(tempid i) (inc i)])) (range n))
            requests (atom [])
            replies (atom [])
            send-fn
            (fn [client req data]
              (swap! requests conj req)
              (if (identical? base client)
                (case failure
                  :rejection
                  {:type :error-response :message "leader changed"
                   :err-data {:error :ha/write-rejected :retryable? true
                              :ha-retry-endpoints [endpoint]}}
                  :transport
                  (throw (ex-info "connection lost" {})))
                (let [response (#'remote/request-datoms client req data)]
                  (swap! replies conj response)
                  response)))]
        (try
          (let [result (#'remote/load-datoms* base db-name tx :txs+info false
                        false source send-fn)
                response (first @replies)
                [tx-data ids max-eid attrs] (#'db/remote-tx-result result)]
            (is (map? result))
            (is (= expected (dissoc ids :db/current-tx)))
            (is (= (inc n) (count ids)))
            (is (not (contains? ids :max-eid)))
            (is (= n max-eid (get-in result [:db-info :max-eid])))
            (is (= (:db/current-tx ids) (get-in result [:db-info :max-tx])))
            (is (pos? (get-in result [:db-info :last-modified])))
            (is (= #{:item/id} (set attrs)))
            (is (= n (count tx-data)))
            (is (every? datom/datom? tx-data))
            (is (= (into #{} (map (fn [i] [(inc i) i])) (range n))
                   (into #{} (map (juxt :e :v)) tx-data)))
            (is (= 2 (count @requests)))
            (is (apply = @requests) "retry retains the client operation ID")
            (is (= (if (< n c/+wire-datom-batch-size+) :request :copy-in)
                   (:mode (first @requests))))
            (is (= (< n (quot c/+wire-datom-batch-size+ 2))
                   (map? (:result response))))
            (when (sequential? (:result response))
              (is (= (:db-info response) (:db-info result)))
              (is (= (:new-attributes response) (:new-attributes result))))
            (testing "replaying the same operation preserves the full response"
              (let [replay (cl/decode-tx-response
                            (#'remote/request-datoms
                             (cl/active-ha-request-client base)
                             (last @requests) tx))]
                (is (= result replay))))
            (is (= n (d/q '[:find (count ?e) . :where [?e :item/id]] @conn)))
            (testing "subsequent writes retain the entity counter"
              (is (= (inc n)
                     (get (:tempids (d/transact! conn [{:db/id -1 :item/id n}]))
                          -1)))))
          (finally
            (#'cl/disconnect-retry-clients! base cl/disconnect)
            (d/close conn)))))))
