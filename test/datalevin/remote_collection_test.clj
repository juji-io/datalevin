(ns datalevin.remote-collection-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.test.core :refer [server-fixture]]))

(use-fixtures :each server-fixture)

(def ^:private boundary-sizes
  [(dec c/+wire-datom-batch-size+) c/+wire-datom-batch-size+
   (inc c/+wire-datom-batch-size+)])

(deftest remote-scalar-collections-retain-their-types
  (doseq [compression [:none :zstd]]
    (let [conn (d/create-conn
                 (str "dtlv://datalevin:datalevin@localhost/collections-"
                      (name compression))
                 nil {:client-opts {:wire-compression compression
                                    :wire-compression-threshold 0}})]
      (try
        (doseq [n boundary-sizes
                [value pred] [[(assoc (into {} (map (fn [i] [i i])) (range n))
                                     0 nil 1 false) map?]
                              [(set (range n)) set?]
                              [(vec (range n)) vector?]]]
          (testing (str n " items with " compression " compression, " pred)
            (d/transact! conn [{:db/id 1 :item/value value}])
            (let [result (d/q '[:find ?value . :where [1 :item/value ?value]] @conn)]
              (is (pred result))
              (is (= n (count result)))
              (is (= value result))
              (when (map? value)
                (is (contains? result 0))
                (is (nil? (get result 0 ::missing)))
                (is (false? (get result 1 ::missing)))
                (is (= (dec n) (get result (dec n) ::missing))))
              (when (set? value)
                (is (contains? result (dec n)))))))
        (finally (d/close conn))))))

(deftest remote-wide-pulls-retain-map-semantics
  (let [conn (d/create-conn "dtlv://datalevin:datalevin@localhost/wide-pulls"
                            {:item/ref {:db/valueType :db.type/ref}})]
    (try
      (d/transact! conn [{:db/id 1 :item/name "target"}])
      (doseq [n boundary-sizes]
        (let [attrs (into {} (map (fn [i] [(keyword "wide" (str "k" i)) i]))
                          (range (- n 2)))
              eid (+ n 10000)
              expected (assoc attrs :db/id eid :item/ref {:db/id 1})]
          (d/transact! conn [(assoc attrs :db/id eid :item/ref 1)])
          (doseq [[label read-result]
                  [[:explicit-ref #(d/pull @conn '[* {:item/ref [:db/id]}] eid)]
                   [:implicit-ref #(d/pull @conn '[*] eid)]
                   [:scalar-pull #(d/q '[:find (pull ?e [* {:item/ref [:db/id]}]) .
                                        :in $ ?e] @conn eid)]]]
            (testing (str label " with " n " entries")
              ;; Repeat reads to cover prepared requests and cached query results.
              (dotimes [_ 2]
                (let [result (read-result)]
                  (is (map? result))
                  (is (= n (count result)))
                  (is (= expected result))
                  (is (= eid (:db/id result)))
                  (is (= {:db/id 1} (:item/ref result)))
                  (is (= (get attrs :wide/k0) (:wide/k0 result)))))))))
      (finally (d/close conn)))))
