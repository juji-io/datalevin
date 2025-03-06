(ns datalevin.concurrent-test
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :as t :refer [is deftest use-fixtures]]
   [datalevin.util :as u]
   [datalevin.constants :as c]
   [datalevin.core :as d])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest large-data-concurrent-write-test
  (let [dir  (u/tmp-dir (str "large-concurrent-" (UUID/randomUUID)))
        conn (d/get-conn dir)
        d1   (apply str (repeat 1000 \a))
        d2   (apply str (repeat 1000 \a))
        tx1  [{:a d1 :b 1}]
        tx2  [{:a d2 :b 2}]
        f1   (future (d/transact! conn tx1))]
    @(future (d/transact! conn tx2))
    @f1
    (is (= 4 (count (d/datoms @conn :eav))))
    (d/close conn)))

(deftest test-multi-threads-transact
  ;; we serialize writes, so as not to violate uniqueness constraint
  (let [dir  (u/tmp-dir (str "multi-" (UUID/randomUUID)))
        conn (d/create-conn
               dir
               {:instance/id
                #:db{:valueType   :db.type/long
                     :unique      :db.unique/identity
                     :cardinality :db.cardinality/one}}
               {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
    (dorun (pmap #(d/transact! conn [{:instance/id %}])
                 (range 100)))
    (is (= 100 (d/q '[:find (max ?e) . :where [?e :instance/id]] @conn)))
    (let [res (d/q '[:find ?e ?a ?v :where [?e ?a ?v]] @conn)]
      (is (thrown-with-msg? Exception #"unique constraint"
                            (d/transact! conn [(into [:db/add 3]
                                                     (next (first res)))]))))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-multi-threads-reads-writes
  (let [dir     (u/tmp-dir (str "multi-rw-" (UUID/randomUUID)))
        conn    (d/create-conn dir {} {:validate-data?    true
                                       :auto-entity-time? true
                                       :kv-opts
                                       {:flags
                                        (conj c/default-env-flags :nosync)}})
        q+      '[:find ?i+j .
                  :in $ ?i ?j
                  :where [?e :i+j ?i+j] [?e :i ?i] [?e :j ?j]]
        q*      '[:find ?i*j .
                  :in $ ?i ?j
                  :where [?e :i*j ?i*j] [?e :i ?i] [?e :j ?j]]
        trials  (atom 0)
        futures (mapv (fn [^long i]
                        (future
                          (dotimes [j 100]
                            (d/transact! conn [{:i+j (+ i j) :i i :j j}])
                            (d/with-transaction [cn conn]
                              (is (= (+ i j) (d/q q+ (d/db cn) i j)))
                              (swap! trials u/long-inc)
                              (d/transact! cn [{:i*j (* i j) :i i :j j}])
                              (is (= (* i j) (d/q q* (d/db cn) i j)))))))
                      (range 5))]
    (doseq [f futures] @f)
    (is (= 500 @trials))
    (is (= 5000 (count (d/datoms @conn :eav))))
    (dorun (for [i (range 5) j (range 100)]
             (is (= (+ ^long i ^long j) (d/q q+ (d/db conn) i j)))))
    (dorun (for [i (range 5) j (range 100)]
             (is (= (* ^long i ^long j) (d/q q* (d/db conn) i j)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-multi-threads-read
  (let [dir   (u/tmp-dir (str "concurrent-read-" (UUID/randomUUID)))
        conn  (d/get-conn dir {:id {:db/unique :db.unique/identity}})
        n     10000
        all   (range n)
        tx    (map (fn [i] {:id i}) (range n))
        query (fn [i]
                (d/q '[:find ?e .
                       :in $ ?i
                       :where [?e :id ?i]]
                     @conn i))
        pull  (fn [^long i] (:id (d/pull @conn '[*] (inc i))))]
    (d/transact! conn tx)
    (is (= n (d/count-datoms @conn nil nil nil)))

    (dotimes [_ 100]
      (let [futs (mapv #(future (pull %)) all)]
        (is (= all (for [f futs] @f))))
      (is (= (range 1 (inc n)) (pmap query all)))
      (is (= all (pmap pull all))))
    (d/close conn)))

(deftest test-multi-threads-read-1
  (let [dir  (u/tmp-dir (str "concurrent-read1-" (UUID/randomUUID)))
        conn (d/get-conn dir {:children {:db/valueType   :db.type/ref
                                         :db/cardinality :db.cardinality/many}})
        n    10000
        rng  (range 1 n)
        tx   (mapv (fn [^long i]
                     (let [id (* -1 i)]
                       {:db/id    id
                        :children [{:db/id  (dec id)
                                    :string "test"}]}))
                   (range 1 n))
        pull (fn [id] (d/pull @conn '[:db/id
                                     :string
                                     {:children 100}] id))]
    (d/transact! conn tx)
    (is (= (* 2 (dec n)) (d/count-datoms @conn nil nil nil)))
    (is (= (dec n) (count (pmap pull rng))))
    (is (= (dec n) (count (pmap pull rng))))
    (is (= (dec n) (count (pmap pull rng))))
    (is (= (dec n) (count (pmap pull rng))))
    (is (= (dec n) (count (pmap pull rng))))
    (is (= (dec n) (count (pmap pull rng))))
    (is (= (dec n) (count (pmap pull rng))))
    (is (= (dec n) (count (pmap pull rng))))
    (is (= (dec n) (count (pmap pull rng))))
    (is (= (dec n) (count (pmap pull rng))))

    (d/close conn)))
