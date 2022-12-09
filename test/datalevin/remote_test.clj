(ns datalevin.remote-test
  (:require [datalevin.remote :as sut]
            [datalevin.storage :as st]
            [datalevin.interpret :as i]
            [datalevin.datom :as d]
            [datalevin.core :as dc]
            [datalevin.db :as db]
            [datalevin.constants :as c]
            [datalevin.client :as cl]
            [datalevin.test.core :refer [server-fixture]]
            [clojure.test :refer [is testing deftest use-fixtures]])
  (:import [java.util UUID]
           [datalevin.datom Datom]))

(use-fixtures :each server-fixture)

(deftest dt-store-ops-test
  (testing "permission"
    (is (thrown? Exception (sut/open "dtlv://someone:wrong@localhost/nodb")))

    (let [client (cl/new-client "dtlv://datalevin:datalevin@localhost")]
      (cl/create-user client "someone" "secret")
      (cl/grant-permission client :datalevin.role/someone
                           :datalevin.server/create
                           :datalevin.server/database
                           nil)
      (is (= (count (cl/list-user-permissions client "someone")) 3))

      (cl/create-user client "viewer" "secret")
      (cl/grant-permission client :datalevin.role/viewer
                           :datalevin.server/view
                           :datalevin.server/database
                           nil)))

  (testing "datalog store ops"
    (let [dir   "dtlv://someone:secret@localhost/ops-test"
          store (sut/open dir)]
      (is (instance? datalevin.remote.DatalogStore store))
      (is (= c/implicit-schema (st/schema store)))
      (is (= c/e0 (st/init-max-eid store)))
      (let [a  :a/b
            v  (UUID/randomUUID)
            d  (d/datom c/e0 a v)
            s  (assoc (st/schema store) a {:db/aid 3})
            b  :b/c
            p1 {:db/valueType :db.type/uuid}
            v1 (UUID/randomUUID)
            d1 (d/datom c/e0 b v1)
            s1 (assoc s b (merge p1 {:db/aid 4}))
            c  :c/d
            p2 {:db/valueType :db.type/ref}
            v2 (long (rand c/emax))
            d2 (d/datom c/e0 c v2)
            s2 (assoc s1 c (merge p2 {:db/aid 5}))
            t1 (st/last-modified store)]
        (st/load-datoms store [d])
        (is (<= t1 (st/last-modified store)))
        (is (= s (st/schema store)))
        (is (= 1 (st/datom-count store :eav)))
        (is (= 1 (st/datom-count store :ave)))
        (is (= 0 (st/datom-count store :vea)))
        (is (= [d] (st/fetch store d)))
        (is (= [d] (st/slice store :eavt d d)))
        (is (= true (st/populated? store :eav d d)))
        (is (= 1 (st/size store :eav d d)))
        (is (= d (st/head store :eav d d)))
        (st/swap-attr store b (i/inter-fn [& ms] (apply merge ms)) p1)
        (st/load-datoms store [d1])
        (is (= s1 (st/schema store)))
        (is (= 2 (st/datom-count store :eav)))
        (is (= 2 (st/datom-count store :ave)))
        (is (= 0 (st/datom-count store :vea)))
        (is (= [] (st/slice store :eav d (d/datom c/e0 :non-exist v1))))
        (is (= 0 (st/size store :eav d (d/datom c/e0 :non-exist v1))))
        (is (nil? (st/populated? store :eav d (d/datom c/e0 :non-exist v1))))
        (is (= d (st/head store :eav d d1)))
        (is (= 2 (st/size store :eav d d1)))
        (is (= [d d1] (st/slice store :eav d d1)))
        (is (= [d d1] (st/slice store :ave d d1)))
        (is (= [d1 d] (st/rslice store :eav d1 d)))
        (is (= [d d1] (st/slice store :eav
                                (d/datom c/e0 a nil)
                                (d/datom c/e0 nil nil))))
        (is (= [d1 d] (st/rslice store :eav
                                 (d/datom c/e0 b nil)
                                 (d/datom c/e0 nil nil))))
        (is (= 1 (st/size-filter store :eav
                                 (i/inter-fn [^Datom d] (= v (dc/datom-v d)))
                                 (d/datom c/e0 nil nil)
                                 (d/datom c/e0 nil nil))))
        (is (= d (st/head-filter store :eav
                                 (i/inter-fn [^Datom d] (= v (dc/datom-v d)))
                                 (d/datom c/e0 nil nil)
                                 (d/datom c/e0 nil nil))))
        (is (= [d] (st/slice-filter store :eav
                                    (i/inter-fn [^Datom d] (= v (dc/datom-v d)))
                                    (d/datom c/e0 nil nil)
                                    (d/datom c/e0 nil nil))))
        (is (= [d1 d] (st/rslice store :ave d1 d)))
        (is (= [d d1] (st/slice store :ave
                                (d/datom c/e0 a nil)
                                (d/datom c/e0 nil nil))))
        (is (= [d1 d] (st/rslice store :ave
                                 (d/datom c/e0 b nil)
                                 (d/datom c/e0 nil nil))))
        (is (= [d] (st/slice-filter store :ave
                                    (i/inter-fn [^Datom d] (= v (dc/datom-v d)))
                                    (d/datom c/e0 nil nil)
                                    (d/datom c/e0 nil nil))))
        (st/swap-attr store c (i/inter-fn [& ms] (apply merge ms)) p2)
        (st/load-datoms store [d2])
        (is (= s2 (st/schema store)))
        (is (= 3 (st/datom-count store c/eav)))
        (is (= 3 (st/datom-count store c/ave)))
        (is (= 1 (st/datom-count store c/vea)))
        (is (= [d2] (st/slice store :vea
                              (d/datom c/e0 nil v2)
                              (d/datom c/emax nil v2))))
        (st/load-datoms store [(d/delete d)])
        (is (= 2 (st/datom-count store c/eav)))
        (is (= 2 (st/datom-count store c/ave)))
        (is (= 1 (st/datom-count store c/vea)))
        (st/close store)
        (is (st/closed? store))
        (let [store (sut/open dir)]
          (is (= [d1] (st/slice store :eav d1 d1)))
          (st/load-datoms store [(d/delete d1)])
          (is (= 1 (st/datom-count store c/eav)))
          (st/load-datoms store [d d1])
          (is (= 3 (st/datom-count store c/eav)))
          (st/close store))
        (let [d     :d/e
              p3    {:db/valueType :db.type/long}
              s3    (assoc s2 d (merge p3 {:db/aid 6}))
              s4    (assoc s3 :f/g {:db/aid 7 :db/valueType :db.type/string})
              store (sut/open dir {d p3})]
          (is (= s3 (st/schema store)))
          (st/set-schema store {:f/g {:db/valueType :db.type/string}})
          (is (= s4 (st/schema store)))
          (st/close store)))))

  (testing "data viewer permission"
    (let [dir   "dtlv://viewer:secret@localhost/ops-test"
          store (sut/open dir)]
      (is (instance? datalevin.remote.DatalogStore store))
      (is (not= c/implicit-schema (st/schema store)))
      (is (= 3 (st/datom-count store c/eav)))

      (is (thrown? Exception (st/set-schema store {:o/p {}})))
      (is (thrown? Exception (st/load-datoms store []))))))

(deftest dt-store-larger-test
  (let [dir   "dtlv://datalevin:datalevin@localhost/larger-test"
        end   1000
        store (sut/open dir)
        vs    (range 0 end)
        txs   (mapv d/datom (range c/e0 (+ c/e0 end)) (repeat :id)
                    vs)
        pred  (i/inter-fn [d] (odd? (dc/datom-v d)))]
    (is (instance? datalevin.remote.DatalogStore store))
    (st/load-datoms store txs)
    (is (= (d/datom c/e0 :id 0)
           (st/head store :eav (d/datom c/e0 :id nil)
                    (d/datom c/emax :id nil))))
    (is (= (d/datom (dec (+ c/e0 end) ) :id (dec (+ c/e0 end)))
           (st/tail store :ave (d/datom c/emax :id nil)
                    (d/datom c/e0 :id nil))))
    (is (= (filter pred txs)
           (st/slice-filter store :eav pred
                            (d/datom c/e0 nil nil)
                            (d/datom c/emax nil nil))))
    (is (= (reverse txs)
           (st/rslice store :eav
                      (d/datom c/emax nil nil) (d/datom c/e0 nil nil))))
    (st/close store)))

(deftest same-client-multiple-dbs-test
  (let [uri-str "dtlv://datalevin:datalevin@localhost"
        client  (cl/new-client uri-str)
        store1  (sut/open-kv client (str uri-str "/mykv") nil)
        store2  (sut/open client (str uri-str "/mydt") nil nil)]
    (is (instance? datalevin.remote.KVStore store1))
    (is (instance? datalevin.remote.DatalogStore store2))

    (dc/open-dbi store1 "a")
    (dc/transact-kv store1 [[:put "a" "hello" "world"]])
    (is (= (dc/get-value store1 "a" "hello") "world"))
    (dc/close-kv store1)

    (let [conn (dc/conn-from-db (db/new-db store2))]
      (dc/transact! conn [{:hello "world"}])
      (is (= (dc/q '[:find ?w .
                     :where
                     [_ :hello ?w]]
                   @conn)
             "world"))
      (dc/close conn))))
