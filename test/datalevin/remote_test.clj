(ns datalevin.remote-test
  (:require [datalevin.remote :as sut]
            [datalevin.server :as srv]
            [datalevin.storage :as st]
            [datalevin.datom :as d]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [clojure.test :refer [is are testing deftest use-fixtures]])
  (:import [java.util UUID]
           [datalevin.remote DatalogStore]
           [datalevin.datom Datom]))

(def ^:dynamic store nil)

(def dir  "dtlv://datalevin:datalevin@localhost/test")

(defn server-fixture
  [f]
  (let [server (srv/create {:port c/default-port
                            :root (u/tmp-dir
                                    (str "remote-test-" (UUID/randomUUID)))})]
    (try
      (srv/start server)
      (with-redefs [store (sut/open dir)]
        (f)
        (st/close store))
      (catch Exception e (throw e))
      (finally (srv/stop server)))))

(use-fixtures :each server-fixture)

(deftest basic-ops-test
  (is (instance? datalevin.remote.DatalogStore store))
  (is (= c/implicit-schema (st/schema store)))
  (is (= c/e0 (st/init-max-eid store)))
  (let [a  :a/b
        v  (UUID/randomUUID)
        d  (d/datom c/e0 a v)
        s  (assoc (st/schema store) a {:db/aid 1})
        b  :b/c
        p1 {:db/valueType :db.type/uuid}
        v1 (UUID/randomUUID)
        d1 (d/datom c/e0 b v1)
        s1 (assoc s b (merge p1 {:db/aid 2}))
        c  :c/d
        p2 {:db/valueType :db.type/ref}
        v2 (long (rand c/emax))
        d2 (d/datom c/e0 c v2)
        s2 (assoc s1 c (merge p2 {:db/aid 3}))
        t1 (st/last-modified store)]
    (st/load-datoms store [d])
    (is (< t1 (st/last-modified store)))
    (is (= s (st/schema store)))
    (is (= 1 (st/datom-count store :eav)))
    (is (= 1 (st/datom-count store :ave)))
    (is (= 0 (st/datom-count store :vea)))
    (is (= [d] (st/fetch store d)))
    (is (= [d] (st/slice store :eavt d d)))
    (is (= true (st/populated? store :eav d d)))
    (is (= 1 (st/size store :eav d d)))
    (is (= d (st/head store :eav d d)))
    (st/swap-attr store b merge p1)
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
                             (fn [^Datom d] (= v (.-v d)))
                             (d/datom c/e0 nil nil)
                             (d/datom c/e0 nil nil))))
    (is (= d (st/head-filter store :eav
                             (fn [^Datom d] (= v (.-v d)))
                             (d/datom c/e0 nil nil)
                             (d/datom c/e0 nil nil))))
    (is (= [d] (st/slice-filter store :eav
                                (fn [^Datom d] (= v (.-v d)))
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
                                (fn [^Datom d] (= v (.-v d)))
                                (d/datom c/e0 nil nil)
                                (d/datom c/e0 nil nil))))
    (st/swap-attr store c merge p2)
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
          s3    (assoc s2 d (merge p3 {:db/aid 4}))
          s4    (assoc s3 :f/g {:db/aid 5 :db/valueType :db.type/string})
          store (sut/open dir {d p3})]
      (is (= s3 (st/schema store)))
      (st/set-schema store {:f/g {:db/valueType :db.type/string}})
      (is (= s4 (st/schema store))))))
