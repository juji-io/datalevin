(ns datalevin.test.conn
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.test.core :as tdc]
   [datalevin.constants :as c]))

(def schema { :aka { :db/cardinality :db.cardinality/many :db/aid 1}})
(def datoms #{(d/datom 1 :age  17)
              (d/datom 1 :name "Ivan")})

(deftest test-close
  (let [conn (d/create-conn)]
    (is (not (d/closed? conn)))
    (d/close conn)
    (is (d/closed? conn))))

(deftest test-update-schema
  (let [conn1 (d/create-conn)
        s {:a/b {:db/valueType :db.type/string}}
        s1 {:c/d {:db/valueType :db.type/string}}
        txs [{:c/d "cd" :db/id -1}]
        conn2 (d/create-conn s)]
    (is (= (d/schema conn2) (d/update-schema conn1 s)))
    (d/update-schema conn1 s1)
    (d/transact! conn1 txs)
    (is (not (nil? (:a (first (d/datoms @conn1 :eavt))))))))


(deftest test-ways-to-create-conn-1
  (let [conn (d/create-conn)]
    (is (= #{} (set (d/datoms @conn :eavt))))
    (is (= c/implicit-schema (:schema @conn)))))

(deftest test-ways-to-create-conn-2
  (let [conn (d/create-conn schema)]
    (is (= #{} (set (d/datoms @conn :eavt))))
    (is (= (:schema @conn) (merge schema c/implicit-schema)))))

(deftest test-ways-to-create-conn-3

  (let [conn (d/conn-from-datoms datoms)]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= c/implicit-schema (:schema @conn))))

  (let [conn (d/conn-from-datoms datoms schema)]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= (merge schema c/implicit-schema) (:schema @conn))))

  (let [conn (d/conn-from-db (d/init-db datoms))]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= c/implicit-schema (:schema @conn))))

  (let [conn (d/conn-from-db (d/init-db datoms schema))]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= (merge schema c/implicit-schema) (:schema @conn)))))
