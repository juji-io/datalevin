(ns datalevin.test.conn
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest]]
      :clj  [clojure.test :as t :refer        [is deftest]])
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [datalevin.util :as u])
  (:import [java.util Date UUID]))

(deftest test-close
  (let [conn (d/create-conn)]
    (is (not (d/closed? conn)))
    (d/close conn)
    (is (d/closed? conn))))

(deftest test-update-schema
  (let [conn1 (d/create-conn)
        s     {:a/b {:db/valueType :db.type/string}}
        s1    {:c/d {:db/valueType :db.type/string}}
        txs   [{:c/d "cd" :db/id -1}]
        conn2 (d/create-conn nil s)]
    (is (= (d/schema conn2) (d/update-schema conn1 s)))
    (d/update-schema conn1 s1)
    (d/transact! conn1 txs)
    (is (not (nil? (:a (first (d/datoms @conn1 :eavt))))))
    (d/close conn1)
    (d/close conn2)))


(deftest test-ways-to-create-conn-1
  (let [conn (d/create-conn)]
    (is (= #{} (set (d/datoms @conn :eavt))))
    (is (= c/implicit-schema (:schema @conn)))
    (d/close conn)))

(deftest test-ways-to-create-conn-2
  (let [schema { :aka { :db/cardinality :db.cardinality/many :db/aid 1}}
        conn   (d/create-conn nil schema)]
    (is (= #{} (set (d/datoms @conn :eavt))))
    (is (= (:schema @conn) (merge schema c/implicit-schema)))
    (d/close conn)))

(deftest test-ways-to-create-conn-3
  (let [datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")}

        conn (d/conn-from-datoms datoms)]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= (d/schema conn) (:schema @conn)))
    (d/close conn))

  (let [schema { :aka { :db/cardinality :db.cardinality/many :db/aid 1}}
        datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")}

        conn (d/conn-from-datoms datoms nil schema)]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= (d/schema conn) (:schema @conn)))
    (d/close conn))

  (let [datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")}

        conn (d/conn-from-db (d/init-db datoms))]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= (d/schema conn) (:schema @conn)))
    (d/close conn))

  (let [schema { :aka { :db/cardinality :db.cardinality/many :db/aid 1}}
        datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")}

        conn (d/conn-from-db (d/init-db datoms nil schema))]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= (d/schema conn) (:schema @conn)))
    (d/close conn)))

(deftest test-recreate-conn
  (let [schema {:name          {:db/valueType :db.type/string}
                :dt/updated-at {:db/valueType :db.type/instant}}
        dir    (u/tmp-dir (str "recreate-conn-test-" (UUID/randomUUID)))
        conn   (d/create-conn dir schema)]
    (d/transact! conn [{:db/id         -1
                        :name          "Namebo"
                        :dt/updated-at (Date.)}])
    (d/close conn)

    (let [conn2 (d/create-conn dir schema)]
      (d/transact! conn2 [{:db/id         -2
                           :name          "Another name"
                           :dt/updated-at (Date.)}])
      (is (= 4 (count (d/datoms @conn2 :eavt))))
      (d/close conn2))))

(deftest test-get-conn
  (let [schema {:name          {:db/valueType :db.type/string}
                :dt/updated-at {:db/valueType :db.type/instant}}
        dir    (u/tmp-dir (str "get-conn-test-" (UUID/randomUUID)))
        conn   (d/get-conn dir schema)]
    (d/transact! conn [{:db/id         -1
                        :name          "Namebo"
                        :dt/updated-at (Date.)}])
    (d/close conn)

    (let [conn2 (d/get-conn dir schema)]
      (d/transact! conn2 [{:db/id         -2
                           :name          "Another name"
                           :dt/updated-at (Date.)}])
      (is (= 4 (count (d/datoms @conn2 :eavt))))
      (d/close conn2))))

(deftest test-with-conn
  (d/with-conn [conn (u/tmp-dir (str "with-conn-test-" (UUID/randomUUID)))]
    (d/transact! conn [{:db/id      -1
                        :name       "something"
                        :updated-at (Date.)}])
    (is (= 2 (count (d/datoms @conn :eav))))))
