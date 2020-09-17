(ns datalevin.test.conn
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest]]
      :clj  [clojure.test :as t :refer        [is deftest]])
   [datalevin.core :as d]
   [datalevin.constants :as c])
  (:import [java.util Date UUID]))

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
        s     {:a/b {:db/valueType :db.type/string}}
        s1    {:c/d {:db/valueType :db.type/string}}
        txs   [{:c/d "cd" :db/id -1}]
        conn2 (d/create-conn nil s)]
    (is (= (d/schema conn2) (d/update-schema conn1 s)))
    (d/update-schema conn1 s1)
    (d/transact! conn1 txs)
    (is (not (nil? (:a (first (d/datoms @conn1 :eavt))))))))


(deftest test-ways-to-create-conn-1
  (let [conn (d/create-conn)]
    (is (= #{} (set (d/datoms @conn :eavt))))
    (is (= c/implicit-schema (:schema @conn)))))

(deftest test-ways-to-create-conn-2
  (let [conn (d/create-conn nil schema)]
    (is (= #{} (set (d/datoms @conn :eavt))))
    (is (= (:schema @conn) (merge schema c/implicit-schema)))))

(deftest test-ways-to-create-conn-3

  (let [conn (d/conn-from-datoms datoms)]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= c/implicit-schema (:schema @conn))))

  (let [conn (d/conn-from-datoms datoms nil schema)]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= (merge schema c/implicit-schema) (:schema @conn))))

  (let [conn (d/conn-from-db (d/init-db datoms))]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= c/implicit-schema (:schema @conn))))

  (let [conn (d/conn-from-db (d/init-db datoms nil schema))]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= (merge schema c/implicit-schema) (:schema @conn)))))

(deftest test-recreate-conn
  (let [schema {:name          {:db/valueType :db.type/string}
                :dt/updated-at {:db/valueType :db.type/instant}}
        dir    (str "/tmp/datalevin-recreate-conn-test" (UUID/randomUUID))
        conn   (d/create-conn dir schema)]
    (d/transact! conn [{:db/id         -1
                        :name          "Namebo"
                        :dt/updated-at (Date.)}])
    (d/close conn)

    (let [conn2 (d/create-conn dir schema)]
      (println @conn2)

      (d/transact! conn2 [{:db/id         -2
                           :name          "Another name"
                           :dt/updated-at (Date.)}])
      (is (= 4 (count (d/datoms @conn2 :eavt)))))))
