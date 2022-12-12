(ns datalevin.remote-entity-test
  (:require
   [datalevin.core :as d]
   [datalevin.util :as u]
   [datalevin.constants :as c]
   [datalevin.interpret :as i]
   [datalevin.test.core :as tdc :refer [server-fixture]]
   [clojure.test :as t :refer [is deftest testing use-fixtures]])
  (:import [java.util UUID]
           [datalevin.entity Entity]))

(use-fixtures :each server-fixture)

(deftest test-transactable-entity-with-remote-store
  (let [db  (-> (d/empty-db "dtlv://datalevin:datalevin@localhost/entity-test"
                            {:user/handle  #:db {:valueType :db.type/string
                                                 :unique    :db.unique/identity}
                             :user/friends #:db{:valueType   :db.type/ref
                                                :cardinality :db.cardinality/many}}
                            {:auto-entity-time? true})
                (d/db-with [{:user/handle  "ava"
                             :user/friends [{:user/handle "fred"}
                                            {:user/handle "jane"}]}]))
        ava (d/entity db [:user/handle "ava"])]
    (testing "cardinality/one"
      (testing "nil attr"
        (is (nil? (:user/age ava))))
      (testing "add/assoc attr"
        (let [ava-with-age (assoc ava :user/age 42)]
          (is (= 42 (:user/age ava-with-age)) "lookup works on tx stage")
          (is (nil? (:user/age ava)) "is immutable")
          (testing "and transact"
            (let [db-ava-with-age        (d/db-with db [(-> ava-with-age
                                                            (d/add :user/foo "bar"))])
                  ava-db-entity-with-age (d/entity db-ava-with-age [:user/handle "ava"])]
              (is (= 42 (:user/age ava-db-entity-with-age)) "value was transacted into db")
              (is (= "bar" (:user/foo ava-db-entity-with-age)) "value was transacted into db")
              (testing "update attr"
                (let [ava-with-age    (update ava-db-entity-with-age :user/age inc)
                      ava-with-points (-> ava-with-age
                                          (assoc :user/points 100)
                                          (update :user/points inc))]
                  (is (= 43 (:user/age ava-with-age)) "update works on entity")
                  (is (= 101 (:user/points ava-with-points)) "update works on stage")
                  (testing "and transact"
                    (let [db-ava-with-age (d/db-with db [ava-with-points])
                          ava-db-entity   (d/entity db-ava-with-age [:user/handle "ava"])]
                      (is (= 43 (:user/age ava-db-entity)) "value was transacted into db")
                      (is (= 101 (:user/points ava-db-entity)) "value was transacted into db")))))))))
      (testing "retract/dissoc attr"
        (let [ava        (d/entity db [:user/handle "ava"])
              dissoc-age (-> ava
                             (dissoc :user/age)
                             (d/retract :user/foo))]
          (is (= 43 (:user/age ava)) "has age")
          (is (nil? (:user/age dissoc-age)))
          (is (nil? (:user/foo dissoc-age)))
          (testing "and transact"
            (let [db-ava-with-age      (d/db-with db [(dissoc ava :user/age)])
                  ava-db-entity-no-age (d/entity db-ava-with-age [:user/handle "ava"])]
              (is (nil? (:user/age ava-db-entity-no-age)) "attrs was retracted from db"))))))

    (testing "cardinality/many"
      (testing "add/retract"
        (let [find-fred             (fn [ent]
                                      (some
                                        #(when (= (:user/handle %) "fred") %)
                                        (:user/friends ent)))
              fred                  (find-fred ava)
              ava-no-fred-friend    (d/retract ava :user/friends fred)
              db-no-fred            (d/db-with db [ava-no-fred-friend])
              ava-db-no-fred-friend (d/entity db-no-fred [:user/handle "ava"])]
          (is (some? fred) "fred is a friend")
          (is (nil? (find-fred ava-db-no-fred-friend)) "fred is not a friend anymore :(")
          ;; ava and fred make up
          (let [ava-friends-with-fred (d/add ava-db-no-fred-friend :user/friends fred)]
                                        ; tx-stage does not handle cardinality properly yet:
                                        ;(is (some? (find-fred ava-friends-with-fred))) ;; fails
            (let [db-with-friends (d/db-with db [ava-friends-with-fred])
                  ava             (d/entity db-with-friends [:user/handle "ava"])]
              (is (some? (find-fred ava)) "officially friends again"))))))

    (d/close-db db)))

(deftest entity-fn-test
  (let [f1 (i/inter-fn [db eid] (d/entity db eid))
        f2 (i/inter-fn [db eid] (d/touch (d/entity db eid)))

        end 3
        vs  (range 0 end)
        txs (mapv d/datom (range c/e0 (+ c/e0 end)) (repeat :value) vs)

        q '[:find [?ent ...] :in $ ent :where [?e _ _] [(ent $ ?e) ?ent]]

        get-eid (fn [^Entity e] (.-eid e))

        uri    "dtlv://datalevin:datalevin@localhost/entity-fn"
        r-conn (d/get-conn uri)

        dir    (u/tmp-dir (str "entity-fn-test-" (UUID/randomUUID)))
        l-conn (d/get-conn dir)]
    (d/transact! r-conn txs)
    (d/transact! l-conn txs)

    (is (i/inter-fn? f1))
    (is (i/inter-fn? f2))

    (is (= (set (map get-eid (d/q q @r-conn f1)))
           (set (map get-eid (d/q q @l-conn d/entity)))))
    (is (= (set (map get-eid (d/q q @r-conn f2)))
           (set (map get-eid (d/q q @l-conn
                                  (fn [db eid]
                                    (d/touch (d/entity db eid))))))))
    (d/close r-conn)
    (d/close l-conn)
    (u/delete-files dir)))

(deftest remote-db-ident-fn
  (let [dir     "dtlv://datalevin:datalevin@localhost/remote-fn-test"
        conn    (d/create-conn dir {:name {:db/unique :db.unique/identity}})
        inc-age (i/inter-fn
                  [db name]
                  (if-some [ent (d/entity db [:name name])]
                    [{:db/id (:db/id ent)
                      :age   (inc ^long (:age ent))}
                     [:db/add (:db/id ent) :had-birthday true]]
                    (throw (ex-info (str "No entity with name: " name) {}))))]
    (d/transact! conn [{:db/id    1
                        :name     "Petr"
                        :age      31
                        :db/ident :Petr}
                       {:db/ident :inc-age
                        :db/fn    inc-age}])
    (is (thrown-with-msg? Exception
                          #"Can’t find entity for transaction fn :unknown-fn"
                          (d/transact! conn [[:unknown-fn]])))
    (is (thrown-with-msg? Exception
                          #"Entity :Petr expected to have :db/fn attribute"
                          (d/transact! conn [[:Petr]])))
    (is (thrown-with-msg? Exception
                          #"No entity with name: Bob"
                          (d/transact! conn [[:inc-age "Bob"]])))
    (d/transact! conn [[:inc-age "Petr"]])
    (let [e (d/entity @conn 1)]
      (is (= (:age e) 32))
      (is (:had-birthday e)))
    (d/close conn)))
