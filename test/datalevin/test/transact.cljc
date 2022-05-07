(ns datalevin.test.transact
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing]])
   [datalevin.core :as d]
   [datalevin.interpret :as i]
   [datalevin.db :as db]
   [datalevin.util :as u]
   [datalevin.constants :refer [tx0]]
   [datalevin.test.core :as tdc]))

(deftest test-auto-update-entity-time
  (let [conn (d/create-conn
               (u/tmp-dir (str "auto-entity-time-" (random-uuid)))
               {:value {:db/valueType :db.type/long}
                :no    {:db/valueType :db.type/long
                        :db/unique    :db.unique/identity}}
               {:auto-entity-time? true})]
    (doseq [n (range 10)]
      (d/transact! conn [{:no n :value n}]))
    (d/transact! conn [{:no 0 :value 10}])
    (let [{:keys [db/created-at db/updated-at]}
          (d/pull (d/db conn) '[*] 1)]
      (is created-at)
      (is updated-at)
      (is (<= created-at updated-at)))
    (let [{:keys [db/created-at db/updated-at]}
          (d/pull (d/db conn) '[*] 2)]
      (is created-at)
      (is updated-at)
      (is (= created-at updated-at)))))

(deftest test-multi-threads-transact
  ;; we serialize writes, so as not to violate uniqueness constraint
  (let [conn (d/create-conn
               nil
               {:instance/id
                #:db{:valueType   :db.type/long
                     :unique      :db.unique/identity
                     :cardinality :db.cardinality/one}})]
    (dorun (pmap #(d/transact! conn [{:instance/id %}])
                 (range 2)))
    (let [res (d/q '[:find ?e ?a ?v :where [?e ?a ?v]] @conn)]
      (is (or (= #{[2 :instance/id 0] [1 :instance/id 1]} res)
              (= #{[1 :instance/id 0] [2 :instance/id 1]} res)))
      (is (thrown-with-msg? Exception #"unique constraint"
                            (d/transact! conn [(into [:db/add 3]
                                                     (next (first res)))]))))))

(deftest test-with-1
  (let [db (-> (d/empty-db nil {:aka {:db/cardinality :db.cardinality/many}})
               (d/db-with [[:db/add 1 :name "Ivan"]])
               (d/db-with [[:db/add 1 :name "Petr"]])
               (d/db-with [[:db/add 1 :aka "Devil"]])
               (d/db-with [[:db/add 1 :aka "Tupen"]]))]

    (is (= (d/q '[:find ?v
                  :where [1 :name ?v]] db)
           #{["Petr"]}))
    (is (= (d/q '[:find ?v
                  :where [1 :aka ?v]] db)
           #{["Devil"] ["Tupen"]}))

    (testing "Retract"
      (let [db (-> db
                   (d/db-with [[:db/retract 1 :name "Petr"]])
                   (d/db-with [[:db/retract 1 :aka "Devil"]]))]

        (is (= (d/q '[:find ?v
                      :where [1 :name ?v]] db)
               #{}))
        (is (= (d/q '[:find ?v
                      :where [1 :aka ?v]] db)
               #{["Tupen"]}))

        (is (= (into {} (d/entity db 1)) {:aka #{"Tupen"}}))))
    (d/close-db db))

  (testing "Skipping nils in tx"
    (let [db (-> (d/empty-db)
                 (d/db-with [[:db/add 1 :attr 2]
                             nil
                             [:db/add 3 :attr 4]]))]
      (is (= [[1 :attr 2], [3 :attr 4]]
             (map (juxt :e :a :v) (d/datoms db :eavt))))
      (d/close-db db))))

(deftest test-with-2
  (let [db (-> (d/empty-db nil {:aka {:db/cardinality :db.cardinality/many}})
               (d/db-with [[:db/add 1 :name "Ivan"]])
               (d/db-with [[:db/add 1 :name "Petr"]])
               (d/db-with [[:db/add 1 :aka "Devil"]])
               (d/db-with [[:db/add 1 :aka "Tupen"]]))]

    (testing "Cannot retract what's not there"
      (let [db (-> db
                   (d/db-with [[:db/retract 1 :name "Ivan"]]))]
        (is (= (d/q '[:find ?v
                      :where [1 :name ?v]] db)
               #{["Petr"]}))))
    (d/close-db db)))

(deftest test-with-datoms-1
  (testing "add"
    (let [db (-> (d/empty-db)
                 (d/db-with [(d/datom 1 :name "Oleg")
                             (d/datom 1 :age 17)
                             [:db/add 1 :aka "x"]]))]
      (is (= #{[1 :age 17]
               [1 :aka "x"]
               [1 :name "Oleg"]}
             (set (map (juxt :e :a :v) (d/datoms db :eavt)))))
      (d/close-db db))))

(deftest test-with-datoms-2
  (testing "retraction"
    (let [db (-> (d/empty-db)
                 (d/db-with [(d/datom 1 :name "Oleg")
                             (d/datom 1 :age 17)
                             (d/datom 1 :name "Oleg" tx0 false)]))]
      (is (= #{[1 :age 17]}
             (set (map (juxt :e :a :v)
                       (d/datoms db :eavt)))))
      (d/close-db db))))

(deftest test-change-ref
  (let [conn (d/create-conn nil {:friend {:db/valueType :db.type/ref}})]
    (d/transact! conn [{:db/id 1, :name "Ivan", :age 15, :friend 2}
                       {:db/id 2, :name "Petr", :age 37}
                       {:db/id 3, :name "Tom", :age 27}])
    (is (= (:tx-data (d/transact! conn [{:db/id 1 :friend 3}]))
           [(d/datom 1 :friend 2 -1)
            (d/datom 1 :friend 3 1)]))
    (d/close conn)))

(deftest test-retract-fns-1
  (let [db (-> (d/empty-db nil {:aka    {:db/cardinality :db.cardinality/many}
                                :friend {:db/valueType :db.type/ref}})
               (d/db-with [{:db/id 1, :name "Ivan", :age 15, :aka ["X" "Y" "Z"], :friend 2}
                           {:db/id 2, :name "Petr", :age 37}]))]
    (let [db (d/db-with db [[:db.fn/retractEntity 1]])]
      (is (= (d/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{}))
      (is (= (d/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[:name "Petr"] [:age 37]})))

    (is (= (d/db-with db [[:db.fn/retractEntity 1]])
           (d/db-with db [[:db/retractEntity 1]])))
    (d/close-db db)))

(deftest test-retract-fns-2
  (let [db (-> (d/empty-db nil {:aka    {:db/cardinality :db.cardinality/many}
                                :friend {:db/valueType :db.type/ref}})
               (d/db-with [{:db/id 1, :name "Ivan", :age 15, :aka ["X" "Y" "Z"], :friend 2}
                           {:db/id 2, :name "Petr", :age 37}]))]

    (testing "Retract entitiy with incoming refs"
      (is (= (d/q '[:find ?e :where [1 :friend ?e]] db)
             #{[2]}))

      (let [db (d/db-with db [[:db.fn/retractEntity 2]])]
        (is (= (d/q '[:find ?e :where [1 :friend ?e]] db)
               #{}))))
    (d/close-db db)))

(deftest test-retract-fns-3
  (let [db (-> (d/empty-db nil {:aka    {:db/cardinality :db.cardinality/many}
                                :friend {:db/valueType :db.type/ref}})
               (d/db-with [{:db/id 1, :name "Ivan", :age 15, :aka ["X" "Y" "Z"], :friend 2}
                           {:db/id 2, :name "Petr", :age 37}]))]

    (let [db (d/db-with db [[:db.fn/retractAttribute 1 :name]])]
      (is (= (d/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{[:age 15] [:aka "X"] [:aka "Y"] [:aka "Z"] [:friend 2]}))
      (is (= (d/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[:name "Petr"] [:age 37]})))
    (d/close-db db)))

(deftest test-retract-fns-4
  (let [db (-> (d/empty-db nil {:aka    {:db/cardinality :db.cardinality/many}
                                :friend {:db/valueType :db.type/ref}})
               (d/db-with [{:db/id 1, :name "Ivan", :age 15, :aka ["X" "Y" "Z"], :friend 2}
                           {:db/id 2, :name "Petr", :age 37}]))]

    (let [db (d/db-with db [[:db.fn/retractAttribute 1 :aka]])]
      (is (= (d/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{[:name "Ivan"] [:age 15] [:friend 2]}))
      (is (= (d/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[:name "Petr"] [:age 37]})))
    (d/close-db db)))

(deftest test-retract-without-value-339-1
  (let [db (-> (d/empty-db nil {:aka    {:db/cardinality :db.cardinality/many}
                                :friend {:db/valueType :db.type/ref}})
               (d/db-with [{:db/id 1, :name "Ivan", :age 15, :aka ["X" "Y" "Z"], :friend 2}
                           {:db/id 2, :name "Petr", :age 37}]))]
    (testing "Retract :name without providing v"
      (let [db (d/db-with db [[:db/retract 1 :name]])]
        (is (= (d/q '[:find ?a ?v
                      :where [1 ?a ?v]]
                    db)
               #{[:friend 2] [:age 15] [:aka "Z"] [:aka "Y"] [:aka "X"]}))))
    (d/close-db db)))

(deftest test-retract-without-value-339-2
  (let [db (-> (d/empty-db nil {:aka    {:db/cardinality :db.cardinality/many}
                                :friend {:db/valueType :db.type/ref}})
               (d/db-with [{:db/id 1, :name "Ivan", :age 15, :aka ["X" "Y" "Z"], :friend 2}
                           {:db/id 2, :name "Petr", :age 37}]))]
    (testing "Retract :aka (cardinality many) without providing v"
      (let [db (d/db-with db [[:db/retract 1 :aka]])]
        (is (= (d/q '[:find ?a ?v
                      :where [1 ?a ?v]]
                    db)
               #{[:friend 2] [:age 15] [:name "Ivan"]}))))
    (d/close-db db)))


(deftest test-retract-fns-not-found
  (let [db  (-> (d/empty-db nil {:name {:db/unique :db.unique/identity}})
                (d/db-with [[:db/add 1 :name "Ivan"]]))
        all #(vec (d/datoms % :eavt))]
    (are [op] (= [(d/datom 1 :name "Ivan")]
                 (all (d/db-with db [op])))
      [:db/retract 2 :name "Petr"]
      [:db.fn/retractAttribute 2 :name]
      [:db.fn/retractEntity 2]
      [:db/retractEntity 2]
      [:db/retract [:name "Petr"] :name "Petr"]
      [:db.fn/retractAttribute [:name "Petr"] :name]
      [:db.fn/retractEntity [:name "Petr"]])

    (are [op] (= [[] []]
                 [(all (d/db-with db [op]))
                  (all (d/db-with db [op op]))])            ;; idempotency
      [:db/retract 1 :name "Ivan"]
      [:db.fn/retractAttribute 1 :name]
      [:db.fn/retractEntity 1]
      [:db/retractEntity 1]
      [:db/retract [:name "Ivan"] :name "Ivan"]
      [:db.fn/retractAttribute [:name "Ivan"] :name]
      [:db.fn/retractEntity [:name "Ivan"]])
    (d/close-db db)))

(deftest test-transact!
  (let [conn (d/create-conn nil {:aka {:db/cardinality :db.cardinality/many}})]
    (d/transact! conn [[:db/add 1 :name "Ivan"]])
    (d/transact! conn [[:db/add 1 :name "Petr"]])
    (d/transact! conn [[:db/add 1 :aka "Devil"]])
    (d/transact! conn [[:db/add 1 :aka "Tupen"]])

    (is (= (d/q '[:find ?v
                  :where [1 :name ?v]] @conn)
           #{["Petr"]}))
    (is (= (d/q '[:find ?v
                  :where [1 :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))
    (d/close conn)))

(deftest test-transact-compare-different-types
  (testing "different scalars"
    (let [conn (d/create-conn)]
      (d/transact! conn [{:foo 1}
                         {:foo "1"}
                         {:foo :bar}])
      (is (= (d/q '[:find ?v
                    :where
                    [?e :foo ?v]
                    [(= ?v "1")]] @conn)
             #{["1"]}))
      (is (= (d/q '[:find ?v
                    :where
                    [?e :foo ?v]] @conn)
             #{["1"] [:bar] [1]}))
      (d/close conn)))

  (testing "different colls"
    (let [conn (d/create-conn)]
      (d/transact! conn [{:foo [1 2]}
                         {:foo [1 "2"]}
                         {:foo [1 :2]}])
      (is (= (d/q '[:find ?v
                    :where
                    [?e :foo ?v]
                    [(= ?v [1 "2"])]] @conn)
             #{[[1 "2"]]}))
      (is (= (d/q '[:find ?v
                    :where
                    [?e :foo ?v]
                    [(= ?v [1 :2])]] @conn)
             #{[[1 :2]]}))
      (is (= (d/q '[:find ?v
                    :where
                    [?e :foo ?v]] @conn)
             #{[[1 2]] [[1 :2]] [[1 "2"]]}))
      (d/close conn))))

(deftest test-transact!-1
  (let [conn (d/create-conn)]
    (d/transact! conn [{:db/id -1 :a 1}])
    (is (= (d/q '[:find ?v
                  :where [_ :a ?v]] @conn)
           #{[1]}))

    (d/transact! conn [{:db/id -1 :a 2}])
    (is (= (d/q '[:find ?v
                  :where [_ :a ?v]] @conn)
           #{[1] [2]}))
    (d/close conn)))

(deftest test-db-fn-cas
  (let [conn (d/create-conn)]
    (d/transact! conn [[:db/add 1 :weight 200]])
    (d/transact! conn [[:db.fn/cas 1 :weight 200 300]])
    (is (= (:weight (d/entity @conn 1)) 300))
    (d/transact! conn [[:db/cas 1 :weight 300 400]])
    (is (= (:weight (d/entity @conn 1)) 400))
    (is (thrown-msg? ":db.fn/cas failed on datom [1 :weight 400], expected 200"
                     (d/transact! conn [[:db.fn/cas 1 :weight 200 210]])))
    (d/close conn))

  (let [conn (d/create-conn nil {:label {:db/cardinality :db.cardinality/many}})]
    (d/transact! conn [[:db/add 1 :label :x]])
    (d/transact! conn [[:db/add 1 :label :y]])
    (d/transact! conn [[:db.fn/cas 1 :label :y :z]])
    (is (= (:label (d/entity @conn 1)) #{:x :y :z}))
    (is (thrown-msg? ":db.fn/cas failed on datom [1 :label (:x :y :z)], expected :s"
                     (d/transact! conn [[:db.fn/cas 1 :label :s :t]])))
    (d/close conn))

  (let [conn (d/create-conn)]
    (d/transact! conn [[:db/add 1 :name "Ivan"]])
    (d/transact! conn [[:db.fn/cas 1 :age nil 42]])
    (is (= (:age (d/entity @conn 1)) 42))
    (is (thrown-msg? ":db.fn/cas failed on datom [1 :age 42], expected nil"
                     (d/transact! conn [[:db.fn/cas 1 :age nil 4711]])))
    (d/close conn))

  (let [conn (d/create-conn)]
    (is (thrown-msg? "Can't use tempid in '[:db.fn/cas -1 :attr nil :val]'. Tempids are allowed in :db/add only"
                     (d/transact! conn [[:db/add -1 :name "Ivan"]
                                        [:db.fn/cas -1 :attr nil :val]])))
    (d/close conn)))

(deftest test-db-fn
  (let [conn    (d/create-conn nil {:aka {:db/cardinality :db.cardinality/many}})
        inc-age (fn [db name]
                  (if-let [[eid age] (first (d/q '{:find  [?e ?age]
                                                   :in    [$ ?name]
                                                   :where [[?e :name ?name]
                                                           [?e :age ?age]]}
                                                 db name))]
                    [{:db/id eid :age (inc ^long age)} [:db/add eid :had-birthday true]]
                    (throw (ex-info (str "No entity with name: " name) {}))))]
    (d/transact! conn [{:db/id 1 :name "Ivan" :age 31}])
    (d/transact! conn [[:db/add 1 :name "Petr"]])
    (d/transact! conn [[:db/add 1 :aka "Devil"]])
    (d/transact! conn [[:db/add 1 :aka "Tupen"]])
    (is (= (d/q '[:find ?v ?a
                  :where [?e :name ?v]
                  [?e :age ?a]] @conn)
           #{["Petr" 31]}))
    (is (= (d/q '[:find ?v
                  :where [?e :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))
    (is (thrown-msg? "No entity with name: Bob"
                     (d/transact! conn [[:db.fn/call inc-age "Bob"]])))
    (let [{:keys [db-after]} (d/transact! conn [[:db.fn/call inc-age "Petr"]])
          e                  (d/entity db-after 1)]
      (is (= (:age e) 32))
      (is (:had-birthday e)))
    (d/close conn)))

(deftest test-db-ident-fn
  (let [conn    (d/create-conn nil {:name {:db/unique :db.unique/identity}})
        inc-age (i/inter-fn [db name]
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
    (is (thrown-msg? "Can’t find entity for transaction fn :unknown-fn"
                     (d/transact! conn [[:unknown-fn]])))
    (is (thrown-msg? "Entity :Petr expected to have :db/fn attribute with fn? value"
                     (d/transact! conn [[:Petr]])))
    (is (thrown-msg? "No entity with name: Bob"
                     (d/transact! conn [[:inc-age "Bob"]])))
    (d/transact! conn [[:inc-age "Petr"]])
    (let [e (d/entity @conn 1)]
      (is (= (:age e) 32))
      (is (:had-birthday e)))))

(deftest test-resolve-eid
  (let [conn (d/create-conn)
        t1   (d/transact! conn [[:db/add -1 :name "Ivan"]
                                [:db/add -1 :age 19]
                                [:db/add -2 :name "Petr"]
                                [:db/add -2 :age 22]])
        t2   (d/transact! conn [[:db/add "Serg" :name "Sergey"]
                                [:db/add "Serg" :age 30]])]
    (is (= (:tempids t1) {-1 1, -2 2, :db/current-tx (+ tx0 1)}))
    (is (= (:tempids t2) {"Serg" 3, :db/current-tx (+ tx0 2)}))
    (is (= #{[1 "Ivan" 19 tx0]
             [2 "Petr" 22 tx0]
             [3 "Sergey" 30 tx0]}
           (d/q '[:find ?e ?n ?a ?t
                  :where [?e :name ?n ?t]
                  [?e :age ?a]] @conn)))
    (d/close conn)))

(deftest test-tempid-ref-295
  (let [db (-> (d/empty-db nil {:ref {:db/unique    :db.unique/identity
                                      :db/valueType :db.type/ref}})
               (d/db-with [[:db/add -1 :name "Ivan"]
                           [:db/add -2 :name "Petr"]
                           [:db/add -1 :ref -2]]))]
    (is (= #{[1 :name "Ivan"]
             [1 :ref 2]
             [2 :name "Petr"]}
           (tdc/all-datoms db)))
    (d/close-db db)))

(deftest test-resolve-eid-refs
  (let [conn (d/create-conn nil {:friend {:db/valueType   :db.type/ref
                                          :db/cardinality :db.cardinality/many}})
        tx   (d/transact! conn [{:name   "Sergey"
                                 :friend [-1 -2]}
                                [:db/add -1 :name "Ivan"]
                                [:db/add -2 :name "Petr"]
                                [:db/add "B" :name "Boris"]
                                [:db/add "B" :friend -3]
                                [:db/add -3 :name "Oleg"]
                                [:db/add -3 :friend "B"]])
        q    '[:find ?fn
               :in $ ?n
               :where [?e :name ?n]
               [?e :friend ?fe]
               [?fe :name ?fn]]]
    (is (= (:tempids tx) {1 1, -1 2, -2 3, "B" 4, -3 5, :db/current-tx (+ tx0 1)}))
    (is (= (d/q q @conn "Sergey") #{["Ivan"] ["Petr"]}))
    (is (= (d/q q @conn "Boris") #{["Oleg"]}))
    (is (= (d/q q @conn "Oleg") #{["Boris"]}))
    (d/close conn)))

(deftest test-resolve-current-tx
  (doseq [tx-tempid [:db/current-tx "datomic.tx" "datalevin.tx"]]
    (testing tx-tempid
      (let [conn (d/create-conn nil {:created-at {:db/valueType :db.type/ref}})
            tx1  (d/transact! conn [{:name "X", :created-at tx-tempid}
                                    {:db/id tx-tempid, :prop1 "prop1"}
                                    [:db/add tx-tempid :prop2 "prop2"]
                                    [:db/add -1 :name "Y"]
                                    [:db/add -1 :created-at tx-tempid]])]
        (is (= (d/q '[:find ?e ?a ?v :where [?e ?a ?v]] @conn)
               #{[1 :name "X"]
                 [1 :created-at (+ tx0 1)]
                 [(+ tx0 1) :prop1 "prop1"]
                 [(+ tx0 1) :prop2 "prop2"]
                 [2 :name "Y"]
                 [2 :created-at (+ tx0 1)]}))
        (is (= (:tempids tx1) (assoc {1 1, -1 2, :db/current-tx (+ tx0 1)}
                                     tx-tempid (+ tx0 1))))
        (let [tx2   (d/transact! conn [[:db/add tx-tempid :prop3 "prop3"]])
              tx-id (get-in tx2 [:tempids tx-tempid])]
          (is (= tx-id (+ tx0 2)))
          (is (= (into {} (d/entity @conn tx-id))
                 {:prop3 "prop3"})))
        (let [tx3   (d/transact! conn [{:db/id tx-tempid, :prop4 "prop4"}])
              tx-id (get-in tx3 [:tempids tx-tempid])]
          (is (= tx-id (+ tx0 3)))
          (is (= (into {} (d/entity @conn tx-id))
                 {:prop4 "prop4"})))
        (d/close conn)))))

;; TODO turn back on after switching to use Links
#_(deftest test-transient-294
   "db.fn/retractEntity retracts attributes of adjacent entities https://github.com/tonsky/datascript/issues/294"
   (let [db     (reduce #(d/db-with %1 [{:db/id %2 :a1 1 :a2 2 :a3 3}])
                        (d/empty-db)
                        (range 1 10))
         report (d/with db [[:db.fn/retractEntity 1]
                            [:db.fn/retractEntity 2]])]
     (is (= [(d/datom 1 :a1 1)
             (d/datom 1 :a2 2)
             (d/datom 1 :a3 3)
             (d/datom 2 :a1 1)
             (d/datom 2 :a2 2)
             (d/datom 2 :a3 3)]
            (:tx-data report)))
     (d/close-db db)))

(deftest test-transact-same
  "same data, transacted twice"
  (let [es   [{:db/id -1 :company "IBM" :country "US"}
              {:db/id -2 :company "PwC" :country "Germany"}]
        db   (d/db-with (d/empty-db) es)
        dts1 (d/datoms db :eavt)
        db   (d/db-with (d/empty-db) es)
        dts2 (d/datoms db :eavt)]
    (is (= dts1 dts2))
    (d/close-db db)))

#?(:clj
   (deftest test-transact-bytes
     "requires comparing byte-arrays"
     (let [schema      {:bytes {:db/valueType :db.type/bytes}}
           byte-arrays (mapv #(.getBytes ^String %) ["foo" "bar" "foo"])]
       (testing "equal bytes"
         (let [db   (d/empty-db nil schema)
               ents (mapv (fn [ba] {:bytes ba}) byte-arrays)]
           (is (every? true?
                       (map #(java.util.Arrays/equals ^bytes %1 ^bytes %2)
                            byte-arrays
                            (map :v (:tx-data (d/with db ents)))))))))))
