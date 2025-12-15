(ns datalevin.rules-test
  (:require
   [clojure.test :refer [deftest are is use-fixtures]]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [datalevin.rules :as sut]
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest strongly-connected-components-test
  (are [graph sccs] (= (set (sut/tarjans-scc graph)) (set sccs))
    {:node        #{:unreachable}
     :reachable   #{:unreachable :reachable}
     :unreachable #{}}
    [#{:reachable} #{:node} #{:unreachable}]

    {1 [2]
     2 [3 4]
     3 [4 6]
     4 [1 5]
     5 [6]
     6 [7]
     7 [5]}
    [#{1 2 3 4} #{5 6 7}]

    {:a [:c :h]
     :b [:a :g]
     :c [:d]
     :d [:f]
     :e [:a :i]
     :f [:j]
     :g [:i]
     :h [:f :g]
     :i [:h]
     :j [:c]}
    [#{:e} #{:b} #{:a} #{:h :i :g} #{:c :j :f :d}]))

(deftest reachability-data-test
  (let [dir (u/tmp-dir (str "reachability-test" (UUID/randomUUID)))
        db  (-> (d/empty-db
                  dir
                  {:node {:db/unique    :db.unique/identity
                          :db/valueType :db.type/symbol}
                   :link {:db/cardinality :db.cardinality/many
                          :db/valueType   :db.type/ref}}
                  {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})
                (d/db-with [{:db/id -1 :node 'a :link -2}
                            {:db/id -2 :node 'b :link -3}
                            {:db/id -3 :node 'c :link [-3 -4]}
                            {:db/id -4 :node 'd}]))]
    (is (= #{['a 'b]
             ['b 'c]
             ['c 'c]
             ['c 'd]
             ['a 'c]
             ['b 'd]
             ['a 'd]}
           (d/q '[:find ?n ?m
                  :in $ %
                  :where
                  (reachable ?X ?Y)
                  [?X :node ?n]
                  [?Y :node ?m]]
                db '[[(reachable ?X ?Y)
                      [?X :link ?Y]]
                     [(reachable ?X ?Y)
                      [?X :link ?Z]
                      (reachable ?Z ?Y)]])
           (d/q '[:find ?n ?m
                  :in $ %
                  :where
                  (reachable ?n ?m)]
                db '[[(reachable ?n ?m)
                      [?X :link ?Y]
                      [?X :node ?n]
                      [?Y :node ?m]
                      ]
                     [(reachable ?n ?m)
                      [?X :node ?n]
                      [?Y :node ?m]
                      [?Z :node ?o]
                      [?X :link ?Z]
                      (reachable ?o ?m)]])))
    (d/close-db db)
    (u/delete-files dir)))

(deftest math-genealogy-test
  (let [dir (u/tmp-dir (str "math-genealogy-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db
                  dir
                  {:dissertation/cid  {:db/valueType :db.type/ref}
                   :dissertation/univ {:db/valueType :db.type/string}
                   :dissertation/area {:db/valueType :db.type/string}
                   :person/name       {:db/valueType :db.type/string}
                   :person/advised    {:db/valueType   :db.type/ref
                                       :db/cardinality :db.cardinality/many}}
                  {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})
                (d/db-with
                  [{:db/id       -1
                    :person/name "David Scott Warren"}
                   {:db/id             -2
                    :dissertation/cid  -1
                    :dissertation/univ "University of Michigan"
                    :dissertation/area "68—Computer science"}
                   {:db/id          -3
                    :person/name    "Joyce Barbara Friedman"
                    :person/advised [-2]}
                   {:db/id             -4
                    :dissertation/cid  -3
                    :dissertation/univ "Harvard University"
                    :dissertation/area "68—Computer science"}
                   {:db/id          -5
                    :person/name    "Stål Olav Aanderaa"
                    :person/advised [-4]}
                   {:db/id             -6
                    :dissertation/cid  -5
                    :dissertation/univ "Harvard University"
                    :dissertation/area "03—Mathematical logic and foundations"}
                   {:db/id          -7
                    :person/name    "Hao Wang"
                    :person/advised [-6]}
                   {:db/id             -8
                    :dissertation/cid  -7
                    :dissertation/univ "Harvard University",
                    :dissertation/area "03—Mathematical logic and foundations"}
                   {:db/id          -9
                    :person/name    "Willard Van Orman Quine"
                    :person/advised [-8]}
                   {:db/id             -10
                    :dissertation/cid  -9
                    :dissertation/univ "Harvard University"}
                   {:db/id          -11
                    :person/name    "Alfred North Whitehead"
                    :person/advised [-10]}
                   {:db/id            -12
                    :dissertation/cid -11}]))

        rule-author '[[(author ?d ?c)
                       [?d :dissertation/cid ?c]]]
        rule-adv    '[[(adv ?x ?y)
                       [?x :person/advised ?d]
                       (author ?d ?y)]]
        rule-area   '[[(area ?c ?a)
                       [?d :dissertation/cid ?c]
                       [?d :dissertation/area ?a]]]
        rule-univ   '[[(univ ?c ?u)
                       [?d :dissertation/cid ?c]
                       [?d :dissertation/univ ?u]]]
        rule-anc    '[[(anc ?x ?y)
                       (adv ?x ?y)]
                      [(anc ?x ?y)
                       (adv ?x ?z)
                       (anc ?z ?y)]]
        rule-q1     (into rule-author rule-adv)
        rule-q2     (into rule-q1 rule-univ)
        rule-q3     (into rule-q1 rule-area)
        rule-q4     (into rule-q1 rule-anc)]
    (is (= ["Stål Olav Aanderaa"]
           (d/q '[:find [?n ...]
                  :in $ %
                  :where
                  [?d :person/name "David Scott Warren"]
                  (adv ?x ?d)
                  (adv ?y ?x)
                  [?y :person/name ?n]]
                db rule-q1)))
    (is (= #{"Hao Wang" "Stål Olav Aanderaa" "Joyce Barbara Friedman"}
           (set (d/q '[:find [?n ...]
                       :in $ %
                       :where
                       (adv ?x ?y)
                       (univ ?x ?u)
                       (univ ?y ?u)
                       [?y :person/name ?n]]
                     db rule-q2))))
    (is (= ["Joyce Barbara Friedman"]
           (d/q '[:find [?n ...]
                  :in $ %
                  :where
                  (adv ?x ?y)
                  (area ?x ?a1)
                  (area ?y ?a2)
                  [(!= ?a1 ?a2)]
                  [?y :person/name ?n]]
                db rule-q3)))
    (is (= #{"Hao Wang" "Stål Olav Aanderaa" "Joyce Barbara Friedman"
             "Alfred North Whitehead" "Willard Van Orman Quine"}
           (set (d/q '[:find [?n ...]
                       :in $ %
                       :where
                       [?x :person/name "David Scott Warren"]
                       (anc ?y ?x)
                       [?y :person/name ?n]]
                     db rule-q4))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest magic-set-test
  (let [dir   (u/tmp-dir (str "magic-set-test-" (UUID/randomUUID)))
        conn  (d/get-conn
                dir {:node {:db/unique :db.unique/identity}
                     :to   {:db/cardinality :db.cardinality/many}}
                {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})
        txs   [{:db/id 1 :node 1 :to [2 3]}
               {:db/id 2 :node 2 :to [4]}
               {:db/id 3 :node 3 :to [5]}
               {:db/id 4 :node 4 :to [6]}
               {:db/id 5 :node 5 :to []}
               {:db/id 6 :node 6 :to []}
               {:db/id 10 :node 10 :to [11]}
               {:db/id 11 :node 11 :to [12]}
               {:db/id 12 :node 12 :to []}]
        rules '[[(path ?x ?y)
                 [?e :node ?x]
                 [?e :to ?z]
                 [?z :node ?y]]
                [(path ?x ?y)
                 [?e :node ?x]
                 [?e :to ?z]
                 [?z :node ?z_val]
                 (path ?z_val ?y)]]]
    (d/transact! conn txs)
    (is (= #{2 3 4 5 6}
           (set (d/q '[:find [?y ...]
                       :in $ % ?x
                       :where (path ?x ?y)]
                     (d/db conn) rules 1))))

    ;; Verify it works for 10
    (is (= #{11 12}
           (set (d/q '[:find [?y ...]
                       :in $ % ?x
                       :where (path ?x ?y)]
                     (d/db conn) rules 10))))

    (d/close conn)
    (u/delete-files dir)))

(deftest sequence-generation-memory-test
  (let [dir (u/tmp-dir (str "memory-test-" (UUID/randomUUID)))
        db  (d/empty-db dir nil
                        {:kv-opts
                         {:flags (conj c/default-env-flags :nosync :nolock)}})]
    (try
      (binding [sut/*auto-optimize-temporal* false]
        ;; Generates a sequence from 0 to limit
        (let [limit 2000
              rules '[[(chain ?limit ?n)
                       [(ground 0) ?n]
                       [(>= ?limit 0)]]
                      [(chain ?limit ?n)
                       (chain ?limit ?prev)
                       [(< ?prev ?limit)]
                       [(inc ?prev) ?n]]]
              res   (d/q '[:find ?n
                           :in $ % ?limit
                           :where (chain ?limit ?n)]
                         db rules limit)]
          (is (= (inc limit) (count res)))
          (is (contains? res [limit]))))
      (finally
        (d/close-db db)
        (u/delete-files dir)))))

(deftest temporal-elimination-test
  (let [dir (u/tmp-dir (str "temporal-test-" (UUID/randomUUID)))
        db  (d/empty-db dir nil
                        {:kv-opts
                         {:flags (conj c/default-env-flags :nosync :nolock)}})]
    (try
      ;; Generates a sequence from 0 to limit, but discards history
      (binding [sut/*temporal-elimination* true]
        (let [limit 10000
              rules '[[(chain ?limit ?n)
                       [(ground 0) ?n]
                       [(>= ?limit 0)]]
                      [(chain ?limit ?n)
                       (chain ?limit ?prev)
                       [(< ?prev ?limit)]
                       [(inc ?prev) ?n]]]
              res   (d/q '[:find ?n
                           :in $ % ?limit
                           :where (chain ?limit ?n)]
                         db rules limit)]
          ;; Should only contain the last element(s) from the final frontier
          ;; Since chain(n) generates chain(n+1) uniquely, the frontier size is 1.
          (is (= 1 (count res)))
          (is (= #{[limit]} res))))
      (finally
        (d/close-db db)
        (u/delete-files dir)))))

(deftest auto-temporal-optimization-test
  (let [dir (u/tmp-dir (str "auto-temporal-test-" (UUID/randomUUID)))
        db  (d/empty-db dir nil
                        {:kv-opts
                         {:flags (conj c/default-env-flags :nosync :nolock)}})]
    (try
      ;; Generates a sequence from 0 to limit
      ;; Should AUTO-DETECT temporal pattern and prune history
      (let [limit 10000
            rules '[[(chain ?limit ?n)
                     [(ground 0) ?n]
                     [(>= ?limit 0)]]
                    [(chain ?limit ?n)
                     (chain ?limit ?prev)
                     [(< ?prev ?limit)]
                     [(inc ?prev) ?n]]]
              res   (d/q '[:find ?n
                           :in $ % ?limit
                           :where (chain ?limit ?n)]
                         db rules limit)]
        ;; Should behave like temporal elimination
        (is (= 1 (count res)))
        (is (= #{[limit]} res)))
      (finally
        (d/close-db db)
        (u/delete-files dir)))))

(deftest mutually-recursive-rules-test
  (let [dir (u/tmp-dir (str "mutual-recursion-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:id {:db/unique :db.unique/identity}
                              :edge1 {:db/valueType :db.type/long}
                              :edge2 {:db/valueType :db.type/long}}
                         {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})]
    (d/transact! conn [{:db/id 1 :id 1 :edge1 2}
                       {:db/id 2 :id 2 :edge2 3}
                       {:db/id 3 :id 3 :edge1 4}
                       {:db/id 4 :id 4 :edge2 5}
                       {:db/id 5 :id 5}])
    (let [rules '[[(foo ?id ?y)
                   [?e :id ?id]
                   [?e :edge1 ?y]]
                  [(foo ?id ?y)
                   [?e :id ?id]
                   [?e :edge1 ?z]
                   (bar ?z ?y)]
                  [(bar ?id ?y)
                   [?e :id ?id]
                   [?e :edge2 ?y]]
                  [(bar ?id ?y)
                   [?e :id ?id]
                   [?e :edge2 ?z]
                   (foo ?z ?y)]]]
      (is (= #{2 3 4 5}
             (set (d/q '[:find [?y ...]
                         :in $ % ?x
                         :where (foo ?x ?y)]
                       (d/db conn) rules 1))))
      (is (= #{}
             (set (d/q '[:find [?y ...]
                         :in $ % ?x
                         :where (bar ?x ?y)]
                       (d/db conn) rules 1))))
      (is (= #{3 4 5}
             (set (d/q '[:find [?y ...]
                         :in $ % ?x
                         :where (bar ?x ?y)]
                       (d/db conn) rules 2)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest diamond-graph-path-test
  (let [dir          (u/tmp-dir (str "diamond-path-test-" (UUID/randomUUID)))
        conn         (d/get-conn dir {:node {:db/unique :db.unique/identity}
                                      :from {:db/valueType :db.type/ref}
                                      :to   {:db/valueType :db.type/ref}}
                                 {:kv-opts
                                  {:flags (conj c/default-env-flags :nosync :nolock)}})
        temp-a       -1
        temp-b       -2
        temp-c       -3
        temp-d       -4
        temp-edge-ab -5
        temp-edge-ac -6
        temp-edge-bd -7
        temp-edge-cd -8]
    (d/transact! conn [{:db/id temp-a :node 'a}
                       {:db/id temp-b :node 'b}
                       {:db/id temp-c :node 'c}
                       {:db/id temp-d :node 'd}
                       {:db/id temp-edge-ab :from temp-a :to temp-b}
                       {:db/id temp-edge-ac :from temp-a :to temp-c}
                       {:db/id temp-edge-bd :from temp-b :to temp-d}
                       {:db/id temp-edge-cd :from temp-c :to temp-d}])
    (let [rules '[[(path ?from ?to)
                   [?e :from ?from]
                   [?e :to ?to]]
                  [(path ?from ?to)
                   [?e :from ?from]
                   [?e :to ?mid]
                   (path ?mid ?to)]]]
      (is (= #{(vec ['a 'd])}
             (set (d/q '[:find ?from-node ?to-node
                         :in $ % ?start-node-sym ?end-node-sym
                         :where
                         [?start-eid :node ?start-node-sym]
                         [?end-eid :node ?end-node-sym]
                         (path ?start-eid ?end-eid)
                         [?start-eid :node ?from-node]
                         [?end-eid :node ?to-node]]
                       (d/db conn) rules 'a 'd))))
      (is (= #{(vec ['a 'b])
               (vec ['a 'c])
               (vec ['b 'd])
               (vec ['c 'd])
               (vec ['a 'd])}
             (set (d/q '[:find ?from-node ?to-node
                         :in $ %
                         :where
                         (path ?from-eid ?to-eid)
                         [?from-eid :node ?from-node]
                         [?to-eid :node ?to-node]]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest multiple-derivations-test
  (let [dir  (u/tmp-dir (str "multiple-derivations-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:foo/from {:db/valueType :db.type/long}
                              :foo/to   {:db/valueType :db.type/long}
                              :bar/from {:db/valueType :db.type/long}
                              :bar/to   {:db/valueType :db.type/long}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    (d/transact! conn [{:db/id -1 :foo/from 1 :foo/to 2}
                       {:db/id -2 :bar/from 1 :bar/to 2}])
    (let [rules '[[(derived ?x ?y)
                   [?e :foo/from ?x]
                   [?e :foo/to ?y]]
                  [(derived ?x ?y)
                   [?e :bar/from ?x]
                   [?e :bar/to ?y]]]]
      (is (= #{[1 2]}
             (set (d/q '[:find ?x ?y
                         :in $ %
                         :where (derived ?x ?y)]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest stratified-negation-test
  (let [dir  (u/tmp-dir (str "stratified-negation-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:node {:db/unique :db.unique/identity}
                              :edge {:db/valueType :db.type/ref}
                              :bad  {:db/unique :db.unique/identity}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    (d/transact! conn [{:db/id -1 :node 1 :edge -2} ;; 1 -> 2
                       {:db/id -2 :node 2 :edge -3} ;; 2 -> 3
                       {:db/id -3 :node 3}
                       {:db/id -4 :node 4 :edge -5} ;; 4 -> 5
                       {:db/id -5 :node 5}
                       {:db/id -6 :node 6}
                       {:db/id -7 :bad 3}]) ;; 3 is initially bad

    (let [rules '[[(bad ?x)
                   [?e :node ?x]
                   [?e :edge ?y]
                   [?y :node ?y_val]
                   (bad ?y_val)]
                  [(bad ?x)
                   [?e :bad ?x]]
                  [(good ?x)
                   [?e :node ?x]
                   (not (bad ?x))]]]
      (is (= #{1 2 3}
             (set (d/q '[:find [?x ...]
                         :in $ %
                         :where (bad ?x)]
                       (d/db conn) rules))))
      (is (= #{4 5 6}
             (set (d/q '[:find [?x ...]
                         :in $ %
                         :where (good ?x)]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest unsafe-negation-rejection-test
  (let [dir (u/tmp-dir (str "unsafe-negation-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:q {:db/valueType :db.type/long}} ;; Schema for :q
                         {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})]
    (d/transact! conn [{:db/id -1 :q 1} ;; q(1) is a fact
                       {:db/id -2 :q 2}]) ;; q(2) is a fact

    (let [rules '[[(p ?x)
                   (not (q ?x))]
                  [(q ?x)
                   [?e :q ?x]]]]
      ;; Expect an exception when trying to query p(x) due to unsafe negation
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Insufficient bindings: none of #\{\?x.*\} is bound"
                            (d/q '[:find ?x
                                   :in $ %
                                   :where (p ?x)]
                                 (d/db conn) rules))))
    (d/close conn)
    (u/delete-files dir)))

(deftest stratified-negation-safe-test
  (let [dir (u/tmp-dir (str "stratified-negation-safe-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:id {:db/unique :db.unique/identity}
                              :value {:db/valueType :db.type/long}
                              :seen-flag {:db/valueType :db.type/boolean}}
                         {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})]
    ;; Data: some values for domain, some marked as seen
    (d/transact! conn [{:db/id -1 :id 1 :value 10}
                       {:db/id -2 :id 2 :value 20 :seen-flag true}
                       {:db/id -3 :id 3 :value 30}
                       {:db/id -4 :id 4 :value 40 :seen-flag true}
                       {:db/id -5 :id 5 :value 50}])

    (let [rules '[[(domain ?x)
                   [?e :id ?x]] ;; domain(x) are all ids
                  [(seen ?x)
                   [?e :id ?x]
                   [?e :seen-flag true]] ;; seen(x) are ids with seen-flag true
                  [(missing ?x)
                   (domain ?x)
                   (not (seen ?x))]]]
      ;; Expected missing: 1, 3, 5
      (is (= #{1 3 5}
             (set (d/q '[:find [?x ...]
                         :in $ %
                         :where (missing ?x)]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest double-negation-test
  (let [dir (u/tmp-dir (str "double-negation-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:entity-id {:db/unique :db.unique/identity}
                              :has-q {:db/valueType :db.type/boolean}}
                         {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})]
    ;; Data: some entities, some with has-q true
    (d/transact! conn [{:db/id -1 :entity-id 1 :has-q true}
                       {:db/id -2 :entity-id 2 :has-q false}
                       {:db/id -3 :entity-id 3 :has-q true}
                       {:db/id -4 :entity-id 4 :has-q false}])

    (let [rules '[[(base-q ?x)
                   [?e :entity-id ?x]
                   [?e :has-q true]] ;; base-q(x) means entity x has q
                  [(p ?x)
                   [?e :entity-id ?x]
                   (not (not (base-q ?x)))]]
          ;; A direct query for base-q to compare
          expected-q (set (d/q '[:find [?x ...]
                                 :in $ %
                                 :where (base-q ?x)]
                                (d/db conn) rules))]
      ;; The result of p(x) should be the same as base-q(x)
      (is (= expected-q
             (set (d/q '[:find [?x ...]
                         :in $ %
                         :where (p ?x)]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

;; TODO Need to extend the Datalog syntax to allow aggregation function in
;; the rule head
#_(deftest single-linear-regression-test
    (let [dir (u/tmp-dir (str "single-regression-test-" (UUID/randomUUID)))
          db  (-> (d/empty-db
                    dir
                    {:time {:db/valueType :db.type/long}
                     :mass {:db/valueType :db.type/long}}
                    {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
                  (d/db-with [{:time 5 :mass 40}
                              {:time 7 :mass 120}
                              {:time 12 :mass 180}
                              {:time 16 :mass 210}
                              {:time 20 :mass 240}]))]
      (is (< 10
             (d/q '[:find ?p .
                    :in $ %
                    :where
                    (linear-regression ?p)]
                  db '[[(xtrain ?id ?v)
                        [?id :time ?v]]
                       [(ytrain ?id ?y)
                        [?id :mass ?y]]
                       [(predict ?t ?id (sum ?y))
                        (model ?t ?p)
                        (xtrain ?id ?v)
                        [(* ?v ?p) ?y]]
                       [(gradient ?t (sum ?g))
                        (ytrain ?id ?y)
                        (predict ?t ?id ?y')
                        (xtrain ?id ?v)
                        [(* 2 (- ?y' ?y) v?) ?g]]
                       [(model ?t ?p)
                        [(ground 0) ?t]
                        [(ground 0.01 ?p)]]
                       [(model ?t+1 ?p')
                        [(inc ?t) ?t+1]
                        (model ?t ?p)
                        (gradient ?t ?g)
                        [(- ?p (/ (* 0.1 ?g) 5)) ?p']
                        [(< ?t 100)]]
                       ])
             13))
      (d/close-db db)
      (u/delete-files dir)))
