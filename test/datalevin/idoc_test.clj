(ns datalevin.idoc-test
  (:require
   [clojure.test :as t :refer [deftest is testing use-fixtures]]
   [datalevin.built-ins :as bi]
   [datalevin.constants :as c]
   [datalevin.core :as sut]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(def ^:private base-schema
  {:doc/edn  {:db/valueType :db.type/idoc
              :db/domain    "profiles"}
   :doc/alt  {:db/valueType :db.type/idoc
              :db/domain    "profiles"}
   :doc/json {:db/valueType :db.type/idoc
              :db/idocFormat :json}
   :doc/md   {:db/valueType :db.type/idoc
              :db/idocFormat :markdown}})

(def ^:private edn-doc
  {:status  "active"
   :age     30
   :profile {:age 30 :name "Alice"}
   :tags    ["a" "b" "b"]
   :orders  [{:product "A" :qty 10}
             {:product "B" :qty 20}]})

(def ^:private json-doc
  "{\"name\":\"Alice\",\"middle\":null,\"age\":30,\"nested\":{\"score\":5},\"tags\":[\"x\",\"y\"]}")

(def ^:private md-doc
  "# User Profile\n## Getting Started!\nName: Alice\nAge: 30\n## Home Address\nCity: NYC")

(deftest idoc-basic-query-test
  (let [dir  (u/tmp-dir (str "idoc-test-" (UUID/randomUUID)))
        conn (sut/create-conn
               dir
               base-schema
               {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})]
    (sut/transact!
      conn
      [{:db/id    1
        :doc/edn  edn-doc
        :doc/json json-doc
        :doc/md   md-doc}
       {:db/id   2
        :doc/alt {:status "active" :team "red"}}])
    (let [db (sut/db conn)]
      (testing "basic match and domain search"
        (is (= #{[1 :doc/edn]}
               (sut/q '[:find ?e ?a
                        :in $ ?q
                        :where
                        [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
                      db
                      {:status "active" :age 30})))
        (is (= #{[1 :doc/edn] [2 :doc/alt]}
               (sut/q '[:find ?e ?a
                        :in $ ?q
                        :where
                        [(idoc-match $ ?q {:domains ["profiles"]})
                         [[?e ?a ?v]]]]
                      db
                      {:status "active"}))))
      (testing "arrays and nested matches"
          (is (= #{[1]}
                 (sut/q '[:find ?e
                          :in $
                          :where
                          [(idoc-match $ :doc/edn {:tags "b"})
                           [[?e ?a ?v]]]]
                       db)))
          (is (= #{[1]}
                 (sut/q '[:find ?e
                          :in $
                          :where
                          [(idoc-match $ :doc/edn
                                       {:orders {:product "A" :qty 10}})
                           [[?e ?a ?v]]]]
                       db)))
          (is (= #{}
                 (sut/q '[:find ?e
                          :in $
                          :where
                          [(idoc-match $ :doc/edn
                                       {:orders {:product "A" :qty 20}})
                           [[?e ?a ?v]]]]
                       db))))
      (testing "predicates and logical combinators"
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:age (> 21)})
                         [[?e ?a ?v]]]]
                     db)))
          (is (= #{}
                 (sut/q '[:find ?e
                          :in $
                          :where
                          [(idoc-match $ :doc/edn {:age (> 30)})
                           [[?e ?a ?v]]]]
                       db)))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $ ?q
                        :where
                        [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
                      db
                      '(>= [:profile :age] 30))))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $ ?q
                        :where
                        [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
                      db
                      '(>= [:profile :?] 30))))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:profile {:? 30}})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:* {:product "B"}})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn
                                       {:profile [:or {:age 40} {:age 30}]})
                           [[?e ?a ?v]]]]
                       db))))
      (testing "markdown and null handling"
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/md
                                     {"User Profile"
                                      {"Getting Started!"
                                       "Name: Alice\nAge: 30"}})
                         [[?e ?a ?v]]]]
                     db)))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/json {"middle" (nil?)})
                         [[?e ?a ?v]]]]
                     db)))))
    (let [db        (sut/db conn)
          edn-doc'  (:doc/edn (sut/entity db 1))
          json-doc' (:doc/json (sut/entity db 1))]
      (testing "idoc-get"
        (is (= ["A" "B"] (bi/idoc-get edn-doc' :orders :product)))
        (is (= "B" (bi/idoc-get edn-doc' :orders 1 :product)))
        (is (= :json/null (bi/idoc-get json-doc' "middle")))
        (is (nil? (bi/idoc-get json-doc' "missing")))))
    (sut/close conn)
    (when-not (u/windows?) (u/delete-files dir))))

(deftest idoc-validation-test
  (let [dir  (u/tmp-dir (str "idoc-validate-test-" (UUID/randomUUID)))
        conn (sut/create-conn
               dir
               base-schema
               {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})]
    (is (thrown-with-msg?
          Exception
          #"String input is not allowed for :edn idoc"
          (sut/transact! conn [{:db/id 1 :doc/edn "{\"a\":1}"}])))
    (is (thrown-with-msg?
          Exception
          #"Lists are not valid idoc values"
          (sut/transact! conn [{:db/id 2 :doc/edn {:bad '(1 2)}}])))
    (is (thrown-with-msg?
          Exception
          #"Literal :json/null is reserved"
          (sut/transact! conn [{:db/id 3 :doc/edn {:bad :json/null}}])))
    (is (thrown-with-msg?
          Exception
          #"Idoc keys must be keywords or strings"
          (sut/transact! conn [{:db/id 4 :doc/edn {1 "bad"}}])))
    (sut/transact! conn [{:db/id 5 :doc/json json-doc}])
    (is (thrown-with-msg?
          Exception
          #"Use \(nil\? :field\) to match null values"
          (sut/q '[:find ?e
                   :where
                   [(idoc-match $ :doc/json {:middle nil})
                    [[?e ?a ?v]]]]
                 (sut/db conn))))
    (sut/close conn)
    (when-not (u/windows?) (u/delete-files dir))))

(deftest idoc-edge-case-test
  (let [dir  (u/tmp-dir (str "idoc-edge-test-" (UUID/randomUUID)))
        conn (sut/create-conn
               dir
               base-schema
               {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})]
    (sut/transact!
      conn
      [{:db/id    1
        :doc/edn  {}
        :doc/json "{}"
        :doc/md   ""
        :doc/alt  {:tags [] :maybe nil}}])
    (let [db (sut/db conn)]
      (testing "empty docs and nil values"
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {}) [[?e ?a ?v]]]]
                      db)))
        (is (= #{}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:status "active"})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $ ?q
                        :where
                        [(idoc-match $ :doc/alt ?q) [[?e ?a ?v]]]]
                      db
                      '(nil? :maybe))))
        (is (= #{}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/alt {:tags "urgent"})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/json {}) [[?e ?a ?v]]]]
                      db)))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/md {}) [[?e ?a ?v]]]]
                      db)))))
    (let [db        (sut/db conn)
          edn-doc'  (:doc/edn (sut/entity db 1))
          json-doc' (:doc/json (sut/entity db 1))
          md-doc'   (:doc/md (sut/entity db 1))
          alt-doc'  (:doc/alt (sut/entity db 1))]
      (testing "idoc-get on empty docs"
        (is (nil? (bi/idoc-get edn-doc' :missing)))
        (is (nil? (bi/idoc-get json-doc' "missing")))
        (is (nil? (bi/idoc-get md-doc' :missing)))
        (is (= :json/null (bi/idoc-get alt-doc' :maybe)))))
    (sut/close conn)
    (when-not (u/windows?) (u/delete-files dir))))
