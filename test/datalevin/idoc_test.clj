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
                      '(< 20 [:profile :age] 40))))
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
          #"Invalid EDN string for idoc"
          (sut/transact! conn [{:db/id 1 :doc/edn "{:a 1"}])))
    (is (thrown-with-msg?
          Exception
          #"Invalid JSON string for idoc"
          (sut/transact! conn [{:db/id 2 :doc/json "{\"a\":1"}])))
    (is (thrown-with-msg?
          Exception
          #"Markdown content appears before any header"
          (sut/transact! conn [{:db/id 3 :doc/md "Just text"}])))
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
    (sut/transact! conn [{:db/id 6 :doc/edn "{:status \"active\"}"}])
    (is (= #{[6]}
           (sut/q '[:find ?e
                    :in $
                    :where
                    [(idoc-match $ :doc/edn {:status "active"})
                     [[?e ?a ?v]]]]
                 (sut/db conn))))
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

(deftest idoc-update-test
  (let [dir  (u/tmp-dir (str "idoc-update-test-" (UUID/randomUUID)))
        conn (sut/create-conn
               dir
               base-schema
               {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})
        doc1 {:status  "active"
              :profile {:age 30 :name "Alice"}
              :tags    ["a" "b"]
              :orders  [{:product "A" :qty 10}
                        {:product "B" :qty 20}]}
        doc2 {:status  "active"
              :profile {:age 31 :name "Alice"}
              :tags    ["b" "c"]
              :orders  [{:product "A" :qty 10}
                        {:product "C" :qty 5}]}]
    (sut/transact! conn [{:db/id 1 :doc/edn doc1}])
    (sut/transact! conn [{:db/id 1 :doc/edn doc2}])
    (let [db (sut/db conn)]
      (testing "updated fields are reflected in queries"
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:status "active"})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:profile {:age 31}})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:profile {:age 30}})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:tags "c"})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:tags "a"})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:orders {:product "C"}})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:orders {:product "B"}})
                         [[?e ?a ?v]]]]
                      db)))))
    (sut/close conn)
    (when-not (u/windows?) (u/delete-files dir))))

(deftest idoc-patch-test
  (let [dir  (u/tmp-dir (str "idoc-patch-test-" (UUID/randomUUID)))
        conn (sut/create-conn
               dir
               base-schema
               {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})
        doc  {:status  "active"
              :profile {:age 30 :name "Alice"}
              :tags    ["a"]
              :orders  [{:product "A" :qty 10}
                        {:product "B" :qty 20}]}]
    (sut/transact! conn [{:db/id 1 :doc/edn doc}])
    (sut/transact! conn
                   [[:db.fn/patchIdoc 1 :doc/edn
                     [[:set [:profile :age] 31]
                      [:update [:tags] :conj "b"]]]])
    (let [db (sut/db conn)]
      (testing "patch set + update reflected in queries"
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:profile {:age 31}})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:profile {:age 30}})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:tags "b"})
                         [[?e ?a ?v]]]]
                      db)))))
    (sut/transact! conn
                   [[:db.fn/patchIdoc 1 :doc/edn
                     [[:set [:orders 1 :product] "C"]]]])
    (let [db (sut/db conn)]
      (testing "patch path with vector index updates idoc index"
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:orders {:product "C"}})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:orders {:product "B"}})
                         [[?e ?a ?v]]]]
                      db)))))
    (sut/transact! conn
                   [[:db.fn/patchIdoc 1 :doc/edn
                     [[:update [:profile] :assoc :title "dev"]
                      [:update [:profile] :merge {:country "US"}]
                      [:update [:profile] :dissoc :name]
                      [:update [:profile :age] :inc]]]])
    (let [db (sut/db conn)]
      (testing "patch update ops reflected in queries"
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:profile {:title "dev"}})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:profile {:country "US"}})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:profile {:name "Alice"}})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:profile {:age 32}})
                         [[?e ?a ?v]]]]
                      db)))))
    (sut/transact! conn
                   [[:db.fn/patchIdoc 1 :doc/edn
                     [[:unset [:profile :title]]
                      [:update [:profile :age] :dec]]]])
    (let [db (sut/db conn)]
      (testing "patch unset + dec reflected in queries"
        (is (= #{}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:profile {:title "dev"}})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/edn {:profile {:age 31}})
                         [[?e ?a ?v]]]]
                      db)))))
    (sut/close conn)
    (when-not (u/windows?) (u/delete-files dir))))

(deftest idoc-patch-many-test
  (let [dir     (u/tmp-dir (str "idoc-patch-many-test-" (UUID/randomUUID)))
        schema  (assoc base-schema
                  :doc/many {:db/valueType  :db.type/idoc
                             :db/cardinality :db.cardinality/many
                             :db/domain     "many-docs"})
        conn    (sut/create-conn
                  dir
                  schema
                  {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})
        doc-a   {:profile {:age 30 :name "A"}}
        doc-b   {:profile {:age 35 :name "B"}}]
    (sut/transact! conn [{:db/id 1 :doc/many [doc-a doc-b]}])
    (sut/transact! conn
                   [[:db.fn/patchIdoc 1 :doc/many doc-b
                     [[:set [:profile :age] 40]]]])
    (let [db (sut/db conn)]
      (testing "patch replaces targeted value in cardinality many"
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/many {:profile {:age 40}})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/many {:profile {:age 35}})
                         [[?e ?a ?v]]]]
                      db)))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $
                        :where
                        [(idoc-match $ :doc/many {:profile {:age 30}})
                         [[?e ?a ?v]]]]
                      db)))))
    (sut/close conn)
    (when-not (u/windows?) (u/delete-files dir))))

(deftest idoc-giant-value-test
  (let [dir      (u/tmp-dir (str "idoc-giant-test-" (UUID/randomUUID)))
        conn     (sut/create-conn
                   dir
                   base-schema
                   {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})
        base-len (+ c/+val-bytes-wo-hdr+ 100)
        big1     (apply str (repeat base-len "a"))
        big2     (str big1 "b")]
    (sut/transact! conn [{:db/id 1 :doc/edn {:bio big1}}
                         {:db/id 2 :doc/edn {:bio big2}}])
    (let [db (sut/db conn)]
      (testing "giant values are indexed by prefix and matched exactly"
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $ ?q
                        :where
                        [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
                      db
                      {:bio big1})))
        (is (= #{[2]}
               (sut/q '[:find ?e
                        :in $ ?q
                        :where
                        [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
                      db
                      {:bio big2})))))
    (sut/close conn)
    (when-not (u/windows?) (u/delete-files dir))))

(deftest idoc-string-key-wildcard-test
  (let [dir  (u/tmp-dir (str "idoc-string-key-test-" (UUID/randomUUID)))
        conn (sut/create-conn
               dir
               base-schema
               {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})]
    (sut/transact!
      conn
      [{:db/id 1 :doc/edn {:facts {"city" "SF" "team" "blue"}}}
       {:db/id 2 :doc/edn {:facts {"city" "NYC" "team" "red"}}}
       {:db/id 3 :doc/edn {:facts {"city" "SF" "team" "green"}}}])
    (let [db (sut/db conn)]
      (testing "single-segment wildcard :? matches string keys"
        (is (= #{[1] [3]}
               (sut/q '[:find ?e
                        :in $ ?q
                        :where
                        [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
                      db
                      {:facts {:? "SF"}})))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $ ?q
                        :where
                        [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
                      db
                      {:facts {:? "blue"}})))
        (is (= #{[2]}
               (sut/q '[:find ?e
                        :in $ ?q
                        :where
                        [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
                      db
                      {:facts {:? "red"}}))))
      (testing "exact string key matches still work"
        (is (= #{[1] [3]}
               (sut/q '[:find ?e
                        :in $ ?q
                        :where
                        [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
                      db
                      {:facts {"city" "SF"}})))
        (is (= #{[1]}
               (sut/q '[:find ?e
                        :in $ ?q
                        :where
                        [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
                      db
                      {:facts {"team" "blue"}})))))
    (sut/close conn)
    (when-not (u/windows?) (u/delete-files dir))))

(deftest idoc-fulltext-combo-test
  (let [dir  (u/tmp-dir (str "idoc-fulltext-test-" (UUID/randomUUID)))
        conn (sut/create-conn
               dir
               {:doc/md {:db/valueType           :db.type/idoc
                         :db/idocFormat          :markdown
                         :db/fulltext            true
                         :db.fulltext/autoDomain true}}
               {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})]
    (sut/transact!
      conn
      [{:db/id  1
        :doc/md "# something\nOfficia laboris id laborum."}])
    (let [db (sut/db conn)]
      (testing "attribute-specific fulltext search"
        (is (some?
              (sut/q '[:find ?v .
                        :in $ ?q
                        :where [(fulltext $ :doc/md ?q) [[?e ?a ?v]]]]
                     db "Officia"))))
      (testing "db-scoped fulltext search"
        (is (some?
              (sut/q '[:find ?v .
                        :in $ ?q
                        :where [(fulltext $ ?q) [[?e ?a ?v]]]]
                     db "Officia")))))
    (sut/close conn)
    (when-not (u/windows?) (u/delete-files dir))))
