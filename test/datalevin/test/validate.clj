(ns datalevin.test.validate
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [datalevin.validate :as vld]
   [datalevin.index :as idx]
   [datalevin.util :as u])
  (:import
   [java.util UUID Date ArrayList]
   [datalevin.storage Store]
   [datalevin.lmdb KVTxData]))

(use-fixtures :each db-fixture)

;; ---- Storage / schema validators ----

(deftest test-validate-closed-schema
  (testing "passes when attr is defined or schema is not closed"
    (let [schema {:name {:db/valueType :db.type/string}}
          opts   {:closed-schema? false}]
      (is (nil? (vld/validate-closed-schema schema opts :name "test")))
      (is (nil? (vld/validate-closed-schema schema opts :unknown "test")))))

  (testing "passes when attr is defined and schema is closed"
    (let [schema {:name {:db/valueType :db.type/string}}
          opts   {:closed-schema? true}]
      (is (nil? (vld/validate-closed-schema schema opts :name "test")))))

  (testing "raises when attr is missing and schema is closed"
    (let [schema {:name {:db/valueType :db.type/string}}
          opts   {:closed-schema? true}]
      (is (thrown-with-msg? Exception #"closed-schema"
            (vld/validate-closed-schema schema opts :unknown "test"))))))

(deftest test-validate-cardinality-change
  (testing "passes for one->many or same cardinality"
    (let [dir  (u/tmp-dir (str "card-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [store (:store (d/db conn))]
          (is (nil? (vld/validate-cardinality-change
                      store :name :db.cardinality/one :db.cardinality/many)))
          (is (nil? (vld/validate-cardinality-change
                      store :name :db.cardinality/one :db.cardinality/one))))
        (finally
          (d/close conn)
          (u/delete-files dir)))))

  (testing "raises for many->one when data exist"
    (let [dir  (u/tmp-dir (str "card-raise-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType   :db.type/string
                                          :db/cardinality :db.cardinality/many}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (d/transact! conn [{:name "Alice"}])
        (let [store (:store (d/db conn))]
          (is (thrown-with-msg? Exception #"Cardinality change is not allowed"
                (vld/validate-cardinality-change
                  store :name :db.cardinality/many :db.cardinality/one))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-validate-value-type-change
  (testing "passes for same type"
    (let [dir  (u/tmp-dir (str "vt-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [store (:store (d/db conn))]
          (is (nil? (vld/validate-value-type-change
                      store :name :db.type/string :db.type/string))))
        (finally
          (d/close conn)
          (u/delete-files dir)))))

  (testing "raises for different type when data exist"
    (let [dir  (u/tmp-dir (str "vt-raise-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (d/transact! conn [{:name "Alice"}])
        (let [store (:store (d/db conn))]
          (is (thrown-with-msg? Exception #"Value type change is not allowed"
                (vld/validate-value-type-change
                  store :name :db.type/string :db.type/long))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-validate-uniqueness-change
  (testing "passes when no duplicates or no data"
    (let [dir  (u/tmp-dir (str "uniq-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [store (:store (d/db conn))
              lmdb  (.-lmdb ^Store store)]
          ;; no data, should pass
          (is (nil? (vld/validate-uniqueness-change
                      store lmdb :name nil :db.unique/identity))))
        (finally
          (d/close conn)
          (u/delete-files dir)))))

  (testing "passes when adding unique and all values are distinct"
    (let [dir  (u/tmp-dir (str "uniq-ok-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (d/transact! conn [{:name "Alice"} {:name "Bob"}])
        (let [store (:store (d/db conn))
              lmdb  (.-lmdb ^Store store)]
          (is (nil? (vld/validate-uniqueness-change
                      store lmdb :name nil :db.unique/identity))))
        (finally
          (d/close conn)
          (u/delete-files dir)))))

  (testing "raises when adding unique and duplicates exist"
    (let [dir  (u/tmp-dir (str "uniq-raise-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (d/transact! conn [{:name "Alice"} {:name "Alice"}])
        (let [store (:store (d/db conn))
              lmdb  (.-lmdb ^Store store)]
          (is (thrown-with-msg? Exception #"uniqueness change is inconsistent"
                (vld/validate-uniqueness-change
                  store lmdb :name nil :db.unique/identity))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-validate-schema-mutation
  (testing "dispatcher delegates correctly"
    (let [dir  (u/tmp-dir (str "schema-mut-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [store (:store (d/db conn))
              lmdb  (.-lmdb ^Store store)]
          ;; should pass: value type same, cardinality same
          (is (nil? (vld/validate-schema-mutation
                      store lmdb :name
                      {:db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                      {:db/valueType :db.type/string :db/cardinality :db.cardinality/one}))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-validate-key-size
  (testing "passes for small keys"
    (is (nil? (vld/validate-key-size "hello" :data)))
    (is (nil? (vld/validate-key-size 42 :long)))
    (is (nil? (vld/validate-key-size 42 :id))))

  (testing "raises for keys > 511 bytes"
    (let [big-key (apply str (repeat 600 "x"))]
      (is (thrown-with-msg? Exception #"511 bytes"
            (vld/validate-key-size big-key :data))))))

;; ---- KV validators ----

(deftest test-validate-kv-op
  (testing "valid ops pass without error"
    (is (nil? (vld/validate-kv-op :put)))
    (is (nil? (vld/validate-kv-op :del)))
    (is (nil? (vld/validate-kv-op :put-list)))
    (is (nil? (vld/validate-kv-op :del-list))))

  (testing "unknown op raises"
    (is (thrown-with-msg? Exception #"Unknown kv transact operator"
          (vld/validate-kv-op :bogus)))
    (is (thrown-with-msg? Exception #"Unknown kv transact operator"
          (vld/validate-kv-op :update)))))

(deftest test-validate-kv-key
  (testing "valid key passes"
    (is (nil? (vld/validate-kv-key "hello" :data false)))
    (is (nil? (vld/validate-kv-key 42 :long false)))
    (is (nil? (vld/validate-kv-key "hello" :data true))))

  (testing "nil key raises"
    (is (thrown-with-msg? Exception #"Key cannot be nil"
          (vld/validate-kv-key nil :data false)))
    (is (thrown-with-msg? Exception #"Key cannot be nil"
          (vld/validate-kv-key nil :data true))))

  (testing "type mismatch raises when validate-data? is true"
    (is (thrown-with-msg? Exception #"Invalid data, expecting"
          (vld/validate-kv-key "hello" :long true))))

  (testing "type mismatch ignored when validate-data? is false"
    (is (nil? (vld/validate-kv-key "hello" :long false)))))

(deftest test-validate-kv-value
  (testing "valid value passes"
    (is (nil? (vld/validate-kv-value "hello" :data false)))
    (is (nil? (vld/validate-kv-value 42 :long false)))
    (is (nil? (vld/validate-kv-value "hello" :data true))))

  (testing "nil value raises"
    (is (thrown-with-msg? Exception #"Value cannot be nil"
          (vld/validate-kv-value nil :data false)))
    (is (thrown-with-msg? Exception #"Value cannot be nil"
          (vld/validate-kv-value nil :data true))))

  (testing "type mismatch raises when validate-data? is true"
    (is (thrown-with-msg? Exception #"Invalid data, expecting"
          (vld/validate-kv-value "hello" :long true))))

  (testing "type mismatch ignored when validate-data? is false"
    (is (nil? (vld/validate-kv-value "hello" :long false)))))

(deftest test-validate-kv-tx-data
  (testing "valid :put tx passes"
    (let [tx (KVTxData. :put "test-dbi" "mykey" "myval" :data :data nil)]
      (is (nil? (vld/validate-kv-tx-data tx false)))))

  (testing "valid :del tx passes"
    (let [tx (KVTxData. :del "test-dbi" "mykey" nil :data nil nil)]
      (is (nil? (vld/validate-kv-tx-data tx false)))))

  (testing "valid :put-list tx passes"
    (let [tx (KVTxData. :put-list "test-dbi" "mykey" ["v1" "v2"] :data :data nil)]
      (is (nil? (vld/validate-kv-tx-data tx false)))))

  (testing "valid :del-list tx passes"
    (let [tx (KVTxData. :del-list "test-dbi" "mykey" ["v1" "v2"] :data :data nil)]
      (is (nil? (vld/validate-kv-tx-data tx false)))))

  (testing "java.util.List values pass for :put-list"
    (let [jlist (doto (ArrayList.) (.add "v1") (.add "v2"))
          tx    (KVTxData. :put-list "test-dbi" "mykey" jlist :data :data nil)]
      (is (nil? (vld/validate-kv-tx-data tx false)))))

  (testing "java.util.List values pass for :del-list"
    (let [jlist (doto (ArrayList.) (.add "v1") (.add "v2"))
          tx    (KVTxData. :del-list "test-dbi" "mykey" jlist :data :data nil)]
      (is (nil? (vld/validate-kv-tx-data tx false)))))

  (testing "unknown op raises"
    (let [tx (KVTxData. :bogus "test-dbi" "mykey" "myval" :data :data nil)]
      (is (thrown-with-msg? Exception #"Unknown kv transact operator"
            (vld/validate-kv-tx-data tx false)))))

  (testing "nil key raises"
    (let [tx (KVTxData. :put "test-dbi" nil "myval" :data :data nil)]
      (is (thrown-with-msg? Exception #"Key cannot be nil"
            (vld/validate-kv-tx-data tx false)))))

  (testing "nil value in :put raises"
    (let [tx (KVTxData. :put "test-dbi" "mykey" nil :data :data nil)]
      (is (thrown-with-msg? Exception #"Value cannot be nil"
            (vld/validate-kv-tx-data tx false)))))

  (testing "oversized key raises"
    (let [big-key (apply str (repeat 600 "x"))
          tx      (KVTxData. :put "test-dbi" big-key "myval" :data :data nil)]
      (is (thrown-with-msg? Exception #"511 bytes"
            (vld/validate-kv-tx-data tx false)))))

  (testing "type mismatch raises when validate-data? is true"
    (let [tx (KVTxData. :put "test-dbi" "mykey" "myval" :long :data nil)]
      (is (thrown-with-msg? Exception #"Invalid data, expecting"
            (vld/validate-kv-tx-data tx true)))))

  (testing "nil value in :put-list raises"
    (let [tx (KVTxData. :put-list "test-dbi" "mykey" nil :data :data nil)]
      (is (thrown-with-msg? Exception #"sequential collection"
            (vld/validate-kv-tx-data tx false)))))

  (testing "non-sequential value in :put-list raises"
    (let [tx (KVTxData. :put-list "test-dbi" "mykey" "scalar" :data :data nil)]
      (is (thrown-with-msg? Exception #"sequential collection"
            (vld/validate-kv-tx-data tx false)))))

  (testing "nil value in :del-list raises"
    (let [tx (KVTxData. :del-list "test-dbi" "mykey" nil :data :data nil)]
      (is (thrown-with-msg? Exception #"sequential collection"
            (vld/validate-kv-tx-data tx false)))))

  (testing "non-sequential value in :del-list raises"
    (let [tx (KVTxData. :del-list "test-dbi" "mykey" 42 :data :data nil)]
      (is (thrown-with-msg? Exception #"sequential collection"
            (vld/validate-kv-tx-data tx false))))))

;; ---- DB validators ----

(deftest test-validate-schema
  (testing "passes for valid schema"
    (is (nil? (vld/validate-schema
                {:name {:db/valueType :db.type/string}
                 :age  {:db/valueType :db.type/long}}))))

  (testing "raises for bad isComponent without ref"
    (is (thrown-with-msg? Exception #"isComponent"
          (vld/validate-schema
            {:item {:db/isComponent true
                    :db/valueType   :db.type/string}}))))

  (testing "raises for bad cardinality spec"
    (is (thrown-with-msg? Exception #"Bad attribute"
          (vld/validate-schema
            {:name {:db/cardinality :wrong}}))))

  (testing "raises for tuple without tuple props"
    (is (thrown-with-msg? Exception #"tupleAttrs"
          (vld/validate-schema
            {:coord {:db/valueType :db.type/tuple}})))))

(deftest test-validate-attr
  (testing "passes for keywords"
    (is (nil? (vld/validate-attr :name {:entity "test"}))))

  (testing "raises for non-keywords"
    (is (thrown-with-msg? Exception #"Bad entity attribute"
          (vld/validate-attr "name" {:entity "test"})))))

(deftest test-validate-val
  (testing "passes for non-nil"
    (is (nil? (vld/validate-val "hello" {:entity "test"})))
    (is (nil? (vld/validate-val 0 {:entity "test"})))
    (is (nil? (vld/validate-val false {:entity "test"}))))

  (testing "raises for nil"
    (is (thrown-with-msg? Exception #"Cannot store nil"
          (vld/validate-val nil {:entity "test"})))))

(deftest test-validate-datom-unique
  (testing "passes when not unique"
    (is (= "test" (vld/validate-datom-unique
                    false
                    (d/datom 1 :name "test")
                    (constantly nil)))))

  (testing "passes when unique but no conflict found"
    (is (= "test" (vld/validate-datom-unique
                    true
                    (d/datom 1 :name "test")
                    (constantly nil)))))

  (testing "raises when unique and conflict found"
    (is (thrown-with-msg? Exception #"unique constraint"
          (vld/validate-datom-unique
            true
            (d/datom 1 :name "test")
            (constantly true))))))

(deftest test-validate-upserts
  (testing "passes when upserts agree on single entity"
    (let [upserts {:email {"alice@test.com" 1}}]
      (is (= 1 (vld/validate-upserts {:db/id -1} upserts
                                          (fn [x] (and (number? x) (neg? ^long x))))))))

  (testing "raises on conflicting upserts"
    (let [upserts {:email {"alice@test.com" 1}
                   :name  {"Bob" 2}}]
      (is (thrown-with-msg? Exception #"Conflicting upserts"
            (vld/validate-upserts {:db/id -1} upserts
                                       (fn [x] (and (number? x) (neg? ^long x)))))))))

(deftest test-validate-type
  (testing "passes when data matches type"
    (let [dir  (u/tmp-dir (str "vtype-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [store (:store (d/db conn))]
          (is (= :db.type/string (vld/validate-type store :name "hello"))))
        (finally
          (d/close conn)
          (u/delete-files dir)))))

  (testing "raises on mismatch when validate-data? is set"
    (let [dir  (u/tmp-dir (str "vtype-raise-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/long}}
                              {:validate-data? true
                               :kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [store (:store (d/db conn))]
          (is (thrown-with-msg? Exception #"Invalid data"
                (vld/validate-type store :name "not-a-long"))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-validate-option-mutation
  (testing "boolean options accept true/false"
    (is (nil? (vld/validate-option-mutation :closed-schema? true)))
    (is (nil? (vld/validate-option-mutation :closed-schema? false)))
    (is (nil? (vld/validate-option-mutation :validate-data? true)))
    (is (nil? (vld/validate-option-mutation :auto-entity-time? false)))
    (is (nil? (vld/validate-option-mutation :background-sampling? true))))

  (testing "boolean options reject non-booleans"
    (is (thrown-with-msg? Exception #"expects a boolean"
          (vld/validate-option-mutation :closed-schema? "yes")))
    (is (thrown-with-msg? Exception #"expects a boolean"
          (vld/validate-option-mutation :validate-data? 1))))

  (testing ":cache-limit accepts non-negative integers"
    (is (nil? (vld/validate-option-mutation :cache-limit 0)))
    (is (nil? (vld/validate-option-mutation :cache-limit 512)))
    (is (nil? (vld/validate-option-mutation :cache-limit 1024))))

  (testing ":cache-limit rejects non-integers and negatives"
    (is (thrown-with-msg? Exception #"non-negative integer"
          (vld/validate-option-mutation :cache-limit -1)))
    (is (thrown-with-msg? Exception #"non-negative integer"
          (vld/validate-option-mutation :cache-limit "big")))
    (is (thrown-with-msg? Exception #"non-negative integer"
          (vld/validate-option-mutation :cache-limit 3.14))))

  (testing ":db-name accepts strings"
    (is (nil? (vld/validate-option-mutation :db-name "my-db"))))

  (testing ":db-name rejects non-strings"
    (is (thrown-with-msg? Exception #"expects a string"
          (vld/validate-option-mutation :db-name 42))))

  (testing "unknown options pass through without error"
    (is (nil? (vld/validate-option-mutation :some-custom-opt "anything")))))

;; ---- Schema key validator ----

(deftest test-validate-schema-key
  (testing "passes when value is nil"
    (is (nil? (vld/validate-schema-key :name :db/unique nil
                #{:db.unique/value :db.unique/identity}))))

  (testing "passes when value is in expected set"
    (is (nil? (vld/validate-schema-key :name :db/unique :db.unique/identity
                #{:db.unique/value :db.unique/identity}))))

  (testing "raises when value is not in expected set"
    (is (thrown-with-msg? Exception #"Bad attribute specification"
          (vld/validate-schema-key :name :db/unique :wrong
            #{:db.unique/value :db.unique/identity})))))

;; ---- Transaction form validators ----

(deftest test-validate-tempid-op
  (testing "passes for :db/add with tempid"
    (is (nil? (vld/validate-tempid-op true :db/add [:db/add -1 :name "x"]))))

  (testing "passes for non-tempid regardless of op"
    (is (nil? (vld/validate-tempid-op false :db/retract [:db/retract 1 :name "x"]))))

  (testing "raises for tempid with non-:db/add op"
    (is (thrown-with-msg? Exception #"Tempids are allowed in :db/add only"
          (vld/validate-tempid-op true :db/retract [:db/retract -1 :name "x"])))))

(deftest test-validate-cas-value
  (testing "passes for multival when old value found among datoms"
    (let [datoms [(d/datom 1 :tags "a") (d/datom 1 :tags "b")]]
      (is (nil? (vld/validate-cas-value true 1 :tags "a" "c" datoms)))))

  (testing "raises for multival when old value not found"
    (let [datoms [(d/datom 1 :tags "a") (d/datom 1 :tags "b")]]
      (is (thrown-with-msg? Exception #":db.fn/cas failed"
            (vld/validate-cas-value true 1 :tags "missing" "c" datoms)))))

  (testing "passes for single-val when datom value matches"
    (let [datoms [(d/datom 1 :name "Alice")]]
      (is (nil? (vld/validate-cas-value false 1 :name "Alice" "Bob" datoms)))))

  (testing "raises for single-val when datom value doesn't match"
    (let [datoms [(d/datom 1 :name "Alice")]]
      (is (thrown-with-msg? Exception #":db.fn/cas failed"
            (vld/validate-cas-value false 1 :name "Bob" "Carol" datoms))))))

(deftest test-validate-patch-idoc-arity
  (testing "passes for arity 4"
    (is (nil? (vld/validate-patch-idoc-arity
                4 [:db.fn/patchIdoc 1 :doc {}] :db.fn/patchIdoc))))

  (testing "passes for arity 5"
    (is (nil? (vld/validate-patch-idoc-arity
                5 [:db.fn/patchIdoc 1 :doc {} "old"] :db.fn/patchIdoc))))

  (testing "raises for wrong arity"
    (is (thrown-with-msg? Exception #"Bad arity for :db.fn/patchIdoc"
          (vld/validate-patch-idoc-arity
            3 [:db.fn/patchIdoc 1 :doc] :db.fn/patchIdoc)))
    (is (thrown-with-msg? Exception #"Bad arity for :db.fn/patchIdoc"
          (vld/validate-patch-idoc-arity
            6 [:db.fn/patchIdoc 1 :doc {} "old" "extra"] :db.fn/patchIdoc)))))

(deftest test-validate-patch-idoc-type
  (testing "passes for idoc type"
    (is (nil? (vld/validate-patch-idoc-type :db.type/idoc :doc))))

  (testing "raises for non-idoc type"
    (is (thrown-with-msg? Exception #"not an idoc type"
          (vld/validate-patch-idoc-type :db.type/string :doc)))))

(deftest test-validate-patch-idoc-cardinality
  (testing "passes for cardinality-many with old value"
    (is (nil? (vld/validate-patch-idoc-cardinality true "old-v" :doc))))

  (testing "passes for cardinality-one without old value"
    (is (nil? (vld/validate-patch-idoc-cardinality false nil :doc))))

  (testing "raises for cardinality-many without old value"
    (is (thrown-with-msg? Exception #"requires old value"
          (vld/validate-patch-idoc-cardinality true nil :doc))))

  (testing "raises for cardinality-one with old value"
    (is (thrown-with-msg? Exception #"only supported for cardinality many"
          (vld/validate-patch-idoc-cardinality false "old-v" :doc)))))

(deftest test-validate-patch-idoc-old-value
  (testing "passes when old datom exists"
    (is (nil? (vld/validate-patch-idoc-old-value
                (d/datom 1 :doc "old") "old" :doc))))

  (testing "raises when old datom is nil"
    (is (thrown-with-msg? Exception #"old value not found"
          (vld/validate-patch-idoc-old-value nil "old" :doc)))))

(deftest test-validate-custom-tx-fn-value
  (testing "passes when fun is a function"
    (is (nil? (vld/validate-custom-tx-fn-value
                inc :my-fn [:db.fn/call :my-fn]))))

  (testing "raises when fun is not a function"
    (is (thrown-with-msg? Exception #"expected to have :db/fn"
          (vld/validate-custom-tx-fn-value
            "not-a-fn" :my-fn [:db.fn/call :my-fn])))
    (is (thrown-with-msg? Exception #"expected to have :db/fn"
          (vld/validate-custom-tx-fn-value
            nil :my-fn [:db.fn/call :my-fn])))))

(deftest test-validate-custom-tx-fn-entity
  (testing "passes when ident is non-nil"
    (is (nil? (vld/validate-custom-tx-fn-entity
                {:db/fn inc} :my-fn [:db.fn/call :my-fn]))))

  (testing "raises when ident is nil"
    (is (thrown-with-msg? Exception #"find entity for transaction fn"
          (vld/validate-custom-tx-fn-entity
            nil :my-fn [:db.fn/call :my-fn])))))

(deftest test-validate-tuple-direct-write
  (testing "passes when match? is true"
    (is (nil? (vld/validate-tuple-direct-write true [:db/add 1 :name "x"]))))

  (testing "raises when match? is false"
    (is (thrown-with-msg? Exception #"modify tuple attrs directly"
          (vld/validate-tuple-direct-write false [:db/add 1 :coord [1 2]])))))

(deftest test-validate-tx-op
  (testing "always raises with unknown op"
    (is (thrown-with-msg? Exception #"Unknown operation"
          (vld/validate-tx-op :bogus [:bogus 1 :name "x"])))
    (is (thrown-with-msg? Exception #"Unknown operation"
          (vld/validate-tx-op :db/update [:db/update 1 :name "x"])))))

(deftest test-validate-tx-entity-type
  (testing "always raises with bad entity type"
    (is (thrown-with-msg? Exception #"Bad entity type"
          (vld/validate-tx-entity-type "not-a-valid-entity")))
    (is (thrown-with-msg? Exception #"Bad entity type"
          (vld/validate-tx-entity-type 42)))))

;; ---- Entity-id / lookup-ref validators ----

(deftest test-validate-lookup-ref-shape
  (testing "passes for 2-element lookup ref"
    (is (nil? (vld/validate-lookup-ref-shape [:name "Alice"]))))

  (testing "raises for wrong element count"
    (is (thrown-with-msg? Exception #"Lookup ref should contain 2 elements"
          (vld/validate-lookup-ref-shape [:name])))
    (is (thrown-with-msg? Exception #"Lookup ref should contain 2 elements"
          (vld/validate-lookup-ref-shape [:name "Alice" :extra])))))

(deftest test-validate-lookup-ref-unique
  (testing "passes when unique? is true"
    (is (nil? (vld/validate-lookup-ref-unique true [:name "Alice"]))))

  (testing "raises when unique? is false"
    (is (thrown-with-msg? Exception #"should be marked as :db/unique"
          (vld/validate-lookup-ref-unique false [:name "Alice"])))))

(deftest test-validate-entity-id-syntax
  (testing "always raises"
    (is (thrown-with-msg? Exception #"Expected number or lookup ref"
          (vld/validate-entity-id-syntax "bad-id")))
    (is (thrown-with-msg? Exception #"Expected number or lookup ref"
          (vld/validate-entity-id-syntax :keyword-id)))))

(deftest test-validate-map-entity-id-syntax
  (testing "always raises"
    (is (thrown-with-msg? Exception #"Expected number, string or lookup ref"
          (vld/validate-map-entity-id-syntax :keyword-id)))
    (is (thrown-with-msg? Exception #"Expected number, string or lookup ref"
          (vld/validate-map-entity-id-syntax true)))))

(deftest test-validate-entity-id-exists
  (testing "passes when result is non-nil"
    (is (nil? (vld/validate-entity-id-exists 42 [:name "Alice"]))))

  (testing "raises when result is nil"
    (is (thrown-with-msg? Exception #"Nothing found for entity id"
          (vld/validate-entity-id-exists nil [:name "Alice"])))))

(deftest test-validate-reverse-ref-attr
  (testing "passes for keyword"
    (is (nil? (vld/validate-reverse-ref-attr :_parent))))

  (testing "raises for non-keyword"
    (is (thrown-with-msg? Exception #"Bad entity attribute"
          (vld/validate-reverse-ref-attr "not-a-keyword")))))

(deftest test-validate-reverse-ref-type
  (testing "passes when ref? is true"
    (is (nil? (vld/validate-reverse-ref-type true :parent 1 [2 3]))))

  (testing "raises when ref? is false"
    (is (thrown-with-msg? Exception #"reverse attribute name requires"
          (vld/validate-reverse-ref-type false :parent 1 [2 3])))))

;; ---- Finalize-phase consistency validators ----

(deftest test-validate-value-tempids
  (testing "passes when no unused tempids"
    (is (nil? (vld/validate-value-tempids [])))
    (is (nil? (vld/validate-value-tempids nil))))

  (testing "raises when unused tempids exist"
    (is (thrown-with-msg? Exception #"Tempids used only as value"
          (vld/validate-value-tempids [-1 -2])))))

(deftest test-validate-upsert-retry-conflict
  (testing "passes when eid is nil"
    (is (nil? (vld/validate-upsert-retry-conflict nil -1 42))))

  (testing "raises when eid is non-nil (conflict)"
    (is (thrown-with-msg? Exception #"Conflicting upsert"
          (vld/validate-upsert-retry-conflict 99 -1 42)))))

(deftest test-validate-upsert-conflict
  (testing "passes when tempid is non-nil"
    (is (nil? (vld/validate-upsert-conflict -1 1 42 {:db/id -1}))))

  (testing "raises when tempid is nil"
    (is (thrown-with-msg? Exception #"Conflicting upsert"
          (vld/validate-upsert-conflict nil 1 42 {:db/id 1})))))

(deftest test-validate-tx-data-shape
  (testing "passes for nil"
    (is (nil? (vld/validate-tx-data-shape nil))))

  (testing "passes for sequential collection"
    (is (nil? (vld/validate-tx-data-shape [])))
    (is (nil? (vld/validate-tx-data-shape [[:db/add 1 :name "x"]])))
    (is (nil? (vld/validate-tx-data-shape '([:db/add 1 :name "x"])))))

  (testing "raises for non-sequential"
    (is (thrown-with-msg? Exception #"Bad transaction data"
          (vld/validate-tx-data-shape {:a 1})))
    (is (thrown-with-msg? Exception #"Bad transaction data"
          (vld/validate-tx-data-shape "bad")))))

(deftest test-validate-attr-deletable
  (testing "passes when not populated"
    (is (nil? (vld/validate-attr-deletable false))))

  (testing "raises when populated"
    (is (thrown-with-msg? Exception #"Cannot delete attribute with datoms"
          (vld/validate-attr-deletable true)))))

(deftest test-validate-datom-list
  (testing "passes for empty list"
    (is (nil? (vld/validate-datom-list []))))

  (testing "passes for valid datom list"
    (is (nil? (vld/validate-datom-list
                [(d/datom 1 :name "Alice") (d/datom 2 :name "Bob")]))))

  (testing "raises when non-datom element found"
    (is (thrown-with-msg? Exception #"init-db expects list of Datoms"
          (vld/validate-datom-list [{:name "Alice"}])))
    (is (thrown-with-msg? Exception #"init-db expects list of Datoms"
          (vld/validate-datom-list [(d/datom 1 :name "Alice") "not-a-datom"])))))

;; ---- Mutation gateway validators ----

(deftest test-validate-trusted-apply
  (testing "passes when *trusted-apply* is true"
    (binding [c/*trusted-apply* true]
      (is (nil? (vld/validate-trusted-apply)))))

  (testing "raises when *trusted-apply* is false (default)"
    (is (thrown-with-msg? Exception #"internal apply is not allowed"
          (vld/validate-trusted-apply))))

  (testing "raises when explicitly bound to false"
    (binding [c/*trusted-apply* false]
      (is (thrown-with-msg? Exception #"internal apply is not allowed"
            (vld/validate-trusted-apply))))))

(deftest test-check-failpoint
  (testing "no-op when *failpoint* is nil"
    (is (nil? (vld/check-failpoint :step-3 :before))))

  (testing "no-op when step or phase don't match"
    (binding [c/*failpoint* {:step :step-5 :phase :before
                             :fn   #(throw (ex-info "boom" {}))}]
      (is (nil? (vld/check-failpoint :step-3 :before)))
      (is (nil? (vld/check-failpoint :step-5 :after)))))

  (testing "invokes fn when step and phase match"
    (let [called (atom false)]
      (binding [c/*failpoint* {:step :step-3 :phase :before
                               :fn   #(reset! called true)}]
        (vld/check-failpoint :step-3 :before)
        (is (true? @called)))))

  (testing "propagates exception from failpoint fn"
    (binding [c/*failpoint* {:step :step-3 :phase :after
                             :fn   #(throw (ex-info "injected crash" {}))}]
      (is (thrown-with-msg? Exception #"injected crash"
            (vld/check-failpoint :step-3 :after))))))

;; ---- Prepare-time validators (moved from prepare.clj) ----

(deftest test-validate-prepared-datoms
  (testing "prepare validator enforces closed schema for added datoms"
    (let [schema {:name {:db/aid 1}}
          opts   {:closed-schema? true}
          datoms [(d/datom 1 :name "Alice")
                  (d/datom 1 :age 42)]]
      (is (thrown-with-msg? Exception #"closed-schema"
                            (vld/validate-prepared-datoms
                              schema opts datoms)))))

  (testing "prepare validator allows datoms when closed schema is disabled"
    (let [schema {:name {:db/aid 1}}
          opts   {:closed-schema? false}
          datoms [(d/datom 1 :name "Alice")
                  (d/datom 1 :age 42)]]
      (is (= datoms (vld/validate-prepared-datoms schema opts datoms))))))

(deftest test-validate-load-datoms
  (testing "load-datoms validator rejects non-datom payloads"
    (let [dir  (u/tmp-dir (str "prepare-load-datoms-shape-" (UUID/randomUUID)))
          conn (d/create-conn dir nil
                              {:kv-opts {:flags (conj c/default-env-flags
                                                      :nosync)}})]
      (try
        (let [store (:store (d/db conn))]
          (is (thrown-with-msg? Exception #"expects list of Datoms"
                                (vld/validate-load-datoms
                                  store [[:db/add 1 :name "Alice"]]))))
        (finally
          (d/close conn)
          (u/delete-files dir)))))

  (testing "load-datoms validator enforces closed schema before apply"
    (let [dir  (u/tmp-dir (str "prepare-load-datoms-closed-schema-"
                               (UUID/randomUUID)))
          conn (d/create-conn
                 dir
                 {:name {:db/valueType :db.type/string}}
                 {:closed-schema? true
                  :kv-opts {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [store (:store (d/db conn))]
          (is (thrown-with-msg? Exception #"closed-schema"
                                (vld/validate-load-datoms
                                  store [(d/datom 1 :age 42)]))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-validate-side-index-domains
  (let [schema {:ft/title {:db/aid                 1
                           :db/valueType           :db.type/string
                           :db/fulltext            true
                           :db.fulltext/domains    ["posts"]
                           :db.fulltext/autoDomain true}
                :vec/emb  {:db/aid         2
                           :db/valueType   :db.type/vec
                           :db.vec/domains ["vectors"]}
                :doc/body {:db/aid       3
                           :db/valueType :db.type/idoc
                           :db/domain    "profiles"}}
        datoms [(d/datom 1 :ft/title "hello")
                (d/datom 1 :vec/emb [0.1 0.2])
                (d/datom 1 :doc/body {:status "ok"})]
        available {:fulltext #{"posts" "ft/title"}
                   :vector   #{"vectors" "vec_emb"}
                   :idoc     #{"profiles"}}]
    (testing "domain validation passes when required domains exist"
      (is (= datoms
             (vld/validate-side-index-domains schema datoms available))))

    (testing "missing fulltext domain fails before apply"
      (is (thrown-with-msg? Exception #"Fulltext domain is not initialized"
                            (vld/validate-side-index-domains
                              schema
                              [(d/datom 1 :ft/title "hello")]
                              (assoc available :fulltext #{"posts"})))))

    (testing "missing vector domain fails before apply"
      (is (thrown-with-msg? Exception #"Vector domain is not initialized"
                            (vld/validate-side-index-domains
                              schema
                              [(d/datom 1 :vec/emb [0.1 0.2])]
                              (assoc available :vector #{"vectors"})))))

    (testing "missing idoc domain fails before apply"
      (is (thrown-with-msg? Exception #"Idoc domain is not initialized"
                            (vld/validate-side-index-domains
                              schema
                              [(d/datom 1 :doc/body {:status "ok"})]
                              (assoc available :idoc #{})))))))

(deftest test-validate-schema-side-index-domains
  (let [schema-update {:vec/emb {:db/valueType :db.type/vec}}
        available     {:fulltext #{}
                       :vector   #{"vec_emb"}
                       :idoc     #{}}]
    (testing "schema side-index domains pass when initialized"
      (is (nil? (vld/validate-schema-side-index-domains
                  schema-update available))))
    (testing "schema side-index domains fail when missing"
      (is (thrown-with-msg? Exception #"Vector domain is not initialized"
                            (vld/validate-schema-side-index-domains
                              schema-update
                              (assoc available :vector #{})))))))

(deftest test-validate-schema-update
  (testing "prepare validator rejects malformed schema updates"
    (is (thrown-with-msg? Exception #"Bad attribute specification"
                          (vld/validate-schema-update
                            nil nil {:bad/attr {:db/valueType :db.type/bogus}}))))

  (testing "nil schema update is a no-op"
    (is (nil? (vld/validate-schema-update nil nil nil)))))

(deftest test-validate-option-update
  (testing "valid option value passes through"
    (is (= [:cache-limit 0]
           (vld/validate-option-update :cache-limit 0))))

  (testing "invalid option value raises"
    (is (thrown-with-msg? Exception #"cache-limit expects a non-negative integer"
                          (vld/validate-option-update :cache-limit -1)))))

(deftest test-validate-swap-attr-update
  (testing "aid-only attr updates pass through prepare swap validator"
    (is (= {:db/aid 3}
           (vld/validate-swap-attr-update nil nil :name
                                          {:db/aid 3}
                                          {:db/aid 3})))))

(deftest test-validate-swap-attr-update-side-index-domain
  (testing "swap-attr side-index domains are validated"
    (is (thrown-with-msg? Exception #"Fulltext domain is not initialized"
                          (vld/validate-swap-attr-update
                            nil nil :ft/title {}
                            {:db/fulltext            true
                             :db.fulltext/autoDomain true}
                            {:fulltext #{}
                             :vector   #{}
                             :idoc     #{}})))))

(deftest test-validate-del-attr
  (testing "del-attr validation passes when attribute has no datoms"
    (with-redefs [datalevin.interface/populated? (fn [& _] false)]
      (is (= :a/b (vld/validate-del-attr nil :a/b)))))

  (testing "del-attr validation fails when attribute has datoms"
    (with-redefs [datalevin.interface/populated? (fn [& _] true)]
      (is (thrown-with-msg? Exception #"Cannot delete attribute with datoms"
                            (vld/validate-del-attr nil :a/b))))))

(deftest test-validate-rename-attr
  (testing "rename validation passes for existing source and new target"
    (with-redefs [datalevin.interface/schema (fn [_] {:a/b {:db/aid 3}})]
      (is (= [:a/b :e/f]
             (vld/validate-rename-attr nil :a/b :e/f)))))

  (testing "rename validation rejects missing source attribute"
    (with-redefs [datalevin.interface/schema (fn [_] {:a/b {:db/aid 3}})]
      (is (thrown-with-msg? Exception #"Cannot rename missing attribute"
                            (vld/validate-rename-attr nil :x/y :e/f)))))

  (testing "rename validation rejects existing target attribute"
    (with-redefs [datalevin.interface/schema
                  (fn [_] {:a/b {:db/aid 3}
                           :e/f {:db/aid 4}})]
      (is (thrown-with-msg? Exception #"Cannot rename to existing attribute"
                            (vld/validate-rename-attr nil :a/b :e/f))))))

(deftest test-validate-rename-map
  (testing "rename map validates against projected schema and returns projection"
    (is (= {:x/y {:db/aid 4}
            :e/f {:db/aid 3}}
           (vld/validate-rename-map
             {:a/b {:db/aid 3}
              :x/y {:db/aid 4}}
             [[:a/b :e/f]]))))

  (testing "rename map validates sequentially across evolving schema"
    (is (= {:g/h {:db/aid 3}}
           (vld/validate-rename-map
             {:a/b {:db/aid 3}}
             [[:a/b :e/f]
              [:e/f :g/h]]))))

  (testing "rename map rejects collisions based on projected schema"
    (is (thrown-with-msg? Exception #"Cannot rename to existing attribute"
                          (vld/validate-rename-map
                            {:a/b {:db/aid 3}
                             :e/f {:db/aid 4}}
                            [[:a/b :e/f]])))))

(deftest test-type-coercion
  (testing "correct coercion for each value type"
    (is (= "hello" (vld/type-coercion :db.type/string "hello")))
    (is (= 42 (vld/type-coercion :db.type/long 42)))
    (is (= 3.14 (vld/type-coercion :db.type/double 3.14)))
    (is (= true (vld/type-coercion :db.type/boolean true)))
    (is (= :foo (vld/type-coercion :db.type/keyword "foo")))
    (is (= 'foo (vld/type-coercion :db.type/symbol "foo")))
    (is (= [1 2] (vld/type-coercion :db.type/tuple [1 2])))
    (is (instance? Date (vld/coerce-inst 1000)))
    (is (instance? UUID (vld/coerce-uuid "550e8400-e29b-41d4-a716-446655440000")))))

;; ---- Index utilities ----

(deftest test-index-value-type
  (testing "value-type extracts type from props"
    (is (= :db.type/string (idx/value-type {:db/valueType :db.type/string})))
    (is (= :db.type/long (idx/value-type {:db/valueType :db.type/long})))
    (is (= :data (idx/value-type {})))
    (is (= :data (idx/value-type nil)))))
