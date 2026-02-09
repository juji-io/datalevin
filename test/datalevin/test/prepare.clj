(ns datalevin.test.prepare
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [datalevin.prepare :as prepare]
   [datalevin.index :as idx]
   [datalevin.util :as u]
   [datalevin.bits :as b]
   [datalevin.interface :as i])
  (:import
   [java.util UUID Date]
   [datalevin.storage Store]))

(use-fixtures :each db-fixture)

(deftest test-prepare-ctx-construction
  (testing "PrepareCtx record can be constructed directly"
    (let [ctx (prepare/->PrepareCtx {} {} {} nil nil nil {} nil)]
      (is (instance? datalevin.prepare.PrepareCtx ctx))
      (is (= {} (:schema ctx)))
      (is (nil? (:attr-cache ctx)))))

  (testing "PreparedTx record can be constructed directly"
    (let [ptx (prepare/->PreparedTx [] {} [] [] {} #{} nil)]
      (is (instance? datalevin.prepare.PreparedTx ptx))
      (is (= [] (:tx-data ptx)))
      (is (= {} (:tempids ptx))))))

(deftest test-make-prepare-ctx
  (testing "make-prepare-ctx builds from a DB snapshot"
    (let [dir  (u/tmp-dir (str "prepare-ctx-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [db   (d/db conn)
              lmdb (.-lmdb ^Store (:store db))
              ctx  (prepare/make-prepare-ctx db lmdb)]
          (is (instance? datalevin.prepare.PrepareCtx ctx))
          (is (map? (:schema ctx)))
          (is (map? (:rschema ctx)))
          (is (map? (:opts ctx)))
          (is (some? (:store ctx)))
          (is (some? (:db ctx)))
          (is (some? (:lmdb ctx)))
          (is (map? (:attrs ctx)))
          (is (nil? (:attr-cache ctx))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-prepare-tx-returns-nil
  (testing "prepare-tx stub returns nil"
    (let [dir  (u/tmp-dir (str "prepare-tx-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [db   (d/db conn)
              lmdb (.-lmdb ^Store (:store db))
              ctx  (prepare/make-prepare-ctx db lmdb)]
          (is (nil? (prepare/prepare-tx ctx [{:name "test"}] 1))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-stage-shells-pass-through
  (testing "Stage boundary functions return input unchanged"
    (let [ctx      nil
          entities [{:name "test"}]]
      (is (= entities (prepare/normalize ctx entities)))
      (is (= entities (prepare/resolve-ids ctx entities)))
      (is (= entities (prepare/apply-op-semantics ctx entities)))
      (is (= entities (prepare/plan-delta ctx entities)))
      (is (= entities (prepare/build-side-index-ops ctx entities)))
      (is (= entities (prepare/finalize ctx entities))))))

(deftest test-transact-with-prepare-path-false
  (testing "transact! works with *use-prepare-path* bound to false (default)"
    (let [dir  (u/tmp-dir (str "prepare-false-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (binding [c/*use-prepare-path* false]
          (let [report (d/transact! conn [{:name "Alice"}])]
            (is (some? report))
            (is (seq (:tx-data report)))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-transact-with-prepare-path-true
  (testing "transact! falls through to legacy path with *use-prepare-path* true (stub returns nil)"
    (let [dir  (u/tmp-dir (str "prepare-true-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (binding [c/*use-prepare-path* true]
          (let [report (d/transact! conn [{:name "Bob"}])]
            (is (some? report))
            (is (seq (:tx-data report)))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

;; ---- Storage / schema validators ----

(deftest test-validate-closed-schema
  (testing "passes when attr is defined or schema is not closed"
    (let [schema {:name {:db/valueType :db.type/string}}
          opts   {:closed-schema? false}]
      (is (nil? (prepare/validate-closed-schema schema opts :name "test")))
      (is (nil? (prepare/validate-closed-schema schema opts :unknown "test")))))

  (testing "passes when attr is defined and schema is closed"
    (let [schema {:name {:db/valueType :db.type/string}}
          opts   {:closed-schema? true}]
      (is (nil? (prepare/validate-closed-schema schema opts :name "test")))))

  (testing "raises when attr is missing and schema is closed"
    (let [schema {:name {:db/valueType :db.type/string}}
          opts   {:closed-schema? true}]
      (is (thrown-with-msg? Exception #"closed-schema"
            (prepare/validate-closed-schema schema opts :unknown "test"))))))

(deftest test-validate-cardinality-change
  (testing "passes for one->many or same cardinality"
    (let [dir  (u/tmp-dir (str "card-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [store (:store (d/db conn))]
          (is (nil? (prepare/validate-cardinality-change
                      store :name :db.cardinality/one :db.cardinality/many)))
          (is (nil? (prepare/validate-cardinality-change
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
                (prepare/validate-cardinality-change
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
          (is (nil? (prepare/validate-value-type-change
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
                (prepare/validate-value-type-change
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
          (is (nil? (prepare/validate-uniqueness-change
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
          (is (nil? (prepare/validate-uniqueness-change
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
                (prepare/validate-uniqueness-change
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
          (is (nil? (prepare/validate-schema-mutation
                      store lmdb :name
                      {:db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                      {:db/valueType :db.type/string :db/cardinality :db.cardinality/one}))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-validate-key-size
  (testing "passes for small keys"
    (is (nil? (prepare/validate-key-size "hello" :data)))
    (is (nil? (prepare/validate-key-size 42 :long)))
    (is (nil? (prepare/validate-key-size 42 :id))))

  (testing "raises for keys > 511 bytes"
    (let [big-key (apply str (repeat 600 "x"))]
      (is (thrown-with-msg? Exception #"511 bytes"
            (prepare/validate-key-size big-key :data))))))

;; ---- DB validators ----

(deftest test-validate-schema
  (testing "passes for valid schema"
    (is (nil? (prepare/validate-schema
                {:name {:db/valueType :db.type/string}
                 :age  {:db/valueType :db.type/long}}))))

  (testing "raises for bad isComponent without ref"
    (is (thrown-with-msg? Exception #"isComponent"
          (prepare/validate-schema
            {:item {:db/isComponent true
                    :db/valueType   :db.type/string}}))))

  (testing "raises for bad cardinality spec"
    (is (thrown-with-msg? Exception #"Bad attribute"
          (prepare/validate-schema
            {:name {:db/cardinality :wrong}}))))

  (testing "raises for tuple without tuple props"
    (is (thrown-with-msg? Exception #"tupleAttrs"
          (prepare/validate-schema
            {:coord {:db/valueType :db.type/tuple}})))))

(deftest test-validate-attr
  (testing "passes for keywords"
    (is (nil? (prepare/validate-attr :name {:entity "test"}))))

  (testing "raises for non-keywords"
    (is (thrown-with-msg? Exception #"Bad entity attribute"
          (prepare/validate-attr "name" {:entity "test"})))))

(deftest test-validate-val
  (testing "passes for non-nil"
    (is (nil? (prepare/validate-val "hello" {:entity "test"})))
    (is (nil? (prepare/validate-val 0 {:entity "test"})))
    (is (nil? (prepare/validate-val false {:entity "test"}))))

  (testing "raises for nil"
    (is (thrown-with-msg? Exception #"Cannot store nil"
          (prepare/validate-val nil {:entity "test"})))))

(deftest test-validate-datom-unique
  (testing "passes when not unique"
    (is (= "test" (prepare/validate-datom-unique
                    false
                    (d/datom 1 :name "test")
                    (constantly nil)))))

  (testing "passes when unique but no conflict found"
    (is (= "test" (prepare/validate-datom-unique
                    true
                    (d/datom 1 :name "test")
                    (constantly nil)))))

  (testing "raises when unique and conflict found"
    (is (thrown-with-msg? Exception #"unique constraint"
          (prepare/validate-datom-unique
            true
            (d/datom 1 :name "test")
            (constantly true))))))

(deftest test-validate-upserts
  (testing "passes when upserts agree on single entity"
    (let [upserts {:email {"alice@test.com" 1}}]
      (is (= 1 (prepare/validate-upserts {:db/id -1} upserts
                                          (fn [x] (and (number? x) (neg? ^long x))))))))

  (testing "raises on conflicting upserts"
    (let [upserts {:email {"alice@test.com" 1}
                   :name  {"Bob" 2}}]
      (is (thrown-with-msg? Exception #"Conflicting upserts"
            (prepare/validate-upserts {:db/id -1} upserts
                                       (fn [x] (and (number? x) (neg? ^long x)))))))))

(deftest test-validate-type
  (testing "passes when data matches type"
    (let [dir  (u/tmp-dir (str "vtype-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [store (:store (d/db conn))]
          (is (= :db.type/string (prepare/validate-type store :name "hello"))))
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
                (prepare/validate-type store :name "not-a-long"))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-type-coercion
  (testing "correct coercion for each value type"
    (is (= "hello" (prepare/type-coercion :db.type/string "hello")))
    (is (= 42 (prepare/type-coercion :db.type/long 42)))
    (is (= 3.14 (prepare/type-coercion :db.type/double 3.14)))
    (is (= true (prepare/type-coercion :db.type/boolean true)))
    (is (= :foo (prepare/type-coercion :db.type/keyword "foo")))
    (is (= 'foo (prepare/type-coercion :db.type/symbol "foo")))
    (is (= [1 2] (prepare/type-coercion :db.type/tuple [1 2])))
    (is (instance? Date (prepare/coerce-inst 1000)))
    (is (instance? UUID (prepare/coerce-uuid "550e8400-e29b-41d4-a716-446655440000")))))

;; ---- Index utilities ----

(deftest test-index-value-type
  (testing "value-type extracts type from props"
    (is (= :db.type/string (idx/value-type {:db/valueType :db.type/string})))
    (is (= :db.type/long (idx/value-type {:db/valueType :db.type/long})))
    (is (= :data (idx/value-type {})))
    (is (= :data (idx/value-type nil)))))

(deftest test-validate-option-mutation-stub
  (testing "validate-option-mutation is a nil stub"
    (is (nil? (prepare/validate-option-mutation nil :closed-schema? true)))))
