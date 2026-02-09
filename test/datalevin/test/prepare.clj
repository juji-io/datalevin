(ns datalevin.test.prepare
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [datalevin.prepare :as prepare]
   [datalevin.index :as idx]
   [datalevin.util :as u])
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
    (let [ptx (prepare/->PreparedTx [] nil {} [] [] {} #{} nil)]
      (is (instance? datalevin.prepare.PreparedTx ptx))
      (is (= [] (:tx-data ptx)))
      (is (nil? (:db-after ptx)))
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

(deftest test-prepare-tx-returns-prepared-tx
  (testing "prepare-tx calls execute-fn and returns PreparedTx"
    (let [dir  (u/tmp-dir (str "prepare-tx-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [db   (d/db conn)
              lmdb (.-lmdb ^Store (:store db))
              ctx  (prepare/make-prepare-ctx db lmdb)
              fake-report {:tx-data [{:fake "datom"}]
                           :db-after db
                           :tempids {-1 1}
                           :new-attributes [:name]}
              ptx  (prepare/prepare-tx ctx [{:name "test"}] 1
                     (fn [es t] fake-report))]
          (is (instance? datalevin.prepare.PreparedTx ptx))
          (is (= [{:fake "datom"}] (:tx-data ptx)))
          (is (= db (:db-after ptx)))
          (is (= {-1 1} (:tempids ptx)))
          (is (= [:name] (:new-attributes ptx)))
          (is (nil? (:tx-redundant ptx)))
          (is (nil? (:side-index-ops ptx)))
          (is (nil? (:touch-summary ptx)))
          (is (nil? (:stats ptx))))
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
  (testing "transact! succeeds through prepare path"
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

(deftest test-prepare-path-differential
  (testing "prepare path and legacy path produce identical results"
    (let [dir-legacy  (u/tmp-dir (str "diff-legacy-" (UUID/randomUUID)))
          dir-prepare (u/tmp-dir (str "diff-prepare-" (UUID/randomUUID)))
          schema      {:name  {:db/valueType :db.type/string
                               :db/unique    :db.unique/identity}
                       :age   {:db/valueType :db.type/long}
                       :alias {:db/valueType   :db.type/string
                               :db/cardinality :db.cardinality/many}}
          opts        {:kv-opts {:flags (conj c/default-env-flags :nosync)}}
          conn-l      (d/create-conn dir-legacy schema opts)
          conn-p      (d/create-conn dir-prepare schema opts)
          tx-data     [{:name "Alice" :age 30 :alias ["A" "Ali"]}
                       {:name "Bob" :age 25}
                       [:db/add -1 :name "Carol"]
                       [:db/add -1 :age 28]]]
      (try
        (let [report-l (binding [c/*use-prepare-path* false]
                         (d/transact! conn-l tx-data))
              report-p (binding [c/*use-prepare-path* true]
                         (d/transact! conn-p tx-data))]
          ;; tx-data should have same count and same datoms (by a/v, ignoring tx)
          (is (= (count (:tx-data report-l))
                 (count (:tx-data report-p))))
          ;; tempids should map the same logical tempids
          (is (= (dissoc (:tempids report-l) :db/current-tx)
                 (dissoc (:tempids report-p) :db/current-tx)))
          ;; new-attributes should be identical
          (is (= (:new-attributes report-l)
                 (:new-attributes report-p)))
          ;; query results should match
          (is (= (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                      (d/db conn-l))
                 (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                      (d/db conn-p)))))
        (finally
          (d/close conn-l)
          (d/close conn-p)
          (u/delete-files dir-legacy)
          (u/delete-files dir-prepare))))))


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

