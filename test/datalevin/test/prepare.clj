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

(deftest test-validate-prepared-datoms
  (testing "prepare validator enforces closed schema for added datoms"
    (let [schema {:name {:db/aid 1}}
          opts   {:closed-schema? true}
          datoms [(d/datom 1 :name "Alice")
                  (d/datom 1 :age 42)]]
      (is (thrown-with-msg? Exception #"closed-schema"
                            (prepare/validate-prepared-datoms
                              schema opts datoms)))))

  (testing "prepare validator allows datoms when closed schema is disabled"
    (let [schema {:name {:db/aid 1}}
          opts   {:closed-schema? false}
          datoms [(d/datom 1 :name "Alice")
                  (d/datom 1 :age 42)]]
      (is (= datoms (prepare/validate-prepared-datoms schema opts datoms))))))

(deftest test-validate-load-datoms
  (testing "load-datoms validator rejects non-datom payloads"
    (let [dir  (u/tmp-dir (str "prepare-load-datoms-shape-" (UUID/randomUUID)))
          conn (d/create-conn dir nil
                              {:kv-opts {:flags (conj c/default-env-flags
                                                      :nosync)}})]
      (try
        (let [store (:store (d/db conn))]
          (is (thrown-with-msg? Exception #"expects list of Datoms"
                                (prepare/validate-load-datoms
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
                                (prepare/validate-load-datoms
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
             (prepare/validate-side-index-domains schema datoms available))))

    (testing "missing fulltext domain fails before apply"
      (is (thrown-with-msg? Exception #"Fulltext domain is not initialized"
                            (prepare/validate-side-index-domains
                              schema
                              [(d/datom 1 :ft/title "hello")]
                              (assoc available :fulltext #{"posts"})))))

    (testing "missing vector domain fails before apply"
      (is (thrown-with-msg? Exception #"Vector domain is not initialized"
                            (prepare/validate-side-index-domains
                              schema
                              [(d/datom 1 :vec/emb [0.1 0.2])]
                              (assoc available :vector #{"vectors"})))))

    (testing "missing idoc domain fails before apply"
      (is (thrown-with-msg? Exception #"Idoc domain is not initialized"
                            (prepare/validate-side-index-domains
                              schema
                              [(d/datom 1 :doc/body {:status "ok"})]
                              (assoc available :idoc #{})))))))

(deftest test-validate-schema-side-index-domains
  (let [schema-update {:vec/emb {:db/valueType :db.type/vec}}
        available     {:fulltext #{}
                       :vector   #{"vec_emb"}
                       :idoc     #{}}]
    (testing "schema side-index domains pass when initialized"
      (is (nil? (prepare/validate-schema-side-index-domains
                  schema-update available))))
    (testing "schema side-index domains fail when missing"
      (is (thrown-with-msg? Exception #"Vector domain is not initialized"
                            (prepare/validate-schema-side-index-domains
                              schema-update
                              (assoc available :vector #{})))))))

(deftest test-validate-schema-update
  (testing "prepare validator rejects malformed schema updates"
    (is (thrown-with-msg? Exception #"Bad attribute specification"
                          (prepare/validate-schema-update
                            nil nil {:bad/attr {:db/valueType :db.type/bogus}}))))

  (testing "nil schema update is a no-op"
    (is (nil? (prepare/validate-schema-update nil nil nil)))))

(deftest test-validate-option-update
  (testing "valid option value passes through"
    (is (= [:cache-limit 0]
           (prepare/validate-option-update :cache-limit 0))))

  (testing "invalid option value raises"
    (is (thrown-with-msg? Exception #"cache-limit expects a non-negative integer"
                          (prepare/validate-option-update :cache-limit -1)))))

(deftest test-validate-swap-attr-update
  (testing "aid-only attr updates pass through prepare swap validator"
    (is (= {:db/aid 3}
           (prepare/validate-swap-attr-update nil nil :name
                                              {:db/aid 3}
                                              {:db/aid 3})))))

(deftest test-validate-swap-attr-update-side-index-domain
  (testing "swap-attr side-index domains are validated"
    (is (thrown-with-msg? Exception #"Fulltext domain is not initialized"
                          (prepare/validate-swap-attr-update
                            nil nil :ft/title {}
                            {:db/fulltext            true
                             :db.fulltext/autoDomain true}
                            {:fulltext #{}
                             :vector   #{}
                             :idoc     #{}})))))

(deftest test-validate-del-attr
  (testing "del-attr validation passes when attribute has no datoms"
    (with-redefs [datalevin.interface/populated? (fn [& _] false)]
      (is (= :a/b (prepare/validate-del-attr nil :a/b)))))

  (testing "del-attr validation fails when attribute has datoms"
    (with-redefs [datalevin.interface/populated? (fn [& _] true)]
      (is (thrown-with-msg? Exception #"Cannot delete attribute with datoms"
                            (prepare/validate-del-attr nil :a/b))))))

(deftest test-validate-rename-attr
  (testing "rename validation passes for existing source and new target"
    (with-redefs [datalevin.interface/schema (fn [_] {:a/b {:db/aid 3}})]
      (is (= [:a/b :e/f]
             (prepare/validate-rename-attr nil :a/b :e/f)))))

  (testing "rename validation rejects missing source attribute"
    (with-redefs [datalevin.interface/schema (fn [_] {:a/b {:db/aid 3}})]
      (is (thrown-with-msg? Exception #"Cannot rename missing attribute"
                            (prepare/validate-rename-attr nil :x/y :e/f)))))

  (testing "rename validation rejects existing target attribute"
    (with-redefs [datalevin.interface/schema
                  (fn [_] {:a/b {:db/aid 3}
                           :e/f {:db/aid 4}})]
      (is (thrown-with-msg? Exception #"Cannot rename to existing attribute"
                            (prepare/validate-rename-attr nil :a/b :e/f))))))

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

(deftest test-closed-schema-enforced-in-both-paths
  (doseq [use-prepare? [false true]]
    (testing (str "closed schema check with *use-prepare-path*=" use-prepare?)
      (let [dir  (u/tmp-dir (str "closed-schema-" use-prepare? "-"
                                 (UUID/randomUUID)))
            conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                                {:closed-schema? true
                                 :kv-opts {:flags (conj c/default-env-flags
                                                        :nosync)}})]
        (try
          (binding [c/*use-prepare-path* use-prepare?]
            (is (thrown-with-msg? Exception #"closed-schema"
                                  (d/transact! conn [{:age 10}]))))
          (finally
            (d/close conn)
            (u/delete-files dir)))))))

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
