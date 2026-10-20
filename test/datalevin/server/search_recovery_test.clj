(ns datalevin.server.search-recovery-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.client :as client]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.interface :as i]
   [datalevin.search :as sc]
   [datalevin.search-utils :as su]
   [datalevin.server :as server]
   [datalevin.server.api :as api]
   [datalevin.test.core :refer [allocate-port db-fixture]]
   [datalevin.util :as u])
  (:import
   [datalevin.remote KVStore]
   [datalevin.server Server]
   [java.util.concurrent CountDownLatch TimeUnit]))

(use-fixtures :once db-fixture)

(deftest search-open-reuses-live-engine-across-sessions
  (let [root (u/tmp-dir (str "shared-search-open-" (random-uuid)))
        port (allocate-port)
        ^Server srv (server/create {:root root :port port})
        db-name "shared-search"
        uri (str "dtlv://datalevin:datalevin@localhost:" port "/" db-name)
        opts {:domain "named-search" :include-text? true
              :analyzer (su/create-analyzer
                          {:tokenizer (su/create-regexp-tokenizer #"\s+")
                           :token-filters [su/lower-case-token-filter]})}]
    (try
      (server/start srv)
      (let [kv1 (d/open-kv uri)
            kv2 (d/open-kv uri)]
        (try
          (let [engine1 (d/new-search-engine kv1 opts)
                live (get-in (.-dbs srv) [db-name :engine])]
            (d/add-doc engine1 :one "red fox")
            (let [engine2 (d/new-search-engine kv2 opts)]
              (is (identical? live (get-in (.-dbs srv) [db-name :engine])))
              (is (= 1 (d/doc-count engine2)))
              (d/add-doc engine2 :two "blue bird")
              (is (= 2 (d/doc-count engine1) (d/doc-count engine2)))
              (is (= [[:one "red fox"]]
                     (d/search engine2 "fox" {:display :texts})))
              (is (= [:two] (d/search engine1 "bird")))))
          (finally
            (d/close-kv kv2)
            (d/close-kv kv1))))
      (finally
        (server/stop srv)
        (u/delete-files root)))))

(deftest search-open-reuses-equivalent-default-options
  (let [root (u/tmp-dir (str "default-search-open-" (random-uuid)))
        port (allocate-port)
        ^Server srv (server/create {:root root :port port})
        uri (str "dtlv://datalevin:datalevin@localhost:" port "/default-search")]
    (try
      (server/start srv)
      (let [kv (d/open-kv uri)]
        (try
          (let [engine (d/new-search-engine kv)
                live (get-in (.-dbs srv) ["default-search" :engine])]
            (d/add-doc engine :one "red fox")
            (doseq [opts [nil {} {:domain c/default-domain
                                 :query-analyzer nil
                                 :index-position? false
                                 :include-text? false
                                 :search-opts {}}]]
              (is (= [:one] (d/search (d/new-search-engine kv opts) "fox")))
              (is (identical? live (get-in (.-dbs srv)
                                          ["default-search" :engine])))))
          (finally (d/close-kv kv))))
      (finally
        (server/stop srv)
        (u/delete-files root)))))

(deftest search-open-rejects-conflicting-options-across-sessions
  (let [root (u/tmp-dir (str "conflicting-search-open-" (random-uuid)))
        port (allocate-port)
        ^Server srv (server/create {:root root :port port})
        db-name "conflicting-search"
        uri (str "dtlv://datalevin:datalevin@localhost:" port "/" db-name)
        opts {:domain "named-search" :include-text? true
              :index-position? true :search-opts {:top 5}}]
    (try
      (server/start srv)
      (let [kv1 (d/open-kv uri)
            ^KVStore kv2 (d/open-kv uri)]
        (try
          (let [engine (d/new-search-engine kv1 opts)
                live (get-in (.-dbs srv) [db-name :engine])
                engine-opts (get-in (.-dbs srv) [db-name :engine-opts])
                dbis (set (d/list-dbis kv1))
                client-id (client/get-id (.-client kv2))]
            (d/add-doc engine :one "red fox")
            (doseq [[k value] [[:domain "other-search"]
                              [:include-text? false]
                              [:index-position? false]
                              [:search-opts {:top 10}]
                              [:analyzer (su/create-analyzer {})]
                              [:query-analyzer (su/create-analyzer {})]]]
              (testing (name k)
                (let [error (try
                              (d/new-search-engine kv2 (assoc opts k value))
                              nil
                              (catch clojure.lang.ExceptionInfo e e))
                      data (:err-data (ex-data error))]
                  (is (some? error))
                  (is (= :search/options-mismatch (:error data)))
                  (is (= db-name (:db-name data)))
                  (is (= "named-search" (:existing-domain data)))
                  (is (= (if (= k :domain) value "named-search")
                         (:requested-domain data)))
                  (is (contains? (:conflicting-options data) k)))
                (is (identical? live (get-in (.-dbs srv) [db-name :engine])))
                (is (= engine-opts (get-in (.-dbs srv) [db-name :engine-opts])))
                (is (= dbis (set (d/list-dbis kv1))))
                (is (not (contains? (:engines (get (.-clients srv) client-id))
                                    db-name)))))
            (let [shared (d/new-search-engine kv2 opts)]
              (d/add-doc shared :two "blue bird")
              (is (= 2 (d/doc-count engine) (d/doc-count shared)))
              (is (= [[:one "red fox"]]
                     (d/search shared {:phrase "red fox"} {:display :texts})))
              (is (= [:two] (d/search engine "bird"))))
            (let [new-opts (assoc opts :index-position? false)]
              (d/re-index engine new-opts)
              (is (= [:one] (d/search (d/new-search-engine kv2 new-opts) "fox")))
              (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                    #"Search engine options conflict"
                                    (d/new-search-engine kv2 opts)))))
          (finally
            (d/close-kv kv2)
            (d/close-kv kv1))))
      (finally
        (server/stop srv)
        (u/delete-files root)))))

(deftest concurrent-search-opens-preserve-writes-and-document-ids
  (let [root (u/tmp-dir (str "concurrent-search-open-" (random-uuid)))
        kv (d/open-kv root)
        opts {:domain "named-search" :include-text? true}
        state (atom {:store kv})
        clients (atom {:one {:engines #{}} :two {:engines #{}}})
        both-open (CountDownLatch. 2)
        initializing (CountDownLatch. 1)
        published (CountDownLatch. 1)
        release (CountDownLatch. 1)
        publications (atom 0)
        deps {:get-store (fn [_ _] (.countDown both-open) kv)
              :db-state (fn [_ _] @state)
              :update-db
              (fn [_ _ f]
                (let [engine (:engine (swap! state f))]
                  (when (= 1 (swap! publications inc))
                    (d/add-doc engine :one "red fox")
                    (.countDown published))))
              :update-client (fn [_ client-id f] (swap! clients update client-id f))
              :write-message (fn [_ message] message)
              :with-index-write-admission
              (fn [_ _ f]
                ;; Hold an empty engine before publication. An uncoordinated
                ;; second open can reload it, publish, and accept a write that
                ;; the first engine's stale state would subsequently replace.
                (let [engine (f)]
                  (.countDown initializing)
                  (when-not (.await release 5 TimeUnit/SECONDS)
                    (throw (ex-info "Timed out waiting to publish engine" {})))
                  engine))}
        open! (fn [client-id]
                (api/new-search-engine deps nil nil client-id
                                       {:args ["db" opts]}))
        first-open (future (open! :one))]
    (try
      (is (.await initializing 5 TimeUnit/SECONDS))
      (let [second-open (future (open! :two))]
        (try
          (is (.await both-open 5 TimeUnit/SECONDS))
          ;; Give the second open a chance to publish while initialization is
          ;; paused. Correct opens wait for the first engine to be published.
          (.await published 200 TimeUnit/MILLISECONDS)
          (.countDown release)
          (is (= {:type :command-complete} (deref first-open 5000 ::timeout)))
          (is (= {:type :command-complete} (deref second-open 5000 ::timeout)))
          (let [engine (:engine @state)]
            (is (= 1 @publications))
            (is (= 1 (d/doc-count engine)))
            (is (= [:one] (d/search engine "fox")))
            (d/add-doc engine :two "blue bird")
            (let [doc-ids (mapv #(first (i/get-value kv "named-search/docs" %
                                                      :data :doc-info))
                                [:one :two])]
              (is (every? pos-int? doc-ids))
              (is (= 2 (count (set doc-ids)))))
            (is (= 2 (d/doc-count engine)))
            (is (= 2 (d/doc-count (sc/open-search-engine kv opts)))))
          (is (= {:one {:engines #{"db"}} :two {:engines #{"db"}}} @clients))
          (finally (future-cancel second-open))))
      (finally
        (.countDown release)
        (future-cancel first-open)
        (d/close-kv kv)
        (u/delete-files root)))))

(deftest live-search-engine-retains-options-after-server-restart
  (let [root (u/tmp-dir (str "search-restart-recovery-" (random-uuid)))
        port (allocate-port)
        server-opts {:root root :port port}
        srv (atom (server/create server-opts))
        kv (atom nil)
        db-name "search-restart"
        uri (str "dtlv://datalevin:datalevin@localhost:" port "/" db-name)]
    (try
      (server/start @srv)
      (let [^KVStore store (d/open-kv uri {:client-opts {:time-out 5000}})
            _ (reset! kv store)
            c (.-client store)
            client-id (client/get-id c)
            engine (d/new-search-engine
                     store {:domain "named-search"
                            :index-position? true
                            :include-text? true
                            :analyzer
                            (su/create-analyzer
                              {:token-filters
                               [(su/create-stemming-token-filter "english")]})})]
        (d/add-doc engine :original "red fox horses running")
        (let [dbis (set (d/list-dbis store))]
          (doseq [phase [:before-restart :after-restart]]
            (testing (name phase)
              (when (= phase :after-restart)
                ;; Keep the store, engine, and client session open. Closing the
                ;; store first would remove it from startup session recovery.
                (server/stop @srv)
                (let [^Server restored (server/create server-opts)]
                  (reset! srv restored)
                  (is (contains? (:engines (get (.-clients restored) client-id))
                                 db-name))
                  (is (nil? (get-in (.-dbs restored) [db-name :engine])))
                  (server/start restored)))
              (is (= 1 (d/doc-count engine)))
              (is (d/doc-indexed? engine :original))
              (is (= [:original] (d/search engine "horse")))
              (is (= [:original] (d/search engine {:phrase "red fox"})))
              (is (= [[:original "red fox horses running"]]
                     (d/search engine "horse" {:display :texts})))
              (is (= dbis (set (d/list-dbis store)))
                  "recovery must not create a default-domain search index")
              (is (= client-id (client/get-id c))))))
        (d/add-doc engine :added "blue bird foxes jumping")
        (is (= 2 (d/doc-count engine)))
        (is (= [:added] (d/search engine {:phrase "blue bird"})))
        (is (= [[:added "blue bird foxes jumping"]]
               (d/search engine "jump" {:display :texts}))))
      (finally
        (try
          (when @kv (d/close-kv @kv))
          (finally
            (server/stop @srv)
            (u/delete-files root)))))))
