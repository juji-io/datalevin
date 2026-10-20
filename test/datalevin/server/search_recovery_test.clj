(ns datalevin.server.search-recovery-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.client :as client]
   [datalevin.core :as d]
   [datalevin.search-utils :as su]
   [datalevin.server :as server]
   [datalevin.test.core :refer [allocate-port db-fixture]]
   [datalevin.util :as u])
  (:import
   [datalevin.remote KVStore]
   [datalevin.server Server]))

(use-fixtures :once db-fixture)

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
