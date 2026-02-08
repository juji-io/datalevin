(ns datalevin.server-test
  (:require [datalevin.server :as sut]
            [datalevin.client :as cl]
            [datalevin.remote :as r]
            [taoensso.timbre :as log]
            [datalevin.core :as d]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [datalevin.test.core :as tdc :refer [db-fixture]]
            [clojure.test :refer [deftest testing is use-fixtures]])
  (:import [java.util UUID]
           [java.nio.channels SocketChannel]
           [java.net Socket]
           [datalevin.client Client ConnectionPool Connection]))

(use-fixtures :each db-fixture)

(deftest password-test
  (let [s  (sut/salt)
        s1 (sut/salt)
        p  "superDoperSecret"
        p1 "superdopersecret"
        h  (sut/password-hashing p s)
        h1 (sut/password-hashing p s1)]
    (is (sut/password-matches? p h s))
    (is (sut/password-matches? p h1 s1))
    (is (not (sut/password-matches? p1 h s)))
    (is (not (sut/password-matches? p h s1)))
    (is (not (sut/password-matches? p h1 s)))))

(deftest reset-by-peer-test
  (let [dir             (u/tmp-dir (str "reset-test-" (UUID/randomUUID)))
        ^Server server  (sut/create {:port c/default-port
                                     :root dir})
        _               (sut/start server)
        ^Client client1 (cl/new-client "dtlv://datalevin:datalevin@localhost"
                                       {:pool-size 1 :time-out 100})
        ^Client client2 (cl/new-client "dtlv://datalevin:datalevin@localhost")]
    (log/set-min-level! :report)
    (is (= (cl/list-databases client1) []))
    (is (= (cl/list-databases client2) []))

    ;; client send RST packet to server to close connection, will trigger an
    ;; exception on server, but the server should not crash
    (let [^ConnectionPool pool (cl/get-pool client1)
          ^Connection conn     (cl/get-connection pool)
          ^SocketChannel ch    (.-ch conn)
          ^Socket socket       (.socket ch)]
      (.setSoLinger socket true 0)
      (.close ch))

    (is (thrown? Exception (cl/list-databases client1)))

    ;; the other connections should work as before
    (is (= (cl/list-databases client2) []))
    (sut/stop server)
    (u/delete-files dir)))

(deftest server-restart-kv-test
  (let [dir            "dtlv://datalevin:datalevin@localhost/server-restart-test"
        root           (u/tmp-dir
                         (str "server-restart-kv-test-" (UUID/randomUUID)))
        ^Server server (sut/create {:port c/default-port :root root})
        _              (sut/start server)
        lmdb           (d/open-kv dir)
        engine         (d/new-search-engine lmdb)
        ^Client client (cl/new-client "dtlv://datalevin:datalevin@localhost"
                                      {:pool-size 1 :time-out 2000})]
    (log/set-min-level! :report)
    (is (= (cl/list-databases client) ["server-restart-test"]))

    (d/open-dbi lmdb "raw")
    (d/transact-kv
      lmdb
      [[:put "raw" 1 "The quick red fox jumped over the lazy red dogs."]
       [:put "raw" 2 "Mary had a little lamb whose fleece was red as fire."]
       [:put "raw" 3 "Moby Dick is a story of a whale and a man obsessed."]])
    (doseq [i [1 2 3]]
      (d/add-doc engine i (d/get-value lmdb "raw" i)))
    (is (= [3] (d/search engine "moby")))
    (sut/stop server)

    ;; existing clients should work with restarted server normally
    (let [^Server server (sut/create {:port c/default-port :root root})
          _              (sut/start server)]


      (log/set-min-level! :report)
      (is (= (cl/list-databases client) ["server-restart-test"]))

      (d/transact-kv
        lmdb
        [[:put "raw" 4
          "The robber wore a red fleece jacket and a baseball cap."]])
      (d/add-doc engine 4 (d/get-value lmdb "raw" 4))

      (is (= [4] (d/search engine "robber")))

      (sut/stop server))
    (u/delete-files root)))

(deftest server-restart-dl-test
  (let [dir            "dtlv://datalevin:datalevin@localhost/server-restart"
        root           (u/tmp-dir (str "server-restart-dl-test-"
                                       (UUID/randomUUID)))
        ^Server server (sut/create {:port c/default-port :root root})
        _              (sut/start server)
        conn           (d/create-conn dir)]

    (log/set-min-level! :report)
    (d/transact! conn [{:name "John" :id 2}
                       {:name "Matt" :id 3}])
    (is (= 2 (d/q '[:find ?i .
                    :in $ ?n
                    :where
                    [?e :name ?n]
                    [?e :id ?i]] (d/db conn) "John")))
    (sut/stop server)

    ;; existing clients should work with restarted server normally
    (let [^Server server (sut/create {:port c/default-port :root root})
          _              (sut/start server)]

      (log/set-min-level! :report)
      (is (= 2 (d/q '[:find ?i .
                      :in $ ?n
                      :where
                      [?e :name ?n]
                      [?e :id ?i]] (d/db conn) "John")))

      (sut/stop server))
    (u/delete-files root)))

(deftest msg-handler-error-test
  (let [dir            "dtlv://datalevin:datalevin@localhost/server-error"
        root           (u/tmp-dir (str "server-error-test-"
                                       (UUID/randomUUID)))
        ^Server server (sut/create {:root root})
        _              (sut/start server)
        conn           (d/create-conn dir)]

    (log/set-min-level! :report)
    (d/transact! conn [{:name "John" :id 2}
                       {:name "Matt" :id 3}])
    (is (thrown? Exception
                 (d/q '[:find ?i ?j
                        :in $ ?n
                        :where
                        [(/ 3 0) ?j]
                        [?e :name ?n]
                        [?e :id ?i]] (d/db conn) "John")))

    ;; server still respond normally
    (is (= 2 (d/q '[:find ?i .
                    :in $ ?n
                    :where
                    [?e :name ?n]
                    [?e :id ?i]] (d/db conn) "John")))
    (sut/stop server)
    (u/delete-files root)))

(deftest restart-server-test
  (let [root    (u/tmp-dir (str "restart-server-test-" (UUID/randomUUID)))
        server1 (sut/create {:port c/default-port
                             :root root})
        _       (sut/start server1)
        client  (cl/new-client "dtlv://datalevin:datalevin@localhost"
                               {:time-out 5000})]

    (log/set-min-level! :report)
    (is (= (cl/list-databases client) []))
    (sut/stop server1)
    (is (thrown? Exception (cl/list-databases client)))
    (let [server2 (sut/create {:port c/default-port
                               :root root})
          _       (sut/start server2)]

      (log/set-min-level! :report)
      (is (= (cl/list-databases client) []))
      (sut/stop server2))
    (u/delete-files root)))

(deftest idle-timeout-test
  (let [root    (u/tmp-dir (str "idle-timeout-test-" (UUID/randomUUID)))
        server  (sut/create {:port         c/default-port
                             :root         root
                             :idle-timeout 100})
        _       (sut/start server)
        client1 (cl/new-client "dtlv://datalevin:datalevin@localhost")
        client2 (cl/new-client "dtlv://datalevin:datalevin@localhost")
        db      (r/open-kv
                  client2
                  "dtlv://datalevin:datalevin@localhost/db?store=kv"
                  {})]
    ;; (log/set-min-level! :report)
    (d/open-dbi db "a")
    (d/transact-kv db [[:put "a" 1 1]])
    (Thread/sleep 1000)
    (is (= 1 (count (cl/show-clients client1))))
    (is (= 1 (d/get-value db "a" 1)))
    (sut/stop server)
    (u/delete-files root)))

(deftest default-password-env-test
  (let [root (u/tmp-dir (str "env-password-test-" (UUID/randomUUID)))]
    (try
      (testing "uses custom password when provided"
        (let [sys-conn (#'sut/init-sys-db root "my-secret-pw")
              user     (d/q '[:find (pull ?e [:user/pw-hash :user/pw-salt]) .
                              :where [?e :user/name "datalevin"]]
                            (d/db sys-conn))]
          (is (sut/password-matches? "my-secret-pw"
                                     (:user/pw-hash user)
                                     (:user/pw-salt user)))
          (is (not (sut/password-matches? "datalevin"
                                          (:user/pw-hash user)
                                          (:user/pw-salt user))))
          (d/close sys-conn)))
      (finally
        (u/delete-files root)))))

(deftest default-password-fallback-test
  (let [root (u/tmp-dir (str "default-password-test-" (UUID/randomUUID)))]
    (try
      (testing "falls back to default password"
        (let [sys-conn (#'sut/init-sys-db root c/default-password)
              user     (d/q '[:find (pull ?e [:user/pw-hash :user/pw-salt]) .
                              :where [?e :user/name "datalevin"]]
                            (d/db sys-conn))]
          (is (sut/password-matches? c/default-password
                                     (:user/pw-hash user)
                                     (:user/pw-salt user)))
          (d/close sys-conn)))
      (finally
        (u/delete-files root)))))
