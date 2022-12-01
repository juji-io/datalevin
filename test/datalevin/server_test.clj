(ns datalevin.server-test
  (:require [datalevin.server :as sut]
            [datalevin.client :as cl]
            [datalevin.core :as d]
            [datalevin.storage :as st]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [clojure.test :refer [is deftest testing]])
  (:import [java.nio ByteBuffer]
           [java.util UUID]
           [java.nio.channels SocketChannel]
           [java.net Socket]
           [datalevin.db DB]
           [datalevin.client Client ConnectionPool Connection]))

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
  (let [^Server server  (sut/create {:port c/default-port
                                     :root (u/tmp-dir
                                             (str "remote-basic-test-"
                                                  (UUID/randomUUID)))})
        _               (sut/start server)
        ^Client client1 (cl/new-client "dtlv://datalevin:datalevin@localhost"
                                       {:pool-size 1 :time-out 100})
        ^Client client2 (cl/new-client "dtlv://datalevin:datalevin@localhost")]
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
    (sut/stop server)))

(deftest server-restart-kv-test
  (let [dir  "dtlv://datalevin:datalevin@localhost/server-restart-test"
        root (u/tmp-dir (str "server-restart-kv-test-" (UUID/randomUUID)))]
    (let [^Server server (sut/create {:port c/default-port :root root})
          _              (sut/start server)
          lmdb           (d/open-kv dir)
          engine         (d/new-search-engine lmdb)
          ^Client client (cl/new-client "dtlv://datalevin:datalevin@localhost"
                                        {:pool-size 1 :time-out 2000})]
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
        (is (= (cl/list-databases client) ["server-restart-test"]))

        (d/transact-kv
          lmdb
          [[:put "raw" 4
            "The robber wore a red fleece jacket and a baseball cap."]])
        (d/add-doc engine 4 (d/get-value lmdb "raw" 4))

        (is (= [4] (d/search engine "robber")))

        (sut/stop server)))))

(deftest server-restart-dl-test
  (let [dir  "dtlv://datalevin:datalevin@localhost/server-restart"
        root (u/tmp-dir (str "server-restart-dl-test-" (UUID/randomUUID)))]
    (let [^Server server (sut/create {:port c/default-port :root root})
          _              (sut/start server)
          conn           (d/create-conn dir)]
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

        (is (= 2 (d/q '[:find ?i .
                        :in $ ?n
                        :where
                        [?e :name ?n]
                        [?e :id ?i]] (d/db conn) "John")))

        (sut/stop server)))))

(deftest msg-handler-error-test
  (let [dir  "dtlv://datalevin:datalevin@localhost/server-error"
        root (u/tmp-dir (str "server-error-test-" (UUID/randomUUID)))]
    (let [^Server server (sut/create {:root root})
          _              (sut/start server)
          conn           (d/create-conn dir)]
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
      (sut/stop server))))

(deftest dl-txn-test
  (let [server (sut/create {:root (u/tmp-dir (str "remote-dl-test-"
                                                  (UUID/randomUUID)))})
        _      (sut/start server)
        schema {:country/short {:db/valueType :db.type/string
                                :db/unique    :db.unique/identity}
                :country/long  {:db/valueType :db.type/string}}
        conn   (d/create-conn "dtlv://datalevin:datalevin@localhost:8898/dl-test"
                              schema)
        conn1  (d/create-conn nil schema)]

    (is (= (:max-eid @conn) (:max-eid @conn1) c/e0))
    (d/transact! conn [{:country/short "RU" :country/long "Russia"}
                       {:country/short "FR" :country/long "France"}
                       {:country/short "DE" :country/long "Germany"}])
    (d/transact! conn1 [{:country/short "RU" :country/long "Russia"}
                        {:country/short "FR" :country/long "France"}
                        {:country/short "DE" :country/long "Germany"}])
    (is (= (:max-eid @conn) (:max-eid @conn1) 3))

    (d/transact! conn [{:country/short "AZ" :country/long "Azerbaijan"}])
    (d/transact! conn1 [{:country/short "AZ" :country/long "Azerbaijan"}])
    (is (= (:max-eid @conn) (:max-eid @conn1) 4))
    (is (= 4 (d/q '[:find (count ?e) . :in $ :where [?e]] (d/db conn))))

    (d/close conn)
    (d/close conn1)
    (sut/stop server)
    ))

(deftest restart-server-test
  (let [root    (u/tmp-dir (str "remote-schema-test-" (UUID/randomUUID)))
        server1 (sut/create {:port c/default-port
                             :root root})
        _       (sut/start server1)
        client  (cl/new-client "dtlv://datalevin:datalevin@localhost"
                               {:time-out 5000})]
    (is (= (cl/list-databases client) []))
    (sut/stop server1)
    (is (thrown? Exception (cl/list-databases client)))
    (let [server2 (sut/create {:port c/default-port
                               :root root})
          _       (sut/start server2)]
      (is (= (cl/list-databases client) []))
      (sut/stop server2))))
