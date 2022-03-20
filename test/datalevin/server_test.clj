(ns datalevin.server-test
  (:require [datalevin.server :as sut]
            [datalevin.client :as cl]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [clojure.test :refer [is deftest testing]])
  (:import [java.nio ByteBuffer]
           [java.util UUID]
           [java.nio.channels SocketChannel]
           [java.net Socket]
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
