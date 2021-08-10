(ns datalevin.client-test
  (:require [datalevin.client :as sut]
            [clojure.test :refer [is deftest]]
            [datalevin.constants :as c])
  (:import [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer BufferOverflowException]
           [java.nio.channels SocketChannel]
           [java.util UUID]
           [java.net  URI]))

(deftest uri-test
  (let [uri (URI. "dtlv://juji:nice!1@juji.io/mydb")]
    (is (= {:username "juji" :password "nice!1"} (sut/parse-user-info uri)))
    (is (= c/default-port (sut/parse-port uri)))
    (is (= "mydb" (sut/parse-db uri)))))
