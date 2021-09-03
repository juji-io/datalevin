(ns datalevin.client-test
  (:require [datalevin.client :as sut]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [datalevin.test.core :refer [server-fixture]]
            [clojure.test :refer [is testing deftest use-fixtures]])
  (:import [java.util UUID Arrays]
           [java.net URI]))

(use-fixtures :each server-fixture)

(deftest basic-ops-test
  (let [uri-str "dtlv://datalevin:datalevin@localhost/clientdb"
        uri     (URI. uri-str)
        client  (sut/new-client uri-str)]
    (is (= {:username "datalevin" :password "datalevin"}
           (sut/parse-user-info uri)))
    (is (= c/default-port (sut/parse-port uri)))
    (is (= "clientdb" (sut/parse-db uri)))
    (sut/create-user client "juji" "secret")
    (is (instance? UUID (sut/authenticate "localhost" c/default-port
                                          "juji" "secret")))
    (let [client1 (sut/new-client "dtlv://juji:secret@localhost")]
      (is (thrown? Exception (sut/create-database client1 "hr" c/dl-type)))
      (sut/create-role client :juji/admin)
      (sut/assign-role client "juji" :juji/admin )
      (sut/assign-permission client :juji/admin
                             :datalevin.server/create
                             :datalevin.server/database
                             nil)
      (sut/create-database client1 "hr" c/dl-type)
      )))
