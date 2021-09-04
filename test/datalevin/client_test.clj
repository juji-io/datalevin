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
        uri     (URI. uri-str)]
    (is (= {:username "datalevin" :password "datalevin"}
           (sut/parse-user-info uri)))
    (is (= c/default-port (sut/parse-port uri)))
    (is (= "clientdb" (sut/parse-db uri)))

    (let [client (sut/new-client uri-str)]
      (is (= ["datalevin"] (sut/list-users client)))
      (is (= ["clientdb"] (sut/list-databases client)))
      (is (= [:datalevin.role/datalevin] (sut/list-roles client)))
      (is (thrown? Exception (sut/assign-role client "noone" :nothing)))

      (sut/create-user client "juji" "secret")
      (is (instance? UUID (sut/authenticate "localhost" c/default-port
                                            "juji" "secret")))
      (is (= #{"datalevin" "juji"} (set (sut/list-users client))))
      (is (= #{:datalevin.role/datalevin :datalevin.role/juji}
             (set (sut/list-roles client))))

      (let [client1 (sut/new-client "dtlv://juji:secret@localhost")]
        (is (thrown? Exception (sut/list-users client1)))
        (is (thrown? Exception (sut/list-databases client1)))
        (is (thrown? Exception (sut/list-roles client1)))
        (is (thrown? Exception (sut/create-database client1 "hr" c/dl-type)))
        (is (thrown? Exception (sut/create-user client1 "hr-admin" "secret")))
        (sut/reset-password client1 "juji" "new-secret")
        (sut/disconnect client1))

      (sut/create-role client :juji/admin)
      (sut/assign-role client "juji" :juji/admin )
      (sut/grant-permission client :juji/admin
                            :datalevin.server/create
                            :datalevin.server/database
                            nil)

      (let [client2 (sut/new-client "dtlv://juji:new-secret@localhost")]
        (sut/create-database client2 "hr" c/dl-type)
        )

      )))
