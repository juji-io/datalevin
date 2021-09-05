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
    (is (= (sut/parse-user-info uri)
           {:username "datalevin" :password "datalevin"}))
    (is (= (sut/parse-port uri) c/default-port))
    (is (= (sut/parse-db uri) "clientdb" ))

    (let [client (sut/new-client uri-str)]
      (is (= (sut/list-users client) ["datalevin"]))
      (is (= (sut/list-databases client) ["clientdb"]))
      (is (= (sut/list-roles client) [:datalevin.role/datalevin]))
      (is (= (count (sut/show-clients client)) 1))
      (is (thrown? Exception (sut/assign-role client "noone" :nothing)))

      (sut/create-user client "juji" "secret")

      (let [client1 (sut/new-client "dtlv://juji:secret@localhost")]
        (is (= (count (sut/show-clients client)) 2))
        (is (= (set (sut/list-users client)) #{"datalevin" "juji"}))
        (is (= (set (sut/list-roles client))
               #{:datalevin.role/datalevin :datalevin.role/juji}))

        (is (thrown? Exception (sut/list-users client1)))
        (is (thrown? Exception (sut/list-databases client1)))
        (is (thrown? Exception (sut/list-roles client1)))
        (is (thrown? Exception (sut/create-database client1 "hr" c/dl-type)))
        (is (thrown? Exception (sut/create-user client1 "hr-admin" "secret")))
        (sut/reset-password client1 "juji" "new-secret")
        (sut/disconnect client1))

      (sut/create-role client :juji/admin)
      (sut/assign-role client :juji/admin "juji")
      (sut/grant-permission client :juji/admin
                            :datalevin.server/create
                            :datalevin.server/database
                            nil)

      (let [client2 (sut/new-client "dtlv://juji:new-secret@localhost")]
        (sut/create-database client2 "hr" c/dl-type)
        )

      )))
