(ns datalevin.client-test
  (:require [datalevin.client :as sut]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [datalevin.test.core :refer [server-fixture]]
            [clojure.test :refer [is testing deftest use-fixtures]])
  (:import [java.util UUID Arrays]
           [java.net URI]
           [datalevin.client Client]))

(use-fixtures :each server-fixture)

(deftest basic-ops-test
  (let [uri-str "dtlv://datalevin:datalevin@localhost/clientdb"
        uri     (URI. uri-str)]
    (testing "uri parsing"
      (is (= (sut/parse-user-info uri)
             {:username "datalevin" :password "datalevin"}))
      (is (= (sut/parse-port uri) c/default-port))
      (is (= (sut/parse-db uri) "clientdb" )))

    (let [client (sut/new-client uri-str)]
      (testing "fresh server after the superuser connecting to a db"
        (is (= (sut/list-users client) ["datalevin"]))
        (is (= (sut/list-databases client)
               (sut/list-databases-in-use client)
               ["clientdb"]))
        (is (= (sut/list-roles client) [:datalevin.role/datalevin]))
        (is (= (count (sut/show-clients client)) 1))
        (is (thrown? Exception (sut/assign-role client "no-one" :nothing))))

      (testing "create user"
        (sut/create-user client "juji" "secret")
        (is (= (set (sut/list-users client)) #{"datalevin" "juji"}))
        (is (= (set (sut/list-roles client))
               #{:datalevin.role/datalevin :datalevin.role/juji})))

      (testing "new user's default permission"
        (let [client1 (sut/new-client "dtlv://juji:secret@localhost")]
          (is (= (count (sut/show-clients client)) 2))

          (is (thrown? Exception (sut/list-users client1)))
          (is (thrown? Exception (sut/list-databases client1)))
          (is (thrown? Exception (sut/list-roles client1)))
          (is (thrown? Exception (sut/create-database client1 "hr" c/dl-type)))
          (is (thrown? Exception (sut/create-user client1 "hr-admin" "secret")))
          (sut/reset-password client1 "juji" "new-secret")
          (sut/disconnect client1)))

      (testing "give user new role and permission"
        (sut/create-role client :juji/admin)
        (sut/assign-role client :juji/admin "juji")
        (sut/grant-permission client :juji/admin
                              :datalevin.server/create
                              :datalevin.server/database
                              nil)
        (is (= (set (sut/list-roles client))
               #{:datalevin.role/datalevin :datalevin.role/juji :juji/admin}))
        (is (= (count (sut/list-role-permissions client :juji/admin)) 1))
        (is (= (count (sut/list-user-roles client "juji")) 2))
        (is (= (count (sut/list-user-permissions client "juji")) 2))
        )

      (testing "a user created a db, then is forcibly disconnected by superuser"
        (let [client2 (sut/new-client "dtlv://juji:new-secret@localhost")]
          (is (= (count (sut/show-clients client)) 2))

          (is (= (count (sut/list-user-roles client2 "juji")) 2))

          (sut/create-database client2 "hr" c/dl-type)
          (is (= (count (sut/list-databases client)) 2))
          (is (= (count (sut/list-user-permissions client "juji")) 3))

          (is (thrown? Exception (sut/list-databases client2)))

          (let [client-id2 (.-id ^Client client2)]
            (sut/disconnect-client client client-id2)

            (is (= (count (sut/show-clients client)) 1))
            (is (thrown? Exception (sut/list-user-roles client2 "juji"))))))

      (testing "db is forcibly closed by superuser, then dropped"
        (let [client3 (sut/new-client "dtlv://juji:new-secret@localhost/hr")]
          (is (= (count (sut/list-databases-in-use client)) 2))
          (is (thrown? Exception (sut/drop-database client "hr")))

          (sut/close-database client "hr")
          (is (= (count (sut/list-databases-in-use client)) 1))
          (is (= (count (sut/show-clients client)) 1))
          (is (thrown? Exception (sut/list-user-roles client3 "juji")))

          (sut/drop-database client "hr")
          (is (= (count (sut/list-databases client)) 1))
          (is (= (count (sut/list-user-permissions client "juji")) 2))
          ))

      )))
