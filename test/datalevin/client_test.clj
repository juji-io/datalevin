(ns datalevin.client-test
  (:require [datalevin.client :as sut]
            [datalevin.constants :as c]
            [datalevin.test.core :refer [server-fixture]]
            [clojure.test :refer [is testing deftest use-fixtures]])
  (:import [java.net URI]
           [datalevin.client Client]))

(use-fixtures :each server-fixture)

(deftest basic-ops-test
  (let [uri-str  "dtlv://datalevin:datalevin@localhost"
        uri      (URI. uri-str)
        uri-str1 "dtlv://MyName:d%40t%21@localhost"
        uri1     (URI. uri-str1)]
    (testing "uri parsing"
      (is (= (sut/parse-user-info uri)
             {:username "datalevin" :password "datalevin"}))
      (is (= (sut/parse-user-info uri1)
             {:username "MyName" :password "d@t!"}))
      (is (= (sut/parse-port uri) c/default-port)))

    (let [client (sut/new-client uri-str)]
      (testing "fresh server after the superuser connecting to a db"
        (sut/show-clients client)
        (sut/show-clients client)
        (is (= (count (sut/show-clients client)) 1))
        ;; #278
        (is (= (count (sut/show-clients client)) 1))
        (is (map? (sut/show-clients client)))

        (is (= (sut/list-users client) ["datalevin"]))
        (sut/open-database client "clientdb" "datalog")
        (is (= (sut/list-databases client)
               (sut/list-databases-in-use client)
               ["clientdb"]))
        (is (= (sut/list-roles client) [:datalevin.role/datalevin]))

        (is (= (sut/query-system client
                                 '[:find [?rk ...]
                                   :in $ ?un
                                   :where
                                   [?u :user/name ?un]
                                   [?ur :user-role/user ?u]
                                   [?ur :user-role/role ?r]
                                   [?r :role/key ?rk]]
                                 "datalevin")
               [:datalevin.role/datalevin]))
        (is (thrown? Exception (sut/assign-role client "no-one" :nothing))))

      (testing "create user"
        (sut/create-user client "juji" "secret")
        (is (= (set (sut/list-users client)) #{"datalevin" "juji"}))
        (is (= (set (sut/list-roles client))
               #{:datalevin.role/datalevin :datalevin.role/juji})))

      (testing "new user's default permissions"
        (let [client1 (sut/new-client "dtlv://juji:secret@localhost")]
          (is (= (count (sut/show-clients client)) 2))

          (is (= (sut/list-user-roles client1 "juji") [:datalevin.role/juji]))
          (is (= (count (sut/list-user-permissions client1 "juji")) 2))

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
        (is (= (count (sut/list-role-permissions client :juji/admin)) 2))
        (is (= (count (sut/list-user-roles client "juji")) 2))
        (is (= (count (sut/list-user-permissions client "juji")) 4)))

      (testing "a user created a db, then is forcibly disconnected by superuser"
        (let [client2 (sut/new-client "dtlv://juji:new-secret@localhost")]
          (is (= (count (sut/show-clients client)) 2))

          (is (= (count (sut/list-user-roles client2 "juji")) 2))

          (sut/create-database client2 "hr" c/dl-type)
          (is (= (count (sut/list-databases client)) 2))
          ;; TODO
          ;; (is (= (count (sut/list-role-permissions
          ;;                 client2 :datalevin.role/juji)) 3))
          ;; (is (= (count (sut/list-user-permissions client2 "juji")) 5))

          ;; (is (thrown? Exception (sut/list-databases client2)))

          (let [client-id2 (sut/get-id ^Client client2)]
            (sut/disconnect-client client client-id2)
            (is (= (count (sut/show-clients client)) 1))
            ;; auto reconnected
            ;; (is (= (count (sut/list-user-roles client2 "juji")) 2))
            )))

      (testing "db is forcibly closed by superuser, then dropped"
        (let [client3 (sut/new-client "dtlv://juji:new-secret@localhost")]
          (sut/open-database client3 "hr" "kv")
          (is (= (count (sut/list-databases-in-use client)) 2))
          (is (thrown? Exception (sut/drop-database client "hr")))

          (sut/close-database client "hr")
          (is (= (count (sut/list-databases-in-use client)) 1))
          ;; TODO
          ;; the old client reconnected
          ;; (is (= (count (sut/show-clients client)) 2))
          (is (= (count (sut/list-user-roles client3 "juji")) 2))

          (sut/drop-database client "hr")
          (is (= (count (sut/list-databases client)) 1))
          (is (= (count (sut/list-user-permissions client "juji")) 4))))

      (testing "revoke permission from role"
        (let [client4 (sut/new-client "dtlv://juji:new-secret@localhost")]
          (sut/revoke-permission client :juji/admin
                                 :datalevin.server/create
                                 :datalevin.server/database
                                 nil)
          (is (= (count (sut/list-user-permissions client4 "juji")) 3))
          (is (thrown? Exception (sut/create-database client4 "ml" c/kv-type)))
          (sut/disconnect client4)))

      (testing "withdraw role from user, then drop the role"
        (sut/withdraw-role client :juji/admin "juji")
        (is (= (sut/list-user-roles client "juji") [:datalevin.role/juji]))
        (is (= (count (sut/list-user-permissions client "juji")) 2))

        (sut/drop-role client :juji/admin)
        (is (thrown? Exception (sut/drop-role client :datalevin.role/juji)))
        (is (= (count (sut/list-roles client)) 2)))

      (testing "drop user"
        (sut/drop-user client "juji")
        (is (= (count (sut/list-users client)) 1))
        (is (= (count (sut/list-roles client)) 1)))

      (sut/reset-password client "datalevin" "nondefault")
      (sut/disconnect client))

    (testing "reset default user's password"
      (let [client5 (sut/new-client "dtlv://datalevin:nondefault@localhost")]
        (is (= (sut/list-users client5) ["datalevin"]))))))
