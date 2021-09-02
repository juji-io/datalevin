(ns datalevin.client-test
  (:require [datalevin.client :as sut]
            [datalevin.server :as srv]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [datalevin.test.core :refer [server-fixture]]
            [clojure.test :refer [is testing deftest use-fixtures]])
  (:import [java.util UUID Arrays]
           [java.net URI]))

(use-fixtures :each server-fixture)

(deftest basic-ops-test
  (let [uri (URI. "dtlv://juji:nice!1@juji.io/mydb")]
    (is (= {:username "juji" :password "nice!1"} (sut/parse-user-info uri)))
    (is (= c/default-port (sut/parse-port uri)))
    (is (= [nil "mydb"] (sut/parse-db uri)))))
