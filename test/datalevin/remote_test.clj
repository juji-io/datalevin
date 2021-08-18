(ns datalevin.remote-test
  (:require [datalevin.remote :as sut]
            [datalevin.server :as srv]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [clojure.test :as t])
  (:import [java.util UUID]))

(defn server-fixture
  [f]
  (let [server (srv/start {:port c/default-port
                           :root (u/tmp-dir (str "remote-test-"
                                                 (UUID/randomUUID)))})]))
