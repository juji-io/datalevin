(ns datalevin.query-runtime-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.query :as query])
  (:import [clojure.lang Var]))

(deftest native-cache-hits-preserve-caller-binding-frame
  (let [conn (d/create-conn nil {:next {:db/valueType :db.type/ref}
                                :value {:db/valueType :db.type/long}}
                            {:kv-opts {:inmemory? true} :background-sampling? false})
        frames (atom [])
        probe (reify Object
                (hashCode [_]
                  (swap! frames conj (Var/getThreadBindingFrame))
                  42))
        q '[:find ?e ?next :in $ ?probe
            :where [?e :next ?next] [?next :value 2]]]
    (try
      (d/transact! conn [{:db/id 1 :next 2} {:db/id 2 :value 2}])
      (let [database @conn
            prepared (d/prepare-q database q)
            reader (query/query-reader q)]
        (doseq [read [#(d/q q database probe)
                     #(prepared [probe])
                     #(reader database [database probe] false)]]
          (is (= #{[1 2]} (read)))
          (reset! frames [])
          (let [frame (Var/getThreadBindingFrame)]
            (is (= #{[1 2]} (read)))
            (is (seq @frames))
            (is (every? #(identical? frame %) @frames)))))
      (finally (d/close conn)))))
