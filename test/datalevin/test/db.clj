(ns ^:no-doc datalevin.test.db
  (:require
   [clojure.data]
   [clojure.test :as t :refer [is are deftest testing]]
   [datalevin.core :as d]
   [datalevin.util :refer [defrecord-updatable]]))

;;
;; verify that defrecord-updatable works with compiler/core macro configuration
;; define dummy class which redefines hash, could produce either
;; compiler or runtime error
;;
(defrecord-updatable HashBeef [x]
  clojure.lang.IHashEq (hasheq [hb] 0xBEEF))

(deftest test-defrecord-updatable
  (is (= 0xBEEF (-> (map->HashBeef {:x :ignored}) hash))))

(defn- now [] (System/currentTimeMillis))

(deftest test-uuid
  (let [now-ms (loop []
                 (let [ts (now)]
                   (if (> ^long (mod ts 1000) 900) ;; sleeping over end of a second
                     (recur)
                     ts)))
        now    (int (/ ^long now-ms 1000))]
    (is (= (* 1000 now) (d/squuid-time-millis (d/squuid))))
    (is (not= (d/squuid) (d/squuid)))
    (is (= (subs (str (d/squuid)) 0 8)
           (subs (str (d/squuid)) 0 8)))))
