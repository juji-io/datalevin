(ns datalevin.spill-test
  (:require
   [datalevin.lmdb :as l]
   [datalevin.spill :as sp]
   [datalevin.interpret :as i]
   [datalevin.util :as u]
   [datalevin.core :as dc]
   [datalevin.constants :as c]
   [datalevin.datom :as d]
   [clojure.test :refer [deftest testing is]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop])
  (:import
   [datalevin.spill SpillableVector]))

(if (u/graal?)
  (require 'datalevin.binding.graal)
  (require 'datalevin.binding.java))

(sp/uninstall-memory-updater)

(deftest before-spill-test
  (let [^SpillableVector vs (sp/new-spillable-vector)]

    (is (nil? (get vs 0)))
    (is (nil? (peek vs)))
    (is (= 0 (.length vs)))
    (is (= 0 (count vs)))
    (is (not (contains? vs 0)))
    (is (thrown? Exception (nth vs 0)))
    (is (thrown? Exception (pop vs)))

    (assoc vs 0 :start)
    (is (= :start (get vs 0)))
    (is (= :start (peek vs)))
    (is (= 1 (.length vs)))
    (is (= 1 (count vs)))
    (is (contains? vs 0))
    (is (= :start (nth vs 0)))

    (assoc vs 1 :second)
    (assoc vs 2 :third)
    (is (= :second (nth vs 1)))
    ;; (sp/spill vs)
    ;; (is (not (l/closed-kv? @(.-disk-lmdb vs))))
    ))
