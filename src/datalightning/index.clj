(ns datalightning.index
  "Implement indices on LMDB"
  (:refer-clojure :exclude [conj disj empty])
  (:require [datalightning.lmdb :as lmdb]
            [datascript.db :as d]))

(defprotocol IIndex
  (empty [this i-type])
  (init [this i-type datoms])
  (conj [this datom])
  (disj [this datom])
  (slice [this start-datom end-datom])
  (rslice [this start-datom end-datom]))

;; TODO: define a protocol for this, dispatch on type instead
(defn empty-index
  "create an empty-index of the given type"
  [type]
  )

(defn init-index
  "create an empty-index of the given type"
  [type datoms]
  )
