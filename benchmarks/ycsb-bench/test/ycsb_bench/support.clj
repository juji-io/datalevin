(ns ycsb-bench.support
  (:require [ycsb-bench.store :as store]))

(def no-host-control
  {:enabled? false :pause (constantly []) :resume (fn [_])})

(defn with-stores
  "Supply fresh test records with the same callback and cleanup lifecycle."
  [open]
  (fn [opts f]
    (f (fn [callback]
         (let [db (open opts)]
           (try
             (callback db)
             (finally (store/close-store! db))))))))
