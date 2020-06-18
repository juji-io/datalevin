(ns datalevin.constants)


(def ^:const e0    0)
;; TODO change to 64 bits
(def ^:const tx0   0x20000000)
(def ^:const emax  0x7FFFFFFF)
(def ^:const txmax 0x7FFFFFFF)
(def ^:const implicit-schema {:db/ident {:db/unique :db.unique/identity}})
