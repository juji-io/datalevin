(ns datalevin.constants)


(def ^:const e0    0)
(def ^:const tx0   0x2000000000000000)
(def ^:const emax  0x7FFFFFFFFFFFFFFF)
(def ^:const txmax 0x7FFFFFFFFFFFFFFF)
(def ^:const implicit-schema {:db/ident {:db/unique :db.unique/identity}})
