;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.inline
  "Inlined versions of common functions"
  (:refer-clojure :exclude [assoc update]))

(defn assoc
  {:inline
   (fn
     ([m k v]
      `(clojure.lang.RT/assoc ~m ~k ~v))
     ([m k v & kvs]
      (assert (even? (count kvs)))
      `(assoc (assoc ~m ~k ~v) ~@kvs)))}
  ([map key val] (clojure.lang.RT/assoc map key val))
  ([map key val & kvs]
   (let [ret (clojure.lang.RT/assoc map key val)]
     (if kvs
       (if (next kvs)
         (recur ret (first kvs) (second kvs) (nnext kvs))
         (throw (IllegalArgumentException.
                  "assoc expects even number of arguments after map/vector, found odd number")))
       ret))))

(defn update
  {:inline
   (fn
     ([m k f & more]
      `(let [m# ~m k# ~k]
         (assoc m# k# (~f (get m# k#) ~@more)))))}
  ([m k f]
   (assoc m k (f (get m k))))
  ([m k f x]
   (assoc m k (f (get m k) x)))
  ([m k f x y]
   (assoc m k (f (get m k) x y)))
  ([m k f x y z]
   (assoc m k (f (get m k) x y z)))
  ([m k f x y z & more]
   (assoc m k (apply f (get m k) x y z more))))
