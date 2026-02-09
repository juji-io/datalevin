;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.index
  "Shared index utility functions used by storage and prepare."
  (:require
   [datalevin.interface :refer [get-value]]
   [datalevin.bits :as b]
   [datalevin.datom :as d]
   [datalevin.constants :as c]
   [datalevin.util :as u])
  (:import
   [datalevin.bits Retrieved]
   [datalevin.datom Datom]))

(defn value-type
  [props]
  (if-let [vt (:db/valueType props)]
    (if (identical? vt :db.type/tuple)
      (if-let [tts (props :db/tupleTypes)]
        tts
        (if-let [tt (props :db/tupleType)] [tt] :data))
      vt)
    :data))

(defn datom->indexable
  [schema ^Datom d high?]
  (let [e  (if-some [e (.-e d)] e (if high? c/emax c/e0))
        vm (if high? c/vmax c/v0)
        gm (if high? c/gmax c/g0)]
    (if-let [a (.-a d)]
      (if-let [p (schema a)]
        (if-some [v (.-v d)]
          (b/indexable e (p :db/aid) v (value-type p) gm)
          (b/indexable e (p :db/aid) vm (value-type p) gm))
        (b/indexable e c/a0 c/v0 nil gm))
      (let [am (if high? c/amax c/a0)]
        (if-some [v (.-v d)]
          (if (or (integer? v)
                  (identical? v :db.value/sysMax)
                  (identical? v :db.value/sysMin))
            (if e
              (b/indexable e am v :db.type/ref gm)
              (b/indexable (if high? c/emax c/e0) am v :db.type/ref gm))
            (u/raise
              "When v is known but a is unknown, v must be a :db.type/ref"
              {:v v}))
          (b/indexable e am vm :db.type/sysMin gm))))))

(defonce index->dbi {:eav c/eav :ave c/ave})

(defonce index->ktype {:eav :id :ave :avg})

(defonce index->vtype {:eav :avg :ave :id})

(defn index->k
  [index schema ^Datom datom high?]
  (case index
    :eav (or (.-e datom) (if high? c/emax c/e0))
    :ave (datom->indexable schema datom high?)))

(defn index->v
  [index schema ^Datom datom high?]
  (case index
    :eav (datom->indexable schema datom high?)
    :ave (or (.-e datom) (if high? c/emax c/e0))))

(defn gt->datom [lmdb gt] (get-value lmdb c/giants gt :id :data))

(defn retrieved->v
  [lmdb ^Retrieved r]
  (let [g (.-g r)]
    (if (= g c/normal)
      (.-v r)
      (d/datom-v (gt->datom lmdb g)))))
