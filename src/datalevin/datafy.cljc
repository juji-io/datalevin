(ns datalevin.datafy
  (:require [clojure.core.protocols :as cp]
            [datalevin.pull-api :as dp]
            [datalevin.db :as db]
            [datalevin.entity :as e]))

(declare datafy-entity-seq)

(defn- navize-pulled-entity [db-val pulled-entity]
  (let [rschema    (db/-rschema db-val)
        ref-attrs  (rschema :db.type/ref )
        ref-rattrs (set (map db/reverse-ref ref-attrs))
        many-attrs (rschema :db.cardinality/many)]
    (with-meta pulled-entity
      {`cp/nav (fn [coll k v]
                 (cond
                   (or (and (many-attrs k) (ref-attrs k))
                       (ref-rattrs k))
                   (datafy-entity-seq db-val v)
                   (ref-attrs k)
                   (e/entity db-val (:db/id v))
                   :else v))})))

(defn- navize-pulled-entity-seq [db-val entities]
  (with-meta entities
    {`cp/nav (fn [coll k v]
               (e/entity db-val (:db/id v)))}))

(defn- datafy-entity-seq [db-val entities]
  (with-meta entities
    {`cp/datafy (fn [entities] (navize-pulled-entity-seq db-val entities))}))

(extend-protocol cp/Datafiable
  datalevin.entity.Entity
  (datafy [this]
    (let [db           (.-db this)
          ref-attrs    ((db/-rschema db) :db.type/ref )
          ref-rattrs   (set (map db/reverse-ref ref-attrs))
          pull-pattern (into ["*"] ref-rattrs)]
      (navize-pulled-entity db (dp/pull db pull-pattern (:db/id this))))))
