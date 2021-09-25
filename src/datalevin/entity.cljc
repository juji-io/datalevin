(ns ^:no-doc datalevin.entity
  (:refer-clojure :exclude [keys get])
  (:require [#?(:cljs cljs.core :clj clojure.core) :as c]
            [datalevin.db :as db]
            [datalevin.storage :as st]
            [datalevin.util :as u]
            [taoensso.nippy :as nippy]
            [clojure.set :as set])
  (:import [datalevin.db DB]
           [datalevin.storage IStore]
           [java.io DataInput DataOutput]))

(declare entity ->Entity equiv-entity lookup-entity touch entity->txs
         lookup-stage-then-entity)

(defn- entid [db eid]
  (when (or (number? eid)
            (sequential? eid)
            (keyword? eid))
    (db/entid db eid)))

(defn entity
  [db eid]
  (when-let [e (entid db eid)]
    (->Entity db e (volatile! false) (volatile! {}) {} {})))

(defn- entity-attr [db a datoms]
  (if (db/multival? db a)
    (if (db/ref? db a)
      (reduce #(conj %1 (entity db (:v %2))) #{} datoms)
      (reduce #(conj %1 (:v %2)) #{} datoms))
    (if (db/ref? db a)
      (entity db (:v (first datoms)))
      (:v (first datoms)))))

(defn- -lookup-backwards [db eid attr not-found]
  (if-let [datoms (not-empty (db/-search db [nil attr eid]))]
    (if (db/component? db attr)
      (entity db (:e (first datoms)))
      (reduce #(conj %1 (entity db (:e %2))) #{} datoms))
    not-found))

#?(:cljs
   (defn- multival->js [val]
          (when val (to-array val))))

#?(:cljs
   (defn- js-seq [e]
          (touch e)
          (for [[a v] @(.-cache e)]
               (if (db/multival? (.-db e) a)
                 [a (multival->js v)]
                 [a v]))))

#?(:cljs
   (deftype Entity [db eid touched cache]
            Object
            (toString [this]
                      (pr-str* this))
            (equiv [this other]
                   (equiv-entity this other))

            ;; js/map interface
            (keys [this]
                  (es6-iterator (c/keys this)))
            (entries [this]
                     (es6-entries-iterator (js-seq this)))
            (values [this]
                    (es6-iterator (map second (js-seq this))))
            (has [this attr]
                 (not (nil? (.get this attr))))
            (get [this attr]
                 (if (= attr ":db/id")
                   eid
                   (if (db/reverse-ref? attr)
                     (-> (-lookup-backwards db eid (db/reverse-ref attr) nil)
                         multival->js)
                     (cond-> (lookup-entity this attr)
                             (db/multival? db attr) multival->js))))
            (forEach [this f]
                     (doseq [[a v] (js-seq this)]
                            (f v a this)))
            (forEach [this f use-as-this]
                     (doseq [[a v] (js-seq this)]
                            (.call f use-as-this v a this)))

            ;; js fallbacks
            (key_set [this] (to-array (c/keys this)))
            (entry_set [this] (to-array (map to-array (js-seq this))))
            (value_set [this] (to-array (map second (js-seq this))))

            IEquiv
            (-equiv [this o] (equiv-entity this o))

            IHash
            (-hash [_]
                   (hash eid))                              ;; db?

            ISeqable
            (-seq [this]
                  (touch this)
                  (seq @cache))

            ICounted
            (-count [this]
                    (touch this)
                    (count @cache))

            ILookup
            (-lookup [this attr] (lookup-entity this attr nil))
            (-lookup [this attr not-found] (lookup-entity this attr not-found))

            IAssociative
            (-contains-key? [this k]
                            (not= ::nf (lookup-entity this k ::nf)))

            IFn
            (-invoke [this k]
                     (lookup-entity this k))
            (-invoke [this k not-found]
                     (lookup-entity this k not-found))

            IPrintWithWriter
            (-pr-writer [_ writer opts]
                        (-pr-writer (assoc @cache :db/id eid) writer opts))))

(defprotocol Transactable
  (add [this attr v])
  (retract
    [this attr]
    [this attr v])
  (->txs [this]))

#?(:clj
   (deftype Entity [db eid touched cache tbd meta-map]
     clojure.lang.IPersistentMap
     (equiv [e o] (equiv-entity e o))
     (containsKey [e k] (not= ::nf (lookup-stage-then-entity e k ::nf)))
     (entryAt [e k] (some->> (lookup-stage-then-entity e k) (clojure.lang.MapEntry. k)))

     (empty [e] (throw (UnsupportedOperationException.)))
     (assoc [this k v]
       (assert (keyword? k) "attribute must be keyword")
       (Entity. db eid touched cache (assoc tbd k [{:op :assoc} v]) meta-map))
     (without [this k]
       (assert (keyword? k) "attribute must be keyword")
       (Entity. db eid touched cache (assoc tbd k [{:op :dissoc}]) meta-map))
     (cons [this [k v]]
       (assoc this k v))
     (count [this] (touch this) (count @(.-cache this)))
     (seq [this] (touch this) (seq @cache))

     Iterable
     (iterator [this] (clojure.lang.SeqIterator. (seq this)))

     clojure.lang.ILookup
     (valAt [e k] (lookup-stage-then-entity e k))
     (valAt [e k not-found] (lookup-stage-then-entity e k not-found))

     clojure.lang.IFn
     (invoke [e k] (lookup-stage-then-entity e k))
     (invoke [e k not-found] (lookup-stage-then-entity e k not-found))

     Transactable
     (add [this attr v]
       (assert (keyword? attr) "attribute must be keyword")
       (Entity. db eid touched cache (assoc tbd attr [{:op :add} v]) meta-map))
     (retract [this attr]
       (assert (keyword? attr) "attribute must be keyword")
       (Entity. db eid touched cache (assoc tbd attr [{:op :retract}]) meta-map))
     (retract [this attr v]
       (assert (keyword? attr) "attribute must be keyword")
       (Entity. db eid touched cache (assoc tbd attr [{:op :retract} v]) meta-map))
     (->txs [this]
       (entity->txs this))

     Object
     (toString [this] (pr-str (assoc @cache :db/id eid)))
     (hashCode [this] (hash eid))
     (equals [this o] (equiv-entity this o))))

#?(:clj
   (defmethod print-method Entity [^Entity e, ^java.io.Writer w]
     (let [staged (not-empty (.-tbd e))
           ent    (assoc @(.cache e) :db/id (.eid e))]
       (.write w
               (pr-str
                 (cond-> ent
                   staged (assoc :<STAGED> staged)))))))

(defn- rschema->attr-types
  [{ref-attrs  :db.type/ref
    many-attrs :db.cardinality/many
    components :db/isComponent}]
  {:ref-attrs       (set/difference ref-attrs many-attrs)
   :ref-rattrs      (into #{} (map db/reverse-ref) components)
   :ref-many-rattrs (into #{} (map db/reverse-ref) (set/difference ref-attrs components))
   :ref-many-attrs  (set many-attrs)})

(def ^:private mem-rschema->attr-types
  (u/memoize-1 rschema->attr-types))

(defn- db->attr-types [db]
  (mem-rschema->attr-types (db/-rschema db)))

(defn- lookup-ref? [x]
  (and (vector? x)
       (= 2 (count x))
       (keyword? (first x))))

(defn lookup-stage-then-entity
  ([e k] (lookup-stage-then-entity e k nil))
  ([^Entity e k default-value]
   (if-let [[_ v] (c/get (.-tbd e) k)]
     v
     (if-some [v (lookup-entity e k)]
       v
       default-value))))

(defn- entity->txs [^Entity e]
  (let [eid (.-eid e)
        db  (.-db e)
        {:keys [ref-attrs ref-rattrs ref-many-rattrs ref-many-attrs]}
        (db->attr-types db)]
    (into
      []
      (mapcat
        (fn [[k [meta v]]]
          (let [{:keys [op]} meta]
            (cond
              (or (ref-many-attrs k)
                  (ref-many-rattrs k))
              (let [v (if (lookup-ref? v)
                        [v]
                        (u/ensure-vec v))]
                (case op
                  :assoc   [[:db.fn/retractAttribute eid k]
                            {:db/id eid
                             k      (mapv (fn [e] (or (:db/id e) e)) v)}]
                  :add     [{:db/id eid
                             k      (mapv (fn [e] (or (:db/id e) e)) v)}]
                  :dissoc  [[:db.fn/retractAttribute eid k]]
                  :retract (into []
                                 (map (fn [e]
                                        [:db/retract eid k (:db/id e)]))
                                 v)))

              :else
              (case op
                (:dissoc :retract) [[:db.fn/retractAttribute eid k]]
                (:assoc :add)      [[:db/add eid k v]])))))
      (.-tbd e))))

(defn entity? [x] (instance? Entity x))

(defn- equiv-entity [^Entity this that]
  (and
    (instance? Entity that)
    ;; (= db  (.-db ^Entity that))
    (= (.-eid this) (.-eid ^Entity that))))

(defn- lookup-entity
  ([this attr] (lookup-entity this attr nil))
  ([^Entity this attr not-found]
   (if (= attr :db/id)
     (.-eid this)
     (if (db/reverse-ref? attr)
       (-lookup-backwards (.-db this) (.-eid this) (db/reverse-ref attr) not-found)
       (if-some [v (@(.-cache this) attr)]
         v
         (if @(.-touched this)
           not-found
           (if-some [datoms (not-empty (db/-search (.-db this) [(.-eid this) attr]))]
             (let [value (entity-attr (.-db this) attr datoms)]
               (vreset! (.-cache this) (assoc @(.-cache this) attr value))
               value)
             not-found)))))))

(defn touch-components [db a->v]
  (reduce-kv (fn [acc a v]
               (assoc acc a
                          (if (db/component? db a)
                            (if (db/multival? db a)
                              (set (map touch v))
                              (touch v))
                            v)))
             {} a->v))

(defn- datoms->cache [db datoms]
  (reduce (fn [acc part]
            (let [a (:a (first part))]
              (assoc acc a (entity-attr db a part))))
          {} (partition-by :a datoms)))

(defn touch [^Entity e]
  {:pre [(entity? e)]}
  (when-not @(.-touched e)
    (when-let [datoms (not-empty (db/-search (.-db e) [(.-eid e)]))]
      (vreset! (.-cache e) (->> datoms
                                (datoms->cache (.-db e))
                                (touch-components (.-db e))))
      (vreset! (.-touched e) true)))
  e)

(defn- load-cache [^Entity e cache]
  (vreset! (.-cache e) cache)
  (vreset! (.-touched e) true)
  e)

#?(:cljs (goog/exportSymbol "datalevin.entity.Entity" Entity))

(nippy/extend-freeze Entity :datalevin/entity
                     [^Entity x ^DataOutput out]
                     (nippy/freeze-to-out!
                       out
                       (let [^DB db  (.-db x)
                             db-name (st/db-name ^IStore (.-store db))
                             m       {:db/id   (.-eid x)
                                      :db-name db-name}]
                         (if @(.-touched x)
                           (assoc m :touched true :cache @(.-cache x))
                           m))))

(nippy/extend-thaw :datalevin/entity
                   [^DataInput in]
                   (let [{:keys [db-name touched cache db/id]}
                         (nippy/thaw-from-in! in)
                         e (entity (@db/dbs db-name) id)]
                     (if touched
                       (load-cache e cache)
                       e)))
