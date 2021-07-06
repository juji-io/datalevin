(ns datalevin.core
  "API for Datalevin database"
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   [datalevin.util :as u]
   [datalevin.db :as db]
   [datalevin.datom :as dd]
   [datalevin.storage :as s]
   [datalevin.lmdb :as l]
   [datalevin.pull-parser]
   [datalevin.pull-api :as dp]
   [datalevin.query :as dq]
   [datalevin.impl.entity :as de]
   [datalevin.bits :as b])
  (:import
   [datalevin.impl.entity Entity]
   [datalevin.storage Store]
   [datalevin.db DB]
   [java.util UUID]))

(if (u/graal?)
  (require 'datalevin.binding.graal)
  (require 'datalevin.binding.java))

;; Entities

(def ^{:arglists '([db eid])
       :doc      "Retrieves an entity by its id from Datalog database. Entities
are lazy map-like structures to navigate Datalevin database content.

             `db` is a Datalog database.

             For `eid` pass entity id or lookup attr:

                 (entity db 1)
                 (entity db [:unique-attr :value])

             If entity does not exist, `nil` is returned:

                 (entity db 100500) ; => nil

             Creating an entity by id is very cheap, almost no-op, as attr access
is on-demand:

                 (entity db 1) ; => {:db/id 1}

             Entity attributes can be lazily accessed through key lookups:

                 (:attr (entity db 1)) ; => :value
                 (get (entity db 1) :attr) ; => :value

             Cardinality many attributes are returned sequences:

                 (:attrs (entity db 1)) ; => [:v1 :v2 :v3]

             Reference attributes are returned as another entities:

                 (:ref (entity db 1)) ; => {:db/id 2}
                 (:ns/ref (entity db 1)) ; => {:db/id 2}

             References can be walked backwards by prepending `_` to name part
of an attribute:

                 (:_ref (entity db 2)) ; => [{:db/id 1}]
                 (:ns/_ref (entity db 2)) ; => [{:db/id 1}]

             Reverse reference lookup returns sequence of entities unless
attribute is marked as `:db/component`:

                 (:_component-ref (entity db 2)) ; => {:db/id 1}

             Entity gotchas:

             - Entities print as map, but are not exactly maps (they have
compatible get interface though).
             - Entities retain reference to the database.
             - You can’t change database through entities, only read.
             - Creating an entity by id is very cheap, almost no-op
(attributes are looked up on demand).
             - Comparing entities just compares their ids. Be careful when
comparing entities taken from differenct dbs or from different versions of the
same db.
             - Accessed entity attributes are cached on entity itself (except
backward references).
             - When printing, only cached attributes (the ones you have accessed
before) are printed. See [[touch]]."}
  entity de/entity)

(def add de/add)
(def retract de/retract)


(def ^{:arglists '([db eid])
       :doc      "Given lookup ref `[unique-attr value]`, returns numberic entity id.

             `db` is a Datalog database.

             If entity does not exist, returns `nil`.

             For numeric `eid` returns `eid` itself (does not check for entity
existence in that case)."}
  entid db/entid)


(defn entity-db
  "Returns a Datalog db that entity was created from."
  [^Entity entity]
  {:pre [(de/entity? entity)]}
  (.-db entity))


(def ^{:arglists '([e])
       :doc      "Forces all entity attributes to be eagerly fetched and cached.
Only usable for debug output.

             Usage:

             ```
             (entity db 1) ; => {:db/id 1}
             (touch (entity db 1)) ; => {:db/id 1, :dislikes [:pie], :likes [:pizza]}
             ```"}
  touch de/touch)


;; Pull API

(def ^{:arglists '([db selector eid])
       :doc      "Fetches data from Datalog database using recursive declarative
description. See [docs.datomic.com/on-prem/pull.html](https://docs.datomic.com/on-prem/pull.html).

             Unlike [[entity]], returns plain Clojure map (not lazy).

             Usage:

                 (pull db [:db/id, :name, :likes, {:friends [:db/id :name]}] 1)
                 ; => {:db/id   1,
                 ;     :name    \"Ivan\"
                 ;     :likes   [:pizza]
                 ;     :friends [{:db/id 2, :name \"Oleg\"}]}"}
  pull dp/pull)


(def ^{:arglists '([db selector eids])
       :doc      "Same as [[pull]], but accepts sequence of ids and returns
sequence of maps.

             Usage:

             ```
             (pull-many db [:db/id :name] [1 2])
             ; => [{:db/id 1, :name \"Ivan\"}
             ;     {:db/id 2, :name \"Oleg\"}]
             ```"}
  pull-many dp/pull-many)


;; Query

(def
  ^{:arglists '([query & inputs])
    :doc      "Executes a datalog query. See [docs.datomic.com/on-prem/query.html](https://docs.datomic.com/on-prem/query.html).

          Usage:

          ```
          (q '[:find ?value
               :where [_ :likes ?value]]
             db)
          ; => #{[\"fries\"] [\"candy\"] [\"pie\"] [\"pizza\"]}
          ```"}
  q dq/q)


;; Creating DB

(def ^{:arglists '([] [dir] [dir schema])
       :doc      "Open Datalog database at the given data directory. Creates an
empty database there if it does not exist yet. Update the schema if one is
given. Return reference to the database.

             Usage:

             ```
             (empty-db)

             (empty-db \"/tmp/test-empty-db\")

             (empty-db \"/tmp/test-empty-db\" {:likes {:db/cardinality :db.cardinality/many}})
             ```"}
  empty-db db/empty-db)


(def ^{:arglists '([x])
       :doc      "Returns `true` if the given value is a Datalog database,
`false` otherwise."}
  db? db/db?)


(def ^{:arglists '([e a v] [e a v tx] [e a v tx added])
       :doc      "Low-level fn to create raw datoms.

             Optionally with transaction id (number) and `added` flag (`true` for addition, `false` for retraction).

             See also [[init-db]]."}
  datom dd/datom)

(def ^{:arglists '([x])
       :doc      "Returns `true` if the given value is a datom, `false` otherwise."}
  datom? dd/datom?)

(def ^{:arglists '([d])
       :doc      "Return the entity id of a datom"}
  datom-e dd/datom-e)

(def ^{:arglists '([d])
       :doc      "Return the attribute id of a datom"}
  datom-a dd/datom-a)

(def ^{:arglists '([d])
       :doc      "Return the value id of a datom"}
  datom-v dd/datom-v)

(def ^{:arglists '([datoms] [datoms dir] [datoms dir schema])
       :doc      "Low-level fn for creating database quickly from a trusted sequence of datoms.

             Does no validation on inputs, so `datoms` must be well-formed and match schema.

             See also [[datom]]."}
  init-db db/init-db)

(def ^{:arglists '([db])
       :doc      "Close the Datalog database"}
  close-db db/close-db)

;; Changing DB

(defn ^:no-doc with
  "Same as [[transact!]]. Returns transaction report (see [[transact!]])."
  ([db tx-data] (with db tx-data nil))
  ([db tx-data tx-meta]
   {:pre [(db/db? db)]}
   (db/transact-tx-data (db/map->TxReport
                          {:db-before db
                           :db-after  db
                           :tx-data   []
                           :tempids   {}
                           :tx-meta   tx-meta}) tx-data)))


(defn ^:no-doc db-with
  "Applies transaction. Return the db."
  [db tx-data]
  {:pre [(db/db? db)]}
  (:db-after (with db tx-data)))


;; Index lookups

(defn datoms
  "Index lookup. Returns a sequence of datoms (lazy iterator over actual DB index) which components (e, a, v) match passed arguments.

   Datoms are sorted in index sort order. Possible `index` values are: `:eav`, `:ave`, or `:vea` (only available for :db.type/ref datoms).

   Usage:

       ; find all datoms for entity id == 1 (any attrs and values)
       ; sort by attribute, then value
       (datoms db :eav 1)
       ; => (#datalevin/Datom [1 :friends 2]
       ;     #datalevin/Datom [1 :likes \"fries\"]
       ;     #datalevin/Datom [1 :likes \"pizza\"]
       ;     #datalevin/Datom [1 :name \"Ivan\"])

       ; find all datoms for entity id == 1 and attribute == :likes (any values)
       ; sorted by value
       (datoms db :eav 1 :likes)
       ; => (#datalevin/Datom [1 :likes \"fries\"]
       ;     #datalevin/Datom [1 :likes \"pizza\"])

       ; find all datoms for entity id == 1, attribute == :likes and value == \"pizza\"
       (datoms db :eav 1 :likes \"pizza\")
       ; => (#datalevin/Datom [1 :likes \"pizza\"])

       ; find all datoms that have attribute == `:likes` and value == `\"pizza\"` (any entity id)
       (datoms db :ave :likes \"pizza\")
       ; => (#datalevin/Datom [1 :likes \"pizza\"]
       ;     #datalevin/Datom [2 :likes \"pizza\"])

       ; find all datoms sorted by entity id, then attribute, then value
       (datoms db :eav) ; => (...)

   Useful patterns:

       ; get all values of :db.cardinality/many attribute
       (->> (datoms db :eav eid attr) (map :v))

       ; lookup entity ids by attribute value
       (->> (datoms db :ave attr value) (map :e))

       ; find N entities with lowest attr value (e.g. 10 earliest posts)
       (->> (datoms db :ave attr) (take N))

       ; find N entities with highest attr value (e.g. 10 latest posts)
       (->> (datoms db :ave attr) (reverse) (take N))

   Gotchas:

   - Index lookup is usually more efficient than doing a query with a single clause.
   - Resulting iterator is calculated in constant time and small constant memory overhead.
   "
  ([db index]             {:pre [(db/db? db)]} (db/-datoms db index []))
  ([db index c1]          {:pre [(db/db? db)]} (db/-datoms db index [c1]))
  ([db index c1 c2]       {:pre [(db/db? db)]} (db/-datoms db index [c1 c2]))
  ([db index c1 c2 c3]    {:pre [(db/db? db)]} (db/-datoms db index [c1 c2 c3]))
  ([db index c1 c2 c3 c4] {:pre [(db/db? db)]} (db/-datoms db index [c1 c2 c3 c4])))


(defn seek-datoms
  "Similar to [[datoms]], but will return datoms starting from specified components and including rest of the database until the end of the index.

   If no datom matches passed arguments exactly, iterator will start from first datom that could be considered “greater” in index order.

   Usage:

       (seek-datoms db :eavt 1)
       ; => (#datalevin/Datom [1 :friends 2]
       ;     #datalevin/Datom [1 :likes \"fries\"]
       ;     #datalevin/Datom [1 :likes \"pizza\"]
       ;     #datalevin/Datom [1 :name \"Ivan\"]
       ;     #datalevin/Datom [2 :likes \"candy\"]
       ;     #datalevin/Datom [2 :likes \"pie\"]
       ;     #datalevin/Datom [2 :likes \"pizza\"])

       (seek-datoms db :eavt 1 :name)
       ; => (#datalevin/Datom [1 :name \"Ivan\"]
       ;     #datalevin/Datom [2 :likes \"candy\"]
       ;     #datalevin/Datom [2 :likes \"pie\"]
       ;     #datalevin/Datom [2 :likes \"pizza\"])

       (seek-datoms db :eavt 2)
       ; => (#datalevin/Datom [2 :likes \"candy\"]
       ;     #datalevin/Datom [2 :likes \"pie\"]
       ;     #datalevin/Datom [2 :likes \"pizza\"])

       ; no datom [2 :likes \"fish\"], so starts with one immediately following such in index
       (seek-datoms db :eavt 2 :likes \"fish\")
       ; => (#datalevin/Datom [2 :likes \"pie\"]
       ;     #datalevin/Datom [2 :likes \"pizza\"])"
  ([db index]             {:pre [(db/db? db)]} (db/-seek-datoms db index []))
  ([db index c1]          {:pre [(db/db? db)]} (db/-seek-datoms db index [c1]))
  ([db index c1 c2]       {:pre [(db/db? db)]} (db/-seek-datoms db index [c1 c2]))
  ([db index c1 c2 c3]    {:pre [(db/db? db)]} (db/-seek-datoms db index [c1 c2 c3]))
  ([db index c1 c2 c3 c4] {:pre [(db/db? db)]} (db/-seek-datoms db index [c1 c2 c3 c4])))


(defn rseek-datoms
  "Same as [[seek-datoms]], but goes backwards until the beginning of the index."
  ([db index]             {:pre [(db/db? db)]} (db/-rseek-datoms db index []))
  ([db index c1]          {:pre [(db/db? db)]} (db/-rseek-datoms db index [c1]))
  ([db index c1 c2]       {:pre [(db/db? db)]} (db/-rseek-datoms db index [c1 c2]))
  ([db index c1 c2 c3]    {:pre [(db/db? db)]} (db/-rseek-datoms db index [c1 c2 c3]))
  ([db index c1 c2 c3 c4] {:pre [(db/db? db)]} (db/-rseek-datoms db index [c1 c2 c3 c4])))


(defn index-range
  "Returns part of `:avet` index between `[_ attr start]` and `[_ attr end]` in AVET sort order.

   Same properties as [[datoms]].

   Usage:

       (index-range db :likes \"a\" \"zzzzzzzzz\")
       ; => (#datalevin/Datom [2 :likes \"candy\"]
       ;     #datalevin/Datom [1 :likes \"fries\"]
       ;     #datalevin/Datom [2 :likes \"pie\"]
       ;     #datalevin/Datom [1 :likes \"pizza\"]
       ;     #datalevin/Datom [2 :likes \"pizza\"])

       (index-range db :likes \"egg\" \"pineapple\")
       ; => (#datalevin/Datom [1 :likes \"fries\"]
       ;     #datalevin/Datom [2 :likes \"pie\"])

   Useful patterns:

       ; find all entities with age in a specific range (inclusive)
       (->> (index-range db :age 18 60) (map :e))"
  [db attr start end]
  {:pre [(db/db? db)]}
  (db/-index-range db attr start end))


;; Conn

(defn conn?
  "Returns `true` if this is a connection to a Datalevin db, `false` otherwise."
  [conn]
  (and #?(:clj  (instance? clojure.lang.IDeref conn)
          :cljs (satisfies? cljs.core/IDeref conn))
       (db/db? @conn)))

(defn conn-from-db
  "Creates a mutable reference to a given database. See [[create-conn]]."
  [db]
  (atom db :meta { :listeners (atom {}) }))

(defn conn-from-datoms
  "Create a mutable reference to a database with the given datoms added to it."
  ([datoms] (conn-from-db (init-db datoms)))
  ([datoms dir] (conn-from-db (init-db datoms dir)))
  ([datoms dir schema] (conn-from-db (init-db datoms dir schema))))


(defn create-conn
  "Creates a mutable reference (a “connection”) to a database at the given
  data directory and opens the database. Creates the database if it doesn't
  exist yet. Update the schema if one is given. Return the connection.

  Please note that the connection should be managed like a stateful resource.
  Application should hold on to the same connection rather than opening
  multiple connections to the same database in the same process.

  Connections are lightweight in-memory structures (~atoms).  See also
  [[transact!]], [[db]], [[close]], [[get-conn]], and [[open-kv]].

  To access underlying DB, deref: `@conn`.

  Usage:

             (create-conn)

             (create-conn \"/tmp/test-create-conn\")

             (create-conn \"/tmp/test-create-conn\" {:likes {:db/cardinality :db.cardinality/many}})
  "
  ([] (conn-from-db (empty-db)))
  ([dir] (conn-from-db (empty-db dir)))
  ([dir schema] (conn-from-db (empty-db dir schema))))

(defn close
  "Close the connection"
  [conn]
  (s/close ^Store (.-store ^DB @conn))
  (reset! conn nil)
  nil)

(defn closed?
  "Return true when the underlying DB is closed or when `conn` is nil or contains nil"
  [conn]
  (or (nil? conn)
      (nil? @conn)
      (s/closed? ^Store (.-store ^DB @conn))))

(defn ^:no-doc -transact! [conn tx-data tx-meta]
  {:pre [(conn? conn)]}
  (let [report (atom nil)]
    (swap! conn (fn [db]
                  (let [r (with db tx-data tx-meta)]
                    (reset! report r)
                    (:db-after r))))
    @report))


(defn transact!
  "Applies transaction to the underlying database.

  Returns transaction report, a map:

       { :db-before ...
         :db-after  ...
         :tx-data   [...]     ; plain datoms that were added/retracted from db-before
         :tempids   {...}     ; map of tempid from tx-data => assigned entid in db-after
         :tx-meta   tx-meta } ; the exact value you passed as `tx-meta`

  Note! `conn` will be updated in-place and is not returned from [[transact!]].

  Usage:

      ; add a single datom to an existing entity (1)
      (transact! conn [[:db/add 1 :name \"Ivan\"]])

      ; retract a single datom
      (transact! conn [[:db/retract 1 :name \"Ivan\"]])

      ; retract single entity attribute
      (transact! conn [[:db.fn/retractAttribute 1 :name]])

      ; ... or equivalently (since Datomic changed its API to support this):
      (transact! conn [[:db/retract 1 :name]])

      ; retract all entity attributes (effectively deletes entity)
      (transact! conn [[:db.fn/retractEntity 1]])

      ; create a new entity (`-1`, as any other negative value, is a tempid
      ; that will be replaced with Datalevin to a next unused eid)
      (transact! conn [[:db/add -1 :name \"Ivan\"]])

      ; check assigned id (here `*1` is a result returned from previous `transact!` call)
      (def report *1)
      (:tempids report) ; => {-1 296}

      ; check actual datoms inserted
      (:tx-data report) ; => [#datalevin/Datom [296 :name \"Ivan\"]]

      ; tempid can also be a string
      (transact! conn [[:db/add \"ivan\" :name \"Ivan\"]])
      (:tempids *1) ; => {\"ivan\" 297}

      ; reference another entity (must exist)
      (transact! conn [[:db/add -1 :friend 296]])

      ; create an entity and set multiple attributes (in a single transaction
      ; equal tempids will be replaced with the same unused yet entid)
      (transact! conn [[:db/add -1 :name \"Ivan\"]
                       [:db/add -1 :likes \"fries\"]
                       [:db/add -1 :likes \"pizza\"]
                       [:db/add -1 :friend 296]])

      ; create an entity and set multiple attributes (alternative map form)
      (transact! conn [{:db/id  -1
                        :name   \"Ivan\"
                        :likes  [\"fries\" \"pizza\"]
                        :friend 296}])

      ; update an entity (alternative map form). Can’t retract attributes in
      ; map form. For cardinality many attrs, value (fish in this example)
      ; will be added to the list of existing values
      (transact! conn [{:db/id  296
                        :name   \"Oleg\"
                        :likes  [\"fish\"]}])

      ; ref attributes can be specified as nested map, that will create netsed entity as well
      (transact! conn [{:db/id  -1
                        :name   \"Oleg\"
                        :friend {:db/id -2
                                 :name \"Sergey\"}])

      ; reverse attribute name can be used if you want created entity to become
      ; a value in another entity reference
      (transact! conn [{:db/id  -1
                        :name   \"Oleg\"
                        :_friend 296}])
      ; equivalent to
      (transact! conn [{:db/id  -1, :name   \"Oleg\"}
                       {:db/id 296, :friend -1}])
      ; equivalent to
      (transact! conn [[:db/add  -1 :name   \"Oleg\"]
                       {:db/add 296 :friend -1]])"
  ([conn tx-data] (transact! conn tx-data nil))
  ([conn tx-data tx-meta]
   {:pre [(conn? conn)]}
   (let [report (-transact! conn tx-data tx-meta)]
     (doseq [[_ callback] (some-> (:listeners (meta conn)) (deref))]
       (callback report))
     report)))


(defn reset-conn!
  "Forces underlying `conn` value to become `db`. Will generate a tx-report that
  will remove everything from old value and insert everything from the new one."
  ([conn db] (reset-conn! conn db nil))
  ([conn db tx-meta]
   (let [report (db/map->TxReport
                  { :db-before @conn
                   :db-after   db
                   :tx-data    (concat
                                (map #(assoc % :added false) (datoms @conn :eavt))
                                (datoms db :eavt))
                   :tx-meta    tx-meta})]
     (reset! conn db)
     (doseq [[_ callback] (some-> (:listeners (meta conn)) (deref))]
       (callback report))
     db)))


(defn- atom? [a]
  #?(:cljs (instance? Atom a)
     :clj  (instance? clojure.lang.IAtom a)))


(defn listen!
  "Listen for changes on the given connection. Whenever a transaction is applied
  to the database via [[transact!]], the callback is called with the transaction
  report. `key` is any opaque unique value.

  Idempotent. Calling [[listen!]] with the same key twice will override old
  callback with the new value.

   Returns the key under which this listener is registered. See also [[unlisten!]]."
  ([conn callback] (listen! conn (rand) callback))
  ([conn key callback]
   {:pre [(conn? conn) (atom? (:listeners (meta conn)))]}
   (swap! (:listeners (meta conn)) assoc key callback)
   key))


(defn unlisten!
  "Removes registered listener from connection. See also [[listen!]]."
  [conn key]
  {:pre [(conn? conn) (atom? (:listeners (meta conn)))]}
  (swap! (:listeners (meta conn)) dissoc key))


;; Data Readers

(def ^{:no-doc true}
  data-readers {'datalevin/Datom dd/datom-from-reader
                'datalevin/DB    db/db-from-reader})

#?(:cljs
   (doseq [[tag cb] data-readers] (edn/register-tag-parser! tag cb)))


;; Datomic compatibility layer

(def ^:private last-tempid (atom -1000000))


(defn tempid
  "Allocates and returns an unique temporary id (a negative integer). Ignores `part`. Returns `x` if it is specified.

   Exists for Datomic API compatibility. Prefer using negative integers directly if possible."
  ([part]
   (if (= part :db.part/tx)
     :db/current-tx
     (swap! last-tempid dec)))
  ([part x]
   (if (= part :db.part/tx)
     :db/current-tx
     x)))


(defn resolve-tempid
  "Does a lookup in tempids map, returning an entity id that tempid was resolved to.

   Exists for Datomic API compatibility. Prefer using map lookup directly if possible."
  [_db tempids tempid]
  (get tempids tempid))


(defn db
  "Returns the underlying database object from a connection. Note that Datalevin does not have \"db as a value\" feature, the returned object is NOT a database value, but a reference to the database object.

  Exists for Datomic API compatibility. "
  [conn]
  {:pre [(conn? conn)]}
  @conn)

;; schema

(defn schema
  "Return the schema"
  [conn]
  (s/schema ^Store (.-store ^DB @conn)))

  (defn update-schema
  "Update the schema of an open connection. `schema-update` is a map from
  attribute keywords to maps of corresponding properties. Return the updated
  schema.

  Example:

  (update-schema conn {:new/attr {:db/valueType :db.type/string}})"
  [conn schema-update]
  (let [^DB db (db conn)
        s      (s/set-schema ^Store (.-store db) schema-update)]
    (swap! conn (fn [db]
                  (assoc db
                         :schema s
                         :rschema (db/rschema s))))
    (schema conn)))

  (defonce ^:private connections (atom {}))

  (defn- add-conn [dir conn] (swap! connections assoc dir conn))

  (defn- new-conn
  [dir schema]
  (let [conn (if schema
               (create-conn dir schema)
               (create-conn dir))]
    (add-conn dir conn)
    conn))

  (defn get-conn
  "Obtain an open connection to a database. Create the database if it does not
  exist. Reuse the same connection if a connection to the same database already
  exists. Open the database if it is closed. Return the connection.

  See also [[create-conn]] and [[with-conn]]"
  ([dir]
   (get-conn dir nil))
  ([dir schema]
   (if-let [c (get @connections dir)]
     (if (closed? c) (new-conn dir schema) c)
     (new-conn dir schema))))

(defmacro with-conn
  "Evaluate body in the context of an connection to the database.

  If the database does not exist, this will create it. If it is closed,
  this will open it. However, the connection will be closed in the end of
  this call. If a database needs to be kept open, use `create-conn` and
  hold onto the returned connection. See also [[create-conn]] and [[get-conn]]

  `spec` is a vector of an identifier of the database connection, a data path
  string, and optionally a schema map.

  Example:

  (with-conn [conn \"my-data-path\"]
    ...conn...)

  (with-conn [conn \"my-data-path\" {:likes {:db/cardinality :db.cardinality/many}}]
    ...conn...)
  "
  [spec & body]
  `(let [dir#    ~(second spec)
         schema# ~(second (rest spec))
         conn#   (get-conn dir#)]
     (when schema# (update-schema conn# schema#))
     (try
       (let [~(first spec) conn#] ~@body)
       (finally
         (close conn#)))))

(defn transact
  "Same as [[transact!]], but returns an immediately realized future.

  Exists for Datomic API compatibility. Prefer using [[transact!]] if possible."
  ([conn tx-data] (transact conn tx-data nil))
  ([conn tx-data tx-meta]
   {:pre [(conn? conn)]}
   (let [res (transact! conn tx-data tx-meta)]
     #?(:cljs
        (reify
          IDeref
          (-deref [_] res)
          IDerefWithTimeout
          (-deref-with-timeout [_ _ _] res)
          IPending
          (-realized? [_] true))
        :clj
        (reify
          clojure.lang.IDeref
          (deref [_] res)
          clojure.lang.IBlockingDeref
          (deref [_ _ _] res)
          clojure.lang.IPending
          (isRealized [_] true))))))


;; ersatz future without proper blocking
#?(:cljs
   (defn- future-call [f]
     (let [res      (atom nil)
           realized (atom false)]
       (js/setTimeout #(do (reset! res (f)) (reset! realized true)) 0)
       (reify
         IDeref
         (-deref [_] @res)
         IDerefWithTimeout
         (-deref-with-timeout [_ _ timeout-val] (if @realized @res timeout-val))
         IPending
         (-realized? [_] @realized)))))


(defn transact-async
  "Calls [[transact!]] on a future thread pool, returning immediately."
  ([conn tx-data] (transact-async conn tx-data nil))
  ([conn tx-data tx-meta]
   {:pre [(conn? conn)]}
   (future-call #(transact! conn tx-data tx-meta))))


(defn- rand-bits [^long pow]
  (rand-int (bit-shift-left 1 pow)))

#?(:cljs
   (defn- to-hex-string [n l]
     (let [s (.toString n 16)
           c (count s)]
       (cond
         (> c l) (subs s 0 l)
         (< c l) (str (apply str (repeat (- l c) "0")) s)
         :else   s))))

(defn ^:no-doc squuid
  "Generates a UUID that grow with time. Such UUIDs will always go to the end  of the index and that will minimize insertions in the middle.

  Consist of 64 bits of current UNIX timestamp (in seconds) and 64 random bits (2^64 different unique values per second)."
  ([]
   (squuid #?(:clj  (System/currentTimeMillis)
              :cljs (.getTime (js/Date.)))))
  ([^long msec]
   #?(:clj
      (let [uuid     (UUID/randomUUID)
            time     (int (/ msec 1000))
            high     (.getMostSignificantBits uuid)
            low      (.getLeastSignificantBits uuid)
            new-high (bit-or (bit-and high 0x00000000FFFFFFFF)
                             (bit-shift-left time 32)) ]
        (UUID. new-high low))
      :cljs
      (uuid
        (str
          (-> (int (/ msec 1000))
              (to-hex-string 8))
          "-" (-> (rand-bits 16) (to-hex-string 4))
          "-" (-> (rand-bits 16) (bit-and 0x0FFF) (bit-or 0x4000) (to-hex-string 4))
          "-" (-> (rand-bits 16) (bit-and 0x3FFF) (bit-or 0x8000) (to-hex-string 4))
          "-" (-> (rand-bits 16) (to-hex-string 4))
          (-> (rand-bits 16) (to-hex-string 4))
          (-> (rand-bits 16) (to-hex-string 4)))))))

(defn ^:no-doc squuid-time-millis
  "Returns time that was used in [[squuid]] call, in milliseconds, rounded to the closest second."
  [uuid]
  #?(:clj (-> (.getMostSignificantBits ^UUID uuid)
              (bit-shift-right 32)
              (* 1000))
     :cljs (-> (subs (str uuid) 0 8)
               (js/parseInt 16)
               (* 1000))))

;; -------------------------------------

;; key value store API

(def ^{:arglists '([kv])
       :doc      "Key of a key value pair"}
  k l/k)

(def ^{:arglists '([kv])
       :doc      "Value of a key value pair"}
  v l/v)

(def ^{:arglists '([dir])
       :doc      "Open a LMDB key-value database, return the connection.

  `dir` is a string directory path in which the data are to be stored;

  Will detect the platform this code is running in, and dispatch accordingly.

  Please note:

  > LMDB uses POSIX locks on files, and these locks have issues if one process
  > opens a file multiple times. Because of this, do not mdb_env_open() a file
  > multiple times from a single process. Instead, share the LMDB environment
  > that has opened the file across all threads. Otherwise, if a single process
  > opens the same environment multiple times, closing it once will remove all
  > the locks held on it, and the other instances will be vulnerable to
  > corruption from other processes.'

  Therefore, a LMDB connection should be managed as a stateful resource.
  Multiple connections to the same DB in the same process are not recommended.
  The recommendation is to use a mutable state management library, for
  example, in Clojure, use [component](https://github.com/stuartsierra/component),
  [mount](https://github.com/tolitius/mount),
  [integrant](https://github.com/weavejester/integrant), or something similar
  to hold on to and manage the connection. "}
  open-kv l/open-kv)

(def ^{:arglists '([db])
       :doc      "Close this key-value store"}
  close-kv l/close-kv)

(def ^{:arglists '([db])
       :doc      "Return true if this key-value store is closed"}
  closed-kv? l/closed-kv?)

(def ^{:arglists '([db])
       :doc      "Return the directory path of the key-value store"}
  dir l/dir)

(def ^{:arglists '([db]
                   [db dbi-name]
                   [db dbi-name key-size]
                   [db dbi-name key-size val-size]
                   [db dbi-name key-size val-size flags])
       :doc      "Open a named DBI (i.e. sub-db) or unamed main DBI in the key-value store"}
  open-dbi l/open-dbi)

(def ^{:arglists '([db dbi-name])
       :doc      "Clear data in the DBI (i.e sub-database) of the key-value store, but leave it open"}
  clear-dbi l/clear-dbi)

(def ^{:arglists '([db dbi-name])
       :doc      "Clear data in the DBI (i.e. sub-database) of the key-value store, then delete it"}
  drop-dbi l/drop-dbi)

(def ^{:arglists '([db])
       :doc      "List the names of the sub-databases in the key-value store"}
  list-dbis l/list-dbis)

(def ^{:arglists '([db dest] [db dest compact?])
       :doc      "Copy the database to a destination directory path, optionally compact while copying, default not compact. "}
  copy l/copy)

(def ^{:arglists '([db] [db dbi-name])
       :doc      "Return the statitics of the unnamed top level database or a named DBI (i.e. sub-database) of the key-value store as a map:
  * `:psize` is the size of database page
  * `:depth` is the depth of the B-tree
  * `:branch-pages` is the number of internal pages
  * `:leaf-pages` is the number of leaf pages
  * `:overflow-pages` is the number of overflow-pages
  * `:entries` is the number of data entries"}
  stat l/stat)

(def ^{:arglists '([db dbi-name])
       :doc      "Get the number of data entries in a DBI (i.e. sub-db) of the key-value store"}
  entries l/entries)

(def ^{:arglists '([db txs])
       :doc      "Update DB, insert or delete key value pairs in the key-value store.

  `txs` is a seq of `[op dbi-name k v k-type v-type append?]`
  when `op` is `:put`, for insertion of a key value pair `k` and `v`;
  or `[op dbi-name k k-type]` when `op` is `:del`, for deletion of key `k`;

  `dbi-name` is the name of the DBI (i.e sub-db) to be transacted, a string.

  `k-type`, `v-type` and `append?` are optional.

  `k-type` indicates the data type of `k`, and `v-type` indicates the data type
  of `v`. The allowed data types are described in [[put-buffer]]

  Set `append?` to true when the data is sorted to gain better write performance.

  Example:

  (transact-kv
    lmdb
    [ [:put \"a\" 1 2]
     [:put \"a\" 'a 1]
     [:put \"a\" 5 {}]
     [:put \"a\" :annunaki/enki true :attr :data]
     [:put \"a\" :datalevin [\"hello\" \"world\"]]
     [:put \"a\" 42 (d/datom 1 :a/b {:id 4}) :long :datom]
     [:put \"a\" (byte 0x01) #{1 2} :byte :data]
     [:put \"a\" (byte-array [0x41 0x42]) :bk :bytes :data]
     [:put \"a\" [-1 -235254457N] 5]
     [:put \"a\" :a 4]
     [:put \"a\" :bv (byte-array [0x41 0x42 0x43]) :data :bytes]
     [:put \"a\" :long 1 :data :long]
     [:put \"a\" 2 3 :long :long]
     [:del \"a\" 1]
     [:del \"a\" :non-exist] ])"}
  transact-kv l/transact-kv)

(def ^{:arglists '([db dbi-name k]
                   [db dbi-name k k-type]
                   [db dbi-name k k-type v-type]
                   [db dbi-name k k-type v-type ignore-key?])
       :doc      "Get kv pair of the specified key `k` in the key-value store.

  `k-type` and `v-type` are data types of `k` and `v`, respectively.
  The allowed data types are described in [[read-buffer]].

  If `ignore-key?` is true (default `true`), only return the value,
  otherwise return `[k v]`, where `v` is the value

  Examples:

  (get-value lmdb \"a\" 1)
  ;;==> 2

  ;; specify data types
  (get-value lmdb \"a\" :annunaki/enki :attr :data)
  ;;==> true

  ;; return key value pair
  (get-value lmdb \"a\" 1 :data :data false)
  ;;==> [1 2]

  ;; key doesn't exist
  (get-value lmdb \"a\" 2)
  ;;==> nil "}
  get-value l/get-value)

(def ^{:arglists '([db dbi-name k-range]
                   [db dbi-name k-range k-type]
                   [db dbi-name k-range k-type v-type]
                   [db dbi-name k-range k-type v-type ignore-key?])
       :doc      "Return the first kv pair in the specified key range in the key-value store;

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

    `k-type` and `v-type` are data types of `k` and `v`, respectively.
     The allowed data types are described in [[read-buffer]].

     Only the value will be returned if `ignore-key?` is `true`;
     If value is to be ignored, put `:ignore` as `v-type`

     If both key and value are ignored, return true if found an entry, otherwise
     return nil.

     Examples:


              (get-first lmdb \"c\" [:all] :long :long)
              ;;==> [0 1]

              ;; ignore value
              (get-first lmdb \"c\" [:all-back] :long :ignore)
              ;;==> [999 nil]

              ;; ignore key
              (get-first lmdb \"a\" [:greater-than 9] :long :data true)
              ;;==> {:some :data}

              ;; ignore both, this is like testing if the range is empty
              (get-first lmdb \"a\" [:greater-than 5] :long :ignore true)
              ;;==> true"}
  get-first l/get-first)

(def ^{:arglists '([db dbi-name k-range]
                   [db dbi-name k-range k-type]
                   [db dbi-name k-range k-type v-type]
                   [db dbi-name k-range k-type v-type ignore-key?])
       :doc      "Return a seq of kv pairs in the specified key range in the key-value store;

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

    `k-type` and `v-type` are data types of `k` and `v`, respectively.
     The allowed data types are described in [[read-buffer]].

     Only the value will be returned if `ignore-key?` is `true`,
     default is `false`;

     If value is to be ignored, put `:ignore` as `v-type`

     Examples:


              (get-range lmdb \"c\" [:at-least 9] :long :long)
              ;;==> [[10 11] [11 15] [13 14]]

              ;; ignore value
              (get-range lmdb \"c\" [:all-back] :long :ignore)
              ;;==> [[999 nil] [998 nil]]

              ;; ignore keys, only return values
              (get-range lmdb \"a\" [:closed 9 11] :long :long true)
              ;;==> [10 11 12]

              ;; out of range
              (get-range lmdb \"c\" [:greater-than 1500] :long :ignore)
              ;;==> [] "}
  get-range l/get-range)

(def ^{:arglists '([db dbi-name k-range]
                   [db dbi-name k-range k-type])
       :doc      "Return the number of kv pairs in the specified key range in the key-value store, does not process the kv pairs.

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

    `k-type` and `v-type` are data types of `k` and `v`, respectively.
     The allowed data types are described in [[read-buffer]].

     Examples:


              (range-count lmdb \"c\" [:at-least 9] :long)
              ;;==> 10 "}
  range-count l/range-count)

(def ^{:arglists '([db dbi-name pred k-range]
                   [db dbi-name pred k-range k-type]
                   [db dbi-name pred k-range k-type v-type]
                   [db dbi-name pred k-range k-type v-type ignore-key?])
       :doc      "Return the first kv pair that has logical true value of `(pred x)` in the key-value store,
     where `pred` is a function, `x` is an `IKV` fetched from the store,
     with both key and value fields being a `ByteBuffer`.

     `pred` can use [[read-buffer]] to read the content.

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

    `k-type` and `v-type` are data types of `k` and `v`, respectively.
     The allowed data types are described in [[read-buffer]].

     Only the value will be returned if `ignore-key?` is `true`;
     If value is to be ignored, put `:ignore` as `v-type`

     Examples:

              (def pred (fn [kv]
                         (let [^long lk (read-buffer (k kv) :long)]
                          (> lk 15)))

              (get-some lmdb \"c\" pred [:less-than 20] :long :long)
              ;;==> [16 2]

              ;; ignore key
              (get-some lmdb \"c\" pred [:greater-than 9] :long :data true)
              ;;==> 16 "}
  get-some l/get-some)

(def ^{:arglists '([db dbi-name pred k-range]
                   [db dbi-name pred k-range k-type]
                   [db dbi-name pred k-range k-type v-type]
                   [db dbi-name pred k-range k-type v-type ignore-key?])
       :doc      "Return a seq of kv pair in the specified key range in the key-value store, for only those
     return true value for `(pred x)`, where `pred` is a function, and `x`
     is an `IKV`, with both key and value fields being a `ByteBuffer`.

     `pred` can use [[read-buffer]] to read the buffer content.

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

    `k-type` and `v-type` are data types of `k` and `v`, respectively.
     The allowed data types are described in [[read-buffer]].

     Only the value will be returned if `ignore-key?` is `true`;
     If value is to be ignored, put `:ignore` as `v-type`

     Examples:

              (def pred (fn [kv]
                         (let [^long lk (read-buffer (k kv) :long)]
                          (> lk 15)))

              (range-filter lmdb \"a\" pred [:less-than 20] :long :long)
              ;;==> [[16 2] [17 3]]

              ;; ignore key
              (range-filter lmdb \"a\" pred [:greater-than 9] :long :data true)
              ;;==> [16 17] "}
  range-filter l/range-filter)

(def ^{:arglists '([db dbi-name pred k-range]
                   [db dbi-name pred k-range k-type])
       :doc      "Return the number of kv pairs in the specified key range in the key-value store, for only those
     return true value for `(pred x)`, where `pred` is a function, and `x`
     is an `IMapEntry`, with both key and value fields being a `ByteBuffer`.
     Does not process the kv pairs.

     `pred` can use [[read-buffer]] to read the buffer content.

    `k-type` indicates data type of `k` and the allowed data types are described
    in [[read-buffer]].

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

     Examples:


              (def pred (fn [kv]
                         (let [^long lk (read-buffer (k kv) :long)]
                          (> lk 15)))

              (range-filter-count lmdb \"a\" pred [:less-than 20] :long)
              ;;==> 3"}
  range-filter-count l/range-filter-count)

;; byte buffer

(def ^{:arglists '([bf x] [bf x x-type])
       :doc      "Put the given type of data `x` in buffer `bf`. `x-type` can be
    one of the following data types:

    - `:data` (default), arbitrary EDN data, avoid this as keys for range queries
    - `:string`, UTF-8 string
    - `:int`, 32 bits integer
    - `:long`, 64 bits integer
    - `:id`, 64 bits integer, not prefixed with a type header
    - `:float`, 32 bits IEEE754 floating point number
    - `:double`, 64 bits IEEE754 floating point number
    - `:byte`, single byte
    - `:bytes`, byte array
    - `:keyword`, EDN keyword
    - `:symbol`, EDN symbol
    - `:boolean`, `true` or `false`
    - `:instant`, timestamp, same as `java.util.Date`
    - `:uuid`, UUID, same as `java.util.UUID`

  or one of the following Datalog specific data types

    - `:datom`
    - `:attr`
    - `:eav`
    - `:ave`
    - `:vea`

  If the value is to be put in a LMDB key buffer, it must be less than
  511 bytes."}
  put-buffer b/put-buffer)

(def ^{:arglists '([bf] [bf v-type])
       :doc      "Get the given type of data from buffer `bf`, `v-type` can be
one of the following data types:

    - `:data` (default), arbitrary EDN data
    - `:string`, UTF-8 string
    - `:int`, 32 bits integer
    - `:long`, 64 bits integer
    - `:id`, 64 bits integer, not prefixed with a type header
    - `:float`, 32 bits IEEE754 floating point number
    - `:double`, 64 bits IEEE754 floating point number
    - `:byte`, single byte
    - `:bytes`, an byte array
    - `:keyword`, EDN keyword
    - `:symbol`, EDN symbol
    - `:boolean`, `true` or `false`
    - `:instant`, timestamp, same as `java.util.Date`
    - `:uuid`, UUID, same as `java.util.UUID`

  or one of the following Datalog specific data types

    - `:datom`
    - `:attr`
    - `:eav`
    - `:ave`
    - `:vea`"}
  read-buffer b/read-buffer)
