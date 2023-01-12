(ns datalevin.core
  "User facing API for Datalevin library features"
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   [datalevin.util :as u]
   [datalevin.remote :as r]
   [datalevin.search :as sc]
   [datalevin.db :as db]
   [datalevin.datom :as dd]
   [datalevin.storage :as s]
   [datalevin.constants :as c]
   [datalevin.lmdb :as l]
   [datalevin.pull-parser]
   [datalevin.pull-api :as dp]
   [datalevin.query :as dq]
   [datalevin.built-ins :as dbq]
   [datalevin.entity :as de]
   [datalevin.bits :as b])
  (:import
   [datalevin.entity Entity]
   [datalevin.storage Store]
   [datalevin.db DB]
   [datalevin.datom Datom]
   [datalevin.remote DatalogStore KVStore]
   [java.util UUID]))

(if (u/graal?)
  (require 'datalevin.binding.graal)
  (require 'datalevin.binding.java))

;; Entities

(defn entity
  "Retrieves an entity by its id from a Datalog database. Entities
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
  attribute is marked as `:db/isComponent`:

      (:_component-ref (entity db 2)) ; => {:db/id 1}

  Entity gotchas:

    - Entities print as map, but are not exactly maps (they have
    compatible get interface though).
    - Entities retain reference to the database.
    - Creating an entity by id is very cheap, almost no-op
    (attributes are looked up on demand).
    - Comparing entities just compares their ids. Be careful when
    comparing entities taken from different dbs or from different versions of the
    same db.
    - Accessed entity attributes are cached on entity itself (except
    backward references).
    - When printing, only cached attributes (the ones you have accessed
      before) are printed. See [[touch]]."
  [db eid]
  {:pre [(db/db? db)]}
  (de/entity db eid))

(def ^{:arglists '([ent attr value])
       :doc      "Add an attribute value to an entity of a Datalog database"}
  add de/add)

(def ^{:arglists '([ent attr][ent attr value])
       :doc      "Remove an attribute from an entity of a Datalog database"}
  retract de/retract)

(defn entid
  "Given lookup ref `[unique-attr value]`, returns numberic entity id.

  `db` is a Datalog database.

  If entity does not exist, returns `nil`.

  For numeric `eid` returns `eid` itself (does not check for entity
  existence in that case)."
  [db eid]
  {:pre [(db/db? db)]}
  (db/entid db eid))

(defn entity-db
  "Returns a Datalog db that entity was created from."
  [^Entity entity]
  {:pre [(de/entity? entity)]}
  (.-db entity))

(def ^{:arglists '([e])
       :doc      "Forces all entity attributes to be eagerly fetched and cached.
Only usable for debug output.

  Usage:

             (entity db 1) ; => {:db/id 1}
             (touch (entity db 1)) ; => {:db/id 1, :dislikes [:pie], :likes [:pizza]}
             "}
  touch de/touch)


;; Pull API

(def ^{:arglists '([db pattern id] [db pattern id opts])
       :doc      "Fetches data from a Datalog database using recursive declarative
  description. See [docs.datomic.com/on-prem/pull.html](https://docs.datomic.com/on-prem/pull.html).

  Unlike [[entity]], returns plain Clojure map (not lazy).

  Supported opts:

   `:visitor` a fn of 4 arguments, will be called for every entity/attribute pull touches

   (:db.pull/attr     e   a   nil) - when pulling a normal attribute, no matter if it has value or not
   (:db.pull/wildcard e   nil nil) - when pulling every attribute on an entity
   (:db.pull/reverse  nil a   v  ) - when pulling reverse attribute

  Usage:

                (pull db [:db/id, :name, :likes, {:friends [:db/id :name]}] 1)
                ; => {:db/id   1,
                ;     :name    \"Ivan\"
                ;     :likes   [:pizza]
                ;     :friends [{:db/id 2, :name \"Oleg\"}]}"}
  pull dp/pull)

(def ^{:arglists '([db pattern ids] [db pattern ids opts])
       :doc
       "Same as [[pull]], but accepts sequence of ids and returns
  sequence of maps.

  Usage:

             (pull-many db [:db/id :name] [1 2])
             ; => [{:db/id 1, :name \"Ivan\"}
             ;     {:db/id 2, :name \"Oleg\"}]"}
  pull-many dp/pull-many)

;; Query

(defn- only-remote-db
  "Return [remote-db [updated-inputs]] if the inputs contain only one db
  and its backing store is a remote one, where the remote-db in the inputs is
  replaced by `:remote-db-placeholder, otherwise return nil"
  [inputs]
  (let [dbs (filter db/db? inputs)]
    (when-let [rdb (first dbs)]
      (let [rstore (.-store ^DB rdb)]
        (when (and (= 1 (count dbs)) (instance? DatalogStore rstore))
          [rstore (vec (replace {rdb :remote-db-placeholder} inputs))])))))

(defn q
  "Executes a Datalog query. See [docs.datomic.com/on-prem/query.html](https://docs.datomic.com/on-prem/query.html).

          Usage:

          ```
          (q '[:find ?value
               :where [_ :likes ?value]
               :timeout 5000]
             db)
          ; => #{[\"fries\"] [\"candy\"] [\"pie\"] [\"pizza\"]}
          ```"
  [query & inputs]
  (if-let [[store inputs'] (only-remote-db inputs)]
    (r/q store query inputs')
    (apply dq/q query inputs)))

;; Creating DB

(def ^{:arglists '([] [dir] [dir schema] [dir schema opts])
       :doc      "Open a Datalog database at the given location.

`dir` could be a local directory path or a dtlv connection URI string. Creates an empty database there if it does not exist yet. Update the schema if one is given. Return reference to the database.

 `opts` map has keys:

   * `:validate-data?`, a boolean, instructing the system to validate data type during transaction. Default is `false`.

   * `:auto-entity-time?`, a boolean indicating whether to maintain `:db/created-at` and `:db/updated-at` values for each entity. Default is `false`.

   * `:search-opts`, an option map that will be passed to the built-in full-text search engine

   * `:kv-opts`, an option map that will be passed to the underlying kV store

   * `:client-opts` is the option map passed to the client if `dir` is a remote URI string.


  Usage:

             (empty-db)

             (empty-db \"/tmp/test-empty-db\")

             (empty-db \"/tmp/test-empty-db\" {:likes {:db/cardinality :db.cardinality/many}})

             (empty-db \"dtlv://datalevin:secret@example.host/mydb\" {} {:auto-entity-time? true :search-engine {:analyzer blank-space-analyzer}})"}
  empty-db db/empty-db)


(def ^{:arglists '([x])
       :doc      "Returns `true` if the given value is a Datalog database. Has the side effect of updating the cache of the db to the most recent. Return `false` otherwise. "}
  db? db/db?)


(def ^{:arglists '([e a v] [e a v tx] [e a v tx added])
       :doc      "Low-level fn to create raw datoms in a Datalog db.

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
       :doc      "Return the attribute of a datom"}
  datom-a dd/datom-a)

(def ^{:arglists '([d])
       :doc      "Return the value of a datom"}
  datom-v dd/datom-v)

(def ^{:arglists '([datoms] [datoms dir] [datoms dir schema] [datoms dir schema opts])
       :doc      "Low-level function for creating a Datalog database quickly from a trusted sequence of datoms, useful for bulk data loading. `dir` could be a local directory path or a dtlv connection URI string. Does no validation on inputs, so `datoms` must be well-formed and match schema.

 `opts` map has keys:

   * `:validate-data?`, a boolean, instructing the system to validate data type during transaction. Default is `false`.

   * `:auto-entity-time?`, a boolean indicating whether to maintain `:db/created-at` and `:db/updated-at` values for each entity. Default is `false`.

   * `:search-opts`, an option map that will be passed to the built-in full-text search engine

   * `:kv-opts`, an option map that will be passed to the underlying kV store

             See also [[datom]], [[new-search-engine]]."}
  init-db db/init-db)

(def ^{:arglists '([db])
       :doc      "Close the Datalog database"}
  close-db db/close-db)

;; Changing DB

(def ^{:arglists '([db])
       :doc      "Rollback writes of the transaction from inside [[with-transaction-kv]]."}
  abort-transact-kv l/abort-transact-kv)

(defmacro with-transaction-kv
  "Evaluate body within the context of a single new read/write transaction,
  ensuring atomicity of key-value operations.

  `db` is a new identifier of the kv database with a new read/write transaction attached, and `orig-db` is the original kv database.

  `body` should refer to `db`.

  Example:

          (with-transaction-kv [kv lmdb]
            (let [^long now (get-value kv \"a\" :counter)]
              (transact-kv kv [[:put \"a\" :counter (inc now)]])
              (get-value kv \"a\" :counter)))"
  [[db orig-db] & body]
  `(locking (l/write-txn ~orig-db)
     (let [writing# (l/writing? ~orig-db)]
       (try
         (let [~db (if writing# ~orig-db (l/open-transact-kv ~orig-db))]
           (try
             ~@body
             (catch Exception ~'e
               (if (and (:resized (ex-data ~'e)) (not writing#))
                 (do ~@body)
                 (throw ~'e)))))
         (finally
           (when-not writing# (l/close-transact-kv ~orig-db)))))))

(defmacro with-transaction
  "Evaluate body within the context of a single new read/write transaction,
  ensuring atomicity of Datalog database operations.

  `conn` is a new identifier of the Datalog database connection with a new read/write transaction attached, and `orig-conn` is the original database connection.

  `body` should refer to `conn`.

  Example:

          (with-transaction [cn conn]
            (let [query  '[:find ?c .
                           :in $ ?e
                           :where [?e :counter ?c]]
                  ^long now (q query @cn 1)]
              (transact! cn [{:db/id 1 :counter (inc now)}])
              (q query @cn 1))) "
  [[conn orig-conn] & body]
  `(locking (l/write-txn (.-store ^DB (deref ~orig-conn)))
     (let [s# (.-store ^DB (deref ~orig-conn))]
       (if (instance? DatalogStore s#)
         (let [res# (if (l/writing? s#)
                      (let [~conn ~orig-conn]
                        ~@body)
                      (let [s1# (r/open-transact s#)
                            w#  #(let [~conn (atom (db/new-db s1#)
                                                   :meta (meta ~orig-conn))]
                                   ~@body) ]
                        (try
                          (w#)
                          (catch Exception ~'e
                            (if (:resized (ex-data ~'e))
                              (w#)
                              (throw ~'e)))
                          (finally (r/close-transact s#)))))]
           (reset! ~orig-conn (db/new-db s#))
           res#)
         (let [kv#   (.-lmdb ^Store s#)
               s1#   (volatile! nil)
               res1# (with-transaction-kv [kv1# kv#]
                       (let [conn1# (atom (db/new-db (s/transfer s# kv1#))
                                          :meta (meta ~orig-conn))
                             res#   (let [~conn conn1#]
                                      ~@body)]
                         (vreset! s1# (.-store ^DB (deref conn1#)))
                         res#))]
           (reset! ~orig-conn (db/new-db (s/transfer (deref s1#) kv#)))
           res1#)))))

(def ^{:arglists '([conn])
       :doc      "Rollback writes of the transaction from inside [[with-transaction]]."}
  abort-transact db/abort-transact)

(defn ^:no-doc with
  "Same as [[transact!]]. Returns transaction report (see [[transact!]])."
  ([db tx-data] (with db tx-data {} false))
  ([db tx-data tx-meta] (with db tx-data tx-meta false))
  ([db tx-data tx-meta simulated?]
   {:pre [(db/db? db)]}
   (db/transact-tx-data (db/->TxReport db db [] {} tx-meta)
                        tx-data simulated?)))

(def ^{:arglists '([db tx-data])
       :doc      "Returns a transaction report without side-effects. Useful for obtaining
  the would-be db state and the would-be set of datoms."}
  tx-data->simulated-report db/tx-data->simulated-report)

(defn ^:no-doc db-with
  "Applies transaction. Return the Datalog db."
  [db tx-data]
  {:pre [(db/db? db)]}
  (:db-after (with db tx-data)))

;; Index lookups

(defn datoms
  "Index lookup in Datalog db. Returns a sequence of datoms (lazy iterator over actual DB index) which components (e, a, v) match passed arguments.

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

(defn fulltext-datoms
  "Return datoms that found by the given fulltext search query"
  ([db query]
   (fulltext-datoms db query nil))
  ([^DB db query opts]
   (let [store (.-store db)]
     (if (instance? DatalogStore store)
       (r/fulltext-datoms store query opts)
       (dbq/fulltext db query opts)))))

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
  "Returns `true` if this is an open connection to a local Datalog db, `false`
  otherwise."
  [conn]
  (and #?(:clj  (instance? clojure.lang.IDeref conn)
          :cljs (satisfies? cljs.core/IDeref conn))
       (db/db? @conn)))

(defn conn-from-db
  "Creates a mutable reference to a given Datalog database. See [[create-conn]]."
  [db]
  {:pre [(db/db? db)]}
  (atom db :meta { :listeners (atom {}) }))

(defn conn-from-datoms
  "Create a mutable reference to a Datalog database with the given datoms added to it.
  `dir` could be a local directory path or a dtlv connection URI string.

  `opts` map has keys:

   * `:validate-data?`, a boolean, instructing the system to validate data type during transaction. Default is `false`.

   * `:auto-entity-time?`, a boolean indicating whether to maintain `:db/created-at` and `:db/updated-at` values for each entity. Default is `false`.

   * `:search-opts`, an option map that will be passed to the built-in full-text search engine

   * `:kv-opts`, an option map that will be passed to the underlying kV store

  "
  ([datoms] (conn-from-db (init-db datoms)))
  ([datoms dir] (conn-from-db (init-db datoms dir)))
  ([datoms dir schema] (conn-from-db (init-db datoms dir schema)))
  ([datoms dir schema opts] (conn-from-db (init-db datoms dir schema opts))))


(defn create-conn
  "Creates a mutable reference (a “connection”) to a Datalog database at the given
  location and opens the database. Creates the database if it doesn't
  exist yet. Update the schema if one is given. Return the connection.

  `dir` could be a local directory path or a dtlv connection URI string.

  `opts` map may have keys:

   * `:validate-data?`, a boolean, instructing the system to validate data type during transaction. Default is `false`.

   * `:auto-entity-time?`, a boolean indicating whether to maintain `:db/created-at` and `:db/updated-at` values for each entity. Default is `false`.

   * `:search-opts`, an option map that will be passed to the built-in full-text search engine

   * `:kv-opts`, an option map that will be passed to the underlying kV store

   * `:client-opts` is the option map passed to the client if `dir` is a remote URI string.

  Please note that the connection should be managed like a stateful resource.
  Application should hold on to the same connection rather than opening
  multiple connections to the same database in the same process.

  Connections are lightweight in-memory structures (atoms). To access underlying DB, call [[db]] on it.

  See also [[transact!]], [[db]], [[close]], [[get-conn]], and [[open-kv]].


  Usage:


             (create-conn)

             (create-conn \"/tmp/test-create-conn\")

             (create-conn \"/tmp/test-create-conn\" {:likes {:db/cardinality :db.cardinality/many}})

             (create-conn \"dtlv://datalevin:secret@example.host/mydb\" {} {:auto-entity-time? true})"
  ([] (conn-from-db (empty-db)))
  ([dir] (conn-from-db (empty-db dir)))
  ([dir schema] (conn-from-db (empty-db dir schema)))
  ([dir schema opts] (conn-from-db (empty-db dir schema opts))))

(defn close
  "Close the connection to a Datalog db"
  [conn]
  (when-let [store (.-store ^DB @conn)]
    (s/close ^Store store))
  nil)

(defn closed?
  "Return true when the underlying Datalog DB is closed or when `conn` is nil or contains nil"
  [conn]
  (or (nil? conn)
      (nil? @conn)
      (s/closed? ^Store (.-store ^DB @conn))))

(defn ^:no-doc -transact! [conn tx-data tx-meta]
  (let [report (with-transaction [c conn]
                 (assert (conn? c))
                 (with @c tx-data tx-meta))]
    (assoc report :db-after @conn)))

(defn transact!
  "Applies transaction to the underlying Datalog database of a connection.

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
      ; equal tempids will be replaced with the same yet unused yet entid)
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

      ; ref attributes can be specified as nested map, that will create nested entity as well
      (transact! conn [{:db/id  -1
                        :name   \"Oleg\"
                        :friend {:db/id -2
                                 :name \"Sergey\"}}])

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
                       [:db/add 296 :friend -1]])"
  ([conn tx-data] (transact! conn tx-data nil))
  ([conn tx-data tx-meta]
   ;; {:pre [(conn? conn)]}
   (let [report (-transact! conn tx-data tx-meta)]
     (doseq [[_ callback] (some-> (:listeners (meta conn)) (deref))]
       (callback report))
     report)))


(defn reset-conn!
  "Forces underlying `conn` value to become a Datalog `db`. Will generate a tx-report that
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
  "Listen for changes on the given connection to a Datalog db. Whenever a transaction is applied
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
  "Returns the underlying Datalog database object from a connection. Note that Datalevin does not have \"db as a value\" feature, the returned object is NOT a database value, but a reference to the database object.

  Exists for Datomic API compatibility. "
  [conn]
  {:pre [(conn? conn)]}
  @conn)

(defn opts
  "Return the option map of the Datalog DB"
  [conn]
  {:pre [(conn? conn)]}
  (s/opts ^Store (.-store ^DB @conn)))

(defn schema
  "Return the schema of Datalog DB"
  [conn]
  {:pre [(conn? conn)]}
  (s/schema ^Store (.-store ^DB @conn)))

(defn update-schema
  "Update the schema of an open connection to a Datalog db.

  * `schema-update` is a map from attribute keywords to maps of corresponding
  properties.

  * `del-attrs` is a set of attributes to be removed from the schema, if there is
  no datoms associated with them, otherwise an exception will be thrown.

  * `rename-map` is a map of old attributes to new attributes, for renaming
  attributes

  Return the updated schema.

  Example:

        (update-schema conn {:new/attr {:db/valueType :db.type/string}})
        (update-schema conn {:new/attr {:db/valueType :db.type/string}}
                            #{:old/attr1 :old/attr2})
        (update-schema conn nil nil {:old/attr :new/attr}) "
  ([conn schema-update]
   (update-schema conn schema-update nil nil))
  ([conn schema-update del-attrs]
   (update-schema conn schema-update del-attrs nil))
  ([conn schema-update del-attrs rename-map]
   {:pre [(conn? conn)]}
   (let [^DB db       (db conn)
         ^Store store (.-store db)]
     (s/set-schema store schema-update)
     (doseq [attr del-attrs] (s/del-attr store attr))
     (doseq [[old new] rename-map] (s/rename-attr store old new))
     (schema conn))))

(defonce ^:private connections (atom {}))

(defn- add-conn [dir conn] (swap! connections assoc dir conn))

(defn- new-conn
  [dir schema opts]
  (let [conn (create-conn dir schema opts)]
    (add-conn dir conn)
    conn))

(defn get-conn
  "Obtain an open connection to a Datalog database. `dir` could be a local directory path or a dtlv connection URI string. Create the database if it does not exist. Reuse the same connection if a connection to the same database already exists. Open the database if it is closed. Return the connection.

  See also [[create-conn]] and [[with-conn]]"
  ([dir]
   (get-conn dir nil nil))
  ([dir schema]
   (get-conn dir schema nil))
  ([dir schema opts]
   (if-let [c (get @connections dir)]
     (if (closed? c) (new-conn dir schema opts) c)
     (new-conn dir schema opts))))

(defmacro with-conn
  "Evaluate body in the context of an connection to the Datalog database.

  If the database does not exist, this will create it. If it is closed,
  this will open it. However, the connection will be closed in the end of
  this call. If a database needs to be kept open, use `create-conn` and
  hold onto the returned connection. See also [[create-conn]] and [[get-conn]]

  `spec` is a vector of an identifier of the database connection, a path or
  dtlv URI string, and optionally a schema map.

  Example:

          (with-conn [conn \"my-data-path\"]
            ;; body)

          (with-conn [conn \"my-data-path\" {:likes {:db/cardinality :db.cardinality/many}}]
            ;; body)
  "
  [spec & body]
  `(let [dir#    ~(second spec)
         schema# ~(second (rest spec))
         opts#   ~(second (rest (rest spec)))
         conn#   (get-conn dir# schema# opts#)]
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

(defn open-kv
  "Open a LMDB key-value database, return the connection.

  `dir` is a directory path or a dtlv connection URI string.
  `opts` is an option map that may have the following keys:
  * `:mapsize` is the initial size of the database. This will be expanded as needed
  * `:flags` is a vector of keywords corresponding to LMDB environment flags, e.g.
     `:rdonly-env` for MDB_RDONLY_ENV, `:nosubdir` for MDB_NOSUBDIR, and so on. See [LMDB Documentation](http://www.lmdb.tech/doc/group__mdb__env.html)
  * `:temp?` a boolean, indicating if this db is temporary, if so, the file will be deleted on JVM exit.
  * `:client-opts` is the option map passed to the client if `dir` is a remote server URI string.
  * `:spill-opts` is the option map that controls the spill-to-disk behavior for `get-range` and `range-filter` functions, which may have the following keys:
      - `:spill-threshold`, memory pressure in percentage of JVM `-Xmx` (default 80), above which spill-to-disk will be triggered.
      - `:spill-root`, a file directory, in which the spilled data is written (default is the system temporary directory).


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
  to hold on to and manage the connection. "
  ([dir]
   (open-kv dir nil))
  ([dir opts]
   (if (r/dtlv-uri? dir)
     (r/open-kv dir opts)
     (l/open-kv dir opts))))

(def ^{:arglists '([db])
       :doc      "Close this key-value store"}
  close-kv l/close-kv)

(def ^{:arglists '([db])
       :doc      "Return true if this key-value store is closed"}
  closed-kv? l/closed-kv?)

(def ^{:arglists '([db])
       :doc      "Return the path or URI string of the key-value store"}
  dir l/dir)

(def ^{:arglists '([db dbi-name]
                   [db dbi-name opts])
       :doc      "Open a named DBI (i.e. sub-db) in the key-value store. `opts` is an option map that may have the following keys:

      * `:validate-data?`, a boolean, instructing the system to validate data type during transaction. Default is `false`.

      * `:key-size` is the max size of the key in bytes, cannot be greater than 511, default is 511.

      * `:val-size` is the default size of the value in bytes, Datalevin will automatically increase the size if a larger value is transacted.

      * `:flags` is a vector of LMDB Dbi flag keywords, may include `:reversekey`, `:dupsort`, `integerkey`, `dupfixed`, `integerdup`, `reversedup`, or `create`, default is `[:create]`, see [LMDB documentation](http://www.lmdb.tech/doc/group__mdb__dbi__open.html)."}
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

(defn copy
  "Copy a Datalog or key-value database to a destination directory path, optionally compact while copying, default not compact. "
  ([db dest]
   (copy db dest false))
  ([db dest compact?]
   (cond
     (instance? datalevin.db.DB db)
     (l/copy (.-lmdb ^Store (.-store ^DB db)) dest compact?)
     (satisfies? l/ILMDB db)
     (l/copy db dest compact?)
     :else (u/raise "Can only copy a local database." {}))))

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

  `txs` is a seq of Clojure vectors, `[op dbi-name k v k-type v-type flags]`
  when `op` is `:put`, for insertion of a key value pair `k` and `v`;
  or `[op dbi-name k k-type]` when `op` is `:del`, for deletion of key `k`;

  `dbi-name` is the name of the DBI (i.e sub-db) to be transacted, a string.

  `k-type`, `v-type` and `flags` are optional.

  `k-type` indicates the data type of `k`, and `v-type` indicates the data type
  of `v`. The allowed data types are described in [[put-buffer]]

  `:flags` is a vector of LMDB Write flag keywords, may include `:nooverwrite`, `:nodupdata`, `:current`, `:reserve`, `:append`, `:appenddup`, `:multiple`, see [LMDB documentation](http://www.lmdb.tech/doc/group__mdb__put.html).
       Pass in `:append` when the data is sorted to gain better write performance.

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

  If `ignore-key?` is `true` (default), only return the value,
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
       :doc      "Return a seq of kv pairs in the specified key range in the key-value store.

This function is eager and attempts to load all data in range into memory. When the memory pressure is high, the remaining data is spilled on to a temporary disk file. The spill-to-disk mechanism is controlled by `:spill-opts` map passed to [[open-kv]]. See [[range-seq]] for a lazy version of this function.

`k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`, `:less-than`, `:open`, `:open-closed`, plus backward variants that put a `-back` suffix to each of the above, e.g. `:all-back`.

`k-type` and `v-type` are data types of `k` and `v`, respectively. The allowed data types are described in [[read-buffer]].

Only the value will be returned if `ignore-key?` is `true`, default is `false`.

If value is to be ignored, put `:ignore` as `v-type`.


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
                   [db dbi-name k-range k-type]
                   [db dbi-name k-range k-type v-type]
                   [db dbi-name k-range k-type v-type ignore-key?]
                   [db dbi-name k-range k-type v-type ignore-key? opts])
       :doc      "Return a seq of kv pairs in the specified key range in the key-value store. This function is similar to `get-range`, but the result is lazy, as it loads the data items in batches into memory. `:batch-size` in `opts` controls the batch size (default 100).

The returned data structure implements `Seqable` and `IReduceInit`. It represents only one pass over the data range, and `seq` function needs to be called to obtain a persistent collection.

Be aware that the returned structure holds an open read transaction. It implements `AutoCloseable`, and `close` should be invoked on it after done with data access, otherwise an open read transaction may blow up the database size and return stale data. It is strongly recommended to use it in `with-open`.

`k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`, `:less-than`, `:open`, `:open-closed`, plus backward variants that put a `-back` suffix to each of the above, e.g. `:all-back`;

`k-type` and `v-type` are data types of `k` and `v`, respectively.

The allowed data types are described in [[read-buffer]].

Only the value will be returned if `ignore-key?` is `true`, default is `false`;

If value is to be ignored, put `:ignore` as `v-type`

See [[get-range]] for usage of the augments.

     Examples:

              (with-open [^AutoCloseable range (range-seq db \"c\" [:at-least 9] :long)]
                (doseq [item range]
                  ;; do processing on each item
                  ))"}
  range-seq l/range-seq)

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
       :doc
       "Return the first kv pair that has logical true value of `(pred x)` in
        the key-value store, where `pred` is a function, `x` is an `IKV`
        fetched from the store, with both key and value fields being a
        `ByteBuffer`.

     `pred` can use [[read-buffer]] to read the content.

      To access store on a server, [[interpret.inter-fn]] should be used to define the `pred`.

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

    `k-type` and `v-type` are data types of `k` and `v`, respectively.
     The allowed data types are described in [[read-buffer]].

     Only the value will be returned if `ignore-key?` is `true`;
     If value is to be ignored, put `:ignore` as `v-type`

     Examples:

              (require '[datalevin.interpret :as i])

              (def pred (i/inter-fn [kv]
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

This function is eager and attempts to load all matching data in range into memory. When the memory pressure is high, the remaining data is spilled on to a temporary disk file. The spill-to-disk mechanism is controlled by `:spill-opts` map passed to [[open-kv]].

`pred` can use [[read-buffer]] to read the buffer content.

To access store on a server, [[interpret.inter-fn]] should be used to define the `pred`.

`k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`, `:less-than`, `:open`, `:open-closed`, plus backward variants that put a `-back` suffix to each of the above, e.g. `:all-back`;

`k-type` and `v-type` are data types of `k` and `v`, respectively. The allowed data types are described in [[read-buffer]].

Only the value will be returned if `ignore-key?` is `true`;

If value is to be ignored, put `:ignore` as `v-type`

     Examples:

              (require '[datalevin.interpret :as i])

              (def pred (i/inter-fn [kv]
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
       :doc      "Return the number of kv pairs in the specified key range in the
key-value store, for only those return true value for `(pred x)`, where `pred` is a
function, and `x`is an `IKV`, with both key and value fields being a `ByteBuffer`.
Does not process the kv pairs.

`pred` can use [[read-buffer]] to read the buffer content.

To access store on a server, [[interpret.inter-fn]] should be used to define the `pred`.

`k-type` indicates data type of `k` and the allowed data types are described in [[read-buffer]].

`k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
`:less-than`, `:open`, `:open-closed`, plus backward variants that put a `-back` suffix to each of the above, e.g. `:all-back`;

     Examples:

              (require '[datalevin.interpret :as i])

              (def pred (i/inter-fn [kv]
                         (let [^long lk (read-buffer (k kv) :long)]
                          (> lk 15))))

              (range-filter-count lmdb \"a\" pred [:less-than 20] :long)
              ;;==> 3"}
  range-filter-count l/range-filter-count)

(def ^{:arglists '([db dbi-name pred k-range]
                   [db dbi-name pred k-range k-type])
       :doc      "Call `visitor` function on each kv pairs in the specified key range, presumably for side effects. Return nil. Each kv pair is an `IKV`, with both key and value fields being a `ByteBuffer`. `visitor` function can use [[read-buffer]] to read the buffer content.

      If `visitor` function returns a special value `:datalevin/terminate-visit`, the visit will stop immediately.

      For client/server usage, [[interpret.inter-fn]] should be used to define the `visitor` function. For babashka pod usage, `defpodfn` should be used.

    `k-type` indicates data type of `k` and the allowed data types are described
    in [[read-buffer]].

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

     Examples:

              (require '[datalevin.interpret :as i])
              (import '[java.util Hashmap])

              (def hashmap (HashMap.))
              (def visitor (i/inter-fn [kv]
                             (let [^long k (b/read-buffer (l/k kv) :long)
                                   ^long v (b/read-buffer (l/v kv) :long)]
                                  (.put hashmap k v))))
              (visit lmdb \"a\" visitor [:closed 11 19] :long)
              "}
  visit l/visit)

(defn clear
  "Close the Datalog database, then clear all data, including schema."
  [conn]
  (close conn)
  (let [dir  (s/dir ^Store (.-store ^DB @conn))
        lmdb (open-kv dir)]
    (doseq [dbi [c/eav c/ave c/vea c/giants c/schema]]
      (clear-dbi lmdb dbi))
    (close-kv lmdb)))

;; -------------------------------------
;; Search API

(defn new-search-engine
  "Create a search engine. The search index is stored in the passed-in
  key-value database opened by [[open-kv]].

  `opts` is an option map that may contain these keys:

   * `:domain` is an identifier string, indicates the domain of this search engine.
      This way, multiple independent search engines can reside in the same
      key-value database, each with its own domain identifier.

   * `:analyzer` is a function that takes a text string and return a seq of
    [term, position, offset], where term is a word, position is the sequence
     number of the term in the document, and offset is the character offset of
     the term in the document. E.g. for a blank space analyzer and the document
    \"The quick brown fox jumps over the lazy dog\", [\"quick\" 1 4] would be
    the second entry of the resulting seq.

   * `:query-analyzer` is a similar function that overrides the analyzer at
    query time (and not indexing time). Mostly useful for autocomplete search in
    conjunction with the `datalevin.search-utils/prefix-token-filter`.

  See [[datalevin.search-utils]] for some functions to customize search.
  "
  ([lmdb]
   (new-search-engine lmdb nil))
  ([lmdb opts]
   (if (instance? KVStore lmdb)
     (r/new-search-engine lmdb opts)
     (sc/new-search-engine lmdb opts))))

(def ^{:arglists '([engine doc-ref doc-text])
       :doc      "Add a document to the search engine, `doc-ref` can be
     arbitrary Clojure data that uniquely refers to the document in the system.
     `doc-text` is the content of the document as a string.  The search engine
     does not store the original text, and assumes that caller can retrieve them
     by `doc-ref`. This function is for online update of search engine index.
     Search index is updated with the new text if the `doc-ref` already exists.
     For index creation of bulk data, use `search-index-writer`."}
  add-doc sc/add-doc)

(def ^{:arglists '([engine doc-ref])
       :doc      "Remove a document referred to by `doc-ref` from the search
engine index. A slow operation."}
  remove-doc sc/remove-doc)

(def ^{:arglists '([engine])
       :doc      "Remove all documents from the search engine index. It is useful
  because rebuilding search index may be faster than updating some documents."}
  clear-docs sc/clear-docs)

(def ^{:arglists '([engine doc-ref])
       :doc      "Test if a `doc-ref` is already in the search index"}
  doc-indexed? sc/doc-indexed?)

(def ^{:arglists '([engine])
       :doc      "Return the number of documents in the search index"}
  doc-count sc/doc-count)

(def ^{:arglists '([engine])
       :doc      "Return a seq of `doc-ref` in the search index"}
  doc-refs sc/doc-refs)

(def ^{:arglists '([engine query] [engine query opts])
       :doc      "Issue a `query` to the search engine. `query` is a string of
words.

`opts` map may have these keys:

  * `:display` can be one of `:refs` (default), `:offsets`.
    - `:refs` return a lazy sequence of `doc-ref` ordered by relevance.
    - `:offsets` return a lazy sequence of
      `[doc-ref [term1 [offset ...]] [term2 [...]] ...]`,
      ordered by relevance. `term` and `offset` can be used to
      highlight the matched terms and their locations in the documents.
  * `:top` is an integer (default 10), the number of results desired.
  * `:doc-filter` is a boolean function that takes a `doc-ref` and
    determines whether or not to include the corresponding document in the
    results (default is `(constantly true)`)"}
  search sc/search)

(defn search-index-writer
  "Create a writer for writing documents to the search index in bulk.
  The search index is stored in the passed-in key value database opened
  by [[open-kv]]. See also [[write]] and [[commit]].

  `opts` is an option map that may contain these keys:

   * `:domain` is an identifier string, indicates the domain of this search engine.
      This way, multiple independent search engines can reside in the same
      key-value database, each with its own domain identifier.

  * `:analyzer` is a function that takes a text string and return a seq of
    [term, position, offset], where term is a word, position is the sequence
     number of the term, and offset is the character offset of this term.
  "
  ([lmdb]
   (search-index-writer lmdb nil))
  ([lmdb opts]
   (if (instance? KVStore lmdb)
     (r/search-index-writer lmdb opts)
     (sc/search-index-writer lmdb opts))))

(def ^{:arglists '([writer doc-ref doc-text])
       :doc      "Write a document to search index."}
  write sc/write)

(def ^{:arglists '([writer])
       :doc      "Commit writes to search index, must be called after writing
all documents."}
  commit sc/commit)

;; -------------------------------------
;; byte buffer

(def ^{:arglists '([bf x] [bf x x-type])
       :doc      "Put the given type of data `x` in buffer `bf`. `x-type` can be
    one of the following data types:

    - `:data` (default), arbitrary EDN data, avoid this as keys for range queries
    - `:string`, UTF-8 string
    - `:long`, 64 bits integer
    - `:float`, 32 bits IEEE754 floating point number
    - `:double`, 64 bits IEEE754 floating point number
    - `:byte`, single byte
    - `:bytes`, byte array
    - `:keyword`, EDN keyword
    - `:symbol`, EDN symbol
    - `:boolean`, `true` or `false`
    - `:instant`, timestamp, same as `java.util.Date`
    - `:uuid`, UUID, same as `java.util.UUID`

  If the value is to be put in a LMDB key buffer, it must be less than
  511 bytes."}
  put-buffer b/put-buffer)

(def ^{:arglists '([bf] [bf v-type])
       :doc      "Get the given type of data from buffer `bf`, `v-type` can be
one of the following data types:

  - `:data` (default), arbitrary EDN data
  - `:string`, UTF-8 string
  - `:long`, 64 bits integer
  - `:float`, 32 bits IEEE754 floating point number
  - `:double`, 64 bits IEEE754 floating point number
  - `:byte`, single byte
  - `:bytes`, an byte array
  - `:keyword`, EDN keyword
  - `:symbol`, EDN symbol
  - `:boolean`, `true` or `false`
  - `:instant`, timestamp, same as `java.util.Date`
  - `:uuid`, UUID, same as `java.util.UUID`"}
  read-buffer b/read-buffer)

(def ^{:arglists '([s])
       :doc      "Turn a string into a hexified string"}
  hexify-string b/hexify-string)

(def ^{:arglists '([s])
       :doc      "Turn a hexified string back into a normal string"}
  unhexify-string b/unhexify-string)
