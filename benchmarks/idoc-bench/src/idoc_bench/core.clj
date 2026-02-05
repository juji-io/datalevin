;;
;; Copyright (c) Nikita Prokopov, Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns idoc-bench.core
  "YCSB-style benchmark with idoc query workload."
  (:require
   [clojure.string :as str]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.idoc :as idoc]
   [datalevin.query :as dq]
   [datalevin.util :as u]
   [jsonista.core :as json]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [next.jdbc.sql :as sql])
  (:import
   [com.mongodb.client MongoClients]
   [com.mongodb.client.model Filters]
   [java.util ArrayList Random UUID]
   [java.util.concurrent CountDownLatch]
   [org.bson Document]
   [org.postgresql.util PGobject]))

(def schema
  {:mem/doc {:db/valueType :db.type/idoc
             :db/domain    "memory"}})

(def workloads
  {:A {:read 50 :update 50}
   :C {:read 100}
   :F {:rmw 100}})

(def tags
  ["urgent" "todo" "followup" "meeting" "project" "customer" "issue"
   "feature" "blocker" "idea" "note" "action"])

(def langs ["en" "es" "fr" "de" "zh"])
(def personas ["developer" "manager" "founder" "analyst" "support"])
(def topics ["roadmap" "billing" "onboarding" "incident" "design" "infra"])
(def sources ["chat" "email" "call" "doc" "note"])
(def cities ["SF" "NYC" "LA" "SEA" "LON" "BER" "PAR" "TOK"])
(def teams ["red" "blue" "green" "yellow"])
(def entities ["acme" "globex" "initech" "umbrella" "stark" "wayne"])
(def kinds ["chat" "email" "note" "task"])

(def base-ts 1700000000000)

(def q-idoc
  '[:find ?e
    :in $ ?q
    :where [(idoc-match $ :mem/doc ?q) [[?e ?a ?v]]]])

(def default-opts
  {:system      :datalevin
   :workload    :C
   :records     100000
   :ops         100000
   :warmup      2000
   :threads     1
   :batch-size  1000
   :idoc-ratio  20
   :idoc-trace? false
   :hotset      1.0
   :seed        42
   :dir         nil
   :keep-db?    false
   :env-flags   [:nosync :nolock]
   :pg-url      "jdbc:postgresql://localhost:5432/postgres"
   :pg-user     nil
   :pg-password nil
   :pg-table    "idoc_bench_docs"
   :sqlite-path nil
   :mongo-uri   "mongodb://localhost:27017"
   :mongo-db    "idoc_bench"
   :mongo-coll  "docs"})

(def ^:private json-mapper
  (json/object-mapper {:encode-key-fn name
                       :decode-key-fn keyword}))

(defn- encode-json
  [v]
  (json/write-value-as-string v json-mapper))

(defn- decode-json
  [^String s]
  (json/read-value s json-mapper))

(defn- rand-choice
  [^Random r coll]
  (nth coll (.nextInt r (count coll))))

(defn- rand-tags
  [^Random r max-count]
  (let [n (inc (.nextInt r max-count))]
    (vec (repeatedly n #(rand-choice r tags)))))

(defn- rand-entities
  [^Random r]
  (let [n (inc (.nextInt r 3))]
    (vec (repeatedly n #(rand-choice r entities)))))

(defn- make-event
  [^Random r id idx]
  {:ts     (+ base-ts id (* idx 1000) (.nextInt r 100000))
   :kind   (rand-choice r kinds)
   :tags   (rand-tags r 3)
   :entity {:name (rand-choice r entities)}
   :score  (double (.nextDouble r))})

(defn- make-doc
  [^Random r id]
  {:profile {:age     (+ 18 (.nextInt r 50))
             :lang    (rand-choice r langs)
             :persona (rand-choice r personas)}
   :stats   {:score     (double (.nextDouble r))
             :last_seen (+ base-ts (.nextInt r 2000000))}
   :facts   {"city" (rand-choice r cities)
             "team" (rand-choice r teams)}
   :memory  {:topic    (rand-choice r topics)
             :source   (rand-choice r sources)
             :entities (rand-entities r)}
   :tags    (rand-tags r 4)
   :events  (vec (map-indexed (fn [idx _] (make-event r id idx))
                              (range (inc (.nextInt r 4)))))})

(defn- generate-docs
  [records seed]
  (let [r (Random. seed)]
    (vec (map (fn [id] (make-doc r id)) (range 1 (inc records))))))

(defn- update-doc
  [doc ^Random r]
  (-> doc
      (assoc-in [:stats :score] (double (.nextDouble r)))
      (assoc-in [:stats :last_seen] (+ base-ts (.nextInt r 2000000)))))

(defn- update-doc-patch
  [doc ^Random r]
  (let [score     (double (.nextDouble r))
        last-seen (+ base-ts (.nextInt r 2000000))
        doc'      (-> doc
                      (assoc-in [:stats :score] score)
                      (assoc-in [:stats :last_seen] last-seen))]
    [doc' [[:set [:stats :score] score]
           [:set [:stats :last_seen] last-seen]]]))

(defn- rmw-doc
  [doc ^Random r]
  (-> doc
      (update-in [:stats :score] (fnil #(+ % 0.01) 0.0))
      (assoc-in [:stats :last_seen] (+ base-ts (.nextInt r 2000000)))))

(defn- rmw-doc-patch
  [doc ^Random r]
  (let [score     (double ((fnil #(+ % 0.01) 0.0) (get-in doc [:stats :score])))
        last-seen (+ base-ts (.nextInt r 2000000))
        doc'      (-> doc
                      (assoc-in [:stats :score] score)
                      (assoc-in [:stats :last_seen] last-seen))]
    [doc' [[:set [:stats :score] score]
           [:set [:stats :last_seen] last-seen]]]))

(defn- rand-id
  [^Random r records hotset]
  (let [max-id (if (and hotset (< hotset 1.0))
                 (max 1 (int (* records hotset)))
                 records)]
    (inc (.nextInt r max-id))))

(def query-types [:nested :range :wildcard-one :wildcard-depth :array])

(defn- rand-query-spec
  [^Random r]
  (case (rand-choice r query-types)
    :nested {:type :nested :value (rand-choice r langs)}
    :range {:type :range :lo 0.3 :hi 0.8}
    :wildcard-one {:type :wildcard-one :value (rand-choice r cities)}
    :wildcard-depth {:type :wildcard-depth :value (rand-choice r entities)}
    :array {:type :array :value (rand-choice r tags)}))

(defn- spec->idoc-query
  [spec]
  (case (:type spec)
    :nested {:profile {:lang (:value spec)}}
    :range (list '< (:lo spec) [:stats :score] (:hi spec))
    :wildcard-one {:facts {:? (:value spec)}}
    :wildcard-depth {:* {:entity {:name (:value spec)}}}
    :array {:events {:tags (:value spec)}}))

(defn- build-selector
  [weights]
  (let [table (loop [acc []
                     sum 0
                     [[op w] & more] weights]
                (if op
                  (recur (conj acc [op (+ sum w)])
                         (+ sum w)
                         more)
                  acc))
        total (double (second (peek table)))]
    (fn [^Random r]
      (let [x (* total (.nextDouble r))]
        (loop [[[op bound] & more] table]
          (if (or (nil? op) (<= x bound) (empty? more))
            op
            (recur more)))))))

(defn- workload->weights
  [workload idoc-ratio]
  (let [base (get workloads workload)]
    (cond-> (vec base)
      (pos? idoc-ratio) (conj [:idoc idoc-ratio]))))

(defn- update-stat
  [stats op nanos]
  (update stats op
          (fnil (fn [{:keys [count nanos-sum]}]
                  {:count     (inc count)
                   :nanos-sum (+ nanos-sum nanos)})
                {:count 0 :nanos-sum 0})))

(defn- merge-stats
  [a b]
  (merge-with (fn [x y]
                {:count     (+ (:count x) (:count y))
                  :nanos-sum (+ (:nanos-sum x) (:nanos-sum y))})
              a b))

(def ^:private empty-idoc-trace
  {:count          0
   :cand-sum       0
   :verify-sum     0
   :doc-fetch-sum  0
   :match-sum      0
   :elapsed-ns-sum 0
   :exact-count    0})

(defn- trace-key
  [spec event]
  (if-let [domain (:domain event)]
    [(:type spec) domain]
    (:type spec)))

(defn- accumulate-idoc-trace
  [m event]
  (let [m (or m empty-idoc-trace)
        cand-count   (long (or (:candidate-count event) 0))
        verify-count (long (or (:verify-count event) 0))
        doc-fetches  (long (or (:doc-fetch-count event) 0))
        match-count  (long (or (:match-count event) 0))
        elapsed-ns   (long (or (:elapsed-ns event) 0))
        exact?       (boolean (:exact? event))]
    (-> m
        (update :count inc)
        (update :cand-sum + cand-count)
        (update :verify-sum + verify-count)
        (update :doc-fetch-sum + doc-fetches)
        (update :match-sum + match-count)
        (update :elapsed-ns-sum + elapsed-ns)
        (update :exact-count + (if exact? 1 0)))))

(defn- update-idoc-trace!
  [trace spec event]
  (when (and trace (= (:event event) :idoc-match-domain))
    (let [k (trace-key spec event)]
      (swap! trace update k accumulate-idoc-trace event))))

(defn- print-idoc-trace
  [trace]
  (when (and trace (seq @trace))
    (println)
    (println "idoc trace")
    (println "type\tcount\tavg-cand\tavg-verify\tavg-docs\tavg-match\tavg-ms\texact%")
    (doseq [[k {:keys [count cand-sum verify-sum doc-fetch-sum
                       match-sum elapsed-ns-sum exact-count]}]
            (sort-by key @trace)]
      (let [avg     (fn [sum] (if (pos? count) (/ sum (double count)) 0.0))
            avg-ms  (if (pos? count) (/ elapsed-ns-sum (* count 1e6)) 0.0)
            exact-p (if (pos? count) (* 100.0 (/ exact-count count)) 0.0)
            kstr    (if (vector? k)
                      (str (name (first k)) "/" (second k))
                      (name k))]
        (println (str kstr
                      "\t" count
                      "\t" (format "%.1f" (avg cand-sum))
                      "\t" (format "%.1f" (avg verify-sum))
                      "\t" (format "%.1f" (avg doc-fetch-sum))
                      "\t" (format "%.1f" (avg match-sum))
                      "\t" (format "%.4f" (double avg-ms))
                      "\t" (format "%.1f" (double exact-p))))))))

(defn- print-summary
  [stats total-ops elapsed-ms]
  (println "Total ops:" total-ops)
  (println "Elapsed (ms):" (format "%.2f" (double elapsed-ms)))
  (println "Throughput (ops/sec):"
           (format "%.2f" (double (/ total-ops (/ elapsed-ms 1000.0)))))
  (println)
  (println "op\tcount\tavg-ms")
  (doseq [[op {:keys [count nanos-sum]}] (sort-by key stats)]
    (let [avg-ms (if (pos? count) (/ nanos-sum (* count 1e6)) 0.0)]
      (println (str (name op)
                    "\t" count
                    "\t" (format "%.4f" (double avg-ms)))))))

(defn- load-docs-datalevin!
  [conn docs batch-size]
  (doseq [batch (partition-all batch-size (map-indexed
                                           (fn [idx doc]
                                             {:db/id  (inc idx)
                                              :mem/doc doc})
                                           docs))]
    (d/transact! conn batch)))

(defn- pg-jsonb
  [^String s]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue s)))

(defn- sql-doc->json
  [v]
  (cond
    (nil? v) nil
    (instance? PGobject v) (.getValue ^PGobject v)
    :else (str v)))

(defn- sql-count
  [conn sql params]
  (let [row (jdbc/execute-one! conn (into [sql] params)
                              {:builder-fn rs/as-unqualified-lower-maps})]
    (:cnt row)))

(defn- sql-read-doc
  [conn table id]
  (let [row (jdbc/execute-one! conn
                               [(str "SELECT doc FROM " table " WHERE id = ?") id]
                               {:builder-fn rs/as-unqualified-lower-maps})
        doc-json (sql-doc->json (:doc row))]
    (when doc-json
      (decode-json doc-json))))

(defn- pg-idoc-query
  [table spec]
  (case (:type spec)
    :nested [(str "SELECT count(1) AS cnt FROM " table
                  " WHERE doc->'profile'->>'lang' = ?")
             [(:value spec)]]
    :range [(str "SELECT count(1) AS cnt FROM " table
                 " WHERE (doc->'stats'->>'score')::double precision"
                 " BETWEEN ? AND ?")
            [(:lo spec) (:hi spec)]]
    :wildcard-one [(str "SELECT count(1) AS cnt FROM " table
                        " WHERE EXISTS (SELECT 1 FROM jsonb_each_text(doc->'facts') kv"
                        " WHERE kv.value = ?)")
                   [(:value spec)]]
    :wildcard-depth [(str "SELECT count(1) AS cnt FROM " table
                          " WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(doc->'events') e"
                          " WHERE e->'entity'->>'name' = ?)")
                     [(:value spec)]]
    :array [(str "SELECT count(1) AS cnt FROM " table
                 " WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(doc->'events') e,"
                 " jsonb_array_elements_text(e->'tags') t WHERE t = ?)")
            [(:value spec)]]))

(defn- pg-create-indexes!
  [ds table]
  (jdbc/execute! ds [(str "CREATE INDEX IF NOT EXISTS " table "_profile_lang_idx"
                          " ON " table " ((doc->'profile'->>'lang'))")])
  (jdbc/execute! ds [(str "CREATE INDEX IF NOT EXISTS " table "_stats_score_idx"
                          " ON " table " (((doc->'stats'->>'score')::double precision))")])
  (jdbc/execute! ds [(str "CREATE INDEX IF NOT EXISTS " table "_facts_gin_idx"
                          " ON " table " USING GIN ((doc->'facts'))")])
  (jdbc/execute! ds [(str "CREATE INDEX IF NOT EXISTS " table "_events_gin_idx"
                          " ON " table " USING GIN ((doc->'events'))")]))

(defn- pg-analyze!
  [ds table]
  (jdbc/execute! ds [(str "ANALYZE " table)]))

(defn- sqlite-idoc-query
  [table spec]
  (case (:type spec)
    :nested [(str "SELECT count(1) AS cnt FROM " table
                  " WHERE json_extract(doc, '$.profile.lang') = ?")
             [(:value spec)]]
    :range [(str "SELECT count(1) AS cnt FROM " table
                 " WHERE CAST(json_extract(doc, '$.stats.score') AS REAL)"
                 " BETWEEN ? AND ?")
            [(:lo spec) (:hi spec)]]
    :wildcard-one [(str "SELECT count(1) AS cnt FROM " table
                        " WHERE EXISTS (SELECT 1 FROM json_each(doc, '$.facts')"
                        " WHERE json_each.value = ?)")
                   [(:value spec)]]
    :wildcard-depth [(str "SELECT count(1) AS cnt FROM " table
                          " WHERE EXISTS (SELECT 1 FROM json_each(doc, '$.events') e"
                          " WHERE json_extract(e.value, '$.entity.name') = ?)")
                     [(:value spec)]]
    :array [(str "SELECT count(1) AS cnt FROM " table
                 " WHERE EXISTS (SELECT 1 FROM json_each(doc, '$.events') e"
                 " JOIN json_each(e.value, '$.tags') t"
                 " WHERE t.value = ?)")
            [(:value spec)]]))

(defn- sqlite-create-indexes!
  [conn]
  (jdbc/execute! conn [(str "CREATE INDEX IF NOT EXISTS idoc_profile_lang_idx"
                            " ON idoc_bench_docs (json_extract(doc, '$.profile.lang'))")])
  (jdbc/execute! conn [(str "CREATE INDEX IF NOT EXISTS idoc_stats_score_idx"
                            " ON idoc_bench_docs"
                            " (CAST(json_extract(doc, '$.stats.score') AS REAL))")])
  (jdbc/execute! conn [(str "CREATE INDEX IF NOT EXISTS idoc_facts_city_idx"
                            " ON idoc_bench_docs (json_extract(doc, '$.facts.city'))")])
  (jdbc/execute! conn [(str "CREATE INDEX IF NOT EXISTS idoc_facts_team_idx"
                            " ON idoc_bench_docs (json_extract(doc, '$.facts.team'))")]))

(defn- sqlite-analyze!
  [conn]
  (jdbc/execute! conn ["ANALYZE idoc_bench_docs"]))

(defn- mongo-filter
  [spec]
  (case (:type spec)
    :nested (Filters/eq "profile.lang" (:value spec))
    :range (Filters/and (list
                          (Filters/gte "stats.score" (:lo spec))
                          (Filters/lte "stats.score" (:hi spec))))
    :wildcard-one (Filters/or (list
                                (Filters/eq "facts.city" (:value spec))
                                (Filters/eq "facts.team" (:value spec))))
    :wildcard-depth (Filters/eq "events.entity.name" (:value spec))
    :array (Filters/eq "events.tags" (:value spec))))

(defn- mongo-doc
  [id doc]
  (let [bson (Document/parse (encode-json doc))]
    (.put bson "_id" (long id))
    bson))

(defn- bson->clj
  [v]
  (cond
    (instance? java.util.Map v)
    (into {}
          (map (fn [[k val]] [(keyword (str k)) (bson->clj val)]) v))

    (instance? java.util.List v)
    (mapv bson->clj v)

    :else v))

(defn- mongo-read-doc
  [coll id]
  (when-let [doc (.first (.find coll (Filters/eq "_id" (long id))))]
    (dissoc (bson->clj doc) :_id)))

(defn- mongo-write-doc!
  [coll id doc]
  (.replaceOne coll (Filters/eq "_id" (long id)) (mongo-doc id doc)))

(defn- mongo-create-indexes!
  [coll]
  (.createIndex coll (Document. {"profile.lang" 1}))
  (.createIndex coll (Document. {"stats.score" 1}))
  (.createIndex coll (Document. {"facts.city" 1}))
  (.createIndex coll (Document. {"facts.team" 1}))
  (.createIndex coll (Document. {"events.entity.name" 1}))
  (.createIndex coll (Document. {"events.tags" 1})))

(defn- sqlite-conn
  [ds]
  (let [conn (jdbc/get-connection ds)]
    (jdbc/execute! conn ["PRAGMA journal_mode=WAL;"])
    (jdbc/execute! conn ["PRAGMA synchronous=NORMAL;"])
    (jdbc/execute! conn ["PRAGMA busy_timeout=5000;"])
    conn))

(defn- datalevin-handlers
  [opts docs]
  (let [{:keys [records hotset batch-size env-flags keep-db? idoc-trace?]} opts

        dir    (or (:dir opts)
                   (str (u/tmp-dir (str "idoc-bench-" (UUID/randomUUID)))))
        conn   (d/create-conn
                 dir
                 schema
                 {:kv-opts {:flags (into c/default-env-flags env-flags)}})
        trace  (when idoc-trace? (atom {}))
        tracer (fn [spec]
                 (when trace
                   (fn [event] (update-idoc-trace! trace spec event))))]
    {:system       :datalevin
     :label        dir
     :load!        (fn []
                     (load-docs-datalevin! conn @docs batch-size)
                     (println "Running analyze ...")
                     (d/analyze (d/db conn)))
     :make-thread  (fn [_] {:conn conn})
     :close-thread (fn [_] nil)
     :op-read      (fn [{:keys [conn]} r]
                     (let [id (rand-id r records hotset)
                           db (d/db conn)]
                       (:mem/doc (d/entity db id))))
     :op-update    (fn [{:keys [conn]} r]
                     (let [id         (rand-id r records hotset)
                           idx        (dec id)
                           doc        (nth @docs idx)
                           [doc' ops] (update-doc-patch doc r)]
                       (swap! docs assoc idx doc')
                       (d/transact! conn [[:db.fn/patchIdoc id :mem/doc ops]])))
     :op-rmw       (fn [{:keys [conn]} r]
                     (let [id         (rand-id r records hotset)
                           db         (d/db conn)
                           doc        (:mem/doc (d/entity db id))
                           [doc' ops] (rmw-doc-patch doc r)]
                       (swap! docs assoc (dec id) doc')
                       (d/transact! conn [[:db.fn/patchIdoc id :mem/doc ops]])))
     :op-idoc      (fn [{:keys [conn]} r]
                     (let [spec (rand-query-spec r)
                           q    (spec->idoc-query spec)
                           db   (d/db conn)]
                       (binding [dq/*cache?* false]
                         (if trace
                           (binding [idoc/*trace* (tracer spec)]
                             (count (d/q q-idoc db q)))
                           (count (d/q q-idoc db q))))))
     :close!       (fn [] (d/close conn))
     :trace-report (when trace #(print-idoc-trace trace))
     :cleanup!     (fn []
                     (when-not keep-db?
                       (when-not (u/windows?)
                         (u/delete-files dir))))}))

(defn- postgres-handlers
  [opts docs]
  (let [{:keys [records hotset batch-size keep-db? pg-url pg-user
                pg-password pg-table]} opts
        ds                             (jdbc/get-datasource
             (cond-> {:jdbcUrl pg-url}
               pg-user (assoc :user pg-user)
               pg-password (assoc :password pg-password)))
        init!                          (fn []
                (jdbc/execute! ds [(str "DROP TABLE IF EXISTS " pg-table)])
                (jdbc/execute! ds [(str "CREATE TABLE " pg-table
                                        " (id BIGINT PRIMARY KEY, doc JSONB NOT NULL)")]))
        load!                          (fn []
                (jdbc/with-transaction [tx ds]
                  (doseq [batch (partition-all
                                  batch-size
                                  (map-indexed
                                    (fn [idx doc]
                                      {:id  (inc idx)
                                       :doc (pg-jsonb (encode-json doc))})
                                    @docs))]
                    (sql/insert-multi! tx (keyword pg-table) batch)))
                (println "Building indexes ...")
                (pg-create-indexes! ds pg-table)
                (println "Running ANALYZE ...")
                (pg-analyze! ds pg-table))
        op-count                       (fn [conn spec]
                   (let [[sql params] (pg-idoc-query pg-table spec)]
                     (sql-count conn sql params)))
        read-doc                       (fn [conn id] (sql-read-doc conn pg-table id))
        update-doc!                    (fn [conn id doc]
                      (jdbc/execute! conn
                                     [(str "UPDATE " pg-table " SET doc = ? WHERE id = ?")
                                      (pg-jsonb (encode-json doc))
                                      id]))]
    {:system       :postgres
     :label        (str pg-url " (" pg-table ")")
     :load!        (fn []
                     (init!)
                     (load!))
     :make-thread  (fn [_] {:conn (jdbc/get-connection ds)})
     :close-thread (fn [{:keys [conn]}] (.close conn))
     :op-read      (fn [{:keys [conn]} r]
                     (let [id (rand-id r records hotset)]
                       (read-doc conn id)))
     :op-update    (fn [{:keys [conn]} r]
                     (let [id   (rand-id r records hotset)
                           idx  (dec id)
                           doc  (nth @docs idx)
                           doc' (update-doc doc r)]
                       (swap! docs assoc idx doc')
                       (update-doc! conn id doc')))
     :op-rmw       (fn [{:keys [conn]} r]
                     (let [id   (rand-id r records hotset)
                           doc  (read-doc conn id)
                           doc' (rmw-doc doc r)]
                       (swap! docs assoc (dec id) doc')
                       (update-doc! conn id doc')))
     :op-idoc      (fn [{:keys [conn]} r]
                     (op-count conn (rand-query-spec r)))
     :close!       (fn [] nil)
     :cleanup!     (fn []
                     (when-not keep-db?
                       (jdbc/execute! ds [(str "DROP TABLE IF EXISTS " pg-table)])))}))

(defn- sqlite-handlers
  [opts docs]
  (let [{:keys [records hotset batch-size keep-db? sqlite-path]} opts
        default-dir (str (u/tmp-dir (str "idoc-bench-sqlite-" (UUID/randomUUID))))
        db-path (or sqlite-path (str default-dir "/idoc-bench.sqlite"))
        cleanup-dir? (nil? sqlite-path)
        _ (when-let [parent (.getParent (java.io.File. db-path))]
            (u/file parent))
        ds (jdbc/get-datasource {:jdbcUrl (str "jdbc:sqlite:" db-path)})
        init! (fn []
                (let [conn (sqlite-conn ds)]
                  (try
                    (jdbc/execute! conn [(str "DROP TABLE IF EXISTS idoc_bench_docs")])
                    (jdbc/execute! conn [(str "CREATE TABLE idoc_bench_docs"
                                              " (id INTEGER PRIMARY KEY, doc TEXT NOT NULL)")])
                    (finally
                      (.close conn)))))
        load! (fn []
                (jdbc/with-transaction [tx ds]
                  (doseq [batch (partition-all
                                  batch-size
                                  (map-indexed
                                   (fn [idx doc]
                                     {:id  (inc idx)
                                      :doc (encode-json doc)})
                                   @docs))]
                    (sql/insert-multi! tx :idoc_bench_docs batch)))
                (let [conn (sqlite-conn ds)]
                  (try
                    (println "Building indexes ...")
                    (sqlite-create-indexes! conn)
                    (println "Running ANALYZE ...")
                    (sqlite-analyze! conn)
                    (finally
                      (.close conn)))))
        op-count (fn [conn spec]
                   (let [[sql params] (sqlite-idoc-query "idoc_bench_docs" spec)]
                     (sql-count conn sql params)))
        read-doc (fn [conn id] (sql-read-doc conn "idoc_bench_docs" id))
        update-doc! (fn [conn id doc]
                      (jdbc/execute! conn
                                     [(str "UPDATE idoc_bench_docs SET doc = ? WHERE id = ?")
                                      (encode-json doc)
                                      id]))]
    {:system       :sqlite
     :label        db-path
     :load!        (fn []
                     (init!)
                     (load!))
     :make-thread  (fn [_] {:conn (sqlite-conn ds)})
     :close-thread (fn [{:keys [conn]}] (.close conn))
     :op-read      (fn [{:keys [conn]} r]
                     (let [id (rand-id r records hotset)]
                       (read-doc conn id)))
     :op-update    (fn [{:keys [conn]} r]
                     (let [id   (rand-id r records hotset)
                           idx  (dec id)
                           doc  (nth @docs idx)
                           doc' (update-doc doc r)]
                       (swap! docs assoc idx doc')
                       (update-doc! conn id doc')))
     :op-rmw       (fn [{:keys [conn]} r]
                     (let [id   (rand-id r records hotset)
                           doc  (read-doc conn id)
                           doc' (rmw-doc doc r)]
                       (swap! docs assoc (dec id) doc')
                       (update-doc! conn id doc')))
     :op-idoc      (fn [{:keys [conn]} r]
                     (op-count conn (rand-query-spec r)))
     :close!       (fn [] nil)
     :cleanup!     (fn []
                     (when-not keep-db?
                       (when-not (u/windows?)
                         (u/delete-files db-path)
                         (when (and cleanup-dir?
                                    (u/file-exists default-dir))
                           (u/delete-files default-dir)))))}))

(defn- mongo-handlers
  [opts docs]
  (let [{:keys [records hotset batch-size keep-db? mongo-uri mongo-db mongo-coll]}
        opts
        client (MongoClients/create mongo-uri)
        database (.getDatabase client mongo-db)
        coll (.getCollection database mongo-coll)]
    {:system       :mongo
     :label        (str mongo-uri " (" mongo-db "/" mongo-coll ")")
     :load!        (fn []
                     (.drop coll)
                     (doseq [batch (partition-all batch-size (map-indexed
                                                              (fn [idx doc]
                                                                (mongo-doc (inc idx) doc))
                                                              @docs))]
                       (.insertMany coll (ArrayList. batch)))
                     (println "Building indexes ...")
                     (mongo-create-indexes! coll))
     :make-thread  (fn [_] {:coll coll})
     :close-thread (fn [_] nil)
     :op-read      (fn [{:keys [coll]} r]
                     (let [id (rand-id r records hotset)]
                       (mongo-read-doc coll id)))
     :op-update    (fn [{:keys [coll]} r]
                     (let [id   (rand-id r records hotset)
                           idx  (dec id)
                           doc  (nth @docs idx)
                           doc' (update-doc doc r)]
                       (swap! docs assoc idx doc')
                       (mongo-write-doc! coll id doc')))
     :op-rmw       (fn [{:keys [coll]} r]
                     (let [id   (rand-id r records hotset)
                           doc  (mongo-read-doc coll id)
                           doc' (rmw-doc doc r)]
                       (swap! docs assoc (dec id) doc')
                       (mongo-write-doc! coll id doc')))
     :op-idoc      (fn [{:keys [coll]} r]
                     (.countDocuments coll (mongo-filter (rand-query-spec r))))
     :close!       (fn [] nil)
     :cleanup!     (fn []
                     (if keep-db?
                       (.close client)
                       (do
                         (.drop coll)
                         (.close client))))}))

(defn- build-handlers
  [system opts docs]
  (case system
    :datalevin (datalevin-handlers opts docs)
    :postgres (postgres-handlers opts docs)
    :sqlite (sqlite-handlers opts docs)
    :mongo (mongo-handlers opts docs)
    (throw (ex-info "Unsupported system" {:system system}))))

(defn- run-thread
  [handlers opts tid]
  (let [r        (Random. (+ (:seed opts) tid))
        selector (build-selector (workload->weights (:workload opts)
                                                    (:idoc-ratio opts)))
        ctx      ((:make-thread handlers) tid)]
    (try
      (loop [i 0
             stats {}]
        (if (< i (:ops opts))
          (let [op    (selector r)
                start (System/nanoTime)]
            (case op
              :read   ((:op-read handlers) ctx r)
              :update ((:op-update handlers) ctx r)
              :rmw    ((:op-rmw handlers) ctx r)
              :idoc   ((:op-idoc handlers) ctx r))
            (let [elapsed (- (System/nanoTime) start)]
              (recur (inc i) (update-stat stats op elapsed))))
          stats))
      (finally
        ((:close-thread handlers) ctx)))))

(defn- warmup
  [handlers opts]
  (let [warmup-ops (:warmup opts)]
    (when (pos? warmup-ops)
      (let [r        (Random. (:seed opts))
            selector (build-selector (workload->weights (:workload opts)
                                                        (:idoc-ratio opts)))
            ctx      ((:make-thread handlers) :warmup)]
        (try
          (dotimes [_ warmup-ops]
            (case (selector r)
              :read   ((:op-read handlers) ctx r)
              :update ((:op-update handlers) ctx r)
              :rmw    ((:op-rmw handlers) ctx r)
              :idoc   ((:op-idoc handlers) ctx r)))
          (finally
            ((:close-thread handlers) ctx)))))))

(defn- run-bench
  [system opts base-docs]
  (let [docs     (atom (vec base-docs))
        handlers (build-handlers system opts docs)
        {:keys [records ops threads]} opts
        warmup-ops (:warmup opts)]
    (println)
    (println "System:" (name system))
    (println "Loading" records "documents into" (:label handlers) "...")
    (try
      ((:load! handlers))
      (println "Warmup" warmup-ops "ops ...")
      (warmup handlers opts)
      (println "Running workload" (:workload opts)
               "with idoc weight" (:idoc-ratio opts)
               "on" threads "threads ...")
      (let [start-ms       (System/currentTimeMillis)
            ops-per-thread (long (Math/floor (/ ops (double threads))))
            extra          (mod ops threads)
            latch          (CountDownLatch. threads)
            results        (atom [])]
        (dotimes [tid threads]
          (let [n-ops (+ ops-per-thread (if (< tid extra) 1 0))]
            (future
              (try
                (let [stats (run-thread handlers (assoc opts :ops n-ops) tid)]
                  (swap! results conj stats))
                (catch Throwable t
                  (binding [*out* *err*]
                    (println "idoc-bench worker failed"
                             {:tid tid
                              :ops n-ops
                              :workload (:workload opts)
                              :system (:system opts)}))
                  (.printStackTrace t))
                (finally
                  (.countDown latch))))))
        (.await latch)
        (let [elapsed-ms (- (System/currentTimeMillis) start-ms)
              stats      (reduce merge-stats {} @results)]
          (print-summary stats ops elapsed-ms)
          (when-let [report (:trace-report handlers)]
            (report))))
      (finally
        ((:close! handlers))
        ((:cleanup! handlers))))))

(defn- parse-args
  [args]
  (loop [opts default-opts
         more args]
    (if-let [arg (first more)]
      (case arg
        "--system"     (recur (assoc opts :system
                                     (keyword (str/lower-case (second more))))
                              (nnext more))
        "--workload"   (recur (assoc opts :workload
                                     (keyword (str/upper-case (second more))))
                              (nnext more))
        "--records"    (recur (assoc opts :records (Long/parseLong (second more)))
                              (nnext more))
        "--ops"        (recur (assoc opts :ops (Long/parseLong (second more)))
                              (nnext more))
        "--warmup"     (recur (assoc opts :warmup (Long/parseLong (second more)))
                              (nnext more))
        "--threads"    (recur (assoc opts :threads (Long/parseLong (second more)))
                              (nnext more))
        "--batch"      (recur (assoc opts :batch-size (Long/parseLong (second more)))
                              (nnext more))
        "--idoc"       (recur (assoc opts :idoc-ratio (Long/parseLong (second more)))
                              (nnext more))
        "--idoc-trace" (recur (assoc opts :idoc-trace? true) (next more))
        "--hotset"     (recur (assoc opts :hotset (Double/parseDouble (second more)))
                              (nnext more))
        "--dir"        (recur (assoc opts :dir (second more))
                              (nnext more))
        "--sqlite-path" (recur (assoc opts :sqlite-path (second more))
                               (nnext more))
        "--pg-url"     (recur (assoc opts :pg-url (second more))
                              (nnext more))
        "--pg-user"    (recur (assoc opts :pg-user (second more))
                              (nnext more))
        "--pg-password" (recur (assoc opts :pg-password (second more))
                               (nnext more))
        "--pg-table"   (recur (assoc opts :pg-table (second more))
                              (nnext more))
        "--mongo-uri"  (recur (assoc opts :mongo-uri (second more))
                              (nnext more))
        "--mongo-db"   (recur (assoc opts :mongo-db (second more))
                              (nnext more))
        "--mongo-coll" (recur (assoc opts :mongo-coll (second more))
                              (nnext more))
        "--keep"       (recur (assoc opts :keep-db? true) (next more))
        "--seed"       (recur (assoc opts :seed (Long/parseLong (second more)))
                              (nnext more))
        "--help"       (recur (assoc opts :help? true) (next more))
        (recur opts (next more)))
      opts)))

(defn- usage []
  (println "idoc-bench options:")
  (println "  --system datalevin|postgres|sqlite|mongo|all  (default datalevin)")
  (println "  --workload A|C|F   Workload type (default C)")
  (println "  --records N        Number of documents (default 100000)")
  (println "  --ops N            Number of operations (default 100000)")
  (println "  --warmup N         Warmup ops (default 2000)")
  (println "  --threads N        Number of worker threads (default 1)")
  (println "  --batch N          Load batch size (default 1000)")
  (println "  --idoc N           Weight for idoc queries (default 20)")
  (println "  --idoc-trace       Trace idoc candidate sizes and match stats")
  (println "  --hotset P         Hotset fraction (0-1, default 1.0)")
  (println "  --dir PATH         Datalevin DB directory (default /tmp/idoc-bench-<uuid>)")
  (println "  --sqlite-path PATH SQLite DB file path")
  (println "  --pg-url URL       Postgres JDBC URL")
  (println "  --pg-user USER     Postgres user")
  (println "  --pg-password PWD  Postgres password")
  (println "  --pg-table NAME    Postgres table (default idoc_bench_docs)")
  (println "  --mongo-uri URI    MongoDB URI")
  (println "  --mongo-db NAME    MongoDB database")
  (println "  --mongo-coll NAME  MongoDB collection")
  (println "  --seed N           RNG seed (default 42)")
  (println "  --keep             Keep DB artifacts after run")
  (println "  --help             Show this help"))

(defn -main
  [& args]
  (let [opts (parse-args args)]
    (when (:help? opts)
      (usage)
      (System/exit 0))
    (let [systems (if (= :all (:system opts))
                    [:datalevin :postgres :sqlite :mongo]
                    [(:system opts)])
          base-docs (generate-docs (:records opts) (:seed opts))]
      (doseq [system systems]
        (run-bench system opts base-docs)))))
