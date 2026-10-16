(ns datalevin.idoc-fulltext-fuzz
  "Stateful, shrinking fuzzer for indexed documents and fulltext (issue #353)."
  (:require
   [clojure.edn :as edn]
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [clojure.test.check :as tc]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.util :as u]))

(def ^:private attributes [:doc/md :doc/parts :doc/edn])
(def ^:private headings ["something" "preface" "heading"])
(def ^:private words ["officia" "laboris" "quartz" "zephyr"])
(def ^:private queries (conj (into headings words) "padding" "absenttoken"))

(def ^:private schema
  (into {}
        (for [attr attributes]
          [attr (cond-> {:db/valueType           :db.type/idoc
                         :db/idocFormat          :markdown
                         :db/fulltext            true
                         :db.fulltext/autoDomain true}
                  (= attr :doc/parts)
                  (assoc :db/cardinality :db.cardinality/many)
                  (= attr :doc/edn) (assoc :db/idocFormat :edn))])))

(def ^:private opts
  ;; Each trial owns its environment; avoid fsync and background sampling.
  {:kv-opts {:flags (conj c/default-env-flags :nosync)}})

(def ^:private document-gen
  (gen/let [heading (gen/elements headings)
            word    (gen/elements words)
            nested? gen/boolean
            copies  (gen/elements [0 1 100])
            parsed? gen/boolean]
    (let [body (str (str/capitalize word) "."
                    (apply str (repeat copies " padding")))
          doc  {(keyword heading) (if nested? {:section body} body)}]
      {:doc doc
       :input (if parsed?
                doc
                (str "# " heading "\n"
                     (when nested? "## section\n") body))})))

(def ^:private operation-gen
  (gen/let [eid   (gen/choose 1 4)
            attr  (gen/elements attributes)
            kind  (gen/frequency [[7 (gen/return :put)]
                                  [2 (gen/return :retract)]
                                  [1 (gen/return :retract-attribute)]
                                  [1 (gen/return :retract-entity)]
                                  [2 (gen/return :update-schema)]
                                  [2 (gen/return :re-index)]
                                  [2 (gen/return :reopen)]])
            doc   document-gen
            patch (gen/elements [{:db/doc "fuzz schema patch"}
                                  {:db/fulltext true}
                                  {:db.fulltext/autoDomain true}])]
    (case kind
      :put {:op kind :eid eid :attr attr
            :doc (:doc doc)
            :input (if (= attr :doc/edn)
                     (pr-str (:doc doc))
                     (:input doc))}
      (:retract :retract-attribute) {:op kind :eid eid :attr attr}
      :retract-entity {:op kind :eid eid}
      :update-schema {:op kind :attr attr :patch patch}
      {:op kind})))

(defn- agree!
  [check expected actual context]
  (when-not (= expected actual)
    (throw (ex-info "idoc/fulltext fuzz mismatch"
                    (assoc context :check check
                           :expected expected :actual actual)))))

(defn- fulltext-tuples
  [tuples]
  ;; Inline fulltext refs carry serialized idoc text; giant refs resolve to
  ;; stored maps. Compare their document contents in the same representation.
  (into #{} (map (fn [[eid attr value]]
                  [eid attr (if (string? value)
                              (edn/read-string value)
                              value)]))
        tuples))

(defn- check-model!
  [conn model]
  (let [db       @conn
        expected (into #{} (for [[[eid attr] docs] model, doc docs]
                             [eid attr doc]))
        actual   (set (d/q '[:find ?e ?a ?v
                            :where [?e ?a ?v]] db))]
    (agree! :datoms expected actual {})
    ;; The oracle tokenizes the model's data, without using the production
    ;; parser, analyzer, search index, or a second copy of the database.
    (doseq [query queries
            :let [matches (into #{}
                                (filter
                                  (fn [[_ _ doc]]
                                    (contains?
                                      (set (re-seq #"[a-z]+"
                                                   (str/lower-case (pr-str doc))))
                                      query)))
                                expected)
                  global  (fulltext-tuples
                            (d/q '[:find ?e ?a ?v
                                   :in $ ?q
                                   :where [(fulltext $ ?q) [[?e ?a ?v]]]]
                                 db query))]]
      (agree! :fulltext matches global {:query query})
      (doseq [attr attributes]
        (agree! :attribute-fulltext
                (into #{} (filter #(= attr (nth % 1))) matches)
                (fulltext-tuples
                  (d/q '[:find ?e ?a ?v
                         :in $ ?attr ?q
                         :where [(fulltext $ ?attr ?q) [[?e ?a ?v]]]]
                       db attr query))
                {:attr attr :query query})))
    (doseq [[[eid attr] docs] model, doc docs]
      (let [matches (set
                      (d/q '[:find ?e ?a ?v
                             :in $ ?attr ?q
                             :where [(idoc-match $ ?attr ?q) [[?e ?a ?v]]]]
                           db attr doc))]
        (when-not (contains? matches [eid attr doc])
          (throw (ex-info "idoc fuzz document missing"
                          {:check :idoc-match :eid eid :attr attr :doc doc
                           :actual matches})))))))

(defn- apply-operation!
  [conn* dir model {:keys [op eid attr doc input patch]}]
  (let [conn @conn*
        k    [eid attr]]
    (case op
      :put
      (do (d/transact! conn [[:db/add eid attr input]])
          (if (= attr :doc/parts)
            (update model k (fnil conj #{}) doc)
            (assoc model k #{doc})))

      :retract
      (if-let [value (first (sort-by pr-str (get model k)))]
        (do (d/transact! conn [[:db/retract eid attr value]])
            (let [remaining (disj (get model k) value)]
              (if (seq remaining)
                (assoc model k remaining)
                (dissoc model k))))
        model)

      :retract-attribute
      (do (d/transact! conn [[:db.fn/retractAttribute eid attr]])
          (dissoc model k))

      :retract-entity
      (do (d/transact! conn [[:db.fn/retractEntity eid]])
          (into {} (remove #(= eid (ffirst %))) model))

      :update-schema
      (do (d/update-schema conn {attr patch}) model)

      :re-index
      (do (vreset! conn* (d/re-index conn (d/schema conn) opts)) model)

      :reopen
      (do (d/close conn)
          ;; Read persisted schema and options, without supplying them again.
          (vreset! conn* (d/create-conn dir))
          model))))

(defn run-history!
  "Replay an operation vector in a fresh temporary database. Throw with the
  failing prefix, step, and oracle mismatch; always close and remove the DB."
  [history]
  (when-not (vector? history)
    (throw (ex-info "Expected an operation vector" {:history history})))
  (let [dir     (u/tmp-dir (str "datalevin-idoc-fulltext-fuzz-" (random-uuid)))
        conn*   (volatile! nil)
        prefix* (volatile! [])]
    (binding [c/*db-background-sampling?* false]
      (try
        (vreset! conn* (d/create-conn dir schema opts))
        (loop [model {}, operations (seq history)]
          (when-let [operation (first operations)]
            (vswap! prefix* conj operation)
            (let [model' (apply-operation! conn* dir model operation)]
              (check-model! @conn* model')
              (recur model' (next operations)))))
        true
        (catch Exception e
          (throw (ex-info "idoc/fulltext fuzz history failed"
                          (merge (ex-data e)
                                 {:step (dec (count @prefix*))
                                  :history @prefix*})
                          e)))
        (finally
          (try
            (when-let [conn @conn*]
              (when-not (d/closed? conn) (d/close conn)))
            (finally (u/delete-files dir))))))))

(defn check!
  "Run generated operation histories. test.check shrinks failures by removing
  operations and simplifying documents. Accept :seed, :trials and :steps."
  [{:keys [seed trials steps] :or {trials 100 steps 40}}]
  (when-not (and (pos-int? trials) (pos-int? steps))
    (throw (ex-info ":trials and :steps must be positive integers"
                    {:trials trials :steps steps})))
  (let [property (prop/for-all [history (gen/vector operation-gen 1 steps)]
                   (run-history! history))]
    (apply tc/quick-check trials property
           (when (some? seed) [:seed seed]))))

(defn -main
  "Run with an EDN options map, or replay an EDN failure file."
  [& [options]]
  (try
    (let [options (merge {:trials 100 :steps 40
                         :seed (System/currentTimeMillis)}
                        (when options (edn/read-string options)))]
      (if-let [file (:replay options)]
        (do (run-history! (:history (edn/read-string (slurp file))))
            (println "Replay passed:" file))
        (let [_      (println "Fuzzing with" (pr-str options))
              _      (flush)
              result (check! options)]
          (if (:pass? result)
            (pprint/pprint (select-keys result [:pass? :num-tests :seed]))
            (let [failure (or (get-in result [:shrunk :result]) (:result result))
                  history (or (:history (ex-data failure))
                              (first (get-in result [:shrunk :smallest]))
                              (first (:fail result)))
                  file    (java.io.File/createTempFile
                            "datalevin-idoc-fulltext-failure-" ".edn")]
              (spit file (with-out-str
                           (pprint/pprint
                             {:seed (:seed result)
                              :options options
                              :failure (dissoc (ex-data failure) :history)
                              :history history})))
              (binding [*out* *err*]
                (println "Fuzzer failed. Replay saved to" (.getPath file))
                (println "script/idoc-fulltext-fuzz"
                         (pr-str (str "{:replay " (pr-str (.getPath file)) "}"))))
              (System/exit 1))))))
    (catch Exception e
      (binding [*out* *err*]
        (println (ex-message e))
        (pprint/pprint (ex-data e)))
      (System/exit 1))
    (finally (shutdown-agents))))
