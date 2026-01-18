(ns ldbc-snb-bench.core
  "LDBC SNB Benchmark for Datalevin.

   Main entry point for loading data and running benchmarks.

   Usage:
     clj -M:load <data-dir>              ; Load LDBC SNB data
     clj -M:bench                        ; Run benchmark queries
     clj -M:bench IS1 IS2                ; Run specific queries
     clj -M:bench -o results.csv IS1     ; Run with custom results file
     clj -M:bench -p perf.csv            ; Run with custom perf file"
  (:require [clojure.java.io :as io]
            [clojure.stacktrace :as stacktrace]
            [clojure.string :as str]
            [datalevin.core :as d]
            [datalevin.query :as q]
            [ldbc-snb-bench.schema :as schema]
            [ldbc-snb-bench.loader :as loader]
            [ldbc-snb-bench.queries.common :as common]
            [ldbc-snb-bench.queries.interactive :as ic]
            [ldbc-snb-bench.queries.short :as is])
  (:import [java.time Instant ZoneOffset ZonedDateTime]
           [java.time.format DateTimeFormatter]
           [java.util Date]))

;; ============================================================
;; Configuration
;; ============================================================

(def db-path "db/ldbc-snb")
(def default-results-path "results/results.csv")
(def default-perf-path "results/perf.csv")
(def default-data-path "data")
(def debug-errors? (boolean (System/getenv "LDBC_BENCH_DEBUG")))
(def fail-fast? (boolean (System/getenv "LDBC_BENCH_FAIL_FAST")))
(def show-results? (boolean (System/getenv "LDBC_BENCH_SHOW_RESULTS")))

(def ^DateTimeFormatter date-formatter
  (DateTimeFormatter/ofPattern "yyyy-MM-dd"))

(def ^DateTimeFormatter datetime-formatter
  (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

(defn- midnight-utc?
  [^Date d]
  (let [zdt (ZonedDateTime/ofInstant (.toInstant d) ZoneOffset/UTC)]
    (and (zero? (.getHour zdt))
         (zero? (.getMinute zdt))
         (zero? (.getSecond zdt))
         (zero? (.getNano zdt)))))

(defn- format-date
  [^Date d]
  (let [zdt (ZonedDateTime/ofInstant (.toInstant d) ZoneOffset/UTC)]
    (if (midnight-utc? d)
      (.format date-formatter zdt)
      (.format datetime-formatter zdt))))

(defn- escape-string
  [^String s]
  (str/replace s "\"" "\\\""))

(defn- format-value
  [v]
  (cond
    (nil? v) "null"
    (string? v) (str "\"" (escape-string v) "\"")
    (instance? Date v) (format-date v)
    (instance? Instant v) (.format datetime-formatter (ZonedDateTime/ofInstant v ZoneOffset/UTC))
    (.isArray (class v)) (str "[" (str/join ", " (map format-value (seq v))) "]")
    (sequential? v) (str "[" (str/join ", " (map format-value v)) "]")
    (set? v) (str "[" (str/join ", " (map format-value (sort-by str v))) "]")
    (keyword? v) (name v)
    :else (str v)))

(defn- row->cells
  [row]
  (cond
    (nil? row) []
    (.isArray (class row)) (vec row)
    (sequential? row) row
    :else [row]))

(defn- format-row
  [query row]
  (let [cells (row->cells row)
        values (cons query cells)]
    (str/join ", " (map format-value values))))

(defn- query-find-columns
  [query]
  (when (sequential? query)
    (let [after-find (next (drop-while #(not= % :find) query))]
      (when (seq after-find)
        (->> after-find
             (take-while #(not (keyword? %)))
             (map (fn [x]
                    (if (symbol? x)
                      (subs (str x) 1)
                      (str x))))
             (vec))))))

(defn- result-columns
  [result]
  (or (:columns result)
      (when-let [row (first (:rows result))]
        (mapv #(str "col" %) (range (count (row->cells row)))))))

;; ============================================================
;; Sample query parameters for benchmarking
;; These would typically come from LDBC substitution_parameters/
;; ============================================================

(defn to-date
  "Convert Instant to Date for Datalevin"
  [s]
  (Date/from (Instant/parse s)))

(def sample-params
  "Sample parameters for benchmark queries.
   In a real benchmark, these come from LDBC's substitution_parameters files.
   Note: These IDs are from the SF1 LDBC SNB Datagen output."
  {:ic1  {:person-id  100
          :first-name "John"}
   :ic2  {:person-id 100
          :max-date  (to-date "2012-07-01T00:00:00Z")}
   :ic3  {:person-id      100
          :country-x-name "Germany"
          :country-y-name "France"
          :start-date     (to-date "2011-01-01T00:00:00Z")
          :duration-days  365}
   :ic4  {:person-id     100
          :start-date    (to-date "2011-01-01T00:00:00Z")
          :duration-days 365}
   :ic5  {:person-id 100
          :min-date  (to-date "2011-01-01T00:00:00Z")}
   :ic6  {:person-id 100
          :tag-name  "Mozart"}
   :ic7  {:person-id 100}
   :ic8  {:person-id 100}
   :ic9  {:person-id 100
          :max-date  (to-date "2012-07-01T00:00:00Z")}
   :ic10 {:person-id 100
          :month     5}
   :ic11 {:person-id      100
          :country-name   "Germany"
          :work-from-year 2010}
   :ic12 {:person-id      100
          :tag-class-name "MusicalArtist"}
   :ic13 {:person1-id 100
          :person2-id 6597069770569}
   :ic14 {:person1-id 100
          :person2-id 6597069770569}
   :is1  {:person-id 100}
   :is2  {:person-id 100}
   :is3  {:person-id 100}
   :is4  {:message-id 1099512606636}
   :is5  {:message-id 1099512606636}
   :is6  {:message-id 1099512606636}
   :is7  {:message-id 1099512606636}})


;; ============================================================
;; Database connection
;; ============================================================

(defn get-conn
  "Get a connection to the LDBC SNB database."
  ([]
   (get-conn db-path))
  ([path]
   (d/get-conn path schema/schema schema/db-opts)))

(defn close-conn [conn]
  (d/close conn))

;; ============================================================
;; Data loading
;; ============================================================

(defn load-data
  "Load LDBC SNB data from the given directory."
  [data-dir]
  (println "============================================")
  (println "LDBC SNB Data Loader for Datalevin")
  (println "============================================")
  (println)
  (println "Data directory:" data-dir)
  (println "Database path:" db-path)
  (println)

  ;; Check if data directory exists
  (when-not (.exists (io/file data-dir))
    (println "ERROR: Data directory does not exist:" data-dir)
    (println)
    (println "Please generate LDBC SNB data using:")
    (println "  git clone https://github.com/ldbc/ldbc_snb_datagen_spark.git")
    (println "  cd ldbc_snb_datagen_spark")
    (println "  ./tools/run.py --parallelism 4 --memory 8g -- \\")
    (println "      --format csv --scale-factor 1 --mode raw")
    (println)
    (println "Then copy the output to:" data-dir)
    (System/exit 1))

  ;; Load data using empty-db + fill-db pattern
  (loader/load-all-data db-path data-dir))

;; ============================================================
;; Benchmark execution
;; ============================================================

(defn run-query
  "Run a single query and return timing results."
  [db query-def params]
  (if-let [runner (:runner query-def)]
    (let [start-time (System/nanoTime)
          result-rows (vec (runner db params))
          result-count (count result-rows)
          end-time (System/nanoTime)
          exec-time (/ (- end-time start-time) 1000000.0)]
      {:name (:name query-def)
       :planning-time 0.0
       :execution-time exec-time
       :result-count result-count
       :rows result-rows
       :columns (:columns query-def)})
    (let [query (:query query-def)
          rules (:rules query-def)
          post-process (:post-process query-def)
          param-vals (map #(get params %) (:params query-def))
          ;; Build query inputs
          inputs (if rules
                   (concat [db rules] param-vals)
                   (concat [db] param-vals))
          start-time (System/nanoTime)]
      (let [rows (apply d/q query inputs)
            final-rows (if post-process
                         (post-process db params rows)
                         rows)
            result-rows (vec final-rows)
            result-count (count result-rows)
            end-time (System/nanoTime)
            exec-time (/ (- end-time start-time) 1000000.0)]
        {:name (:name query-def)
         :planning-time 0.0
         :execution-time exec-time
         :result-count result-count
         :rows result-rows
         :columns (query-find-columns query)}))))

(defn- normalize-query-names
  [names]
  (->> names
       (map str/trim)
       (remove str/blank?)
       (map str/upper-case)))

(defn run-all-queries
  "Run benchmark queries and collect results.
   When query-names is provided, only those queries are executed."
  ([db params]
   (run-all-queries db params nil))
  ([db params query-names]
   (let [all-query-defs (concat ic/all-queries is/all-queries)
         wanted (when (seq query-names)
                  (set (normalize-query-names query-names)))
         query-defs (if wanted
                      (filter #(contains? wanted (:name %)) all-query-defs)
                      all-query-defs)]
     (for [query-def query-defs]
       (let [param-key (keyword (str/lower-case (:name query-def)))
             query-params (get params param-key {})]
         (println "Running" (:name query-def) "-" (:description query-def))
         (try
           (let [result (run-query db query-def query-params)]
             (when (and show-results? (contains? result :rows))
               (println "  Results:")
               (if (seq (:rows result))
                 (doseq [row (:rows result)]
                   (println "   " (pr-str row)))
                 (println "   []")))
             result)
           (catch Exception e
             (when debug-errors?
               (println "  DEBUG: stack trace for" (:name query-def))
               (stacktrace/print-stack-trace e)
               (println))
             (when fail-fast?
               (throw e))
             (println "  ERROR:" (.getMessage e))
             {:name (:name query-def)
              :planning-time -1
              :execution-time -1
              :result-count 0
              :error (.getMessage e)})))))))

(defn write-results-rows
  "Write query result rows to a Neo4j-style plain CSV file."
  [results results-path]
  (io/make-parents results-path)
  (with-open [w (io/writer results-path)]
    (doseq [r results]
      (let [rows (:rows r)
            columns (result-columns r)]
        (when (seq columns)
          (.write w (str "query, " (str/join ", " columns) "\n")))
        (when (seq rows)
          (doseq [row rows]
            (.write w (str (format-row (:name r) row) "\n"))))))))

(defn write-perf
  "Write benchmark timing results to CSV file."
  [results perf-path]
  (io/make-parents perf-path)
  (with-open [w (io/writer perf-path)]
    (.write w "Query,Total Time (ms),Result Count,Error\n")
    (doseq [r results]
      (let [has-error (some? (:error r))
            exec-time (if (number? (:execution-time r)) (:execution-time r) 0)
            count-val (if (number? (:result-count r)) (:result-count r) 0)]
        (if has-error
          (.write w (format "%s,,,%s\n"
                            (:name r)
                            (str/replace (str (:error r)) "," ";")))
          (.write w (format "%s,%.3f,%d,\n"
                            (:name r)
                            (double exec-time)
                            (long count-val))))))))

(defn print-summary
  "Print benchmark summary to console."
  [results]
  (println)
  (println "============================================")
  (println "Benchmark Results Summary")
  (println "============================================")
  (println)
  (println (format "%-8s %-35s %12s %8s"
                   "Query" "Description" "Total (ms)" "Results"))
  (println (apply str (repeat 80 "-")))

  (doseq [r results]
    (let [exec (if (number? (:execution-time r)) (:execution-time r) -1)
          cnt (if (number? (:result-count r)) (:result-count r) 0)]
      (println (format "%-8s %-35s %12.3f %8d"
                       (:name r)
                       (or (:description r) "")
                       (double exec)
                       cnt))))

  (println (apply str (repeat 80 "-")))

  ;; Summary statistics
  (let [valid-results (filter #(and (number? (:execution-time %))
                                    (pos? (:execution-time %))) results)
        ic-results (filter #(str/starts-with? (:name %) "IC") valid-results)
        is-results (filter #(str/starts-with? (:name %) "IS") valid-results)]
    (println)
    (println "Interactive Complex (IC) queries:")
    (println (format "  Successful: %d / %d" (count ic-results) (count (filter #(str/starts-with? (:name %) "IC") results))))
    (when (seq ic-results)
      (println (format "  Avg Total Time: %.3f ms"
                       (/ (reduce + (map #(or (:execution-time %) 0) ic-results)) (count ic-results)))))

    (println)
    (println "Interactive Short (IS) queries:")
    (println (format "  Successful: %d / %d" (count is-results) (count (filter #(str/starts-with? (:name %) "IS") results))))
    (when (seq is-results)
      (println (format "  Avg Total Time: %.3f ms"
                       (/ (reduce + (map #(or (:execution-time %) 0) is-results)) (count is-results)))))))

(defn run-benchmark
  "Run the LDBC SNB benchmark.
   Options:
     :query-names  - seq of query names to run (nil for all)
     :results-path - path for results CSV (default: results/results.csv)
     :perf-path    - path for perf CSV (default: results/perf.csv)"
  ([] (run-benchmark {}))
  ([opts]
   (let [opts (if (sequential? opts)
                {:query-names opts}  ; backwards compatible: treat seq as query-names
                opts)
         {:keys [query-names results-path perf-path]
          :or {results-path default-results-path
               perf-path default-perf-path}} opts]
     (println "============================================")
     (println "LDBC SNB Benchmark for Datalevin")
     (println "============================================")
     (println)

     ;; Check if database exists
     (when-not (.exists (io/file db-path))
       (println "ERROR: Database does not exist:" db-path)
       (println "Please run 'clj -M:load <data-dir>' first to load data.")
       (System/exit 1))

     (let [conn (get-conn)]
       (try
         (let [db (d/db conn)]
           (println "Database:" db-path)
           (println)
           (if (seq query-names)
             (println "Running benchmark queries:" (str/join ", " (normalize-query-names query-names)))
             (println "Running benchmark queries..."))
           (println)

           (let [results (doall (run-all-queries db sample-params query-names))]
             (write-results-rows results results-path)
             (write-perf results perf-path)
             (print-summary results)
             (println)
             (println "Results written to:" results-path)
             (println "Perf written to:" perf-path)))
         (finally
           (close-conn conn)))))))

;; ============================================================
;; CLI entry point
;; ============================================================

(defn- parse-bench-args
  "Parse bench command arguments.
   Supports:
     -o, --results <path>  : results CSV output path
     -p, --perf <path>     : perf CSV output path
     [query names...]      : specific queries to run"
  [args]
  (loop [args args
         opts {}]
    (if (empty? args)
      opts
      (let [[arg & rest-args] args]
        (cond
          (or (= arg "-o") (= arg "--results"))
          (recur (rest rest-args)
                 (assoc opts :results-path (first rest-args)))

          (or (= arg "-p") (= arg "--perf"))
          (recur (rest rest-args)
                 (assoc opts :perf-path (first rest-args)))

          :else
          (recur rest-args
                 (update opts :query-names (fnil conj []) arg)))))))

(defn -main
  "Main entry point.

   Usage:
     clj -M -m ldbc-snb-bench.core load <data-dir>
     clj -M -m ldbc-snb-bench.core bench [options] [IS1 IS2 ...]

   Bench options:
     -o, --results <path>  Output path for results CSV (default: results/results.csv)
     -p, --perf <path>     Output path for perf CSV (default: results/perf.csv)"
  [& args]
  (let [cmd (first args)]
    (case cmd
      "load"  (load-data (or (second args) default-data-path))
      "bench" (run-benchmark (parse-bench-args (rest args)))
      ;; Default: show usage
      (do
        (println "LDBC SNB Benchmark for Datalevin")
        (println)
        (println "Usage:")
        (println "  clj -M -m ldbc-snb-bench.core load [data-dir]  ; Load LDBC SNB data")
        (println "  clj -M -m ldbc-snb-bench.core bench [options] [IS1 IS2 ...]  ; Run benchmark")
        (println)
        (println "Bench options:")
        (println "  -o, --results <path>  Output path for results CSV (default: results/results.csv)")
        (println "  -p, --perf <path>     Output path for perf CSV (default: results/perf.csv)")
        (println)
        (println "Example:")
        (println "  # Generate data using LDBC SNB Datagen first")
        (println "  clj -M -m ldbc-snb-bench.core load data/")
        (println "  clj -M -m ldbc-snb-bench.core bench IS1")
        (println "  clj -M -m ldbc-snb-bench.core bench -o my-results.csv IS1 IS2")))))

;; ============================================================
;; REPL helpers
;; ============================================================

(comment
  ;; Load data
  (load-data "data")

  ;; Run benchmark
  (run-benchmark)

  ;; Interactive exploration
  (def conn (get-conn))
  (def db (d/db conn))

  (binding [q/*debug-plan* true]
    (let [{:keys [person-id min-date]} (:ic5 sample-params)]
      (d/explain {:run? true} (:query ic/ic5)
                 db person-id min-date)))



  ;; Count entities
  (d/q '[:find (count ?p) :where [?p :person/id _]] db)
  (d/q '[:find (count ?m) :where [?m :message/id _]] db)

  ;; Sample person
  (d/q '[:find ?id ?first ?last
         :where
         [?p :person/id ?id]
         [?p :person/firstName ?first]
         [?p :person/lastName ?last]
         :limit 5]
       db)

  ;; Close connection
  (close-conn conn)
  )
