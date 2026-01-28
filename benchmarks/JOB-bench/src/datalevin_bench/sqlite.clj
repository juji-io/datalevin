(ns datalevin-bench.sqlite
  (:require
   [datalevin.core :as d]
   [clojure.java.io :as io]
   [clojure.string :as s])
  (:import
   [java.sql DriverManager Connection PreparedStatement]
   [java.util.concurrent Executors TimeUnit TimeoutException]))

(def db-path "sqlite.db")
(def db-url (str "jdbc:sqlite:" db-path))

(def table-columns
  "Map of table name to its column names (in CSV order)."
  {"aka_name"        ["id" "person_id" "name" "imdb_index" "name_pcode_cf"
                      "name_pcode_nf" "surname_pcode" "md5sum"]
   "aka_title"       ["id" "movie_id" "title" "imdb_index" "kind_id"
                      "production_year" "phonetic_code" "episode_of_id"
                      "season_nr" "episode_nr" "note" "md5sum"]
   "cast_info"       ["id" "person_id" "movie_id" "person_role_id" "note"
                      "nr_order" "role_id"]
   "char_name"       ["id" "name" "imdb_index" "imdb_id" "name_pcode_nf"
                      "surname_pcode" "md5sum"]
   "comp_cast_type"  ["id" "kind"]
   "company_name"    ["id" "name" "country_code" "imdb_id" "name_pcode_nf"
                      "name_pcode_sf" "md5sum"]
   "company_type"    ["id" "kind"]
   "complete_cast"   ["id" "movie_id" "subject_id" "status_id"]
   "info_type"       ["id" "info"]
   "keyword"         ["id" "keyword" "phonetic_code"]
   "kind_type"       ["id" "kind"]
   "link_type"       ["id" "link"]
   "movie_companies" ["id" "movie_id" "company_id" "company_type_id" "note"]
   "movie_info"      ["id" "movie_id" "info_type_id" "info" "note"]
   "movie_info_idx"  ["id" "movie_id" "info_type_id" "info" "note"]
   "movie_keyword"   ["id" "movie_id" "keyword_id"]
   "movie_link"      ["id" "movie_id" "linked_movie_id" "link_type_id"]
   "name"            ["id" "name" "imdb_index" "imdb_id" "gender"
                      "name_pcode_cf" "name_pcode_nf" "surname_pcode" "md5sum"]
   "person_info"     ["id" "person_id" "info_type_id" "info" "note"]
   "role_type"       ["id" "role"]
   "title"           ["id" "title" "imdb_index" "kind_id" "production_year"
                      "imdb_id" "phonetic_code" "episode_of_id" "season_nr"
                      "episode_nr" "series_years" "md5sum"]})

;; Integer columns per table (for parsing CSV values)
(def int-columns
  {"aka_name"        #{"id" "person_id"}
   "aka_title"       #{"id" "movie_id" "kind_id" "production_year"
                       "episode_of_id" "season_nr" "episode_nr"}
   "cast_info"       #{"id" "person_id" "movie_id" "person_role_id"
                       "nr_order" "role_id"}
   "char_name"       #{"id" "imdb_id"}
   "comp_cast_type"  #{"id"}
   "company_name"    #{"id" "imdb_id"}
   "company_type"    #{"id"}
   "complete_cast"   #{"id" "movie_id" "subject_id" "status_id"}
   "info_type"       #{"id"}
   "keyword"         #{"id"}
   "kind_type"       #{"id"}
   "link_type"       #{"id"}
   "movie_companies" #{"id" "movie_id" "company_id" "company_type_id"}
   "movie_info"      #{"id" "movie_id" "info_type_id"}
   "movie_info_idx"  #{"id" "movie_id" "info_type_id"}
   "movie_keyword"   #{"id" "movie_id" "keyword_id"}
   "movie_link"      #{"id" "movie_id" "linked_movie_id" "link_type_id"}
   "name"            #{"id" "imdb_id"}
   "person_info"     #{"id" "person_id" "info_type_id"}
   "role_type"       #{"id"}
   "title"           #{"id" "kind_id" "production_year" "imdb_id"
                      "episode_of_id" "season_nr" "episode_nr"}})

(defn set-param!
  "Set a PreparedStatement parameter, using setNull for empty strings on int columns."
  [^PreparedStatement ps idx value int-col?]
  (if int-col?
    (if (s/blank? value)
      (.setNull ps idx java.sql.Types/INTEGER)
      (.setInt ps idx (Integer/parseInt value)))
    (if (s/blank? value)
      (.setNull ps idx java.sql.Types/VARCHAR)
      (.setString ps idx value))))

(defn load-csv!
  "Load a CSV file into the given table using batch inserts."
  [^Connection conn table-name]
  (let [cols     (get table-columns table-name)
        ncols    (count cols)
        icols    (get int-columns table-name #{})
        placeholders (s/join "," (repeat ncols "?"))
        sql      (str "INSERT INTO " table-name " VALUES (" placeholders ")")
        csv-file (io/file (str "data/" table-name ".csv"))]
    (when (.exists csv-file)
      (println "  Loading" table-name "...")
      (with-open [rdr (io/reader csv-file)
                  ps  (.prepareStatement conn sql)]
        (let [cnt (atom 0)]
          (doseq [fields (d/read-csv rdr)]
            (when (= (count fields) ncols)
              (dotimes [i ncols]
                (set-param! ps (inc i) (nth fields i)
                            (contains? icols (nth cols i))))
              (.addBatch ps)
              (when (zero? (mod (swap! cnt inc) 10000))
                (.executeBatch ps))))
          (.executeBatch ps))))))

(defn db
  "Load CSV data into SQLite database."
  [& _opts]
  (println "Loading data into SQLite, please wait...")
  (let [db-file (io/file db-path)]
    (when (.exists db-file)
      (.delete db-file)
      (println "Deleted existing" db-path)))
  (with-open [conn (DriverManager/getConnection db-url)]
    (let [stmt (.createStatement conn)]
      ;; Create tables
      (println "Creating schema...")
      (doseq [ddl (s/split (slurp "data/schema.sql") #";\s*")]
        (let [ddl (s/trim ddl)]
          (when (seq ddl)
            (.executeUpdate stmt ddl))))
      (.close stmt))
    ;; Load CSVs
    (let [start (System/nanoTime)]
      (.setAutoCommit conn false)
      (doseq [table (keys table-columns)]
        (load-csv! conn table))
      (.commit conn)
      (.setAutoCommit conn true)
      (println (format "Load time: %.3f s" (/ (- (System/nanoTime) start) 1e9))))
    ;; Create indexes
    (println "Creating indexes...")
    (let [start (System/nanoTime)
          stmt  (.createStatement conn)]
      (doseq [line (s/split-lines (slurp "data/fkindexes.sql"))]
        (let [line (s/trim line)]
          (when (seq line)
            (.executeUpdate stmt line))))
      (.close stmt)
      (println (format "Index time: %.3f s" (/ (- (System/nanoTime) start) 1e9))))
    ;; Analyze
    (println "Running ANALYZE...")
    (let [start (System/nanoTime)
          stmt  (.createStatement conn)]
      (.executeUpdate stmt "ANALYZE")
      (.close stmt)
      (println (format "Analyze time: %.3f s" (/ (- (System/nanoTime) start) 1e9)))))
  (println "Done. SQLite database created at" db-path))

(defn- query-files
  "Return sorted list of .sql files in the queries directory."
  []
  (->> (.listFiles (io/file "queries"))
       (filter #(and (.isFile %) (s/ends-with? (.getName %) ".sql")))
       (sort-by #(.getName %))))

(defn -main
  "Run JOB benchmark queries against SQLite."
  [& _opts]
  (println "Running JOB benchmark against SQLite...")
  (let [result-file "sqlite_onepass_time.csv"]
    (with-open [conn (DriverManager/getConnection db-url)
                w    (io/writer result-file)]
      ;; Enable WAL mode and mmap for better read performance
      (let [stmt (.createStatement conn)]
        (.execute stmt "PRAGMA journal_mode=WAL")
        (.execute stmt "PRAGMA mmap_size=1073741824")
        (.close stmt))
      (let [pool (Executors/newSingleThreadExecutor)]
        (d/write-csv w [["Query Name" "Execution Time (ms)"]])
        (doseq [f (query-files)]
          (let [qname (s/replace (.getName f) ".sql" "")
                sql   (slurp f)
                _     (print (str "  " qname "... "))
                stmt  (.createStatement conn)
                fut   (.submit pool
                               ^Callable
                               (fn []
                                 (let [start (System/nanoTime)
                                       rs    (.executeQuery stmt sql)]
                                   (while (.next rs))
                                   (.close rs)
                                   (/ (- (System/nanoTime) start) 1e6))))]
            (try
              (let [elapsed (.get fut 60 TimeUnit/SECONDS)]
                (println (format "%.3f ms" elapsed))
                (d/write-csv w [[qname (format "%.3f" elapsed)]]))
              (catch TimeoutException _
                (.cancel fut true)
                (try (.cancel stmt) (catch Exception _))
                (println "TIMEOUT")
                (d/write-csv w [[qname "timeout"]]))
              (catch Exception e
                (println (str "ERROR: " (.getMessage e)))
                (d/write-csv w [[qname "error"]])))))
        (.shutdown pool)))
    (println "Done. Results written to" result-file)))
