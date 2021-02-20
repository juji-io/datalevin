(ns datalevin.main
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as s]
            [clojure.pprint :as p]
            [clojure.stacktrace :as st]
            [datalevin.core :as d]
            [datalevin.util :refer [raise]]
            [datalevin.bits :as b]
            [datalevin.lmdb :as l]
            [datalevin.datom :as dt]
            [datalevin.binding.graal])
  (:gen-class))

(def version "0.4.0")

(def cli-opts
  [["-a" "--all" "Include all of the sub-databases"]
   ["-c" "--compact" "Compact while copying."]
   ["-d" "--dir PATH" "Path to the database directory"]
   ["-D" "--delete" "Delete the sub-database, not just empty it"]
   ["-f" "--file PATH" "Path to the specified file"]
   ["-h" "--help" "Show usage"]
   ["-l" "--list" "List the names of sub-databases instead of the content"]
   ["-t" "--text" "Load data from a simple text format: paired lines of text"]
   ["-V" "--version" "Show Datalevin version and exit"]])

(def stat-help
  "
  Command stat - show statistics of the main database or a sub-database.

  Required option:
      -d --dir PATH   Path to the database directory
  Optional arguments:
      name(s) of sub-database(s)

  Examples:
      dtlv -d /data/companydb stat sales
      dtlv -d /data/companydb stat sales products")

(def dump-help
  "
  Command dump - dump the content of the database or a sub-database.

  Required option:
      -d --dir PATH  Path to the database directory
  Optional options:
      -a --all        All of the sub-databases
      -f --file PATH  Write to the specified file instead of the standard output
      -l --list       List the names of sub-databases instead of the content
  Optional arguments:
      name(s) of sub-database(s)

  Examples:
      dtlv -d /data/companydb -a dump
      dtlv -d /data/companydb -l dump
      dtlv -d /data/companydb -f ~/sales-data dump sales")

(def load-help
  "
  Command load - load data into the database or a sub-database.

  Required option:
      -d --dir  PATH  Path to the database directory
  Optional option:
      -f --file PATH  Load from the specified file instead of the standard input
      -t --text       Input is a simple text format: paired lines text, where
                      the first line is the key, the second the value
  Optional argument:
      Name of the sub-database to load the data into

  Examples:
      dtlv -d /data/companydb -f ~/sales-data load sales")

(def copy-help
  "
  Command copy - Copy the database. This can be done regardless of whether it is
  currently in use.

  Required option:
      -d --dir PATH   Path to the database directory
  Optional option:
      -c --compact    Compact while copying. Only pages in use will be copied.
  Optional argument:
      Path of the destination directory if specified, otherwise, the copy is
      written to the standard output.

  Examples:
      dtlv -d /data/companydb -c copy /backup/companydb-2021-02-14")

(def drop-help
  "
  Command drop - Drop or clear the content of sub-database(s).

  Required option:
      -d --dir PATH   Path to the database directory
  Optional option:
      -D --delete     Delete the sub-database, not just empty it.
  Optional argument:
      Name(s) of the sub-database(s), otherwise, the main database is operated on

  Examples:
      dtlv -d /data/companydb -D drop sales")

(def exec-help
  "
  Command exec - Execute database transaction or query.

  Required argument:
      The code to be executed as a string.

  Examples:
      dtlv exec (def conn (open-lmdb '/data/companydb')) \\
                (transact! conn [{:name \"Dataleinv\" :db/id -1}])")

(defn- show-version []
  (str "Datalevin (version: " version ")"))

(defn usage [options-summary]
  (->> [""
        (show-version)
        ""
        "Usage: dtlv [options] <command> [args]"
        ""
        "Commands:"
        "  exec  Execute database transactions or queries"
        "  copy  Copy a database, regardless of whether it is now in use"
        "  drop  Drop or clear a database"
        "  dump  Dump the content of a database to standard output"
        "  load  Load data from standard input into a database"
        "  stat  Display statistics of database"
        ""
        "Options:"
        options-summary
        ""
        "See 'dtlv help <command>' to read about a specific command."]
       (s/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (s/join \newline errors)))

(defn validate-args
  "Validate command line arguments. Either return a map indicating the program
  should exit (with a error message, and optional ok status), or a map
  indicating the action the program should take and the options provided."
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-opts)]
    (cond
      (:version options)
      {:exit-message (show-version) :ok? true}
      (:help options) ; help => exit OK with usage summary
      {:exit-message (usage summary) :ok? true}
      errors          ; errors => exit with description of errors
      {:exit-message (error-msg errors)}
      (#{"conn" "copy" "dump" "load" "stat" "help"} (first arguments))
      {:command   (first arguments)
       :options   options
       :arguments (rest arguments)
       :summary   summary}
      :else           ; failed custom validation => exit with usage summary
      {:exit-message (usage summary)})))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn- dtlv-help [arguments summary]
  (if (seq arguments)
    (let [command (s/lower-case (first arguments))]
      (exit 0 (case command
                "exec" exec-help
                "copy" copy-help
                "drop" drop-help
                "dump" dump-help
                "load" load-help
                "stat" stat-help
                (str "Unknown command: " command))))
    (exit 0 (usage summary))))

(defn- dtlv-exec [options arguments]
  (try
    (let [schema {:aka  {:db/cardinality :db.cardinality/many}
                  :name {:db/valueType :db.type/string
                         :db/unique    :db.unique/identity}}
          conn   (d/create-conn "/tmp/dtlv-test" schema) ]

      (d/transact! conn
                   [{:name "Frege", :db/id -1, :nation "France", :aka ["foo" "fred"]}
                    {:name "Peirce", :db/id -2, :nation "france"}
                    {:name "De Morgan", :db/id -3, :nation "English"}])
      (prn (d/q '[:find ?nation
                  :in $ ?alias
                  :where
                  [?e :aka ?alias]
                  [?e :nation ?nation]]
                @conn
                "fred"))
      (d/close conn))
    (catch Exception e
      (println (str "Error: " (.getMessage e)))
      (st/print-cause-trace e)))
  (exit 0 "finished"))

(defn- dtlv-copy [options arguments]
  )

(defn- dtlv-dump [options arguments]
  )

(defn- dtlv-load [options arguments]
  )

(defn- dtlv-stat [{:keys [dir]} arguments]
  (assert dir (str "Missing data directory path.\n" stat-help))
  (let [dbi  (first arguments)
        lmdb (l/open-lmdb dir)]
    (p/pprint (if dbi
                (do (l/open-dbi lmdb dbi)
                    (l/stat lmdb dbi))
                (l/stat lmdb)))
    (l/close-lmdb lmdb)))

(defn- dtlv-drop [options arguments]
  )

(defn -main [& args]
  (let [{:keys [command options arguments summary exit-message ok?]}
        (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (case command
        "exec" (dtlv-exec options arguments)
        "copy" (dtlv-copy options arguments)
        "drop" (dtlv-drop options arguments)
        "dump" (dtlv-dump options arguments)
        "load" (dtlv-load options arguments)
        "stat" (dtlv-stat options arguments)
        "help" (dtlv-help arguments summary)))))
