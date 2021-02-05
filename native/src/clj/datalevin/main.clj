(ns datalevin.main
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as s]
            [clojure.stacktrace :as st]
            [datalevin.core :as d]
            [datalevin.bits :as b]
            [datalevin.lmdb :as l]
            [datalevin.binding.graal])
  (:import [datalevin.ni Lib])
  (:gen-class))

(def cli-opts
  [["-h" "--help"]])

(defn usage [options-summary]
  (->> ["Datalevin"
        ""
        "Usage: dtlv [options] <command> [args]"
        ""
        "Options:"
        options-summary
        ""
        "Commands:"
        "  conn  Connect to a database to work with"
        "  copy  Copy a database, regardless of whether it is now in use"
        "  dump  Dump the content of a database as text to standard output"
        "  load  Load text from standard input into a database"
        "  stat  Display status of a database"
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
      (:help options) ; help => exit OK with usage summary
      {:exit-message (usage summary) :ok? true}
      errors          ; errors => exit with description of errors
      {:exit-message (error-msg errors)}
      (#{"conn" "copy" "dump" "load" "stat" "help"} (first arguments))
      {:command   (first arguments)
       :options   options
       :arguments (rest arguments)}
      :else           ; failed custom validation => exit with usage summary
      {:exit-message (usage summary)})))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn- dtlv-help [options arguments]
  )

(defn- dtlv-conn [options arguments]
  (try
    #_(let [db         (l/open-lmdb "/tmp/dtlv-lmdb-test")
            misc-table "misc-test-table"]
        (println "env opened")
        (l/open-dbi db misc-table)
        (println "dbi opened")
        (l/transact db
                    [[:put misc-table "a" "a" :string]
                     [:put misc-table "aa" "aa" :string]
                     [:put misc-table "b" "b" :string]
                     [:put misc-table "bb" "bb" :string]
                     [:put misc-table "bc" "bc" :string]
                     [:put misc-table "c" "c" :string]
                     [:put misc-table "d" "d" :string]
                     [:put misc-table "da" "da" :string]
                     ])
        (println "transacted")
        (println (str "get range:" (l/get-range db misc-table
                                                [:closed-open "b" "d"]
                                                :string)))
        (l/close-env db)
        (println "closed")
        )
    (let [db         (l/open-lmdb "/tmp/dtlv-lmdb-test")
          misc-table "misc-test-table"]
      (println "env opened")
      (l/open-dbi db misc-table (b/type-size :long) (b/type-size :long))
      (println "dbi opened")
      (l/transact db
                  [[:put misc-table 1 1 :long :long]
                   [:put misc-table 2 2 :long :long]
                   [:put misc-table 4 4 :long :long]
                   [:put misc-table 8 8 :long :long]
                   [:put misc-table 16 16 :long :long]
                   [:put misc-table 32 32 :long :long]
                   [:put misc-table 64 64 :long :long]
                   [:put misc-table 128 128 :long :long]
                   [:put misc-table 256 256 :long :long]
                   [:put misc-table 512 512 :long :long]
                   [:put misc-table 1024 1024 :long :long]
                   [:put misc-table 2048 2048 :long :long]
                   [:put misc-table 4096 4096 :long :long]
                   [:put misc-table 8192 8192 :long :long]
                   [:put misc-table 16384 16384 :long :long]])
      (println "transacted")
      (println (str "get range:" (l/get-range db misc-table
                                              [:less-than-back 128]
                                              :long :long true)))
      (l/close-env db)
      (println "closed")
      )
    #_(let [schema {:aka  {:db/cardinality :db.cardinality/many}
                    :name {:db/valueType :db.type/string
                           :db/unique    :db.unique/identity}}
            conn   (d/create-conn "/tmp/dtlv-test" schema) ]

        (println "prepare to transact")
        (d/transact! conn
                     [{:name "Frege", :db/id -1, :nation "France", :aka ["foo" "fred"]}
                      {:name "Peirce", :db/id -2, :nation "france"}
                      {:name "De Morgan", :db/id -3, :nation "English"}])
        (println "transacted")
        (prn (d/q '[:find ?nation
                    :in $ ?alias
                    :where
                    [?e :aka ?alias]
                    [?e :nation ?nation]]
                  @conn
                  "fred"))
        (println "ready to close")
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

(defn -main [& args]
  (let [{:keys [command options arguments exit-message ok?]}
        (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (case command
        "conn" (dtlv-conn options arguments)
        "copy" (dtlv-copy options arguments)
        "dump" (dtlv-dump options arguments)
        "load" (dtlv-load options arguments)
        "help" (dtlv-help options arguments)))))
