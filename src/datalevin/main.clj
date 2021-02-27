(ns datalevin.main
  "Command line tool"
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as s]
            [clojure.pprint :as p]
            [clojure.java.io :as io]
            [clojure.walk :as w]
            [clojure.edn :as edn]
            [clojure.stacktrace :as st]
            [sci.core :as sci]
            [datalevin.core :as d]
            [datalevin.util :refer [raise]]
            [datalevin.bits :as b]
            [datalevin.lmdb :as l]
            [datalevin.binding.graal]
            [datalevin.binding.java])
  (:import [java.io PushbackReader IOException]
           [java.lang RuntimeException])
  (:gen-class))

(def version "0.4.0")

(def version-str
  (str
    "
  Datalevin (version: " version ")"))

(def commands #{"exec" "copy" "drop" "dump" "load" "stat" "help"})

(def stat-help
  "
  Command stat - show statistics of the main database or sub-database(s).

  Required option:
      -d --dir PATH   Path to the database directory
  Optional options:
      -a --all        All of the sub-databases
  Optional arguments:
      name(s) of sub-database(s)

  Examples:
      dtlv -d /data/companydb stat
      dtlv -d /data/companydb -a stat
      dtlv -d /data/companydb stat sales products")

(def dump-help
  "
  Command dump - dump the content of the database or sub-database(s).

  Required option:
      -d --dir PATH   Path to the source database directory
  Optional options:
      -a --all        All of the sub-databases
      -f --file PATH  Write to the specified target file instead of stdout
      -g --datalog    Dump as a Datalog database
      -l --list       List the names of sub-databases instead of the content
  Optional arguments:
      Name(s) of sub-database(s)

  Examples:
      dtlv -d /data/companydb -l dump
      dtlv -d /data/companydb -g dump
      dtlv -d /data/companydb -f ~/sales-data dump sales
      dtlv -d /data/companydb -f ~/company-data -a dump")

(def load-help
  "
  Command load - load data into the database or a sub-database.

  Required option:
      -d --dir  PATH  Path to the target database directory
  Optional option:
      -f --file PATH  Load from the specified source file instead of stdin
      -g --datalog    Load a Datalog database
  Optional argument:
      Name of the single sub-database to load the data into, useful when loading
      data into a sub-database with a name different from the original name

  Examples:
      dtlv -d /data/companydb -f ~/sales-data load new-sales
      dtlv -d /data/companydb -f ~/sales-data -g load")

(def copy-help
  "
  Command copy - Copy the database. This can be done regardless of whether it is
  currently in use.

  Required option:
      -d --dir PATH   Path to the source database directory
  Optional option:
      -c --compact    Compact while copying. Only pages in use will be copied.
  Required argument:
      Path to the destination directory.

  Examples:
      dtlv -d /data/companydb -c copy /backup/companydb-2021-02-14")

(def drop-help
  "
  Command drop - Drop or clear the content of sub-database(s).

  Required option:
      -d --dir PATH   Path to the database directory
  Optional option:
      -D --delete     Delete the sub-database, not just empty it.
  Required argument:
      Name(s) of the sub-database(s)

  Examples:
      dtlv -d /data/companydb -D drop sales")

(def exec-help
  "
  Command exec - Execute database transactions or queries.

  Required argument:
      The code to be executed.

  Examples:
      dtlv exec (def conn (get-conn '/data/companydb')) \\
                (transact! conn [{:name \"Datalevin\"}])")

(def repl-header
  "
  Type (help) to see available functions. Clojure core functions are also available.
  Type (exit) to exit.
  ")

(defn- usage [options-summary]
  (->> [version-str
        ""
        "Usage: dtlv [options] [command] [arguments]"
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
        "Omit the command to enter the interactive shell."
        "See 'dtlv help <command>' to read about a specific command."]
       (s/join \newline)))

(defn- error-msg [errors]
  (s/join \newline ["The following errors occurred while parsing your command:"
                    (s/join \newline errors)]))

(def cli-opts
  [["-a" "--all" "Include all of the sub-databases"]
   ["-c" "--compact" "Compact while copying."]
   ["-d" "--dir PATH" "Path to the database directory"]
   ["-D" "--delete" "Delete the sub-database, not just empty it"]
   ["-f" "--file PATH" "Path to the specified file"]
   ["-g" "--datalog" "Dump/load as a Datalog database"]
   ["-h" "--help" "Show usage"]
   ["-l" "--list" "List the names of sub-databases instead of the content"]
   ["-V" "--version" "Show Datalevin version and exit"]])

(defn- validate-args
  "Validate command line arguments. Either return a map indicating the program
  should exit (with a error message, and optional ok status), or a map
  indicating the action the program should take and the options provided."
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-opts)
        command                                    (first arguments)]
    (cond
      (:version options) {:exit-message version-str :ok? true}
      (:help options)    {:exit-message (usage summary) :ok? true}
      errors             {:exit-message (str (error-msg errors)
                                             \newline
                                             (usage summary))}
      (commands command) {:command   command
                          :options   options
                          :arguments (rest arguments)
                          :summary   summary}
      (nil? command)     {:command "repl" :options options}
      :else              {:exit-message (usage summary)})))

(defn- exit
  ([]
   (exit 0))
  ([status]
   (System/exit status))
  ([status msg]
   (println msg)
   (System/exit status)))

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

(def user-facing-ns #{'datalevin.core 'datalevin.lmdb})

(defn- user-facing? [v]
  (let [m (meta v)]
    (and (:doc m)
         (if-let [p (:protocol m)]
           (and (not (:no-doc (meta p)))
                (not (:no-doc m)))
           (not (:no-doc m))))))

(defn- user-facing-map [var-map]
  (select-keys var-map
               (keep (fn [[k v]] (when (user-facing? v) k)) var-map)))

(defn- user-facing-vars []
  (reduce
    (fn [m ns]
      (assoc m ns (user-facing-map (ns-publics ns))))
    {}
    user-facing-ns))

(defn- resolve-var [s]
  (when (symbol? s)
    (some #(ns-resolve % s) user-facing-ns)))

(defn- qualify-fn [x]
  (if (list? x)
    (let [[f & args] x]
      (if-let [var (resolve-var f)]
        (apply list (symbol var) args)
        x))
    x))

(defn- eval-fn [ctx form]
  (sci/eval-form ctx (if (coll? form)
                       (w/postwalk qualify-fn form)
                       form)))

(def sci-opts {:namespaces (user-facing-vars)})

(defn- dtlv-exec [arguments]
  (assert (seq arguments) (s/join \newline ["Missing code." exec-help]))
  (try
    (let [reader (sci/reader (s/join arguments))
          ctx    (sci/init sci-opts)]
      (sci/with-bindings {sci/ns @sci/ns}
        (loop []
          (let [next-form (sci/parse-next ctx reader)]
            (when-not (= ::sci/eof next-form)
              (prn (eval-fn ctx next-form))
              (recur))))))
    (catch Throwable e
      (st/print-cause-trace e)
      (exit 1 (str "Execution error: " (.getMessage e)))))
  (exit 0))

(defn- dtlv-copy [{:keys [dir compact]} arguments]
  (assert dir (s/join \newline
                      ["Missing source data directory path." copy-help]))
  (assert (seq arguments)
          (s/join \newline
                  ["Missing destination data directory path." copy-help]))
  (try
    (let [lmdb (l/open-kv dir)]
      (println "Opened database, copying...")
      (l/copy lmdb (first arguments) compact)
      (l/close-kv lmdb)
      (println "Copied database."))
    (catch Throwable e
      (st/print-cause-trace e)
      (exit 1 (str "Copy error: " (.getMessage e)))))
  (exit 0))

(defn- dtlv-drop [{:keys [dir delete]} arguments]
  (assert dir (s/join \newline ["Missing data directory path." drop-help]))
  (assert (seq arguments)
          (s/join \newline ["Missing sub-database name." drop-help]))
  (try
    (let [lmdb (l/open-kv dir)]
      (if delete
        (doseq [dbi arguments]
          (l/drop-dbi lmdb dbi)
          (println (str "Dropped " dbi)))
        (doseq [dbi arguments]
          (l/clear-dbi lmdb dbi)
          (println (str "Cleared " dbi))))
      (l/close-kv lmdb))
    (catch Throwable e
      (st/print-cause-trace e)
      (exit 1 (str "Drop error: " (.getMessage e)))))
  (exit 0))

(defn- dump-dbi [lmdb dbi]
  (let [n (l/entries lmdb dbi)]
    (p/pprint {:dbi dbi :entries n})
    (doseq [[k v] (l/get-range lmdb dbi [:all] :raw :raw)]
      (p/pprint [(b/binary-ba->str k) (b/binary-ba->str v)]))))

(defn- dump-all [lmdb]
  (let [dbis (set (l/list-dbis lmdb))]
    (doseq [dbi dbis] (dump-dbi lmdb dbi))))

(defn- dump-datalog [dir]
  (let [conn (d/create-conn dir)]
    (p/pprint (d/schema conn))
    (doseq [datom (d/datoms @conn :eav)]
      (p/pprint datom))))

(defn- dtlv-dump [{:keys [dir all file datalog list]} arguments]
  (assert dir (s/join \newline ["Missing data directory path." dump-help]))
  (try
    (let [f    (when file (io/writer file))
          lmdb (l/open-kv dir)]
      (binding [*out* (or f *out*)]
        (cond
          list            (p/pprint (set (l/list-dbis lmdb)))
          datalog         (dump-datalog dir)
          all             (dump-all lmdb)
          (seq arguments) (doseq [dbi arguments] (dump-dbi lmdb dbi))
          :else           (println dump-help)))
      (l/close-kv lmdb)
      (when f (.close f))
      (exit 0))
    (catch Throwable e
      (st/print-cause-trace e)
      (exit 1 (str "Dump error: " (.getMessage e))))))

(defn- load-datalog [dir in]
  (try
    (with-open [^PushbackReader r in]
      (let [read-form #(edn/read {:eof ::EOF} r)
            schema    (read-form)
            datoms    (->> (repeatedly read-form)
                           (take-while #(not= ::EOF %))
                           (map #(apply d/datom %)))]
        (d/init-db datoms dir schema)))
    (catch IOException e
      (raise "IO error while loading Datalog data: " (ex-message e) {}))
    (catch RuntimeException e
      (raise "Parse error while loading Datalog data: " (ex-message e) {}))
    (catch Exception e
      (raise "Error loading Datalog data: " (ex-message e) {}))))

(defn- load-kv [dbi [k v]]
  [:put dbi (b/binary-str->ba k) (b/binary-str->ba v) :raw :raw])

(defn- load-dbi [lmdb dbi in]
  (try
    (with-open [^PushbackReader r in]
      (let [read-form         #(edn/read {:eof ::EOF} r)
            {:keys [entries]} (read-form)]
        (l/open-dbi lmdb dbi)
        (l/transact-kv lmdb (->> (repeatedly read-form)
                                 (take-while #(not= ::EOF %))
                                 (take entries)
                                 (map (partial load-kv dbi))))))
    (catch IOException e
      (raise "IO error while loading raw data: " (ex-message e) {}))
    (catch RuntimeException e
      (raise "Parse error while loading raw data: " (ex-message e) {}))
    (catch Exception e
      (raise "Error loading raw data: " (ex-message e) {}))))

(defn- load-all [lmdb in]
  (try
    (with-open [^PushbackReader r in]
      (let [read-form #(edn/read {:eof ::EOF} r)
            load-dbi  (fn [[ms vs]]
                        (doseq [{:keys [dbi]} (butlast ms)]
                          (l/open-dbi lmdb dbi))
                        (let [{:keys [dbi entries]} (last ms)]
                          (l/open-dbi lmdb dbi)
                          (->> vs
                               (take entries)
                               (map (partial load-kv dbi)))))]
        (l/transact-kv lmdb (->> (repeatedly read-form)
                                 (take-while #(not= ::EOF %))
                                 (partition-by map?)
                                 (partition 2 2 nil)
                                 (mapcat load-dbi)))))
    (catch IOException e
      (raise "IO error while loading raw data: " (ex-message e) {}))
    (catch RuntimeException e
      (raise "Parse error while loading raw data: " (ex-message e) {}))
    (catch Exception e
      (raise "Error loading raw data: " (ex-message e) {}))))

(defn- dtlv-load [{:keys [dir file datalog]} arguments]
  (assert dir (s/join \newline ["Missing data directory path." load-help]))
  (try
    (let [f    (when file (PushbackReader. (io/reader file)))
          in   (or f *in*)
          lmdb (l/open-kv dir)]
      (cond
        datalog         (load-datalog dir in)
        (seq arguments) (load-dbi lmdb (first arguments) in)
        :else           (load-all lmdb in))
      (l/close-kv lmdb)
      (when f (.close f))
      (exit 0))
    (catch Throwable e
      (st/print-cause-trace e)
      (exit 1 (str "Load error: " (.getMessage e))))))

;; TODO show reader info and free list info as well
(defn- dtlv-stat [{:keys [dir all]} arguments]
  (assert dir (s/join \newline ["Missing data directory path." stat-help]))
  (try
    (let [lmdb (l/open-kv dir)
          dbis (if all (l/list-dbis lmdb) arguments)]
      (if (seq dbis)
        (p/pprint (cond-> []
                    all  (conj {"Main DB" (l/stat lmdb)})
                    true (into (for [dbi  dbis
                                     :let [_ (l/open-dbi lmdb dbi)]]
                                 {dbi (l/stat lmdb dbi)}))))
        (p/pprint {"Main DB" (l/stat lmdb)}))
      (l/close-kv lmdb))
    (catch Throwable e
      (st/print-cause-trace e)
      (exit 1 (str "Stat error: " (.getMessage e)))))
  (exit 0))

(defn- prompt [ctx]
  (let [ns-name (sci/eval-string* ctx "(ns-name *ns*)")]
    (print (str ns-name "> "))
    (flush)))

(defn- handle-error [_ctx last-error e]
  (binding [*out* *err*] (println (ex-message e)))
  (sci/set! last-error e))

(defn- doc [s] (when-let [f (resolve-var s)] (println (:doc (meta f)))))

(defn- repl-help []
  (println "")
  (println "The following Datalevin functions are available:")
  (println "")
  (doseq [ns user-facing-ns]
    (print (str "* In " ns ": "))
    (doseq [f (sort-by name (keys (user-facing-map (ns-publics ns))))]
      (print (name f))
      (print " "))
    (println "")
    (println ""))
  (println "Call function just like in code, i.e. (<function> <args>)")
  (println "")
  (println "Type (doc <function>) to read documentation of the function"))

(defn- dtlv-repl []
  (println version-str)
  (println repl-header)
  (let [reader     (sci/reader *in*)
        last-error (sci/new-dynamic-var '*e nil
                                        {:ns (sci/create-ns 'clojure.core)})
        ctx        (sci/init (update sci-opts :namespaces
                                     merge {'clojure.core {'*e last-error}}))]
    (sci/with-bindings {sci/ns     @sci/ns
                        last-error @last-error}
      (loop []
        (prompt ctx)
        (let [next-form (try (sci/parse-next ctx reader)
                             (catch Throwable e
                               (handle-error ctx last-error e)
                               ::err))]
          (cond
            (= next-form '(exit)) (exit)
            (= next-form '(help)) (do (repl-help) (recur))

            (and (list? next-form) (= ((comp name first) next-form) "doc"))
            (do (doc (first (next next-form))) (recur))

            :else (when-not (= ::sci/eof next-form)
                    (when-not (= ::err next-form)
                      (let [res (try (eval-fn ctx next-form)
                                     (catch Throwable e
                                       (handle-error ctx last-error e)
                                       ::err))]
                        (when-not (= ::err res)
                          (prn res))))
                    (recur))))))))

(defn -main [& args]
  (let [{:keys [command options arguments summary exit-message ok?]}
        (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (case command
        "repl" (dtlv-repl)
        "exec" (dtlv-exec arguments)
        "copy" (dtlv-copy options arguments)
        "drop" (dtlv-drop options arguments)
        "dump" (dtlv-dump options arguments)
        "load" (dtlv-load options arguments)
        "stat" (dtlv-stat options arguments)
        "help" (dtlv-help arguments summary)))))
