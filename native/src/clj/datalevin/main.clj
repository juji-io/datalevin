(ns datalevin.main
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as s])
  (:gen-class))

(def cli-opts
  [["-h" "--help"]])

(defn usage [options-summary]
  (->> ["Datalevin"
        ""
        "Usage: dtlv [options] action"
        ""
        "Options:"
        options-summary
        ""
        "Actions:"
        "  start    Start a new server"
        "  stop     Stop an existing server"
        "  status   Print a server's status"
        ""
        "Please refer to the manual page for more information."]
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
      ;; custom validation on arguments
      ;; (and (= 1 (count arguments))
      ;;      (#{"start" "stop" "status"} (first arguments)))
      ;; {:action (first arguments) :options options}
      :else           ; failed custom validation => exit with usage summary
      {:exit-message (usage summary)})))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main [& args]
  (let [{:keys [action options exit-message ok?]} (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      #_(case action
          "start"  (server/start! options)
          "stop"   (server/stop! options)
          "status" (server/status! options)))))
