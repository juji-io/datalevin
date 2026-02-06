(ns idoc-bench.server
  "Start a Datalevin server for benchmarking."
  (:require [datalevin.server :as srv]))

(defn -main
  [& args]
  (let [port (if (seq args)
               (Long/parseLong (first args))
               8898)
        dir  (or (second args)
                 "/tmp/dtlv-server")]
    (println "Starting Datalevin server on port" port "with data dir" dir)
    (srv/start (srv/create {:port port :root dir}))
    (println "Server running. Press Ctrl+C to stop.")
    @(promise)))
