(ns datalevin.client-recovery-chaos-runner
  "Bound chaos scenarios in disposable JVMs, including native deadlocks that
  cannot be interrupted safely. Preserve the last checkpoint and thread dump."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [datalevin.client-recovery-chaos :as chaos]
   [datalevin.util :as u])
  (:import
   [java.io File]
   [java.lang ProcessHandle]
   [java.util.concurrent TimeUnit]))

(defn- read-edn [^File file]
  (when (.exists file)
    (try (edn/read-string (slurp file)) (catch Exception _ nil))))

(defn- destroy! [^Process process descendants]
  (doseq [^ProcessHandle child descendants :when (.isAlive child)]
    (.destroyForcibly child))
  (when (.isAlive process)
    (.destroyForcibly process)
    (.waitFor process 5 TimeUnit/SECONDS)))

(defn- thread-dump! [^Process process ^File output]
  (try
    (let [jcmd (io/file (System/getProperty "java.home") "bin" "jcmd")]
      (when (.exists jcmd)
        (let [dump (.start (doto (ProcessBuilder.
                                  ^java.util.List [(.getPath jcmd) (str (.pid process)) "Thread.print"])
                            (.redirectErrorStream true)
                            (.redirectOutput output)))]
          (when-not (.waitFor dump 5 TimeUnit/SECONDS) (.destroyForcibly dump)))))
    (catch Exception _ nil)))

(defn run-isolated!
  "Run a scenario with a progress watchdog. A stuck server shutdown or native
  call fails this scenario instead of hanging the entire test suite forever."
  [opts]
  (let [dir (u/tmp-dir (str "client-chaos-runner-" (:seed opts) "-" (random-uuid)))
        _ (.mkdirs (io/file dir))
        input (io/file dir "options.edn")
        result (io/file dir "result.edn")
        checkpoint (io/file dir "progress.edn")
        log (io/file dir "scenario.log")
        dump (io/file dir "threads.txt")
        command [(str (System/getProperty "java.home") "/bin/java")
                 "--enable-native-access=ALL-UNNAMED"
                 "--add-opens=java.base/java.lang=ALL-UNNAMED"
                 "--add-opens=java.base/java.util=ALL-UNNAMED"
                 "--add-opens=java.base/java.nio=ALL-UNNAMED"
                 "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
                 "-Dclojure.compiler.direct-linking=true"
                 "-cp" (System/getProperty "java.class.path")
                 "clojure.main" "-m" "datalevin.client-recovery-chaos-runner"
                 (.getPath input) (.getPath result) (.getPath checkpoint)]
        _ (spit input (pr-str opts))
        process (.start (doto (ProcessBuilder. ^java.util.List command)
                          (.redirectErrorStream true)
                          (.redirectOutput log)))
        descendants (atom #{})
        success? (volatile! false)]
    (try
      (loop [previous nil last-progress (System/nanoTime)]
        (with-open [children (.descendants (.toHandle process))]
          (swap! descendants into (.toList children)))
        (if (.waitFor process 250 TimeUnit/MILLISECONDS)
          (let [report (read-edn result)]
            (if (and (zero? (.exitValue process)) (:result report))
              (do (vreset! success? true) (:result report))
              (throw (ex-info "Isolated client recovery scenario failed"
                              {:options opts :checkpoint (read-edn checkpoint)
                               :artifacts dir :report report :log (slurp log)}))))
          (let [current (read-edn checkpoint)
                now (System/nanoTime)
                changed? (not= previous current)
                elapsed-ms (quot (- now last-progress) 1000000)
                timeout-ms (if (or (nil? current)
                                  (#{:starting-server :boot} (:stage current)))
                             90000 30000)]
            (when (and (not changed?) (> elapsed-ms timeout-ms))
              (thread-dump! process dump)
              (throw (ex-info "Chaos scenario stopped making progress"
                              {:options opts :checkpoint current :artifacts dir
                               :thread-dump (.getPath dump) :log (.getPath log)})))
            (recur current (if changed? now last-progress)))))
      (finally
        (destroy! process @descendants)
        (when @success? (u/delete-files dir))))))

(defn -main [input result checkpoint]
  (let [opts (edn/read-string (slurp input))
        report! #(spit checkpoint (pr-str %))]
    (report! {:stage :boot :seed (:seed opts)})
    (try
      (let [value (chaos/run-chaos! (assoc opts :report-progress! report!))]
        (spit result (pr-str {:result value}))
        (shutdown-agents)
        (System/exit 0))
      (catch Throwable error
        (.printStackTrace error)
        (spit result (pr-str {:error (ex-message error)
                             :data (pr-str (ex-data error))}))
        (System/exit 1)))))
