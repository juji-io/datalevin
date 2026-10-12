(ns ycsb-bench.cli-test
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [ycsb-bench.core :as core]
            [ycsb-bench.support :as support])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.time Instant]))

(defn- cli-main [& args]
  (core/run-cli args {:host-control support/no-host-control}))

(def unexpected-runtime
  {:run-case (fn [_] (throw (AssertionError. "Unexpected benchmark run")))
   :check-postgres (fn [_] (throw (AssertionError. "Unexpected PostgreSQL connection")))
   :host-control {:enabled? true
                  :pause (fn [] (throw (AssertionError. "Unexpected host control")))
                  :resume (fn [_])}})

(def small-args
  ["--api" "DATALOG" "--mode" "embedded" "--workload" "c"
   "--records" "4" "--ops" "5" "--warmup" "0" "--threads" "2"
   "--field-count" "2" "--field-length" "4" "--batch-size" "2"
   "--seed" "23"])

(defn- with-report-path [f]
  (let [directory (Files/createTempDirectory "datalevin-ycsb-cli-"
                                              (make-array FileAttribute 0))
        path (.resolve directory "report.edn")]
    (try
      (f (.toFile path))
      (finally
        (Files/deleteIfExists path)
        (Files/delete directory)))))

(defn- check-report! [report systems]
  (is (= {:format-version 3 :benchmark :datalevin-ycsb-style
          :measurement-model :closed-loop}
         (select-keys report [:format-version :benchmark :measurement-model])))
  (is (string? (:datalevin-version report)))
  (is (instance? Instant (Instant/parse (:started-at report))))
  (is (pos? (get-in report [:environment :processors])))
  (is (= systems (mapv #(get-in % [:configuration :system]) (:results report))))
  (doseq [result (:results report)]
    (is (= (cond-> {:api :datalog :mode :embedded :workload :c :records 4 :ops 5
                    :warmup 0 :threads 2 :field-count 2 :field-length 4 :seed 23}
             (= :datalevin (get-in result [:configuration :system])) (assoc :pool-size 2))
           (select-keys (:configuration result)
                        [:api :mode :workload :records :ops :warmup :threads
                         :pool-size :field-count :field-length :seed])))
    (is (= 0 (get-in result [:warmup :operations])))
    (is (= 5 (get-in result [:measured :by-operation :read :count])))
    (is (number? (get-in result [:measured :latency-us :p99])))
    (is (= {:status :passed :records 4 :all-records-checked? true
            :scope :structure :value-checks :not-performed
            :character-checks :post-measurement}
           (:validation result)))
    (is (not-any? #(contains? (:configuration result) %)
                  [:output :help :pg-url :pg-user :sql-indexes]))
    (is (= :none (get-in result [:configuration :payload-indexes])))
    (is (= :none (get-in result [:storage :payload-indexes])))
    (when (= :sqlite (get-in result [:configuration :system]))
      (is (= [] (get-in result [:storage :configuration :secondary-indexes]))))))

(deftest help-and-invalid-arguments-test
  (let [output (with-out-str (core/run-cli ["--help"] unexpected-runtime))]
    (is (str/includes? output "YCSB-style Datalevin benchmark"))
    (is (str/includes? output "--system"))
    (is (not (str/includes? output "--sql-indexes")))
    (is (str/includes? output "--output")))
  (with-report-path
    (fn [file]
      (doseq [args [["--unknown-option"] ["unexpected-argument"]
                    ["--ops"] ["--ops" "not-a-number"] ["--ops" "0"]
                    ["--sql-indexes" "all"]
                    ["--system" "postgres" "--mode" "embedded"]]]
        (is (thrown? clojure.lang.ExceptionInfo
                     (core/run-cli (concat ["--output" (str file)] args) unexpected-runtime))
            (pr-str args))
        (is (not (.exists file)))))))

(deftest timed-zipfian-inserts-fail-before-preflight-test
  (doseq [workload ["e" "all"]
          timer ["--warmup-ms" "--measurement-ms"]]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"require an explicit --zipfian-keyspace"
                         (core/run-cli ["--system" "all" "--api" "datalog" "--mode" "remote"
                                        "--workload" workload timer "10"]
                                       unexpected-runtime)))))

(deftest cli-file-report-test
  (with-report-path
    (fn [file]
      (let [output (with-out-str
                     (apply cli-main
                            (concat small-args
                                    ["--system" "all" "--output" (str file)
                                     "--pg-url" "unused-secret-url"
                                     "--pg-user" "unused-secret-user"])))
            serialized (slurp file)]
        (check-report! (edn/read-string serialized) [:datalevin :sqlite])
        (is (str/includes? output "datalevin datalog embedded C:"))
        (is (str/includes? output "sqlite datalog embedded C:"))
        (is (str/includes? output (str "Report: " file)))
        (is (not (str/includes? serialized "unused-secret")))))))

(deftest cli-stdout-report-test
  (let [output (with-out-str
                 (apply cli-main (concat small-args ["--system" "sqlite"])))
        ;; Status lines precede the EDN report on stdout.
        report (edn/read-string (subs output (str/index-of output "{")))]
    (check-report! report [:sqlite])
    (is (str/includes? output "sqlite datalog embedded C:"))
    (is (not (str/includes? output "Report:")))))

(deftest cli-value-audit-report-test
  (let [output (with-out-str
                 (apply cli-main (concat small-args ["--system" "sqlite" "--value-audit"])))
        report (edn/read-string (subs output (str/index-of output "{")))
        result (first (:results report))]
    (is (true? (get-in result [:configuration :value-audit?])))
    (is (= :structure-and-observed-values (get-in result [:validation :scope])))
    (is (= :passed (get-in result [:validation :value-checks :status])))
    (is (= 5 (get-in result [:validation :value-checks :point-reads])))
    (is (str/includes? output "timings include recording"))))

(deftest failed-case-does-not-publish-report-test
  (doseq [existing? [false true]]
    (with-report-path
      (fn [file]
        (let [previous "existing report\n"
              failure (ex-info "Injected case validation failure" {})
              calls (atom [])]
          (when existing? (spit file previous))
          (let [runtime {:host-control support/no-host-control
                         :run-case (fn [{:keys [system]}]
                                     (swap! calls conj system)
                                     (if (= system :sqlite)
                                       (throw failure)
                                       {:measured {:ops-per-second 1.0 :latency-us {:p99 1.0}}
                                        :validation {:records 4}}))}]
            (is (identical? failure
                            (try
                              (with-out-str
                                (core/run-cli (concat small-args
                                                      ["--system" "all" "--output" (str file)])
                                              runtime))
                              (catch Exception e e)))))
          (is (= [:datalevin :sqlite] @calls))
          (if existing?
            (is (= previous (slurp file)))
            (is (not (.exists file)))))))))

(deftest timed-zipfian-cli-report-test
  (let [output (with-out-str
                 (apply cli-main
                        (concat small-args
                                ["--system" "sqlite" "--workload" "e"
                                 "--warmup-ms" "10" "--measurement-ms" "20"
                                 "--zipfian-keyspace" "32" "--value-audit"])))
        report (edn/read-string (subs output (str/index-of output "{")))
        result (first (:results report))]
    (is (= 32 (get-in result [:configuration :zipfian-keyspace])
           (get-in result [:warmup :zipfian-keyspace])
           (get-in result [:measured :zipfian-keyspace])))
    (is (>= (get-in result [:warmup :seconds]) 0.01))
    (is (>= (get-in result [:measured :seconds]) 0.02))
    (is (= :passed (get-in result [:warmup :validation :value-checks :status])
           (get-in result [:validation :value-checks :status])))))

(deftest timed-repeated-cli-report-test
  (with-report-path
    (fn [file]
      (with-out-str
        (apply cli-main
               (concat small-args
                       ["--system" "sqlite" "--client-counts" "1,2"
                        "--repetitions" "2" "--warmup-ms" "0" "--measurement-ms" "20"
                        "--server-background-threads" "12" "--output" (str file)])))
      (let [report (edn/read-string (slurp file))
            results (:results report)]
        (is (= 4 (count results)))
        (is (= [1 2 2 1] (mapv #(get-in % [:configuration :threads]) results)))
        (is (= [1 1 2 2] (mapv #(get-in % [:configuration :trial]) results)))
        (is (every? #(>= (get-in % [:measured :seconds]) 0.02) results))
        (is (every? #(= 0 (get-in % [:warmup :operations])) results))
        (is (= [2 2] (mapv :trials (:summary report))))))))
