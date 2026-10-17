(ns ^:no-doc datalevin.test-adapter.csv
  "Selected-backend CSV operations over JVM readers and writers."
  (:require [datalevin.test-adapter :as adapter]))

(defn read-csv [input & options]
  (adapter/call! :csv/read-csv [input options]))

(defn write-csv [writer data & options]
  (adapter/call! :csv/write-csv [writer data options]))
