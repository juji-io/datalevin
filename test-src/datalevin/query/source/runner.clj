(ns datalevin.query.source.runner
  "Run isolated F0 conformance checks without loading the query engine."
  (:require [clojure.test :as test]
            [datalevin.query.source-test]))

(defn -main [& _]
  (let [{:keys [fail error]} (test/run-tests 'datalevin.query.source-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
