(ns datalevin.query.source.execution-runner
  "Run the source contract and embedded execution checks."
  (:require [clojure.test :as test]
            [datalevin.query.source-test]
            [datalevin.query.source-execution-test]))

(defn -main [& _]
  (let [{:keys [fail error]} (test/run-tests 'datalevin.query.source-test
                                           'datalevin.query.source-execution-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
