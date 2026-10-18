(ns datalevin.query.source.execution-runner
  "Run the source contract and embedded execution checks."
  (:require [clojure.test :as test]
            [datalevin.query-key-test]
            [datalevin.query.source-test]
            [datalevin.query.source-runtime-test]
            [datalevin.query.source-execution-test]
            [datalevin.query.source-conformance-test]
            [datalevin.query.source-join-index-test]
            [datalevin.query.source-permissions-test]
            [datalevin.query.source-binding-test]
            [datalevin.query.source-binding-execution-test]
            [datalevin.query.source-access-test]
            [datalevin.query.source-session-test]
            [datalevin.query.source-cursor-test]
            [datalevin.query.source-lowering-test]
            [datalevin.query.source-region-test]
            [datalevin.query.source-scope-test]
            [datalevin.query.source-descriptor-test]
            [datalevin.query.source-envelope-test]
            [datalevin.query.source-planning-test]
            [datalevin.query.source-program-test]
            [datalevin.query.bounded-join-test]
            [datalevin.query.access-dependency-test]
            [datalevin.query.source-native-value-test]
            [datalevin.query.source-native-data-test]
            [datalevin.query.source-native-variable-tuple-test]
            [datalevin.query.source-native-compression-test]
            [datalevin.query.source-native-metadata-test]))

(defn -main [& _]
  (let [{:keys [fail error]} (test/run-tests 'datalevin.query-key-test
                                           'datalevin.query.source-test
                                           'datalevin.query.source-runtime-test
                                           'datalevin.query.source-execution-test
                                           'datalevin.query.source-conformance-test
                                           'datalevin.query.source-join-index-test
                                           'datalevin.query.source-permissions-test
                                           'datalevin.query.source-binding-test
                                           'datalevin.query.source-binding-execution-test
                                           'datalevin.query.source-access-test
                                           'datalevin.query.source-session-test
                                           'datalevin.query.source-cursor-test
                                           'datalevin.query.source-lowering-test
                                           'datalevin.query.source-region-test
                                           'datalevin.query.source-scope-test
                                           'datalevin.query.source-descriptor-test
                                           'datalevin.query.source-envelope-test
                                           'datalevin.query.source-planning-test
                                           'datalevin.query.source-program-test
                                           'datalevin.query.bounded-join-test
                                           'datalevin.query.access-dependency-test
                                           'datalevin.query.source-native-value-test
                                           'datalevin.query.source-native-data-test
                                           'datalevin.query.source-native-variable-tuple-test
                                           'datalevin.query.source-native-compression-test
                                           'datalevin.query.source-native-metadata-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
