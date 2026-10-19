(ns ^:no-doc datalevin.query.source.driver
  "V2 query/explain invocation boundary, ready for the coordinated adapter switch."
  (:require [datalevin.query.source.program :as program]
            [datalevin.query.source.runtime :as rt]
            [datalevin.timeout :as timeout]))

(defn run
  "Run or explain a v2 query without native parse/result caches. Accept the
  normal query, argument collection and optional explain map. Embedded context
  options, query timeouts and inherited limits are enforced before callbacks.
  Public q/explain select this driver only after the coordinated adapter switch."
  [query arguments explain-options]
  (rt/with-runtime {:timeout-ms (timeout/time-left)}
    (fn [runtime]
      (if (nil? explain-options)
        (program/query! runtime query arguments)
        (program/explain! runtime query arguments explain-options)))))
