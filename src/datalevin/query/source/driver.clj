(ns ^:no-doc datalevin.query.source.driver
  "Public v2 query/explain invocation boundary."
  (:require [datalevin.query.source.context :as context]
            [datalevin.query.source.program :as program]
            [datalevin.query.source.runtime :as rt]
            [datalevin.timeout :as timeout]))

(defn run
  "Run or explain a v2 query without native parse/result caches. Accept the
  normal query, argument collection and optional explain map. Embedded context
  options, query timeouts and inherited limits are enforced before callbacks.
  Fresh invocations may adjust defaults; nested invocations retain parent caps."
  [query arguments explain-options]
  ;; Discover local options under bounded admission before constructing the
  ;; actual root control. Its defaults are configurable, not a hidden parent
  ;; cap. A real caller runtime remains the parent of both phases.
  (let [{:keys [options timeout-ms deadline]}
        (rt/with-runtime {:timeout-ms (timeout/time-left)} #(context/read! % query nil))]
    (binding [timeout/*deadline* deadline]
      (rt/with-runtime {:limits (:limits options) :timeout-ms timeout-ms}
        (fn [runtime]
          (if (nil? explain-options)
            (program/query! runtime query arguments options)
            (program/explain! runtime query arguments explain-options options)))))))
