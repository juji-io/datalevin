(ns ^:no-doc datalevin.query.dispatch
  "Optional source detection, independent of the SPI and its executor.
  Loading native query entry points must not initialize external execution.")

(defonce ^:private source-predicate (volatile! nil))

(defn install-source-predicate!
  "Called when the public source protocol loads, including before core loads.
  Publish a predicate, not an executor, so native calls remain independent of
  external planning, validation, and accounting namespaces."
  [predicate]
  (vreset! source-predicate predicate)
  nil)

(defn source-predicate-fn
  "Return nil until the public source protocol has been loaded."
  []
  @source-predicate)
