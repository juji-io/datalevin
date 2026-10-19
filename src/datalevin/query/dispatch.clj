(ns ^:no-doc datalevin.query.dispatch
  "Optional source detection, independent of the SPI and its executor.
  Loading native query entry points must not initialize external execution.")

(defn install-source-predicate!
  "Called when the public source protocol loads, including before core loads.
  Publish a predicate, not an executor, so native calls remain independent of
  external planning, validation, and accounting namespaces."
  [predicate protocol]
  (datalevin.utl.QuerySourceDispatch/install
    predicate (:on-interface protocol)
    (into-array Class (keep (fn [[type methods]] (when (and type methods) type)) (:impls protocol)))
    (boolean (get-in protocol [:impls nil])))
  nil)

