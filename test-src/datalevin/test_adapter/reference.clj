(ns ^:no-doc datalevin.test-adapter.reference
  (:require
   [datalevin.bits :as bits]
   [datalevin.core :as d]
   [datalevin.test-adapter :as adapter]))

(defrecord ReferenceBackend []
  adapter/Backend
  (invoke! [_ operation arguments]
    (case operation
      :bits/serialize (apply bits/serialize arguments)
      :bits/deserialize (apply bits/deserialize arguments)
      :bits/encode-base64 (apply bits/encode-base64 arguments)
      :bits/decode-base64 (apply bits/decode-base64 arguments)
      :bits/encode-bigint (apply bits/encode-bigint arguments)
      :bits/decode-bigint (apply bits/decode-bigint arguments)
      :bits/bitmap (apply bits/bitmap arguments)
      :bits/bitmap-add (apply bits/bitmap-add arguments)
      :bits/bitmap-del (apply bits/bitmap-del arguments)
      :bits/bitmap-and (apply bits/bitmap-and arguments)
      :bits/bitmap-or (apply bits/bitmap-or arguments)
      :bits/bitmaps-and (apply bits/bitmaps-and arguments)
      :bits/bitmaps-or (apply bits/bitmaps-or arguments)
      :bits/bitmap-empty? (apply bits/bitmap-empty? arguments)
      :bits/bitmap64 (apply bits/bitmap64 arguments)
      :bits/bitmap64-add (apply bits/bitmap64-add arguments)
      :bits/bitmap64-contains? (apply bits/bitmap64-contains? arguments)
      :bits/bitmap64-cardinality (apply bits/bitmap64-cardinality arguments)
      :bits/bitmap64-min (apply bits/bitmap64-min arguments)
      :bits/bitmap64-max (apply bits/bitmap64-max arguments)
      :bits/bitmap64->longs (apply bits/bitmap64->longs arguments)
      :bits/serialize-bf (apply bits/serialize-bf arguments)
      :bits/deserialize-bf (apply bits/deserialize-bf arguments)
      :bits/put-buffer (apply bits/put-buffer arguments)
      :bits/read-buffer (apply bits/read-buffer arguments)
      :bits/type-size (apply bits/type-size arguments)
      :bits/indexable (apply bits/indexable arguments)
      :bits/giant? (apply bits/giant? arguments)
      :bits/indexable-bytes (apply bits/indexable-bytes arguments)
      :bits/avg->aid (apply bits/avg->aid arguments)
      :bits/avg->giant-id (apply bits/avg->giant-id arguments)
      :bits/avg->inline-value (apply bits/avg->inline-value arguments)
      :bits/avg->r (apply bits/avg->r arguments)
      :bits/acquire-serialized-bf (apply bits/acquire-serialized-bf arguments)
      :bits/measure-size (apply bits/measure-size arguments)
      :empty-db (apply d/empty-db arguments)
      :db-with (apply d/db-with arguments)
      :q (apply d/q arguments)
      :close-db (apply d/close-db arguments)
      (adapter/unsupported! :adapter operation
                            "Operation is not exposed by the reference adapter")))
  (stop! [_])
  (backend-info [_] {:backend :reference :runtime :clojure}))

(defn start [] (->ReferenceBackend))
