(ns datalevin.search
  "Full-text search engine"
  (:require [clojure.string :as s]
            [datalevin.lmdb :as l]
            [datalevin.bits :as b]
            [datalevin.constants :as c]))

(comment

  (def env (l/open-kv "/tmp/search"))

  (def dbi (l/open-dbi env "dict" c/+max-key-size+ c/+default-val-size+
                       (conj )))


  )
