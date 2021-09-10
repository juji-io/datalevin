(ns datalevin.search
  "Full-text search engine"
  (:require [clojure.string :as s]
            [datalevin.lmdb :as l]
            [datalevin.constants :as c]
            [datalevin.bits :as b]
            [datalevin.constants :as c]))

(comment

  (def env (l/open-kv "/tmp/search"))

  (def dbi (l/open-dbi env "dict" c/+max-key-size+ c/+default-val-size+
                       (conj c/default-dbi-flags :dupsort)))

  (l/transact-kv env [[:put "dict" "a" 1]
                      [:put "dict" "a" 2]])

  (l/get-value env "dict" "a")

  )
