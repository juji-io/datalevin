(ns datalevin.bench
  (:require [datalevin.lmdb :as l]
            [datalevin.util :as u]
            [datalevin.search :as s]
            [datalevin.constants :as c]
            [datalevin.bits :as b])
  (:import [datalevin.sm SymSpell Bigram SuggestItem]
           [java.util HashMap ArrayList HashSet Iterator]
           [java.util.concurrent Executors]
           [java.io FileInputStream]
           [org.roaringbitmap RoaringBitmap RoaringBitmapWriter]
           [com.fasterxml.jackson.databind ObjectMapper]
           [com.fasterxml.jackson.core JsonFactory JsonParser JsonToken]
           ))

(defn index-wiki-json
  [dir ^String filename]
  (let [start  (System/currentTimeMillis)
        lmdb   (l/open-kv dir {:mapsize 100000})
        writer (s/index-writer lmdb)]
    (with-open [f (FileInputStream. filename)]
      (let [jf  (JsonFactory.)
            jp  (.createParser jf f)
            cls (Class/forName "java.util.HashMap")]
        (.setCodec jp (ObjectMapper.))
        (.nextToken jp)
        (loop []
          (when (.hasCurrentToken jp)
            (let [^HashMap m (.readValueAs jp cls)]
              (s/write writer (.get m "url") (.get m "text"))
              (.nextToken jp)
              (recur))))))
    (s/commit writer)
    (l/close-kv lmdb)
    (printf "Indexing took %.2f seconds"
            (float (/ (- (System/currentTimeMillis) start) 1000)))
    (println)
    ))

(defn query
  [engine filename])

(defn run [opts]
  (index-wiki-json "data/wiki-datalevin-14" "wiki.json"))
