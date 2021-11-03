(ns datalevin.bench
  (:require [datalevin.lmdb :as l]
            [datalevin.search :as s]
            [clojure.java.io :as io])
  (:import [java.util HashMap Arrays]
           [java.util.concurrent Executors TimeUnit ConcurrentLinkedQueue]
           [java.io FileInputStream]
           [com.fasterxml.jackson.databind ObjectMapper]
           [com.fasterxml.jackson.core JsonFactory]
           ))

(defn index-wiki-json
  [dir ^String filename]
  (let [start  (System/currentTimeMillis)
        lmdb   (l/open-kv dir {:mapsize 100000})
        ;; txs    (ArrayList.)
        writer (s/index-writer lmdb)]
    ;; (l/open-dbi lmdb "docs")
    (with-open [f (FileInputStream. filename)]
      (let [jf  (JsonFactory.)
            jp  (.createParser jf f)
            cls (Class/forName "java.util.HashMap")]
        (.setCodec jp (ObjectMapper.))
        (.nextToken jp)
        (loop []
          (when (.hasCurrentToken jp)
            (let [^HashMap m (.readValueAs jp cls)
                  url        (.get m "url")
                  text       (.get m "text")]
              ;; (.add txs [:put "docs" url text :string :string])
              ;; (when (< 1000000 (.size txs))
              ;;   (l/transact-kv lmdb txs)
              ;;   (.clear txs))
              (s/write writer url text)
              (.nextToken jp)
              (recur))))))
    ;; (l/transact-kv lmdb txs)
    (s/commit writer)
    (l/close-kv lmdb)
    (printf "Indexing took %.2f seconds"
            (float (/ (- (System/currentTimeMillis) start) 1000)))
    (println)))

(defn query
  [engine filename ^long n]
  (let [times (ConcurrentLinkedQueue.)
        pool  (Executors/newWorkStealingPool)
        begin (System/currentTimeMillis)]
    (with-open [rdr (io/reader filename)]
      (doseq [query (line-seq rdr)]
        (.execute pool
                  #(let [start (System/currentTimeMillis)]
                     (take 10 (s/search engine query {:algo :bitmap}))
                     (.add times (- (System/currentTimeMillis) start)))))
      (.shutdown pool)
      (.awaitTermination pool 2 TimeUnit/HOURS))
    (printf "Querying took %.2f seconds"
            (float (/ (- (System/currentTimeMillis) begin) 1000)))
    (println)
    (let [result (.toArray times)]
      (Arrays/sort result)
      (println "mean:" (long (/ ^long (reduce + result) n)))
      (println "10 percentile:" (aget result (long (* 0.1 n))))
      (println "median:" (aget result (long (* 0.5 n))))
      (println "75 percentile:" (aget result (long (* 0.75 n))))
      (println "90 percentile:" (aget result (long (* 0.9 n))))
      (println "95 percentile:" (aget result (long (* 0.95 n))))
      (println "99 percentile:" (aget result (long (* 0.99 n))))
      (println "99.9 percentile:" (aget result (long (* 0.999 n))))
      (println "max:" (aget result (dec n))))))

(defn run [opts]
  (index-wiki-json "data/wiki-datalevin-2" "output.json")
  ;; (index-wiki-json "data/wiki-datalevin-odd1" "wiki-odd.json")
  #_(query (s/new-engine (l/open-kv "data/wiki-datalevin-odd1"))
           "queries40k.txt" 40000))
