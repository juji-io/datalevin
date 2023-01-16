(ns datalevin.bench
  (:require
   [datalevin.core :as d]
   [clojure.java.io :as io])
  (:import
   [java.util HashMap Arrays]
   [java.util.concurrent Executors TimeUnit ConcurrentLinkedQueue
    ExecutorService]
   [java.io FileInputStream]
   [com.fasterxml.jackson.databind ObjectMapper]
   [com.fasterxml.jackson.core JsonFactory]))

(defn index-wiki-json
  [dir ^String filename]
  (let [start  (System/nanoTime)
        lmdb   (d/open-kv dir {:mapsize 100000})
        writer (d/search-index-writer lmdb)]
    (with-open [f (FileInputStream. filename)]
      (let [jf  (JsonFactory.)
            jp  (.createParser jf f)
            cls (Class/forName "java.util.HashMap")]
        (.setCodec jp (ObjectMapper.))
        (.nextToken jp)
        (loop []
          (when (.hasCurrentToken jp)
            (let [^HashMap m (.readValueAs jp cls)]
              (d/write writer (.get m "url") (.get m "text"))
              (.nextToken jp)
              (recur))))))
    (d/commit writer)
    (d/close-kv lmdb)
    (printf "Indexing took %.5f seconds"
            (/ (/ (- (System/nanoTime) start) 1000.0) 1000000.0))
    (println)))

(defn- search
  [threads ^ExecutorService pool engine filename n]
  (let [n     ^long n
        times (ConcurrentLinkedQueue.)
        begin (System/nanoTime)]
    (with-open [rdr (io/reader filename)]
      (doseq [query (line-seq rdr)]
        (.execute pool
                  #(let [start (System/nanoTime)]
                     (d/search engine query)
                     (.add times (/ (- (System/nanoTime) start) 1000000.0)))))
      (.shutdown pool)
      (.awaitTermination pool 1 TimeUnit/HOURS))
    (println)
    (printf "Querying with %d threads took %.5f seconds"
            threads
            (/ (/ (- (System/nanoTime) begin) 1000.0) 1000000.0))
    (println)
    (println "Latency (ms):")
    (let [result (.toArray times)]
      (Arrays/sort result)
      (println "mean:" (long (/ ^long (reduce + result) ^long n)))
      (println "median:" (aget result (long (* 0.5 n))))
      (println "75 percentile:" (aget result (long (* 0.75 n))))
      (println "90 percentile:" (aget result (long (* 0.9 n))))
      (println "95 percentile:" (aget result (long (* 0.95 n))))
      (println "99 percentile:" (aget result (long (* 0.99 n))))
      (println "99.9 percentile:" (aget result (long (* 0.999 n))))
      (println "max:" (aget result (dec n))))))

(defn query
  "`n` is the total number of queries"
  [dir filename n]
  (println "Fixed thread pool:")
  (dotimes [threads 12]
    (let [threads (inc threads)
          pool    (Executors/newFixedThreadPool threads)]
      (search threads pool dir filename n)))
  (println "Work stealing thread pool:")
  (let [pool (Executors/newWorkStealingPool)]
    (search 0 pool dir filename n)))

(defn -main [&opts]
  (println)
  (println "Datalevin:")

  ;; (index-wiki-json "data/wiki-datalevin-10k" "data/wiki10k.json")
  ;; (println "Done indexing.")
  ;; (query (d/new-search-engine (d/open-kv "data/wiki-datalevin-10k"))
  ;;        "data/queries10k.txt" 10000)
  ;; (println "Done query.")

  (index-wiki-json "data/wiki-datalevin-all" "data/wiki.json")
  (println "Done indexing.")
  (query (d/new-search-engine (d/open-kv "data/wiki-datalevin-all"))
         "queries40k.txt" 40000)
  (println "Done query.")

  )
