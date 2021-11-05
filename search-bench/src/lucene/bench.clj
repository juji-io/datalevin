(ns lucene.bench
  (:require [clojure.java.io :as io])
  (:import [org.apache.lucene.analysis.standard ClassicAnalyzer]
           [org.apache.lucene.document Document Field Field$Store StringField
            TextField]
           [org.apache.lucene.index IndexWriter IndexWriterConfig IndexReader
            DirectoryReader]
           [org.apache.lucene.queryparser.classic QueryParser]
           [org.apache.lucene.search IndexSearcher TopScoreDocCollector Query ScoreDoc TopDocs]
           [org.apache.lucene.store FSDirectory]
           [java.nio.file Paths]
           [java.util HashMap Arrays]
           [java.util.concurrent Executors TimeUnit ConcurrentLinkedQueue]
           [java.io FileInputStream]
           [com.fasterxml.jackson.databind ObjectMapper]
           [com.fasterxml.jackson.core JsonFactory JsonParser JsonToken]))

(defn index-wiki-json
  [dir ^String filename]
  (let [start     (System/currentTimeMillis)
        analyzer  (ClassicAnalyzer.)
        directory (FSDirectory/open (Paths/get dir (into-array String [])))
        config    (IndexWriterConfig. analyzer)
        writer    (IndexWriter. directory config)]
    (with-open [f (FileInputStream. filename)]
      (let [jf  (JsonFactory.)
            jp  (.createParser jf f)
            cls (Class/forName "java.util.HashMap")]
        (.setCodec jp (ObjectMapper.))
        (.nextToken jp)
        (loop []
          (when (.hasCurrentToken jp)
            (let [^HashMap m (.readValueAs jp cls)
                  doc        (Document.)]
              (.add doc (StringField. "url" (.get m "url") Field$Store/YES))
              (.add doc (TextField. "text" (.get m "text") Field$Store/YES))
              (.addDocument writer doc)
              (.nextToken jp)
              (recur))))))
    (.commit writer)
    (.close writer)
    (printf "Indexing took %.2f seconds"
            (float (/ (- (System/currentTimeMillis) start) 1000)))
    (println)))

(defn- search
  [threads pool dir filename n]
  (let [n        ^long n
        times    (ConcurrentLinkedQueue.)
        begin    (System/currentTimeMillis)
        index    (FSDirectory/open (Paths/get dir (into-array String [])))
        reader   (DirectoryReader/open index)
        searcher (IndexSearcher. reader)
        analyzer (ClassicAnalyzer.)]
    (with-open [rdr (io/reader filename)]
      (doseq [query (line-seq rdr)]
        (.execute pool
                  #(let [start  (System/currentTimeMillis)
                         parser (QueryParser. "text" analyzer)
                         q      (.parse parser query)
                         docs   (.search searcher q 10)
                         hits   (.-scoreDocs docs)]
                     (doseq [hit hits]
                       (let [did (.-doc hit)
                             d   (.doc searcher did)]
                         (.get d "url")))
                     (.add times (- (System/currentTimeMillis) start)))))
      (.shutdown pool)
      (.awaitTermination pool 1 TimeUnit/HOURS))
    (println)
    (printf "Querying with %d threads took %.2f seconds"
            threads
            (float (/ (- (System/currentTimeMillis) begin) 1000)))
    (println)
    (println "Latency (ms):")
    (let [result (.toArray times)]
      (Arrays/sort result)
      (println "mean:" (long (/ ^long (reduce + result) n)))
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

(defn run [opts]
  (println)
  (println "Lucene:")
  ;; (index-wiki-json "data/wiki-lucene-all" "wiki.json")
  (query "data/wiki-lucene-all" "queries40k.txt" 40000))
