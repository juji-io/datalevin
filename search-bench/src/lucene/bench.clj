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

(defn query
  [dir filename ^long n]
  (let [times    (ConcurrentLinkedQueue.)
        pool     (Executors/newWorkStealingPool)
        index    (FSDirectory/open (Paths/get dir (into-array String [])))
        reader   (DirectoryReader/open index)
        searcher (IndexSearcher. reader)
        analyzer (ClassicAnalyzer.)
        parser   (QueryParser. "text" analyzer)]
    (with-open [rdr (io/reader filename)]
      (doseq [query (line-seq rdr)]
        (let [start (System/currentTimeMillis)
              q     (.parse parser query)
              docs  (.search searcher q 10)
              hits  (.-scoreDocs docs)]
          (print query ": ")
          (doseq [hit hits]
            (let [did (.-doc hit)
                  d   (.doc searcher did)]
              (print (.get d "url"))
              (println)))
          (.add times (- (System/currentTimeMillis) start)))
        ;; this will crash
        #_(.execute pool
                    #(let [start (System/currentTimeMillis)
                           q     (.parse parser query)
                           docs  (.search searcher q 10)
                           hits  (.-scoreDocs docs)]
                       (doseq [hit hits]
                         (let [did (.-doc hit)
                               d   (.doc searcher did)]
                           (.get d "url")))
                       (.add times (- (System/currentTimeMillis) start)))))
      (.shutdown pool)
      (.awaitTermination pool 2 TimeUnit/HOURS))
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
  ;; (index-wiki-json "data/wiki-lucene-odd" "wiki-odd.json")
  (query "data/wiki-lucene-odd" "queries40k.txt" 40000))
