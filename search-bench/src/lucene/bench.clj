(ns lucene.bench
  (:import [org.apache.lucene.analysis.standard ClassicAnalyzer]
           [org.apache.lucene.document Document Field Field$Store StringField
            TextField]
           [org.apache.lucene.index IndexWriter IndexWriterConfig IndexReader
            DirectoryReader]
           [org.apache.lucene.queryparser.classic QueryParser]
           [org.apache.lucene.search IndexSearcher Query ScoreDoc TopDocs]
           [org.apache.lucene.store FSDirectory]
           [java.nio.file Paths]
           [java.util HashMap ArrayList]
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

(defn run [opts]
  (index-wiki-json "data/wiki-lucene" "wiki.json"))
