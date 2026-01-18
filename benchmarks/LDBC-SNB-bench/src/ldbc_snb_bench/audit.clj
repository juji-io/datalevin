(ns ldbc-snb-bench.audit
  "Audit CSV row counts against Datalevin counts."
  (:require [datalevin.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [datalevin.core :as d]
            [ldbc-snb-bench.schema :as schema]))

(def csv-subpath "graphs/csv/raw/composite-merged-fk")
(def default-data-path "data")
(def default-db-path "db/ldbc-snb")

(def tables
  [{:name "Place" :path "static/Place" :attr :place/id}
   {:name "Organisation" :path "static/Organisation" :attr :organization/id}
   {:name "TagClass" :path "static/TagClass" :attr :tagclass/id}
   {:name "Tag" :path "static/Tag" :attr :tag/id}
   {:name "Person" :path "dynamic/Person" :attr :person/id :explicit? true}
   {:name "Forum" :path "dynamic/Forum" :attr :forum/id :explicit? true}
   {:name "Message" :paths ["dynamic/Post" "dynamic/Comment"] :attr :message/id :explicit? true}
   {:name "Person_hasInterest_Tag" :path "dynamic/Person_hasInterest_Tag" :attr :person/hasInterest}
   {:name "Person_knows_Person" :path "dynamic/Person_knows_Person" :attr :knows/person1 :explicit? true :multiplier 2}
   {:name "Person_studyAt_University" :path "dynamic/Person_studyAt_University" :attr :studyAt/person}
   {:name "Person_workAt_Company" :path "dynamic/Person_workAt_Company" :attr :workAt/person}
   {:name "Forum_hasTag_Tag" :path "dynamic/Forum_hasTag_Tag" :attr :forum/hasTag}
   {:name "Forum_hasMember_Person" :path "dynamic/Forum_hasMember_Person" :attr :hasMember/forum :explicit? true}
   {:name "Message_hasTag_Tag"
    :paths ["dynamic/Post_hasTag_Tag" "dynamic/Comment_hasTag_Tag"]
    :attr :message/hasTag}
   {:name "Person_likes_Message"
    :paths ["dynamic/Person_likes_Post" "dynamic/Person_likes_Comment"]
    :attr :likes/person
    :explicit? true}])

(defn list-part-files [dir-path]
  (let [dir (io/file dir-path)]
    (when (.exists dir)
      (->> (.listFiles dir)
           (filter #(and (.isFile %)
                         (str/starts-with? (.getName %) "part-")
                         (str/ends-with? (.getName %) ".csv")))
           (sort-by #(.getName %))))))

(defn explicitly-deleted-row? [row idx]
  (let [v (some-> (nth row idx nil) str/trim str/lower-case)]
    (= "true" v)))

(defn count-csv-file [file explicit?]
  (with-open [reader (io/reader file)]
    (let [rows (csv/read-csv reader :separator \|)
          header (first rows)
          idx (when explicit?
                (.indexOf ^java.util.List header "explicitlyDeleted"))
          rows* (rest rows)]
      (when (and explicit? (neg? idx))
        (throw (ex-info "explicitlyDeleted column not found"
                        {:file (.getPath file)})))
      (reduce (fn [acc row]
                (if (and explicit? (explicitly-deleted-row? row idx))
                  acc
                  (inc acc)))
              0
              rows*))))

(defn count-csv-path [csv-root path explicit?]
  (let [dir (io/file csv-root path)
        files (list-part-files (.getPath dir))]
    (when-not (seq files)
      (throw (ex-info "CSV files not found" {:path (.getPath dir)})))
    (reduce + (map #(count-csv-file % explicit?) files))))

(defn count-csv-rows [csv-root {:keys [path paths explicit?]}]
  (let [paths (or paths (when path [path]))]
    (reduce + (map #(count-csv-path csv-root % explicit?) paths))))

(defn count-attr [db attr]
  (ffirst (d/q '[:find (count ?e) :in $ ?a :where [?e ?a _]] db attr)))

(defn format-row [{:keys [name]} csv-count db-count expected]
  (let [match? (= db-count expected)]
    (format "%-28s %12d %12d %12d %6s"
            name csv-count db-count expected (if match? "OK" "DIFF"))))

(defn run-audit [data-dir db-path]
  (let [csv-root (str (str/replace data-dir #"/$" "") "/" csv-subpath)]
    (when-not (.exists (io/file csv-root))
      (throw (ex-info "CSV root not found" {:csv-root csv-root})))
    (let [conn (d/get-conn db-path schema/schema schema/db-opts)]
      (try
        (let [db (d/db conn)]
          (println "CSV root:" csv-root)
          (println "DB path:" db-path)
          (println)
          (println (format "%-28s %12s %12s %12s %6s"
                           "Table" "CSV" "DB" "Expected" "Match"))
          (println (apply str (repeat 76 "-")))
          (doseq [table tables]
            (let [csv-count (count-csv-rows csv-root table)
                  db-count (count-attr db (:attr table))
                  multiplier (or (:multiplier table) 1)
                  expected (* csv-count multiplier)]
              (println (format-row table csv-count db-count expected))))
          (println (apply str (repeat 76 "-"))))
        (finally
          (d/close conn))))))

(defn -main [& args]
  (let [[data-dir db-path] args
        data-dir (or data-dir default-data-path)
        db-path (or db-path default-db-path)]
    (run-audit data-dir db-path)))
