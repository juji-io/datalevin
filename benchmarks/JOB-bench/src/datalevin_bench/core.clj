(ns datalevin-bench.core
  (:require
   [datalevin.core :as d]
   [clojure.java.io :as io]
   [clojure.string :as s]))

(def schema
  {:aka-name/person        {:db/valueType :db.type/ref}
   :aka-name/name          {:db/valueType :db.type/string}
   :aka-name/imdb-index    {:db/valueType :db.type/string}
   :aka-name/name-pcode-cf {:db/valueType :db.type/string}
   :aka-name/name-pcode-nf {:db/valueType :db.type/string}
   :aka-name/surname-pcode {:db/valueType :db.type/string}

   :aka-title/movie           {:db/valueType :db.type/ref}
   :aka-title/title           {:db/valueType :db.type/string}
   :aka-title/imdb-index      {:db/valueType :db.type/string}
   :aka-title/kind            {:db/valueType :db.type/ref}
   :aka-title/production-year {:db/valueType :db.type/long}
   :aka-title/phonetic-code   {:db/valueType :db.type/string}
   :aka-title/episode-of      {:db/valueType :db.type/ref}
   :aka-title/season-nr       {:db/valueType :db.type/long}
   :aka-title/episode-nr      {:db/valueType :db.type/long}
   :aka-title/note            {:db/valueType :db.type/string}

   :cast-info/person      {:db/valueType :db.type/ref}
   :cast-info/movie       {:db/valueType :db.type/ref}
   :cast-info/person-role {:db/valueType :db.type/ref}
   :cast-info/note        {:db/valueType :db.type/string}
   :cast-info/nr-order    {:db/valueType :db.type/long}
   :cast-info/role        {:db/valueType :db.type/ref}

   :char-name/name          {:db/valueType :db.type/string}
   :char-name/imdb-index    {:db/valueType :db.type/string}
   :char-name/imdb-id       {:db/valueType :db.type/long}
   :char-name/name-pcode-nf {:db/valueType :db.type/string}
   :char-name/surname-pcode {:db/valueType :db.type/string}

   :comp-cast-type/kind {:db/valueType :db.type/string}

   :company-name/name          {:db/valueType :db.type/string}
   :company-name/country-code  {:db/valueType :db.type/string}
   :company-name/imdb-id       {:db/valueType :db.type/long}
   :company-name/name-pcode-nf {:db/valueType :db.type/string}
   :company-name/name-pcode-sf {:db/valueType :db.type/string}

   :company-type/kind {:db/valueType :db.type/string}

   :complete-cast/movie   {:db/valueType :db.type/ref}
   :complete-cast/subject {:db/valueType :db.type/ref}
   :complete-cast/status  {:db/valueType :db.type/ref}

   :info-type/info {:db/valueType :db.type/string}

   :keyword/keyword       {:db/valueType :db.type/string}
   :keyword/phonetic-code {:db/valueType :db.type/string}

   :kind-type/kind {:db/valueType :db.type/string}

   :link-type/link {:db/valueType :db.type/string}

   :movie-companies/movie        {:db/valueType :db.type/ref}
   :movie-companies/company      {:db/valueType :db.type/ref}
   :movie-companies/company-type {:db/valueType :db.type/ref}
   :movie-companies/note         {:db/valueType :db.type/string}

   :movie-info/movie     {:db/valueType :db.type/ref}
   :movie-info/info-type {:db/valueType :db.type/ref}
   :movie-info/info      {:db/valueType :db.type/string}
   :movie-info/note      {:db/valueType :db.type/string}

   :movie-info-idx/movie     {:db/valueType :db.type/ref}
   :movie-info-idx/info-type {:db/valueType :db.type/ref}
   :movie-info-idx/info      {:db/valueType :db.type/string}
   :movie-info-idx/note      {:db/valueType :db.type/string}

   :movie-keyword/movie   {:db/valueType :db.type/ref}
   :movie-keyword/keyword {:db/valueType :db.type/ref}

   :movie-link/movie        {:db/valueType :db.type/ref}
   :movie-link/linked-movie {:db/valueType :db.type/ref}
   :movie-link/link-type    {:db/valueType :db.type/ref}

   :name/name          {:db/valueType :db.type/string}
   :name/imdb-index    {:db/valueType :db.type/string}
   :name/imdb-id       {:db/valueType :db.type/long}
   :name/gender        {:db/valueType :db.type/string}
   :name/name-pcode-cf {:db/valueType :db.type/string}
   :name/name-pcode-nf {:db/valueType :db.type/string}
   :name/surname-pcode {:db/valueType :db.type/string}

   :person-info/person    {:db/valueType :db.type/ref}
   :person-info/info-type {:db/valueType :db.type/ref}
   :person-info/info      {:db/valueType :db.type/string}
   :person-info/note      {:db/valueType :db.type/string}

   :role-type/role {:db/valueType :db.type/string}

   :title/title           {:db/valueType :db.type/string}
   :title/imdb-index      {:db/valueType :db.type/string}
   :title/kind            {:db/valueType :db.type/ref}
   :title/production-year {:db/valueType :db.type/long}
   :title/imdb-id         {:db/valueType :db.type/long}
   :title/phonetic-code   {:db/valueType :db.type/string}
   :title/episode-of      {:db/valueType :db.type/ref}
   :title/season-nr       {:db/valueType :db.type/long}
   :title/episode-nr      {:db/valueType :db.type/long}
   :title/series-years    {:db/valueType :db.type/string}})

;; loading the data

;; eid base
(def aka-name-base        1000000)
(def aka-title-base       2000000)
(def cast-info-base       100000000)
(def char-name-base       10000000)
(def comp-cast-type-base  0)
(def company-name-base    3000000)
(def company-type-base    10)
(def complete-cast-base   4000000)
(def info-type-base       1000)
(def keyword-base         5000000)
(def kind-type-base       20)
(def link-type-base       30)
(def movie-companies-base 20000000)
(def movie-info-base      30000000)
(def movie-info-idx-base  40000000)
(def movie-keyword-base   50000000)
(def movie-link-base      100000)
(def name-base            60000000)
(def person-info-base     70000000)
(def role-type-base       50)
(def title-base           80000000)

(defn- add-comp-cast-type []
  (map (fn [[id content]]
         (d/datom (+ comp-cast-type-base (Long/parseLong id))
                  :comp-cast-type/kind content))
       (d/read-csv (slurp "data/comp_cast_type.csv"))))

(defn- add-company-type []
  (map (fn [[id content]]
         (d/datom (+ company-type-base (Long/parseLong id))
                  :company-type/kind content))
       (d/read-csv (slurp  "data/company_type.csv"))))

(defn- add-kind-type []
  (map (fn [[id content]]
         (d/datom (+ kind-type-base (Long/parseLong id))
                  :kind-type/kind content))
       (d/read-csv (slurp "data/kind_type.csv"))))

(defn- add-link-type []
  (map (fn [[id content]]
         (d/datom (+ link-type-base (Long/parseLong id))
                  :link-type/link content))
       (d/read-csv (slurp "data/link_type.csv"))))

(defn- add-role-type []
  (map (fn [[id content]]
         (d/datom (+ role-type-base (Long/parseLong id))
                  :role-type/role content))
       (d/read-csv (slurp "data/role_type.csv"))))

(defn- add-info-type []
  (map (fn [[id content]]
         (d/datom (+ info-type-base (Long/parseLong id))
                  :info-type/info content))
       (d/read-csv (slurp "data/info_type.csv"))))

(defn- add-movie-link []
  (sequence
    (comp
      (map (fn [[id movie linked-movie link-type]]
             (let [eid (+ movie-link-base (Long/parseLong id))]
               [(d/datom eid :movie-link/movie
                         (+ title-base (Long/parseLong movie)))
                (d/datom eid :movie-link/linked-movie
                         (+ title-base (Long/parseLong linked-movie)))
                (d/datom eid :movie-link/link-type
                         (+ link-type-base (Long/parseLong link-type)))])))
      cat)
    (d/read-csv (slurp "data/movie_link.csv"))))

(defn- add-aka-name []
  (sequence
    (comp
      (map
        (fn [[id person name imdb-index name-pcode-cf
             name-pcode-nf surname-pcode]]
          (let [eid (+ aka-name-base (Long/parseLong id))]
            (cond-> [(d/datom eid :aka-name/person
                              (+ name-base (Long/parseLong person)))
                     (d/datom eid :aka-name/name name)]
              (not (s/blank? imdb-index))
              (conj (d/datom eid :aka-name/imdb-index imdb-index))
              (not (s/blank? name-pcode-cf))
              (conj (d/datom eid :aka-name/name-pcode-cf name-pcode-cf))
              (not (s/blank? name-pcode-nf))
              (conj (d/datom eid :aka-name/name-pcode-nf name-pcode-nf))
              (not (s/blank? surname-pcode))
              (conj (d/datom eid :aka-name/surname-pcode surname-pcode))))))
      cat)
    (d/read-csv (slurp "data/aka_name.csv"))))

(defn- add-aka-title []
  (sequence
    (comp
      (map
        (fn [[id movie title imdb-index kind production-year phonetic-code
              episode-of season-nr episode-nr note]]
          (let [eid (+ aka-title-base (Long/parseLong id))]
            (cond-> [(d/datom eid :aka-title/movie
                              (+ title-base (Long/parseLong movie)))
                     (d/datom eid :aka-title/title title)]
              (not (s/blank? imdb-index))
              (conj (d/datom eid :aka-title/imdb-index imdb-index))
              (not (s/blank? kind))
              (conj (d/datom eid :aka-title/kind
                             (+ kind-type-base (Long/parseLong kind))))
              (not (s/blank? production-year))
              (conj (d/datom eid :aka-title/production-year
                             (Long/parseLong production-year)))
              (not (s/blank? phonetic-code))
              (conj (d/datom eid :aka-title/phonetic-code phonetic-code))
              (not (s/blank? episode-of))
              (conj (d/datom eid :aka-title/episode-of
                             (+ title-base (Long/parseLong episode-of))))
              (not (s/blank? season-nr))
              (conj (d/datom eid :aka-title/season-nr (Long/parseLong season-nr)))
              (not (s/blank? episode-nr))
              (conj (d/datom eid :aka-title/episode-nr (Long/parseLong episode-nr)))
              (not (s/blank? note))
              (conj (d/datom eid :aka-title/note note))))))
      cat)
    (d/read-csv (slurp "data/aka_title.csv"))))

(defn- add-company-name []
  (sequence
    (comp
      (map
        (fn [[id name country-code imdb-id name-pcode-nf name-pcode-sf]]
          (let [eid (+ company-name-base (Long/parseLong id))]
            (cond-> [(d/datom eid :company-name/name name)]
              (not (s/blank? country-code))
              (conj (d/datom eid :company-name/country-code country-code))
              (not (s/blank? imdb-id))
              (conj (d/datom eid :company-name/imdb-id (Long/parseLong imdb-id)))
              (not (s/blank? name-pcode-nf))
              (conj (d/datom eid :company-name/name-pcode-nf name-pcode-nf))
              (not (s/blank? name-pcode-sf))
              (conj (d/datom eid :company-name/name-pcode-sf name-pcode-sf))))))
      cat)
    (d/read-csv (slurp "data/company_name.csv"))))

(defn- add-complete-cast []
  (sequence
    (comp
      (map
        (fn [[id movie subject status]]
          (let [eid (+ complete-cast-base (Long/parseLong id))]
            [(d/datom eid :complete-cast/movie
                      (+ title-base (Long/parseLong movie)))
             (d/datom eid :complete-cast/subject
                      (+ comp-cast-type-base (Long/parseLong subject)))
             (d/datom eid :complete-cast/status
                      (+ comp-cast-type-base (Long/parseLong status)))])))
      cat)
    (d/read-csv (slurp "data/complete_cast.csv"))))

(defn- add-keyword []
  (sequence
    (comp
      (map
        (fn [[id keyword phonetic-code]]
          (let [eid (+ keyword-base (Long/parseLong id))]
            [(d/datom eid :keyword/keyword keyword)
             (d/datom eid :keyword/phonetic-code phonetic-code)])))
      cat)
    (d/read-csv (slurp "data/keyword.csv"))))

(defn- add-char-name []
  (sequence
    (comp
      (map
        (fn [[id name imdb-index imdb-id name-pcode-nf surname-pcode]]
          (let [eid (+ char-name-base (Long/parseLong id))]
            (cond-> [(d/datom eid :char-name/name name)]
              (not (s/blank? imdb-index))
              (conj (d/datom eid :char-name/imdb-index imdb-index))
              (not (s/blank? imdb-id))
              (conj (d/datom eid :char-name/imdb-id (Long/parseLong imdb-id)))
              (not (s/blank? name-pcode-nf))
              (conj (d/datom eid :char-name/name-pcode-nf name-pcode-nf))
              (not (s/blank? surname-pcode))
              (conj (d/datom eid :char-name/surname-pcode surname-pcode))))))
      cat)
    (d/read-csv (slurp "data/char_name.csv"))))

(defn- add-movie-companies []
  (sequence
    (comp
      (map
        (fn [[id movie company company-type note]]
          (let [eid (+ movie-companies-base (Long/parseLong id))]
            (cond-> [(d/datom eid :movie-companies/movie
                              (+ title-base (Long/parseLong movie)))
                     (d/datom eid :movie-companies/company
                              (+ company-name-base
                                 (Long/parseLong company)))
                     (d/datom eid :movie-companies/company-type
                              (+ company-type-base
                                 (Long/parseLong company-type)))]
              (not (s/blank? note))
              (conj (d/datom eid :movie-companies/note note))))))
      cat)
    (d/read-csv (slurp "data/movie_companies.csv"))))

(defn- add-movie-info [reader]
  (sequence
    (comp
      (map (fn [[id movie info-type info note]]
             (let [eid (+ movie-info-base (Long/parseLong id))]
               (cond-> [(d/datom eid :movie-info/movie
                                 (+ title-base (Long/parseLong movie)))
                        (d/datom eid :movie-info/info-type
                                 (+ info-type-base
                                    (Long/parseLong info-type)))
                        (d/datom eid :movie-info/info info)]
                 (not (s/blank? note))
                 (conj (d/datom eid :movie-info/note note))))))
      cat)
    (d/read-csv reader)))

(defn- add-movie-info-idx [reader]
  (sequence
    (comp
      (map
        (fn [[id movie info-type info note]]
          (let [eid (+ movie-info-idx-base (Long/parseLong id))]
            (cond-> [(d/datom eid :movie-info-idx/movie
                              (+ title-base (Long/parseLong movie)))
                     (d/datom eid :movie-info-idx/info-type
                              (+ info-type-base (Long/parseLong info-type)))
                     (d/datom eid :movie-info-idx/info info)]
              (not (s/blank? note))
              (conj (d/datom eid :movie-info-idx/note note))))))
      cat)
    (d/read-csv reader)))

(defn- add-movie-keyword [reader]
  (sequence
    (comp
      (map (fn [[id movie keyword]]
             (let [eid (+ movie-keyword-base (Long/parseLong id))]
               [(d/datom eid :movie-keyword/movie
                         (+ title-base (Long/parseLong movie)))
                (d/datom eid :movie-keyword/keyword
                         (+ keyword-base (Long/parseLong keyword)))])))
      cat)
    (d/read-csv reader)))

(defn- add-name [reader]
  (sequence
    (comp
      (map
        (fn [[id name imdb-index imdb-id gender name-pcode-cf name-pcode-nf
              surname-pcode]]
          (let [eid (+ name-base (Long/parseLong id))]
            (cond-> [(d/datom eid :name/name name)]
              (not (s/blank? imdb-index))
              (conj (d/datom eid :name/imdb-index imdb-index))
              (not (s/blank? imdb-id))
              (conj (d/datom eid :name/imdb-id (Long/parseLong imdb-id)))
              (not (s/blank? gender))
              (conj (d/datom eid :name/gender gender))
              (not (s/blank? name-pcode-cf))
              (conj (d/datom eid :name/name-pcode-cf name-pcode-cf))
              (not (s/blank? name-pcode-nf))
              (conj (d/datom eid :name/name-pcode-nf name-pcode-nf))
              (not (s/blank? surname-pcode))
              (conj (d/datom eid :name/surname-pcode surname-pcode))))))
      cat)
    (d/read-csv reader)))

(defn- add-person-info [reader]
  (sequence
    (comp
      (map
        (fn [[id person info-type info note]]
          (let [eid (+ person-info-base (Long/parseLong id))]
            (cond-> [(d/datom eid :person-info/person
                              (+ name-base (Long/parseLong person)))
                     (d/datom eid :person-info/info-type
                              (+ info-type-base (Long/parseLong info-type)))
                     (d/datom eid :person-info/info info)]
              (not (s/blank? note))
              (conj (d/datom eid :person-info/note note))))))
      cat)
    (d/read-csv reader)))

(defn- add-title [reader]
  (sequence
    (comp
      (map
        (fn [[id title imdb-index kind production-year imdb-id phonetic-code
              episode-of season-nr episode-nr series-years]]
          (let [eid (+ title-base (Long/parseLong id))]
            (cond-> [(d/datom eid :title/title title)
                     (d/datom eid :title/kind
                              (+ kind-type-base (Long/parseLong kind)))]
              (not (s/blank? imdb-index))
              (conj (d/datom eid :title/imdb-index imdb-index))
              (not (s/blank? production-year))
              (conj (d/datom eid :title/production-year
                             (Long/parseLong production-year)))
              (not (s/blank? imdb-id))
              (conj (d/datom eid :title/imdb-id (Long/parseLong imdb-id)))
              (not (s/blank? phonetic-code))
              (conj (d/datom eid :title/phonetic-code phonetic-code))
              (not (s/blank? episode-of))
              (conj (d/datom eid :title/episode-of
                             (+ title-base (Long/parseLong episode-of))))
              (not (s/blank? season-nr))
              (conj (d/datom eid :title/season-nr (Long/parseLong season-nr)))
              (not (s/blank? episode-nr))
              (conj (d/datom eid :title/episode-nr
                             (Long/parseLong episode-nr)))
              (not (s/blank? series-years))
              (conj (d/datom eid :title/series-years series-years))))))
      cat)
    (d/read-csv reader)))

(defn- add-cast-info [reader]
  (sequence
    (comp
      (map
        (fn [[id person movie person-role note nr-order role]]
          (let [eid (+ cast-info-base (Long/parseLong id))]
            (cond-> [(d/datom eid :cast-info/person
                              (+ name-base (Long/parseLong person)))
                     (d/datom eid :cast-info/movie
                              (+ title-base (Long/parseLong movie)))
                     (d/datom eid :cast-info/role
                              (+ role-type-base (Long/parseLong role)))]
              (not (s/blank? person-role))
              (conj (d/datom eid :cast-info/person-role
                             (+ char-name-base (Long/parseLong person-role))))
              (not (s/blank? note))
              (conj (d/datom eid :cast-info/note note))
              (not (s/blank? nr-order))
              (conj (d/datom eid :cast-info/nr-order

                             (Long/parseLong nr-order)))))))
      cat)
    (d/read-csv reader)))

;; initial loading of data, may take up to 20 minutes
#_(def db
    (with-open [movie-info-rdr     (io/reader "data/movie_info.csv")
                movie-info-idx-rdr (io/reader "data/movie_info_idx.csv")
                movie-keyword-rdr  (io/reader "data/movie_keyword.csv")
                name-rdr           (io/reader "data/name.csv")
                person-info-rdr    (io/reader "data/person_info.csv")
                title-rdr          (io/reader "data/title.csv")
                cast-info-rdr      (io/reader "data/cast_info.csv")]
      (let [start (atom (System/currentTimeMillis))
            show  (fn [db label]
                    (let [now (System/currentTimeMillis)]
                      (println label "took" (- now @start))
                      (reset! start now))
                    db)]
        (-> (d/empty-db "db" schema {:closed-schema? true
                                     :kv-opts        {:mapsize 50000}})
            (show "empty db")
            (d/fill-db (add-comp-cast-type))
            (show "comp-cast-type")
            (d/fill-db (add-company-type))
            (show "company-type")
            (d/fill-db (add-kind-type))
            (show "kind-type")
            (d/fill-db (add-link-type))
            (show "link-type")
            (d/fill-db (add-role-type))
            (show "role-type")
            (d/fill-db (add-info-type))
            (show "info-type")
            (d/fill-db (add-movie-link))
            (show "movie-link")
            (d/fill-db (add-aka-name))
            (show "aka-name")
            (d/fill-db (add-aka-title))
            (show "aka-title")
            (d/fill-db (add-company-name))
            (show "company-name")
            (d/fill-db (add-complete-cast))
            (show "complete-cast")
            (d/fill-db (add-keyword))
            (show "keyword")
            (d/fill-db (add-char-name))
            (show "char-name")
            (d/fill-db (add-movie-companies))
            (show "movie-companies")
            (d/fill-db (add-movie-info movie-info-rdr))
            (show "movie-info")
            (d/fill-db (add-movie-info-idx movie-info-idx-rdr))
            (show "movie-info-idx")
            (d/fill-db (add-movie-keyword movie-keyword-rdr))
            (show "movie-keyword")
            (d/fill-db (add-name name-rdr))
            (show "name")
            (d/fill-db (add-person-info person-info-rdr))
            (show "person-info")
            (d/fill-db (add-title title-rdr))
            (show "title")
            (d/fill-db (add-cast-info cast-info-rdr))
            (show "cast-info")
            ))))

;; assume data is already loaded into db
(def conn (d/get-conn "db"))

;; queries that beat postgres are labeled 'good plan'

(def q-1a '[:find (min ?mc.note) (min ?t.title) (min ?t.production-year)
            :where
            [?ct :company-type/kind "production companies"]
            [?it :info-type/info "top 250 rank"]
            [?mc :movie-companies/note ?mc.note]
            [(not-like ?mc.note "%(as Metro-Goldwyn-Mayer Pictures)%")]
            [(or (like ?mc.note "%(co-production)%")
                 (like ?mc.note "%(presents)%"))]
            [?mc :movie-companies/company-type ?ct]
            [?mc :movie-companies/movie ?t]
            [?mi :movie-info-idx/movie ?t]
            [?mi :movie-info-idx/info-type ?it]
            [?t :title/title ?t.title]
            [?t :title/production-year ?t.production-year]
            ])

;; good plan
(def q-1b '[:find (min ?mc.note) (min ?t.title) (min ?t.production-year)
            :where
            [?ct :company-type/kind "production companies"]
            [?it :info-type/info "bottom 10 rank"]
            [?mc :movie-companies/note ?mc.note]
            [(not-like ?mc.note "%(as Metro-Goldwyn-Mayer Pictures)%")]
            [?t :title/production-year ?t.production-year]
            [(<= 2005 ?t.production-year 2010)]
            [?mc :movie-companies/company-type ?ct]
            [?mc :movie-companies/movie ?t]
            [?mi :movie-info-idx/movie ?t]
            [?mi :movie-info-idx/info-type ?it]
            [?t :title/title ?t.title]])

(def q-1c '[:find (min ?mc.note) (min ?t.title) (min ?t.production-year)
            :where
            [?ct :company-type/kind "production companies"]
            [?it :info-type/info "top 250 rank"]
            [?mc :movie-companies/note ?mc.note]
            [(not-like ?mc.note "%(as Metro-Goldwyn-Mayer Pictures)%")]
            [(like ?mc.note "%(co-production)%")]
            [?t :title/production-year ?t.production-year]
            [(< 2010 ?t.production-year)]
            [?mc :movie-companies/company-type ?ct]
            [?mc :movie-companies/movie ?t]
            [?mi :movie-info-idx/movie ?t]
            [?mi :movie-info-idx/info-type ?it]
            [?t :title/title ?t.title]])

;; good plan
(def q-1d '[:find (min ?mc.note) (min ?t.title) (min ?t.production-year)
            :where
            [?ct :company-type/kind "production companies"]
            [?it :info-type/info "bottom 10 rank"]
            [?mc :movie-companies/note ?mc.note]
            [(not-like ?mc.note "%(as Metro-Goldwyn-Mayer Pictures)%")]
            [?t :title/production-year ?t.production-year]
            [(< 2000 ?t.production-year)]
            [?mc :movie-companies/company-type ?ct]
            [?mc :movie-companies/movie ?t]
            [?mi :movie-info-idx/movie ?t]
            [?mi :movie-info-idx/info-type ?it]
            [?t :title/title ?t.title]])

;; good plan
(def q-2a '[:find (min ?t.title)
            :where
            [?cn :company-name/country-code "[de]"]
            [?k :keyword/keyword "character-name-in-title"]
            [?mc :movie-companies/company ?cn]
            [?mc :movie-companies/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?t :title/title ?t.title]])

;; good plan
(def q-2b '[:find (min ?t.title)
            :where
            [?cn :company-name/country-code "[nl]"]
            [?k :keyword/keyword "character-name-in-title"]
            [?mc :movie-companies/company ?cn]
            [?mc :movie-companies/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?t :title/title ?t.title]])

;; good plan
(def q-2c '[:find (min ?t.title)
            :where
            [?cn :company-name/country-code "[sm]"]
            [?k :keyword/keyword "character-name-in-title"]
            [?mc :movie-companies/company ?cn]
            [?mc :movie-companies/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?t :title/title ?t.title]])

;; good plan
(def q-2d '[:find (min ?t.title)
            :where
            [?cn :company-name/country-code "[us]"]
            [?k :keyword/keyword "character-name-in-title"]
            [?mc :movie-companies/company ?cn]
            [?mc :movie-companies/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?t :title/title ?t.title]])

(def q-3a '[:find (min ?t.title)
            :where
            [?k :keyword/keyword ?k.keyword]
            [(like ?k.keyword "%sequel%")]
            [?mi :movie-info/info ?mi.info]
            [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                           "Denish", "Norwegian", "German"])]
            [?t :title/production-year ?t.production-year]
            [(< 2005 ?t.production-year)]
            [?mi :movie-info/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?t :title/title ?t.title]])

;; good plan
(def q-3b '[:find (min ?t.title)
            :where
            [?k :keyword/keyword ?k.keyword]
            [(like ?k.keyword "%sequel%")]
            [?mi :movie-info/info ?mi.info]
            [(in ?mi.info ["Bulgaria"])]
            [?t :title/production-year ?t.production-year]
            [(< 2010 ?t.production-year)]
            [?mi :movie-info/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?t :title/title ?t.title]])

(def q-3c '[:find (min ?t.title)
            :where
            [?k :keyword/keyword ?k.keyword]
            [(like ?k.keyword "%sequel%")]
            [?mi :movie-info/info ?mi.info]
            [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                           "Denish", "Norwegian", "German", "USA", "American"])]
            [?t :title/production-year ?t.production-year]
            [(< 1990 ?t.production-year)]
            [?mi :movie-info/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?t :title/title ?t.title]])

;; horrible
(def q-4a '[:find (min ?mi-idx.info) (min ?t.title)
            :where
            [?it :info-type/info "rating"]
            [?k :keyword/keyword ?k.keyword]
            [(like ?k.keyword "%sequel%")]
            [?mi-idx :movie-info-idx/info ?mi-idx.info]
            [(< "5.0" ?mi-idx.info)]
            [?t :title/production-year ?t.production-year]
            [(< 2005 ?t.production-year)]
            [?mi-idx :movie-info-idx/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?mi-idx :movie-info-idx/info-type ?it]
            [?t :title/title ?t.title]])

(def q-4b '[:find (min ?mi-idx.info) (min ?t.title)
            :where
            [?it :info-type/info "rating"]
            [?k :keyword/keyword ?k.keyword]
            [(like ?k.keyword "%sequel%")]
            [?mi-idx :movie-info-idx/info ?mi-idx.info]
            [(< "9.0" ?mi-idx.info)]
            [?t :title/production-year ?t.production-year]
            [(< 2010 ?t.production-year)]
            [?mi-idx :movie-info-idx/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?mi-idx :movie-info-idx/info-type ?it]
            [?t :title/title ?t.title]])

;; horrible
(def q-4c '[:find (min ?mi-idx.info) (min ?t.title)
            :where
            [?it :info-type/info "rating"]
            [?k :keyword/keyword ?k.keyword]
            [(like ?k.keyword "%sequel%")]
            [?mi-idx :movie-info-idx/info ?mi-idx.info]
            [(< "2.0" ?mi-idx.info)]
            [?t :title/production-year ?t.production-year]
            [(< 1990 ?t.production-year)]
            [?mi-idx :movie-info-idx/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?mi-idx :movie-info-idx/info-type ?it]
            [?t :title/title ?t.title]])

(def q-5a '[:find (min ?t.title)
            :where
            [?ct :company-type/kind "production companies"]
            [?mc :movie-companies/note ?mc.note]
            [(like ?mc.note "%(theatrical)%")]
            [(like ?mc.note "%(France)%")]
            [?mi :movie-info/info ?mi.info]
            [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                           "Denish", "Norwegian", "German"])]
            [?t :title/production-year ?t.production-year]
            [(< 2005 ?t.production-year)]
            [?mi :movie-info/movie ?t]
            [?mc :movie-companies/movie ?t]
            [?mc :movie-companies/company-type ?ct]
            [?t :title/title ?t.title]])

(def q-5b '[:find (min ?t.title)
            :where
            [?ct :company-type/kind "production companies"]
            [?mc :movie-companies/note ?mc.note]
            [(like ?mc.note "%(VHS)%")]
            [(like ?mc.note "%(USA)%")]
            [(like ?mc.note "%(1994)%")]
            [?mi :movie-info/info ?mi.info]
            [(in ?mi.info ["USA" "America"])]
            [?t :title/production-year ?t.production-year]
            [(< 2010 ?t.production-year)]
            [?mi :movie-info/movie ?t]
            [?mc :movie-companies/movie ?t]
            [?mc :movie-companies/company-type ?ct]
            [?t :title/title ?t.title]])

(def q-5c '[:find (min ?t.title)
            :where
            [?ct :company-type/kind "production companies"]
            [?mc :movie-companies/note ?mc.note]
            [(not-like ?mc.note "%(TV)%")]
            [(like ?mc.note "%(USA)%")]
            [?mi :movie-info/info ?mi.info]
            [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                           "Denish", "Norwegian", "German", "USA", "American"])]
            [?t :title/production-year ?t.production-year]
            [(< 1990 ?t.production-year)]
            [?mi :movie-info/movie ?t]
            [?mc :movie-companies/movie ?t]
            [?mc :movie-companies/company-type ?ct]
            [?t :title/title ?t.title]])

;; good plan
(def q-6a '[:find (min ?n.name) (min ?t.title)
            :where
            [?k :keyword/keyword "marvel-cinematic-universe"]
            [?n :name/name ?n.name]
            [(like ?n.name "%Downey%Robert%")]
            [?t :title/production-year ?t.production-year]
            [(< 2010 ?t.production-year)]
            [?mk :movie-keyword/keyword ?k]
            [?mk :movie-keyword/movie ?t]
            [?ci :cast-info/movie ?t]
            [?ci :cast-info/person ?n]
            [?t :title/title ?t.title]])

;; good plan
(def q-6b '[:find (min ?k.keyword) (min ?n.name) (min ?t.title)
            :where
            [?k :keyword/keyword ?k.keyword]
            [(in ?k.keyword ["superhero", "sequel", "second-part", "marvel-comics",
                             "based-on-comic", "tv-special", "fight", "violence"])]
            [?n :name/name ?n.name]
            [(like ?n.name "%Downey%Robert%")]
            [?t :title/production-year ?t.production-year]
            [(< 2014 ?t.production-year)]
            [?mk :movie-keyword/keyword ?k]
            [?mk :movie-keyword/movie ?t]
            [?ci :cast-info/movie ?t]
            [?ci :cast-info/person ?n]
            [?t :title/title ?t.title]])

;; good plan
(def q-6c '[:find (min ?n.name) (min ?t.title)
            :where
            [?k :keyword/keyword "marvel-cinematic-universe"]
            [?n :name/name ?n.name]
            [(like ?n.name "%Downey%Robert%")]
            [?t :title/production-year ?t.production-year]
            [(< 2014 ?t.production-year)]
            [?mk :movie-keyword/keyword ?k]
            [?mk :movie-keyword/movie ?t]
            [?ci :cast-info/movie ?t]
            [?ci :cast-info/person ?n]
            [?t :title/title ?t.title]])

(def q-6d '[:find (min ?k.keyword) (min ?n.name) (min ?t.title)
            :where
            [?k :keyword/keyword ?k.keyword]
            [(in ?k.keyword ["superhero", "sequel", "second-part", "marvel-comics",
                             "based-on-comic", "tv-special", "fight", "violence"])]
            [?n :name/name ?n.name]
            [(like ?n.name "%Downey%Robert%")]
            [?t :title/production-year ?t.production-year]
            [(< 2000 ?t.production-year)]
            [?mk :movie-keyword/keyword ?k]
            [?mk :movie-keyword/movie ?t]
            [?ci :cast-info/movie ?t]
            [?ci :cast-info/person ?n]
            [?t :title/title ?t.title]])

;; good plan
(def q-6e '[:find (min ?n.name) (min ?t.title)
            :where
            [?k :keyword/keyword "marvel-cinematic-universe"]
            [?n :name/name ?n.name]
            [(like ?n.name "%Downey%Robert%")]
            [?t :title/production-year ?t.production-year]
            [(< 2000 ?t.production-year)]
            [?mk :movie-keyword/keyword ?k]
            [?mk :movie-keyword/movie ?t]
            [?ci :cast-info/movie ?t]
            [?ci :cast-info/person ?n]
            [?t :title/title ?t.title]])

;; good plan
(def q-6f '[:find (min ?k.keyword) (min ?n.name) (min ?t.title)
            :where
            [?k :keyword/keyword ?k.keyword]
            [(in ?k.keyword ["superhero", "sequel", "second-part", "marvel-comics",
                             "based-on-comic", "tv-special", "fight", "violence"])]
            [?n :name/name ?n.name]
            [?t :title/production-year ?t.production-year]
            [(< 2000 ?t.production-year)]
            [?mk :movie-keyword/keyword ?k]
            [?mk :movie-keyword/movie ?t]
            [?ci :cast-info/movie ?t]
            [?ci :cast-info/person ?n]
            [?t :title/title ?t.title]])

;; good plan
(def q-7a '[:find (min ?n.name) (min ?t.title)
            :where
            [?an :aka-name/name ?an.name]
            [(like ?an.name "%a%")]
            [?it :info-type/info "mini biography"]
            [?lt :link-type/link "features"]
            [?n :name/name-pcode-cf ?n.name-pcode-cf]
            [(< "A" ?n.name-pcode-cf "F")]
            [?n :name/gender ?n.gender]
            [?n :name/name ?n.name]
            [(or (= ?n.gender "m") (and (= ?n.gender "f") (like ?n.name "B%")))]
            [?pi :person-info/note "Volker Boehm"]
            [?t :title/production-year ?t.production-year]
            [(< 1980 ?t.production-year 1995)]
            [?an :aka-name/person ?n]
            [?pi :person-info/person ?n]
            [?ci :cast-info/person ?n]
            [?ci :cast-info/movie ?t]
            [?ml :movie-link/linked-movie ?t]
            [?ml :movie-link/link-type ?lt]
            [?pi :person-info/info-type ?it]
            [?t :title/title ?t.title]])

;; (d/explain {:run? true} q-7a (d/db conn))
;; => {:planning-time "62.520 ms", :actual-result-size 4, :execution-time "144.394 ms", :opt-clauses [[?an :aka-name/name ?an.name] [?it :info-type/info "mini biography"] [?lt :link-type/link "features"] [?n :name/name-pcode-cf ?n.name-pcode-cf] [?n :name/gender ?n.gender] [?n :name/name ?n.name] [?pi :person-info/note "Volker Boehm"] [?t :title/production-year ?t.production-year] [?an :aka-name/person ?n] [?pi :person-info/person ?n] [?ci :cast-info/person ?n] [?ci :cast-info/movie ?t] [?ml :movie-link/linked-movie ?t] [?ml :movie-link/link-type ?lt] [?pi :person-info/info-type ?it] [?t :title/title ?t.title] [(like ?an.name "%a%")] [(< "A" ?n.name-pcode-cf "F")] [(< 1980 ?t.production-year 1995)]], :query-graph {$ {?an {:links [{:type :ref, :tgt ?n, :attr :aka-name/person} {:type :val-eq, :tgt ?pi, :var ?n, :attrs {?an :aka-name/person, ?pi :person-info/person}} {:type :val-eq, :tgt ?ci, :var ?n, :attrs {?an :aka-name/person, ?ci :cast-info/person}}], :mpath [:free :aka-name/name], :mcount 180269, :free #:aka-name{:name {:var ?an.name, :count 180269, :pred [(like ?an.name "%a%")]}, :person {:var ?n, :count 180270}}}, ?it {:links [{:type :_ref, :tgt ?pi, :attr :person-info/info-type}], :mpath [:bound :info-type/info], :mcount 1, :bound #:info-type{:info {:val "mini biography", :count 1}}}, ?lt {:links [{:type :_ref, :tgt ?ml, :attr :movie-link/link-type}], :mpath [:bound :link-type/link], :mcount 1, :bound #:link-type{:link {:val "features", :count 1}}}, ?n {:links [{:type :_ref, :tgt ?an, :attr :aka-name/person} {:type :_ref, :tgt ?pi, :attr :person-info/person} {:type :_ref, :tgt ?ci, :attr :cast-info/person}], :mpath [:free :name/name-pcode-cf], :mcount 1103278, :free #:name{:name-pcode-cf {:var ?n.name-pcode-cf, :range [[[:open "A"] [:open "F"]]], :count 1103278}, :gender {:var ?n.gender, :count 2701134}, :name {:var ?n.name, :count 1103278}}}, ?pi #datalevin.query.Node{:links [{:type :ref, :tgt ?n, :attr :person-info/person} {:type :ref, :tgt ?it, :attr :person-info/info-type} {:type :val-eq, :tgt ?an, :var ?n, :attrs {?an :aka-name/person, ?pi :person-info/person}} {:type :val-eq, :tgt ?ci, :var ?n, :attrs {?pi :person-info/person, ?ci :cast-info/person}}], :mpath [:bound :person-info/note], :mcount 64, :bound #:person-info{:note {:val "Volker Boehm", :count 64}}, :free #:person-info{:person {:var ?n, :count 74}, :info-type {:var ?it, :count 75258}}}, ?t {:links [{:type :_ref, :tgt ?ci, :attr :cast-info/movie} {:type :_ref, :tgt ?ml, :attr :movie-link/linked-movie}], :mpath [:free :title/production-year], :mcount 340186, :free #:title{:production-year {:var ?t.production-year, :range [[[:open 1980] [:open 1995]]], :count 340186}, :title {:var ?t.title, :count 340202}}}, ?ci {:links [{:type :ref, :tgt ?n, :attr :cast-info/person} {:type :ref, :tgt ?t, :attr :cast-info/movie} {:type :val-eq, :tgt ?an, :var ?n, :attrs {?an :aka-name/person, ?ci :cast-info/person}} {:type :val-eq, :tgt ?pi, :var ?n, :attrs {?pi :person-info/person, ?ci :cast-info/person}} {:type :val-eq, :tgt ?ml, :var ?t, :attrs {?ci :cast-info/movie, ?ml :movie-link/linked-movie}}], :mpath [:free :cast-info/person], :mcount 36244344, :free #:cast-info{:person {:var ?n, :count 36244344}, :movie {:var ?t, :count 36244344}}}, ?ml {:links [{:type :ref, :tgt ?t, :attr :movie-link/linked-movie} {:type :ref, :tgt ?lt, :attr :movie-link/link-type} {:type :val-eq, :tgt ?ci, :var ?t, :attrs {?ci :cast-info/movie, ?ml :movie-link/linked-movie}}], :mpath [:free :movie-link/linked-movie], :mcount 29997, :free #:movie-link{:linked-movie {:var ?t, :count 29997}, :link-type {:var ?lt, :count 29997}}}}}, :plan {$ [(#datalevin.query.Plan{:steps ["Initialize [?it] by :info-type/info = mini biography."], :cost 1, :size 1, :actual-size 1} #datalevin.query.Plan{:steps ["Merge ?pi by scanning reverse reference of :person-info/info-type." "Merge [?n] by scanning [:person-info/note :person-info/person]."], :cost 22, :size 3, :actual-size 64} #datalevin.query.Plan{:steps ["Merge ?an by equal values of :aka-name/person." "Merge [?an.name] by scanning [:aka-name/name]."], :cost 26, :size 1, :actual-size 98} #datalevin.query.Plan{:steps ["Merge ?ci by equal values of :cast-info/person." "Merge [?t] by scanning [:cast-info/movie]."], :cost 44, :size 9, :actual-size 15021} #datalevin.query.Plan{:steps ["Merge ?ml by equal values of :movie-link/linked-movie." "Merge [?lt] by scanning [:movie-link/link-type]."], :cost 46, :size 1, :actual-size 1175} #datalevin.query.Plan{:steps ["Merge [?n.name ?n.gender ?n.name-pcode-cf] by scanning [:name/name :name/gender :name/name-pcode-cf]."], :cost 55, :size 1, :actual-size 663} #datalevin.query.Plan{:steps ["Merge [?t.title ?t.production-year] by scanning [:title/title :title/production-year]."], :cost 61, :size 1, :actual-size 79} #datalevin.query.Plan{:steps ["Merge [] by scanning [:link-type/link]."], :cost 64, :size 1, :actual-size 25})]}, :late-clauses ([(or (= ?n.gender "m") (and (= ?n.gender "f") (like ?n.name "B%")))]), :result (["Bancroft, Anne" "Love Potion No. 9"])}
