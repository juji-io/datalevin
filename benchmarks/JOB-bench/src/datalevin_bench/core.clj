(ns datalevin-bench.core
  (:require
   [datalevin.core :as d]
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

(defn- add-comp-cast-type [datoms]
  (reduce (fn [ds [id content]]
            (conj! ds (d/datom (+ comp-cast-type-base (Long/parseLong id))
                               :comp-cast-type/kind content)))
          datoms (d/read-csv (slurp "data/comp_cast_type.csv"))))

(defn- add-company-type [datoms]
  (reduce (fn [ds [id content]]
            (conj! ds (d/datom (+ company-type-base (Long/parseLong id))
                               :company-type/kind content)))
          datoms (d/read-csv (slurp  "data/company_type.csv"))))

(defn- add-kind-type [datoms]
  (reduce (fn [ds [id content]]
            (conj! ds (d/datom (+ kind-type-base (Long/parseLong id))
                               :kind-type/kind content)))
          datoms (d/read-csv (slurp "data/kind_type.csv"))))

(defn- add-link-type [datoms]
  (reduce (fn [ds [id content]]
            (conj! ds (d/datom (+ link-type-base (Long/parseLong id))
                               :link-type/link content)))
          datoms (d/read-csv (slurp "data/link_type.csv"))))

(defn- add-role-type [datoms]
  (reduce (fn [ds [id content]]
            (conj! ds (d/datom (+ role-type-base (Long/parseLong id))
                               :role-type/role content)))
          datoms (d/read-csv (slurp "data/role_type.csv"))))

(defn- add-info-type [datoms]
  (reduce (fn [ds [id content]]
            (conj! ds (d/datom (+ info-type-base (Long/parseLong id))
                               :info-type/info content)))
          datoms (d/read-csv (slurp "data/info_type.csv"))))

(defn- add-movie-link [datoms]
  (reduce
    (fn [ds [id movie linked-movie link-type]]
      (let [eid (+ movie-link-base (Long/parseLong id))]
        (-> ds
            (conj! (d/datom eid :movie-link/movie
                            (+ title-base (Long/parseLong movie))))
            (conj! (d/datom eid :movie-link/linked-movie
                            (+ title-base (Long/parseLong linked-movie))))
            (conj! (d/datom eid :movie-link/link-type
                            (+ link-type-base (Long/parseLong link-type)))))))
    datoms (d/read-csv (slurp "data/movie_link.csv"))))

(defn- add-aka-name [datoms]
  (reduce
    (fn [ds [id person name imdb-index name-pcode-cf
            name-pcode-nf surname-pcode]]
      (let [eid (+ aka-name-base (Long/parseLong id))]
        (cond-> (-> ds
                    (conj! (d/datom eid :aka-name/person
                                    (+ name-base (Long/parseLong person))))
                    (conj! (d/datom eid :aka-name/name name)))
          (not (s/blank? imdb-index))
          (conj! (d/datom eid :aka-name/imdb-index imdb-index))
          (not (s/blank? name-pcode-cf))
          (conj! (d/datom eid :aka-name/name-pcode-cf name-pcode-cf))
          (not (s/blank? name-pcode-nf))
          (conj! (d/datom eid :aka-name/name-pcode-nf name-pcode-nf))
          (not (s/blank? surname-pcode))
          (conj! (d/datom eid :aka-name/surname-pcode surname-pcode)))))
    datoms (d/read-csv (slurp "data/aka_name.csv"))))

(defn- add-aka-title [datoms]
  (reduce
    (fn [ds [id movie title imdb-index kind production-year phonetic-code
            episode-of season-nr episode-nr note]]
      (let [eid (+ aka-title-base (Long/parseLong id))]
        (cond-> (-> ds
                    (conj! (d/datom eid :aka-title/movie
                                    (+ title-base (Long/parseLong movie))))
                    (conj! (d/datom eid :aka-title/title title)))
          (not (s/blank? imdb-index))
          (conj! (d/datom eid :aka-title/imdb-index imdb-index))
          (not (s/blank? kind))
          (conj! (d/datom eid :aka-title/kind
                          (+ kind-type-base (Long/parseLong kind))))
          (not (s/blank? production-year))
          (conj! (d/datom eid :aka-title/production-year
                          (Long/parseLong production-year)))
          (not (s/blank? phonetic-code))
          (conj! (d/datom eid :aka-title/phonetic-code phonetic-code))
          (not (s/blank? episode-of))
          (conj! (d/datom eid :aka-title/episode-of
                          (+ title-base (Long/parseLong episode-of))))
          (not (s/blank? season-nr))
          (conj! (d/datom eid :aka-title/season-nr (Long/parseLong season-nr)))
          (not (s/blank? episode-nr))
          (conj! (d/datom eid :aka-title/episode-nr (Long/parseLong episode-nr)))
          (not (s/blank? note))
          (conj! (d/datom eid :aka-title/note note)))))
    datoms (d/read-csv (slurp "data/aka_title.csv"))))

(defn- add-company-name [datoms]
  (reduce
    (fn [ds [id name country-code imdb-id name-pcode-nf name-pcode-sf]]
      (let [eid (+ company-name-base (Long/parseLong id))]
        (cond-> (conj! ds (d/datom eid :company-name/name name))
          (not (s/blank? country-code))
          (conj! (d/datom eid :company-name/country-code country-code))
          (not (s/blank? imdb-id))
          (conj! (d/datom eid :company-name/imdb-id (Long/parseLong imdb-id)))
          (not (s/blank? name-pcode-nf))
          (conj! (d/datom eid :company-name/name-pcode-nf name-pcode-nf))
          (not (s/blank? name-pcode-sf))
          (conj! (d/datom eid :company-name/name-pcode-sf name-pcode-sf)))))
    datoms (d/read-csv (slurp "data/company_name.csv"))))

(defn- add-complete-cast [datoms]
  (reduce
    (fn [ds [id movie subject status]]
      (let [eid (+ complete-cast-base (Long/parseLong id))]
        (-> ds
            (conj! (d/datom eid :complete-cast/movie
                            (+ title-base (Long/parseLong movie))))
            (conj! (d/datom eid :complete-cast/subject
                            (+ comp-cast-type-base (Long/parseLong subject))))
            (conj! (d/datom eid :complete-cast/status
                            (+ comp-cast-type-base (Long/parseLong status)))))))
    datoms (d/read-csv (slurp "data/complete_cast.csv"))))

(defn- add-keyword [datoms]
  (reduce
    (fn [ds [id keyword phonetic-code]]
      (let [eid (+ keyword-base (Long/parseLong id))]
        (-> ds
            (conj! (d/datom eid :keyword/keyword keyword))
            (conj! (d/datom eid :keyword/phonetic-code phonetic-code)))))
    datoms (d/read-csv (slurp "data/keyword.csv"))))

(defn- add-char-name [datoms]
  (reduce
    (fn [ds [id name imdb-index imdb-id name-pcode-nf surname-pcode]]
      (let [eid (+ char-name-base (Long/parseLong id))]
        (cond-> (conj! ds (d/datom eid :char-name/name name))
          (not (s/blank? imdb-index))
          (conj! (d/datom eid :char-name/imdb-index imdb-index))
          (not (s/blank? imdb-id))
          (conj! (d/datom eid :char-name/imdb-id (Long/parseLong imdb-id)))
          (not (s/blank? name-pcode-nf))
          (conj! (d/datom eid :char-name/name-pcode-nf name-pcode-nf))
          (not (s/blank? surname-pcode))
          (conj! (d/datom eid :char-name/surname-pcode surname-pcode)))))
    datoms (d/read-csv (slurp "data/char_name.csv"))))

(persistent! (-> (transient [])
                 ;; add-comp-cast-type
                 ;; add-company-type
                 ;; add-kind-type
                 ;; add-link-type
                 ;; add-role-type
                 ;; add-info-type
                 ;; add-movie-link
                 ;; add-aka-name
                 ;; add-aka-title
                 ;; add-company-name
                 ;; add-complete-cast
                 ;; add-keyword
                 add-char-name
                 ))

(defn- add-movie-companies [ds]
  )

(defn- add-movie-info [ds]
  )

(defn- add-movie-info-idx [ds]
  )

(defn- add-movie-keyword [ds]
  )

(defn- add-name-base [ds]
  )

(defn- add-person-info [ds]
  )

(defn- add-title [ds]
  )

(defn- add-cast-info [ds]
  )

#_(def datoms (-> (transient [])
                  add-comp-cast-type
                  add-company-type
                  add-kind-type
                  add-link-type
                  add-role-type
                  add-info-type
                  add-movie-link
                  add-aka-name
                  add-aka-title
                  add-company-name
                  add-complete-cast
                  add-keyword
                  add-char-name
                  add-movie-companies
                  add-movie-info
                  add-movie-info-idx
                  add-movie-keyword
                  add-name-base
                  add-person-info
                  add-title
                  add-cast-info
                  persistent!))

;;(def db (d/init-db datoms "db" schema {:closed-schema? true}))
