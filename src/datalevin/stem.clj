(ns datalevin.stem
  "Snowball stemmers"
  (:require
   [clojure.string :as str]))

(defonce stemmers (atom {}))

(defn get-stemmer
  "Return a stemmer given the language"
  [^String language]
  (let [language (str/lower-case language)]
    (or (@stemmers language)
        (let [stemmer (.newInstance
                        (Class/forName (str "org.tartarus.snowball.ext."
                                            language "Stemmer")))]
          (swap! stemmers assoc language stemmer)
          stemmer))))
