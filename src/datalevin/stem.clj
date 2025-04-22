;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.stem
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
