(ns ^:no-doc datalevin.search.query
  "Normalize fuzzy/prefix leaves while preserving original query term groups."
  (:require [clojure.string :as str]
            [datalevin.util :refer [raise]]))

(def default-fuzzy-options
  {:max-edits 1 :transpositions? true :max-expansions 50})

(defn fuzzy-options
  [value]
  (when (and value (not (false? value)))
    (when-not (or (true? value) (map? value))
      (raise ":fuzzy must be a boolean or an option map" {:fuzzy value}))
    (when (map? value)
      (let [allowed (set (keys default-fuzzy-options))
            unknown (into [] (sort-by pr-str (remove allowed (keys value))))]
        (when (seq unknown)
          (raise "Unknown fuzzy option keys: " unknown
                 ". Allowed keys: " (vec (keys default-fuzzy-options))
                 {:fuzzy value :unknown-keys unknown
                  :allowed-keys (vec (keys default-fuzzy-options))}))))
    (let [{:keys [max-edits transpositions? max-expansions] :as opts}
          (merge default-fuzzy-options (when (map? value) value))]
      (when-not (and (integer? max-edits) (<= 0 max-edits 2))
        (raise ":max-edits must be an integer between 0 and 2" opts))
      (when-not (boolean? transpositions?)
        (raise ":transpositions? must be a boolean" opts))
      (when-not (and (pos-int? max-expansions) (<= max-expansions 100000))
        (raise ":max-expansions must be an integer between 1 and 100000" opts))
      (select-keys opts [:max-edits :transpositions? :max-expansions]))))

(defn expanded?
  [query fuzzy prefix]
  (or (boolean fuzzy)
      (boolean prefix)
      (cond
        (map? query) (or (contains? query :fuzzy) (contains? query :prefix))
        (vector? query) (boolean (some #(expanded? % nil nil) (rest query)))
        :else false)))

(defn- leaf-text
  [query kind]
  (let [value (get query kind)]
    (when-not (string? value)
      (raise "Invalid " kind " search leaf: expected a string value"
             {:query query :leaf kind :value value}))
    value))

(defn parse-groups
  "Parse a phrase-normalized expression. Explicit :term leaves and protected
  phrase tokens remain exact even when query-wide fuzzy defaults are enabled."
  [analyzer query fuzzy prefix]
  (let [defaults (fuzzy-options fuzzy)
        groups   (volatile! (array-map))]
    (when-not (or (nil? prefix) (boolean? prefix))
      (raise ":prefix must be a boolean" {:prefix prefix}))
    (when (and defaults prefix)
      (raise "Use explicit leaves to combine fuzzy and prefix matching"
             {:fuzzy fuzzy :prefix prefix}))
    (letfn [(term [mode token opts]
              (let [spec (merge {:mode mode :term token} opts)
                    key  (str "\u0000" (pr-str spec))]
                (vswap! groups assoc key spec)
                key))
            (text [mode value opts]
              (when-not (string? value)
                (raise "Search term must be a string" {:term value}))
              (let [tokens (into [] (comp (map first) (remove str/blank?))
                                 (when-not (str/blank? value) (analyzer value)))
                    leaves (mapv #(term mode % opts) tokens)]
                (case (count leaves)
                  0 nil
                  1 (first leaves)
                  (into [:or] leaves))))
            (parse [q]
              (cond
                (string? q) (text (cond defaults :fuzzy, prefix :prefix, :else :exact)
                                 q defaults)
                (keyword? q) q
                (map? q)
                (cond
                  (contains? q ::exact-token) (term :exact (::exact-token q) nil)
                  (not= 1 (count (select-keys q [:term :fuzzy :prefix])))
                  (raise "Invalid search query" {:map q})
                  (contains? q :term) (text :exact (:term q) nil)
                  (contains? q :prefix) (text :prefix (leaf-text q :prefix) nil)
                  :else (text :fuzzy (leaf-text q :fuzzy)
                              (fuzzy-options
                                (merge defaults (dissoc q :fuzzy)))))
                (vector? q)
                (let [[op & args] q]
                  (when-not (and (#{:and :or :not} op) (seq args))
                    (raise "Invalid search query" {:op op}))
                  (let [children (into [] (keep parse) args)]
                    (when (seq children) (into [op] children))))
                :else (raise "Invalid search query" {:query q})))]
      (when-let [parsed (parse query)]
        {:query parsed :matching-groups @groups}))))
