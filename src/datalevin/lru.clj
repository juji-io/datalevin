(ns ^:no-doc datalevin.lru
  "LRU cache"
  (:import [clojure.lang Associative]))

(declare assoc-lru dissoc-lru cleanup-lru)

(deftype LRU [^Associative key-value gen-key key-gen gen limit target]
  clojure.lang.ILookup
  (valAt [_ k]           (.valAt key-value k))
  (valAt [_ k not-found] (.valAt key-value k not-found))

  clojure.lang.Associative
  (containsKey [_ k] (.containsKey key-value k))
  (entryAt [_ k]     (.entryAt key-value k))
  (assoc [this k v]  (assoc-lru this k v))

  clojure.lang.IPersistentMap
  (without [this k] (dissoc-lru this k)))

(defn- assoc-lru [^LRU lru k v]
  (let [key-value (.-key-value lru)
        gen-key   (.-gen-key lru)
        key-gen   (.-key-gen lru)
        gen       (.-gen lru)
        limit     (.-limit lru)
        target    (.-target lru)]
    (if (zero? ^long limit)
      lru
      (if-let [g (key-gen k nil)]
        (LRU. key-value
              (-> gen-key
                  (dissoc g)
                  (assoc gen k))
              (assoc key-gen k gen)
              (inc ^long gen)
              limit
              target)
        (cleanup-lru
          (LRU. (assoc key-value k v)
                (assoc gen-key gen k)
                (assoc key-gen k gen)
                (inc ^long gen)
                limit
                target))))))

(defn- cleanup-lru [^LRU lru]
  (if (> (count (.-key-value lru)) ^long (.-limit lru))
    (let [key-value (.-key-value lru)
          gen-key   (.-gen-key lru)
          key-gen   (.-key-gen lru)
          [g k]     (first gen-key)]
      (LRU. (dissoc key-value k)
            (dissoc gen-key g)
            (dissoc key-gen k)
            (.-gen lru)
            (.-limit lru)
            (.-target lru)))
    lru))

(defn- dissoc-lru [^LRU lru k]
  (let [key-value (.-key-value lru)
        gen-key   (.-gen-key lru)
        key-gen   (.-key-gen lru)
        g         (key-gen k)]
    (LRU. (dissoc key-value k)
          (dissoc gen-key g)
          (dissoc key-gen k)
          (.-gen lru)
          (.-limit lru)
          (.-target lru))))

(defn lru [limit target]
  (LRU. {} (sorted-map) {} 0 limit target))

(defprotocol ICache
  (-get [this key compute-fn])
  (-del [this key] "invalidate a key"))

(defn cache [limit target]
  (let [*impl (volatile! (lru limit target))]
    (reify ICache
      (-get [_ key compute-fn]
        (if-some [cached (get @*impl key nil)]
          (do
            (vswap! *impl assoc key cached)
            cached)
          (let [computed (compute-fn)]
            (vswap! *impl assoc key computed)
            computed)))
      (-del [this key]
        (vswap! *impl dissoc key)
        this))))
