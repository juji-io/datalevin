(ns ^:no-doc datalevin.lru)

(declare assoc-lru cleanup-lru)

#?(:cljs
   (deftype LRU [key-value gen-key key-gen gen limit target]
     IAssociative
     (-assoc [this k v] (assoc-lru this k v))
     (-contains-key? [_ k] (-contains-key? key-value k))
     ILookup
     (-lookup [_ k]    (-lookup key-value k nil))
     (-lookup [_ k nf] (-lookup key-value k nf))
     IPrintWithWriter
     (-pr-writer [_ writer opts]
       (-pr-writer key-value writer opts)))
   :clj
   (deftype LRU [^clojure.lang.Associative key-value gen-key key-gen gen limit
                 target]
     clojure.lang.ILookup
     (valAt [_ k]           (.valAt key-value k))
     (valAt [_ k not-found] (.valAt key-value k not-found))
     clojure.lang.Associative
     (containsKey [_ k] (.containsKey key-value k))
     (entryAt [_ k]     (.entryAt key-value k))
     (assoc [this k v]  (assoc-lru this k v))))

(defn assoc-lru [^LRU lru k v]
  (let [key-value (.-key-value lru)
        gen-key   (.-gen-key lru)
        key-gen   (.-key-gen lru)
        gen       (.-gen lru)
        limit     (.-limit lru)
        target    (.-target lru)]
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
              target)))))

(defn cleanup-lru [^LRU lru]
  (if (> (count (.-key-value lru)) ^long (.-limit lru))
    (let [key-value (.-key-value lru)
          gen-key   (.-gen-key lru)
          key-gen   (.-key-gen lru)
          gen       (.-gen lru)
          limit     (.-limit lru)
          target    (.-target lru)
          [g k]     (first gen-key)]
      (LRU. (dissoc key-value k)
            (dissoc gen-key g)
            (dissoc key-gen k)
            gen
            limit
            target))
    lru))

(defn lru [limit target]
  (LRU. {} (sorted-map) {} 0 limit target))

(defprotocol ICache
  (-get [this key compute-fn]))

(defn cache [limit target]
  (let [*impl (volatile! (lru limit target))]
    (reify ICache
      (-get [_ key compute-fn]
        (if-some [cached (get @*impl key nil)]
          cached
          (let [computed (compute-fn)]
            (vswap! *impl assoc key computed)
            computed))))))
