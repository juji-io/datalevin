(ns ^:no-doc datalevin.datom
  (:require
   [taoensso.nippy :as nippy]
   [datalevin.constants :refer [tx0]]
   [datalevin.util :refer [combine-hashes combine-cmp defcomp opt-apply]])
  (:import
   [datalevin.utl BitOps]
   [java.util Arrays]
   [java.io DataInput DataOutput]))

(declare hash-datom equiv-datom seq-datom nth-datom assoc-datom
         empty-datom val-at-datom)

(defprotocol IDatom
  (datom-tx [this])
  (datom-added [this]))

(deftype Datom [^long e a v ^long tx ^:unsynchronized-mutable ^int _hash]
  IDatom
  (datom-tx [d] (if (pos? tx) tx (- tx)))
  (datom-added [d] (pos? tx))

  Object
  (hashCode [d]
    (if (zero? _hash)
      (let [h (int (hash-datom d))]
        (set! _hash h)
        h)
      _hash))
  (toString [d] (pr-str d))

  clojure.lang.IHashEq
  (hasheq [d] (.hashCode d))

  clojure.lang.Seqable
  (seq [d] (seq-datom d))

  clojure.lang.IPersistentCollection
  (equiv [d o] (and (instance? Datom o) (equiv-datom d o)))
  (empty [d] (empty-datom))
  (count [d] 5)
  (cons [d [k v]] (assoc-datom d k v))

  clojure.lang.Indexed
  (nth [this i] (nth-datom this i))
  (nth [this i not-found] (nth-datom this i not-found))

  clojure.lang.ILookup
  (valAt [d k] (val-at-datom d k nil))
  (valAt [d k nf] (val-at-datom d k nf))

  clojure.lang.Associative
  (entryAt [d k] (some->> (val-at-datom d k nil) (clojure.lang.MapEntry k)))
  (containsKey [_ k] (#{:e :a :v :tx :added} k))
  (assoc [d k v] (assoc-datom d k v)))

(defn ^Datom datom
  ([e a v] (Datom. e a v tx0 0))
  ([e a v tx] (Datom. e a v tx 0))
  ([e a v tx added] (Datom. e a v (if added tx (- ^long tx)) 0)))

(defn delete
  "create a datom that means deleting it"
  [^Datom d]
  (datom (.-e d) (.-a d) (.-v d) (.-tx d) false))

(defn datom? [x] (instance? Datom x))

(defn- hash-datom [^Datom d]
  (-> (hash (.-e d))
      (combine-hashes (hash (.-a d)))
      (combine-hashes (hash (.-v d)))))

(defn- equiv-datom [^Datom d ^Datom o]
  (and (== (.-e d) (.-e o))
       (= (.-a d) (.-a o))
       (= (.-v d) (.-v o))))

(defn- seq-datom [^Datom d]
  (list [:e (.-e d)] [:a (.-a d)] [:v (.-v d)] [:tx (datom-tx d)]
        [:added (datom-added d)]))

(defn- val-at-datom
  [^Datom d k not-found]
  (cond
    (keyword? k)
    (case k
      :e     (.-e d)
      :a     (.-a d)
      :v     (.-v d)
      :tx    (datom-tx d)
      :added (datom-added d)
      not-found)

    (string? k)
    (case k
      "e"     (.-e d)
      "a"     (.-a d)
      "v"     (.-v d)
      "tx"    (datom-tx d)
      "added" (datom-added d)
      not-found)

    :else
    not-found))

(defn- nth-datom
  ([^Datom d ^long i]
   (case i
     0 (.-e d)
     1 (.-a d)
     2 (.-v d)
     3 (datom-tx d)
     4 (datom-added d)
     (throw (IndexOutOfBoundsException.))))
  ([^Datom d ^long i not-found]
   (case i
     0 (.-e d)
     1 (.-a d)
     2 (.-v d)
     3 (datom-tx d)
     4 (datom-added d)
     not-found)))

(defn- ^Datom empty-datom [] (datom 0 nil nil 0 0))

(defn- ^Datom assoc-datom [^Datom d k v]
  (case k
    :e     (datom v (.-a d) (.-v d) (datom-tx d) (datom-added d))
    :a     (datom (.-e d) v (.-v d) (datom-tx d) (datom-added d))
    :v     (datom (.-e d) (.-a d) v (datom-tx d) (datom-added d))
    :tx    (datom (.-e d) (.-a d) (.-v d) v (datom-added d))
    :added (datom (.-e d) (.-a d) (.-v d) (datom-tx d) v)
    (throw (IllegalArgumentException. (str "invalid key for #datalevin/Datom: " k)))))

;; printing and reading

(defn datom-from-reader ^Datom [vec] (opt-apply datom vec))

(defmethod print-method Datom [^Datom d, ^java.io.Writer w]
  (.write w (str "#datalevin/Datom "))
  (binding [*out* w]
    (pr [(.-e d) (.-a d) (.-v d)])))

;; ----------------------------------------------------------------------------
;; datom cmp macros/funcs
;;

(defn nil-check-cmp-fn [cmp-fn]
  (fn nil-check-cmp [o1 o2]
    (if (nil? o1) 0
        (if (nil? o2) 0
            (cmp-fn o1 o2)))))

(defn compare-with-type [a b]
  (if (identical? (class a) (class b))
    (try
      (compare a b)
      (catch ClassCastException e
        (cond
          ;; using `compare` on colls throws when items at the same index of
          ;; the coll are not of the same type, so we use `=`. since `a` and
          ;; `b` are of identical type
          ;; TODO change this when needs arise
          (coll? a)  (if (= a b) 0 1)
          (bytes? a) (if (Arrays/equals ^bytes a ^bytes b)
                       0
                       (BitOps/compareBytes a b))
          :else      (throw e))))
    ;; TODO change this when needs arise
    -1))

(def nil-cmp (nil-check-cmp-fn compare))
(def nil-cmp-type (nil-check-cmp-fn compare-with-type))

(defmacro long-compare [x y] `(Long/compare ^long ~x ^long ~y))

(defcomp cmp-datoms-eavt [^Datom d1, ^Datom d2]
  (combine-cmp
    (long-compare (.-e d1) (.-e d2))
    (nil-cmp (.-a d1) (.-a d2))
    (nil-cmp-type (.-v d1) (.-v d2))
    (long-compare (datom-tx d1) (datom-tx d2))))

(defcomp cmp-datoms-avet [^Datom d1, ^Datom d2]
  (combine-cmp
    (nil-cmp (.-a d1) (.-a d2))
    (nil-cmp-type (.-v d1) (.-v d2))
    (long-compare (.-e d1) (.-e d2))
    (long-compare (datom-tx d1) (datom-tx d2))))

(defcomp cmp-datoms-vaet [^Datom d1, ^Datom d2]
  (combine-cmp
    (nil-cmp-type (.-v d1) (.-v d2))
    (nil-cmp (.-a d1) (.-a d2))
    (long-compare (.-e d1) (.-e d2))
    (long-compare (datom-tx d1) (datom-tx d2))))

(defn datom-e [^Datom d] (.-e d))

(defn datom-a [^Datom d] (.-a d))

(defn datom-v [^Datom d] (.-v d))

(defn datom-eav [^Datom d] [(.-e d) (.-a d) (.-v d)])

(nippy/extend-freeze Datom :datalevin/datom
                     [^Datom x ^DataOutput out]
                     (.writeLong out (.-e x))
                     (nippy/freeze-to-out! out (.-a x))
                     (nippy/freeze-to-out! out (.-v x))
                     (when-let [tx (.-tx x)]
                       (nippy/freeze-to-out! out tx)))

(nippy/extend-thaw :datalevin/datom
                   [^DataInput in]
                   (let [vs [(.readLong in)
                             (nippy/thaw-from-in! in)
                             (nippy/thaw-from-in! in)]
                         tx (nippy/thaw-from-in! in)]
                     (datom-from-reader (if tx
                                          (conj vs tx)
                                          vs))))
