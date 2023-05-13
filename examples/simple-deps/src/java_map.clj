(ns java-map
  (:require [datalevin.core :as d]
            [datalevin.interpret :as i]))


(defn entry-set-iterator
  "Implemnts ^java.util.Iterator for use in entrySet implementation.
   
   https://docs.oracle.com/javase/8/docs/api/java/util/Iterator.html"
  [db dbi & _opts]
  (let [state (atom {})]
    (reify java.util.Iterator
      (hasNext [_this]
        (throw (UnsupportedOperationException. "Not implemented")))
      (next [_this]
        (throw (UnsupportedOperationException. "Not implemented")))
      (remove [_this]
        (throw (UnsupportedOperationException. "Not implemented"))))))


(defn entry-set-view
  [db dbi & opts]
  (reify java.util.Set
    (add [_this _e]
      ;; TODO: we could implement this but we wil break the Map contract
      ;; https://docs.oracle.com/javase/8/docs/api/java/util/HashMap.html#entrySet--
      (throw (UnsupportedOperationException. "Not implemented")))
    (addAll [_this _c]
      ;; TODO: we could implement this but we wil break the Map contract
      ;; https://docs.oracle.com/javase/8/docs/api/java/util/HashMap.html#entrySet--
      (throw (UnsupportedOperationException. "Not implemented")))
    (clear [_this]
      (d/clear-dbi db dbi))
    (contains [_this _o]
      (throw (UnsupportedOperationException. "Not implemented")))
    (containsAll [_this _c]
      (throw (UnsupportedOperationException. "Not implemented")))
    (isEmpty [_this]
      (= 0 (d/entries db dbi)))
    (iterator [_this]
      (entry-set-iterator db dbi opts))
    (remove [_this _o]
      (throw (UnsupportedOperationException. "Not implemented")))
    (removeAll [_this _c]
      (throw (UnsupportedOperationException. "Not implemented")))
    (retainAll [_this _c]
      (throw (UnsupportedOperationException. "Not implemented")))
    (size [_this]
      (d/entries db dbi))))


(defn ->map
  "Provide an implementation of java.util.Map interface backed by datalevin / LMDB.
   
   Implements java.lang.AutoCloseable and can participate in with-open. 
   However,successive open/close operations have a performance impact so be mindfull of this.
   
   IMPORTANT: map keys should not excede 510 bytes (LMDB limitation)

   `db-or-path` - instance of ^datalevin.binding.java.LMDB or a string path / datalevin URI.
   In case a path is provided, database is opened.
   Pass :auto-close-db option to auto close when participating in with-open. 

   `dbi` - name of sub-database to work on

   `opts` is an options map that can have the following keys
   * `db-opts` - datalevin options for opening LMDB database - pass to open-kv
   "
  (^java.util.Map [db-or-path ^:String dbi & {:keys [db-opts put-all-batch-size]
                                              :or {put-all-batch-size 1000}
                                              :as _opts}]
   (let [is-datalevin? (instance? datalevin.lmdb.ILMDB db-or-path)
         db (if is-datalevin? db-or-path (d/open-kv db-or-path db-opts))
        ;; do not close db when it's opened by client
         auto-close-db (if is-datalevin? false true)]
     (d/open-dbi db dbi)
     (reify
       java.lang.AutoCloseable
       (close [_this]
         (when auto-close-db
           (d/close-kv db)))

       java.util.Map

       (clear [_this]
         (d/clear-dbi db dbi))

       (containsKey [_this key]
         (let [kv (d/get-value db dbi key :data :data false)]
           (some? kv)))

       (containsValue [_this value]
         (let [pred (i/inter-fn [kv]
                                (let [v (d/read-buffer (d/v kv))]
                                  (= v value)))]
           (some? (d/get-some db dbi pred [:all]))))

       ;; https://docs.oracle.com/javase/8/docs/api/java/util/HashMap.html#entrySet--
       (entrySet [_this]
         (entry-set-view db dbi _opts))

       (get [_this key]
         (d/get-value db dbi key))

       (isEmpty [_this]
         (= 0 (d/entries db dbi)))

       (keySet [_this]
         (throw (UnsupportedOperationException. "Not implemented")))

       (put [_this key value]
         (d/transact-kv db [[:put dbi key value]]))

       (putAll [_this m]
         (when (nil? m)
           (throw (NullPointerException. "Collection is null.")))
         (let [entries (.entrySet m)
               parts (partition-all put-all-batch-size entries)
               entry->txn (fn entry->txn [e]
                            [:put dbi (.getKey e) (.getValue e)])]
           (doseq [p parts]
             ;; convert entries from p into transactions
             (let [txns (into [] (map entry->txn p))]
               ;; transact batch
               (d/transact-kv db txns)))))

       (remove [_this key]
         (let [kv (d/get-value db dbi key :data :data false)]
           (when (some? kv)
             (d/transact-kv db [[:del dbi key :data]])
             (nth kv 1))))

       (size [_this]
         (d/entries db dbi))

       (values [_this]
         (throw (UnsupportedOperationException. "Not implemented")))))))

(comment

  (def db (d/open-kv "/tmp/datalevin/map"))

  (d/open-dbi db "map-table")

  (instance? datalevin.lmdb.ILMDB db)

  (with-open [m (->map "/tmp/datalevin/map2" "a")]
    (println (.size m))
    (println (.clear m))
    (println (.size m))
    (println (.put m 1 "a"))
    (println (.containsValue m "a"))
    (println (.containsValue m "b"))
    (println (.size m)))

  (with-open [m (->map "/tmp/datalevin/map2" "a")]
    (println (.get m 1)))

  (let [pred (i/inter-fn [kv]
                         (let [v (d/read-buffer (d/v kv))]
                           (= v "Is the secret number")))]
    (d/get-some db "map-table" pred [:all]))

  (d/transact-kv
   db
   [[:put "map-table" :datalevin "Hello, world!"]
    [:put "map-table" 42 {:saying "So Long, and thanks for all the fish"
                          :source "The Hitchhiker's Guide to the Galaxy"}]
    [:put "map-table" #inst "1991-12-25" "USSR broke apart" :instant]
    [:put "map-table" #inst "1989-11-09" "The fall of the Berlin Wall" :instant]])

  (d/get-value db "map-table" 1 :data :data false)
  (d/get-value db "map-table" 36 :data :data false)

  (d/transact-kv db [[:del "map-table" 36 :data]])

  (let [my-map (->map db "map-table")]
    (println (.get my-map 42))
    (.put my-map 36 "Is the secret number")
    (.get my-map 36)
    ;;(println (.remove my-map 36))
    (println (.containsKey my-map 36))
    (println (.containsKey my-map 3))
    (println (.containsKey my-map 42)))

  (d/close-kv db)
  )