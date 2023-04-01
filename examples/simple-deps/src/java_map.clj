(ns java-map
  (:require [datalevin.core :as d]
            [datalevin.interpret :as i]))


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
  [db-or-path ^:String dbi & {:keys [db-opts]
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
      
      (entrySet [_this]
        (throw (ex-info "Unimplemented" {})))
      
      (get [_this key]
        (d/get-value db dbi key))
      
      (isEmpty [_this]
        (= 0 (d/entries db dbi)))
      
      (keySet [_this]
        (throw (ex-info "Unimplemented" {})))
      
      (put [_this key value]
        (d/transact-kv db [[:put dbi key value]]))
      
      (remove [_this key]
        (let [kv (d/get-value db dbi key :data :data false)]
          (when (some? kv)
            (d/transact-kv db [[:del dbi key :data]])
            (nth kv 1))))
      
      (size [_this]
        (d/entries db dbi))
      
      (values [_this]
        (throw (ex-info "Unimplemented" {}))))))

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