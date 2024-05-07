(ns java-map
  (:require [datalevin.core :as d]
            [datalevin.interpret :as i]
            [clojure.tools.logging :as log]
            [clojure.string :as str])
  (:import (java.util Map$Entry
                      Iterator)))


(defn entry-set-iterator
  "Implemnts ^java.util.Iterator for use in entrySet implementation.
   
   https://docs.oracle.com/javase/8/docs/api/java/util/Iterator.html"
  [db dbi & _opts]
  (let [state (atom {:current-key nil})]
    (reify Iterator
      (hasNext [_this]
        ;; TODO: Using an atom is not very performant.
        (let [k (:current-key @state)
              ;; _ (log/info "hasNext" k)
              ;; if v-typa and ignore-key? is true, then return true if 
              new-k (if (some? k)
                      (d/get-first db dbi [:greater-than k] :data :ignore)
                      ;; when current-key is nil use :all
                      (d/get-first db dbi [:all k] :data :ignore))
              ;; _ (log/info "hasNext new-k" new-k)
              ;; get first 
              new-k (get new-k 0)]
          (some? new-k)))
      (next [_this]
        (let [k (:current-key @state)
              ;; _ (log/info "next" k)
                 ;; if v-typa and ignore-key? is true, then return true if 
              kv (if (some? k)
                      (d/get-first db dbi [:greater-than k] :data :data)
                          ;; when current-key is nil use :all
                      (d/get-first db dbi [:all k] :data :data))
                  ;; get first 
              new-k (when kv (nth kv 0))]
          ;; (log/info "next new-k" new-k)
          (swap! state assoc :current-key new-k)
          ;; TODO: first and second are not performant
          (clojure.lang.MapEntry. (first kv) (second kv))))
      (remove [_this]
        (let [k (:current-key @state)]
          ;; (log/trace "Remove" k)
          (d/transact-kv db [:del dbi k :data]))))))

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
    (contains [_this entry]
      ;; (log/info "Contains entry" entry)
      (let [key (.getKey entry)
            val (.getValue entry)
            kv (d/get-value db dbi key :data :data)
            v (second kv)]
        (= val v)))
    (containsAll [_this c]
      ;; (log/info "containsAll" c)
      (some
       (fn [entry]
         (when-not (instance? Map$Entry entry)
           (throw (ClassCastException. "should be Map.Entry")))
         (let [k (.getKey entry)
               v (.getValue entry)
               kv (d/get-value db dbi k :data :data)]
           (= v kv)))
       c))
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


(defn key-set-iterator
  "Implemnts ^java.util.Iterator for use in keySet implementation.
   
   https://docs.oracle.com/javase/8/docs/api/java/util/Iterator.html"
  [db dbi & _opts]
  (let [state (atom {:current-key nil})]
    (reify Iterator
      (hasNext [_this]
        ;; TODO: Using an atom is not very performant.
        (let [k (:current-key @state)
              ;; _ (log/info "hasNext" k)
              ;; if v-typa and ignore-key? is true, then return true if 
              kv (if (some? k)
                      (d/get-first db dbi [:greater-than k] :data :ignore)
                      ;; when current-key is nil use :all
                      (d/get-first db dbi [:all k] :data :ignore))
              ;; _ (log/info "hasNext new-k" kv)
              ;; get first 
              new-k (get kv 0)]
          (some? new-k)))
      (next [_this]
        (let [k (:current-key @state)
              ;; _ (log/info "next" k)
                 ;; if v-typa and ignore-key? is true, then return true if 
              new-k (if (some? k)
                      (d/get-first db dbi [:greater-than k] :data :data)
                          ;; when current-key is nil use :all
                      (d/get-first db dbi [:all k] :data :data))
                  ;; get first 
              new-k (when new-k (nth new-k 0))]
          ;; (log/info "next new-k" new-k)
          (swap! state assoc :current-key new-k)
          new-k))
      (remove [_this]
        (let [k (:current-key @state)]
          ;; (log/trace "Remove" k)
          (d/transact-kv db [:del dbi k :data]))))))

(defn key-set-view
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
    (contains [_this key]
      ;; (log/info "Contains key" key)
      (let [kv (d/get-value db dbi key :data :data false)]
        (some? kv)))
    (containsAll [_this c]
      ;; (log/info "containsAll" c)
      (some
       (fn [k]
         (let [kv (d/get-value db dbi k :data :data false)]
           (some? kv)))
       c))
    (isEmpty [_this]
      (= 0 (d/entries db dbi)))
    (iterator [_this]
      (key-set-iterator db dbi opts))
    (remove [_this _o]
      (throw (UnsupportedOperationException. "Not implemented")))
    (removeAll [_this _c]
      (throw (UnsupportedOperationException. "Not implemented")))
    (retainAll [_this _c]
      (throw (UnsupportedOperationException. "Not implemented")))
    (size [_this]
      (d/entries db dbi))))

(defn map-values
  "Implements Collection view of map values 
   https://docs.oracle.com/javase/8/docs/api/java/util/Map.html#values--"
  [db dbi & opts]
  (reify java.util.Collection
    (^boolean containsAll [_this ^java.util.Collection c]
      (log/info "map-values containsAll" c)
      (some
       (fn [e]
         (let [pred (i/inter-fn [kv]
                                (let [v (d/read-buffer (d/v kv))]
                                  (= v e)))]
           (some? (d/get-some db dbi pred [:all]))))
       c))
    (^objects toArray [_this]
      (into-array (d/get-range db dbi [:all] :data :data true)))
    (^objects toArray [_this ^objects objects]
      ;; TODO: try to put values in provided objects array first
      (into-array (.componentType (.getClass objects))
                  (d/get-range db dbi [:all] :data :data true)))))


(defn entry-to-str
  "Entry as string key=val"
  [^java.util.Map$Entry entry]
  (let [k (.getKey entry)
        v (.getValue entry)]
    (str k "=" v)))

(defn hashMap
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

      ;;  https://docs.oracle.com/javase/8/docs/api/java/util/Map.html#computeIfAbsent-K-java.util.function.Function-
       (computeIfAbsent [_this key mapping-function]
         (log/info "computeIfAbsent" key mapping-function)
         (when-not (some? key)
           (throw (NullPointerException.)))
         (when-not (some? mapping-function)
           (throw (NullPointerException.)))
         (let [v (.apply mapping-function key)]
           (if (some? v)
             ;; v has a value so we try to add it if it's not present
             (let [kv (d/get-value db dbi key :data :data false)]
               (when-not (some? kv)
                 (d/transact-kv db [[:put dbi key v :data :data]])
                 v))
             ;; v was null so we return it and do nothing
             nil)))

       (computeIfPresent [_this key mapping-function]
         (log/info "computeIfPresent" key mapping-function)
         (when-not (some? key)
           (throw (NullPointerException.)))
         (when-not (some? mapping-function)
           (throw (NullPointerException.)))
         (let [kv (d/get-value db dbi key :data :data false)]
           (if (some? kv)
             ;; kv has value (present) so we compute the new value
             (let [old-v (nth kv 1)
                   v (.apply mapping-function key old-v)]
               (when (some? v)
                 ;; new value is present so we transact it
                 (d/transact-kv db [[:put dbi key v :data :data]])))
             ;; v was null so we return it and do nothing
             nil)))

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
         (key-set-view db dbi _opts))

       (put [_this key value]
         (log/info "Put" key "val:" value)
         (when (nil? key)
           (throw (NullPointerException. "Key is null.")))
         (let [v (d/get-value db dbi key)]
           (d/transact-kv db [[:put dbi key value]])
           v))

       (putAll [_this m]
         (when (nil? m)
           (throw (NullPointerException. "Collection is null.")))
         (let [entries (.entrySet m)
               parts (partition-all put-all-batch-size entries)
               entry->txn (fn entry->txn [e]
                            (let [k  (.getKey e)
                                  v (.getValue e)]
                              (when-not (some? k)
                                (throw (NullPointerException. "Null key")))
                              [:put dbi k v]))]
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

       ;; https://docs.oracle.com/javase/8/docs/api/java/util/Map.html#values--
       (values [_this]
         (map-values db dbi _opts))

       ;; https://docs.oracle.com/javase/8/docs/api/java/util/AbstractMap.html#toString--
       (toString [_this]
         (let [entries (.entrySet _this)
               str-entries (str/join "," (map entry-to-str entries))]
           (str "{" str-entries "}")))))))

(comment

  (def db (d/open-kv "/tmp/datalevin/map"))

  (d/open-dbi db "map-table")

  (instance? datalevin.lmdb.ILMDB db)

  (with-open [m (hashMap "/tmp/datalevin/map2" "a")]
    (println (.size m))
    (println (.clear m))
    (println (.size m))
    (println (.put m 1 "a"))
    (println (.containsValue m "a"))
    (println (.containsValue m "b"))
    (println (.size m)))

  (with-open [m (hashMap "/tmp/datalevin/map2" "a")]
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

  (let [my-map (hashMap db "map-table")]
    (println (.get my-map 42))
    (.put my-map 36 "Is the secret number")
    (.get my-map 36)
    ;;(println (.remove my-map 36))
    (println (.containsKey my-map 36))
    (println (.containsKey my-map 3))
    (println (.containsKey my-map 42)))


  (def db (d/open-kv "/netdava/DRE/DocSearch/workspace/projects/app-tasks/tmp/demographics-2023-05"))

  (d/list-dbis db)
  (d/open-dbi db "demographic-tags")
  (d/open-dbi db "semmed-tags")

  (d/open-dbi db "empty-db")

  ;; (d/get-first db "demographic-tags" [:greater-than nil] :string :data)

  (d/get-first db "demographic-tags" [:all] :string :data)

  (let [a (d/get-first db "demographic-tags"
                       [:all] :string :ignore)
        b (d/get-first db "demographic-tags"
                       [:greater-than nil] :string :ignore)]
    (println "a is" a)
    (println "b is" b))

  (d/get-first db "empty-db" [:all] :string :data)

  (d/get-first db "empty-db" [:greater-than nil] :string :ignore true)

  (d/get-first db "demographic-tags" [:all-back] :string :data)

  (with-open [range (d/range-seq db "demographic-tags" [:all] :string :ignore)]
    (doseq [item (take 10 range)]
      (println item)))

  (d/close-kv db)




  )