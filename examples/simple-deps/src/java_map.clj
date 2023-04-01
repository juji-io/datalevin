(ns java-map
  (:require [datalevin.core :as d]))


(defn ->map 
  "Provide an implementation of java.util.Map interface backed by datalevin / LMDB."
  [db table]
  (d/open-dbi db table)
  (reify java.util.Map
    
    (clear [_this]
      (throw (ex-info "Unimplemented" {})))
    
    (containsKey [_this key]
      (let [kv (d/get-value db table key :data :data false)]
        (some? kv)))
    
    (containsValue [_this value]
      (throw (ex-info "Unimplemented" {})))
    
    (entrySet [_this]
      (throw (ex-info "Unimplemented" {})))
    
    (get [_this key]
      (d/get-value db table key))
    
    (isEmpty [_this]
      (throw (ex-info "Unimplemented" {})))
    
    (keySet [_this]
      (throw (ex-info "Unimplemented" {})))
    
    (put [_this key value]
      (d/transact-kv db [[:put table key value]]))
    
    (remove [_this key]
      (let [kv (d/get-value db table key :data :data false)]
        (when (some? kv)
          (d/transact-kv db [[:del table key :data]])
          (nth kv 1))))
    
    (size [_this]
      (throw (ex-info "Unimplemented" {})))
    
    (values [_this]
      (throw (ex-info "Unimplemented" {})))))

(comment

  (def db (d/open-kv "/tmp/datalevin/map"))

  (d/open-dbi db "map-table")

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
    (println (.remove my-map 36))
    (println (.containsKey my-map 36))
    (println (.containsKey my-map 3))
    (println (.containsKey my-map 42)))

  (d/close-kv db)
  )