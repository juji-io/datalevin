;; adopted from https://gist.github.com/zachcp/57e871dcb0869937b86c83a710cc496a
;; thx @zachcp

(ns bulk-load.core
  (:require [datalevin.core :as d]
            [medley.core :as medley]
            [clojure.pprint :as pp]
            [clojure.data.json :as json]))

(def np-schema
  {:smiles      {:db/valueType :db.type/string}
   :exact_mass  {:db/valueType :db.type/double}
   :npaid       {:db/valueType :db.type/string}
   :m_plus_na   {:db/valueType :db.type/double}
   :m_plus_h    {:db/valueType :db.type/double}
   :mol_formula {:db/valueType :db.type/string}

   :name     {:db/valueType :db.type/string}
   :id       {:db/valueType :db.type/long}
   :species  {:db/valueType :db.type/string}
   :genus    {:db/valueType :db.type/string}
   :inchi    {:db/valueType :db.type/string}
   :inchikey {:db/valueType :db.type/string}

   ;; :external_ids flattened to the following
   :mibig-id {:db/valueType :db.type/string}
   :gnps-id  {:db/valueType :db.type/string}})

;; run download.sh to get the file first
(defn- read-np-atlas []
  (json/read-str (slurp "NPAtlas_download.json") :key-fn keyword))

(time (def full-np-atlas (read-np-atlas)))
;; "Elapsed time: 11145.893888 msecs"

(defn- remove-if-empty [coll ky]
  " if {k []} then {}"
  (medley/remove-kv  (fn [k v] (and (= k ky) (empty? v)))  coll))

(defn- convert-external-id-map [m]
  {:pre  [#(contains? m :external_db_name)
          #(contains? m :external_db_code)
          #(= 2 (count m))]
   :post [#(string? (val %))]}

  (let [db     (get m :external_db_name)
        val    (get m :external_db_code)
        new-id (keyword (str db "-id"))]
    {new-id val}))

(defn- external-ids-to-map [coll]
  " [{:id1 1} {:id2 2}] =>  {:id1 1 :id2 2}"
  (reduce merge  {}  (map convert-external-id-map coll)))

(defn- flatten-external-ids [m]
  (if-let [ex-ids (get m :external_ids)]
    (merge (dissoc m :external_ids) (external-ids-to-map ex-ids))
    m))

(defn- txn-map [entry]
  (-> entry
      (remove-if-empty :external_ids)
      (flatten-external-ids)
      (dissoc :origin_organism :origin_reference
              :reassignments :syntheses :node_id :cluster_id)
      (#(into {} (filter val %)))))

(defn- one-by-one
  "transact maps one by one"
  []
  (let [conn (d/get-conn "/tmp/one-by-one" np-schema)]
    (doseq [entry full-np-atlas]
      (d/transact! conn [(txn-map entry)]))
    (d/close conn)))

(defn- par-one-by-one
  "transact maps one by one"
  []
  (let [conn (d/get-conn "/tmp/par-one-by-one" np-schema)]
    (dorun (pmap #(d/transact! conn [(txn-map %)]) full-np-atlas))
    (d/close conn)))

(defn- all-at-once
  "transact all maps at once"
  []
  (let [conn (d/get-conn "/tmp/all-at-once" np-schema)]
    (d/transact! conn (map txn-map full-np-atlas))
    (d/close conn)))

(defn- with-txn-one-by-one
  "transact maps one by one, but within a single transaction"
  []
  (let [conn (d/get-conn "/tmp/single-one-by-one" np-schema)]
    (d/with-transaction [cn conn]
      (doseq [entry full-np-atlas]
        (d/transact! cn [(txn-map entry)])))
    (d/close conn)))

(defn- with-txn-all-at-once
  "transact all maps at once in explicit transaction"
  []
  (let [conn (d/get-conn "/tmp/single-all-at-once" np-schema)]
    (d/with-transaction [cn conn]
      (d/transact! cn (map txn-map full-np-atlas)))
    (d/close conn)))

(defn run []
  (println "one-by-one" (with-out-str (time (one-by-one))))
  (println "par-one-by-one" (with-out-str (time (par-one-by-one))))
  (println "all-at-once" (with-out-str (time (all-at-once))))
  (println "single-txn-one-by-one" (with-out-str (time (with-txn-one-by-one))))
  (println "single-txn-all-at-once" (with-out-str (time (with-txn-all-at-once)))))

(comment

  (run)

  ;; one-by-one "Elapsed time: 14177.199047 msecs"
  ;; total 422M

  ;; par-one-by-one "Elapsed time: 38765.244004 msecs"
  ;; total 423M

  ;; all-at-once "Elapsed time: 13409.082213 msecs"
  ;; total 474M

  ;; single-txn-one-by-one "Elapsed time: 14119.024628 msecs"
  ;; total 422M

  ;; single-txn-all-at-once "Elapsed time: 21659.052822 msecs"
  ;; total 422M

  )
