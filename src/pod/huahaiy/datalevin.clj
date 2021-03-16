(ns pod.huahaiy.datalevin
  (:refer-clojure :exclude [read read-string])
  (:require [bencode.core :as bencode]
            [datalevin.core :as d]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.walk :as w])
  (:import [java.io PushbackInputStream]
           [java.nio.charset StandardCharsets]
           [java.util UUID])
  (:gen-class))

(def pod-ns "pod.huahaiy.datalevin")

(def debug? true)
(defn debug [& args]
  (when debug?
    (binding [*out* (io/writer "/tmp/debug.log" :append true)]
      (apply println args))))

(def stdin (PushbackInputStream. System/in))

(defn write [v]
  (bencode/write-bencode System/out v)
  (.flush System/out))

(defn read-string [^bytes v]
  (String. v StandardCharsets/UTF_8))

(defn read []
  (bencode/read-bencode stdin))

;; uuid -> conn
(defonce dl-conns (atom {}))

;; uuid -> conn
(defonce dl-dbs (atom {}))

(defonce kv-dbs (atom {}))

(defn- get-conn
  ([dir]
   (get-conn dir nil))
  ([dir schema]
   (let [conn (d/get-conn dir schema)
         id   (UUID/randomUUID)]
     (swap! dl-conns assoc id conn)
     {::conn id})))

(defn- close [{:keys [::conn]}]
  (let [[old _] (swap-vals! dl-conns dissoc conn)]
    (when-let [c (get old conn)]
      (d/close c))))

(defn- transact!
  ([conn tx-data]
   (transact! conn tx-data nil))
  ([{:keys [::conn]} tx-data tx-meta]
   (when-let [c (get @dl-conns conn)]
     (d/transact! c tx-data tx-meta))))

(defn- db [{:keys [::conn]}]
  (when-let [c (get @dl-conns conn)]
    (let [d  (d/db c)
          id (UUID/randomUUID)]
      (swap! dl-dbs assoc id d)
      {::db id})))

(defn- q [q & inputs]
  (apply d/q q (w/postwalk #(if-let [db (::db %)] (get @dl-dbs db) %)
                           inputs)))

(def exposed-vars
  {'get-conn  get-conn
   'close     close
   'transact! transact!
   'db        db
   'q         q})

(def lookup
  (zipmap (map (fn [sym] (symbol pod-ns (name sym))) (keys exposed-vars))
          (vals exposed-vars)))

(defn run []
  (loop []
    (let [message (try (read)
                       (catch java.io.EOFException _
                         ::EOF))]
      (when-not (identical? ::EOF message)
        (let [op (-> message (get "op") read-string keyword)
              id (or (some-> message (get "id") read-string) "unknown")]
          (case op
            :describe (do (write {"format"     "edn"
                                  "namespaces" [{"name" "pod.huahaiy.datalevin"
                                                 "vars"
                                                 (mapv (fn [k] {"name" (name k)})
                                                       (keys exposed-vars))}]
                                  "id"         id})
                          (recur))
            :invoke   (do (try
                            (let [var  (-> (get message "var")
                                           read-string
                                           symbol)
                                  args (get message "args")
                                  args (read-string args)
                                  args (edn/read-string args)]
                              (if-let [f (lookup var)]
                                (let [value (pr-str (apply f args))
                                      reply {"value"  value
                                             "id"     id
                                             "status" ["done"]}]
                                  (write reply))
                                (throw (ex-info (str "Var not found: " var) {}))))
                            (catch Throwable e
                              (binding [*out* *err*]
                                (println e))
                              (let [reply {"ex-message" (.getMessage e)
                                           "ex-data"    (pr-str
                                                          (assoc (ex-data e)
                                                                 :type (class e)))
                                           "id"         id
                                           "status"     ["done" "error"]}]
                                (write reply))))
                          (recur))
            (do
              (write {"err" (str "unknown op:" (name op))})
              (recur))))))))
