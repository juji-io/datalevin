(ns datalevin.pprint
  (:require [datalevin.db :as db]
            [datalevin.datom :as dd]
            [clojure.pprint :as pp])
  (:import [datalevin.db DB FilteredDB]
           [datalevin.datom Datom]))

(defmethod pp/simple-dispatch Datom [^Datom d]
  (pp/pprint-logical-block :prefix "#datalevin/Datom [" :suffix "]"
                           (pp/write-out (.-e d))
                           (.write ^java.io.Writer *out* " ")
                           (pp/pprint-newline :linear)
                           (pp/write-out (.-a d))
                           (.write ^java.io.Writer *out* " ")
                           (pp/pprint-newline :linear)
                           (pp/write-out (.-v d))
                           (.write ^java.io.Writer *out* " ")
                           (pp/pprint-newline :linear)
                           (pp/write-out (dd/datom-tx d))
                           (.write ^java.io.Writer *out* " ")
                           (pp/pprint-newline :linear)
                           (pp/write-out (dd/datom-added d))))

(defn- pp-db [db ^java.io.Writer w]
  (pp/pprint-logical-block :prefix "#datalevin/DB {" :suffix "}"
                           (pp/pprint-logical-block
                            (pp/write-out :schema)
                            (.write w " ")
                            (pp/pprint-newline :linear)
                            (pp/write-out (db/-schema db)))
                           (.write w ", ")
                           (pp/pprint-newline :linear)

                           (pp/pprint-logical-block
                            (pp/write-out :datoms)
                            (.write w " ")
                            (pp/pprint-newline :linear)
                            (pp/pprint-logical-block :prefix "[" :suffix "]"
                                                     (pp/print-length-loop [aseq (seq db)]
                                                                           (when aseq
                                                                             (let [^Datom d (first aseq)]
                                                                               (pp/write-out [(.-e d) (.-a d) (.-v d) (dd/datom-tx d)])
                                                                               (when (next aseq)
                                                                                 (.write w " ")
                                                                                 (pp/pprint-newline :linear)
                                                                                 (recur (next aseq))))))))))

(defmethod pp/simple-dispatch DB [db] (pp-db db *out*))
(defmethod pp/simple-dispatch FilteredDB [db] (pp-db db *out*))
