(ns ^:no-doc datalevin.csv
  "CSV reader and writer"
  (:require
   [clojure.string :as s])
  (:import
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.io Reader Writer StringReader]))

(def ^:const lf  \newline)
(def ^:const cr  \return)
(def ^:const esc \\)
(def ^:const eof -1)

(defprotocol IReadCSV
  (read-csv* [input sep quo cb-size]))

(extend-protocol IReadCSV
  String
  (read-csv* [s sep quo cb-size]
    (read-csv* (StringReader. s) sep quo cb-size))

  Reader
  (read-csv* [^Reader reader sep quo cb-size]
    (let [cb      (char-array cb-size)
          res     (FastList.)
          add-rec (fn [rec ^StringBuilder sb]
                    (.add res (persistent! (conj! rec (.toString sb)))))]
      (loop [len 0
             i   0
             rec (transient [])
             sb  (StringBuilder.)
             nl? false
             q?  false
             e?  false]
        (if (zero? len)
          (let [readn (.read reader cb 0 cb-size)]
            (if (= readn eof)
              (if (zero? (.length sb)) res (do (add-rec rec sb) res))
              (recur readn 0 rec sb nl? q? e?)))
          (if (< i len)
            (let [c (aget cb i)]
              (condp = c
                sep (if q?
                      (recur len (inc i) rec (.append sb c) false q? false)
                      (recur len (inc i) (conj! rec (.toString sb))
                             (StringBuilder.) false q? false))
                lf  (if q?
                      (recur len (inc i) rec (.append sb c) false q? false)
                      (if nl?
                        (recur len (inc i) rec sb false q? false)
                        (do (add-rec rec sb)
                            (recur len (inc i) (transient []) (StringBuilder.)
                                   false q? false))))
                cr  (if q?
                      (recur len (inc i) rec (.append sb c) false q? false)
                      (if nl?
                        (recur len (inc i) rec sb false q? false)
                        (do (add-rec rec sb)
                            (recur len (inc i) (transient []) (StringBuilder.)
                                   true q? false))))
                quo (if q?
                      (if e?
                        (recur len (inc i) rec (.append sb c) false q? false)
                        (recur len (inc i) rec sb false false false))
                      (if (zero? (.length sb))
                        (recur len (inc i) rec sb false true false)
                        (recur len (inc i) rec (.append sb c) false q? false)))
                esc (if q?
                      (recur len (inc i) rec sb false q? true)
                      (recur len (inc i) rec (.append sb c) false q? false))
                (recur len (inc i) rec (.append sb c) false q? false)))
            (recur 0 0 rec sb nl? q? e?)))))))

(defn read-csv
  [input & options]
  (let [{:keys [separator quote cb-size]
         :or   {separator \, quote \" cb-size 4096}} options]
    (read-csv* input separator quote cb-size)))

;; minor modification from clojure.data.csv, included for convenience

;; Copyright (c) Jonas Enlund. All rights reserved.  The use and
;; distribution terms for this software are covered by the Eclipse
;; Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this
;; distribution.  By using this software in any fashion, you are
;; agreeing to be bound by the terms of this license.  You must not
;; remove this notice, or any other, from this software.

(defn- write-cell [^Writer writer obj quote quote?]
  (let [string     (str obj)
	      must-quote (quote? string)]
    (when must-quote (.write writer (int quote)))
    (.write writer (if must-quote
		                 (s/escape string {quote (str quote quote)})
		                 string))
    (when must-quote (.write writer (int quote)))))

(defn- write-record [^Writer writer record sep quote quote?]
  (loop [record record]
    (when-first [cell record]
      (write-cell writer cell quote quote?)
      (when-let [more (next record)]
	      (.write writer (int sep))
	      (recur more)))))

(defn- write-csv*
  [^Writer writer records sep quote quote? ^String newline]
  (loop [records records]
    (when-first [record records]
      (write-record writer record sep quote quote?)
      (.write writer newline)
      (recur (next records)))))

(defn write-csv
  [writer data & options]
  (let [opts      (apply hash-map options)
        separator (or (:separator opts) \,)
        quote     (or (:quote opts) \")
        quote?    (or (:quote? opts)
                      (let [should-quote #{separator quote \return \newline}]
                        #(some should-quote %)))
        newline   (or (:newline opts) :lf)]
    (write-csv* writer
		            data
		            separator
		            quote
                quote?
		            ({:lf "\n" :cr+lf "\r\n"} newline))))
