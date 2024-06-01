(ns ^:no-doc datalevin.csv
  "CSV reader and writer"
  (:require
   [clojure.string :as s])
  (:import
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.io Reader Writer StringReader]))

(defprotocol IReadCSV
  (read-csv* [input sep quo]))

(extend-protocol IReadCSV
  String
  (read-csv* [s sep quo]
    (read-csv* (StringReader. s) sep quo))

  Reader
  (read-csv* [^Reader reader sep quo]
    (let [res     (FastList.)
          add-rec (fn [rec ^StringBuilder sb]
                    (.add res (persistent! (conj! rec (.toString sb)))))]
      (loop [c   (.read reader)
             rec (transient [])
             sb  (StringBuilder.)
             nl? false
             q?  false
             e?  false]
        (if (== c -1)
          (if (zero? (.length sb)) res (do (add-rec rec sb) res))
          (let [cc (char c)
                nc (.read reader)]
            (condp == c
              sep (if q?
                    (recur nc rec (.append sb cc) false q? false)
                    (recur nc (conj! rec (.toString sb))
                           (StringBuilder.) false q? false))
              quo (if q?
                    (if e?
                      (recur nc rec (.append sb cc) false q? false)
                      (recur nc rec sb false false false))
                    (if (zero? (.length sb))
                      (recur nc rec sb false true false)
                      (recur nc rec (.append sb cc) false q? false)))
              (case cc
                \newline
                (if q?
                  (recur nc rec (.append sb cc) false q? false)
                  (if nl?
                    (recur nc rec sb false q? false)
                    (do (add-rec rec sb)
                        (recur nc (transient []) (StringBuilder.)
                               false q? false))))
                \return
                (if q?
                  (recur nc rec (.append sb cc) false q? false)
                  (if nl?
                    (recur nc rec sb false q? false)
                    (do (add-rec rec sb)
                        (recur nc (transient []) (StringBuilder.)
                               true q? false))))
                \\
                (if q?
                  (recur nc rec sb false q? true)
                  (recur nc rec (.append sb cc) false q? false))
                (recur nc rec (.append sb cc) false q? false)))))))))

(defn read-csv
  [input & options]
  (let [{:keys [separator quote]
         :or   {separator \, quote \"}} options]
    (read-csv* input (int separator) (int quote))))

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
