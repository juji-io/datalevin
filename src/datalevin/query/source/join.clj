(ns ^:no-doc datalevin.query.source.join
  "Quota-bound hash indexes over retained query-source join tables."
  (:require [datalevin.query.source :as source]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value]
            [datalevin.relation :as relation])
  (:import [java.util ArrayList]
           [datalevin.utl QueryKey$KeyMap]))

(defn- fill-key! [^objects key variables row]
  (dotimes [i (alength key)]
    (aset key i (get row (nth variables i))))
  key)

(defn- clear-key! [^objects key]
  (dotimes [i (alength key)] (aset key i nil)))

(defn- index! [runtime variables rows]
  (let [control (:control runtime)
        width (count variables)
        ;; Rows and their values are already charged. Reserve index containers,
        ;; key arrays/wrappers, bucket references, and hash-table growth before
        ;; allocating. Charging every row as a distinct bucket also covers
        ;; transient duplicate keys and resizing. No logical values are copied.
        token (source/reserve! control
                {:retained-bytes
                 (value/add-charge (+ 1024 (* 256 width))
                   (value/multiply-charge (count rows)
                     (+ 512 (* 8 width))))})
        index (QueryKey$KeyMap.)
        scratch (object-array width)
        lookup (relation/array-lookup)
        close (rt/own! runtime #(do (.clear index)
                                   (clear-key! scratch)
                                   (source/release! token)))
        single? (= 1 width)
        variable (first variables)]
    (try
      (doseq [row rows]
        (source/check-active! control)
        (let [key (if single? (get row variable)
                      (relation/wrap-array (fill-key! (object-array width) variables row)))
              ^ArrayList bucket (or (.get index key)
                                    (let [bucket (ArrayList.)]
                                      (.put index key bucket)
                                      bucket))]
          (.add bucket row)))
      {:index index
       :lookup (if single?
                 (fn [row] (.get index (get row variable)))
                 (fn [row]
                   (try
                     (.get index (relation/reset-array-lookup! lookup
                                   (fill-key! scratch variables row)))
                     ;; A probe must not retain a streamed value after its batch
                     ;; reservation has been released, even when hashing fails.
                     (finally (clear-key! scratch)))))}
      (catch Throwable e (close) (throw e)))))

(defn prepare!
  "Build each shared-variable index once, on the first streamed row. All tables
   have uniform binding keys, established by input and pattern validation. A
   table with no shared variables remains a cross product. Preserve table and
   bucket order, including duplicate rows."
  [runtime initial-row tables]
  (let [control (:control runtime)
        columns (reduce (fn [n rows] (value/add-charge (count (first rows)) n))
                        (count initial-row) tables)
        token (source/reserve! control
                {:retained-bytes (value/add-charge (+ 1024 (* 256 (count tables)))
                                   (value/multiply-charge 256 columns))})]
    (rt/own! runtime #(source/release! token))
    (loop [bound (set (keys initial-row)), tables (seq tables), steps []]
      (source/check-active! control)
      (if-let [rows (first tables)]
        (let [variables (keys (first rows))
              shared (filterv bound variables)
              step (if (seq shared) (index! runtime shared rows) {:rows rows})]
          (recur (into bound variables) (next tables) (conj steps step)))
        steps))))

(defn candidates
  "Return the matching bucket, or all rows for a cross product."
  [step row]
  (if-let [lookup (:lookup step)] (lookup row) (:rows step)))
