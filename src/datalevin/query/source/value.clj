(ns ^:no-doc datalevin.query.source.value
  "Portable owned values and conservative F0 accounting. No storage dependency."
  (:import [java.math BigDecimal BigInteger]
           [java.util Date UUID]))

(def scalar-types
  #{:boolean :long :bigint :double :float :bigdec :string :keyword :symbol
    :uuid :instant :bytes :data})

(defn fail!
  ([category phase] (fail! category phase {}))
  ([category phase context]
   ;; All callers supply structural context, never provider values or causes.
   (throw (ex-info "Query-source contract violation"
                   (merge {:error category :phase phase} context)))))

(defn plain-map? [x] (and (map? x) (not (record? x))))

(defn portable?
  "Metadata must be immutable EDN data; mutable dates/arrays are row values."
  [x]
  (cond
    (or (nil? x) (boolean? x) (string? x) (keyword? x) (symbol? x)
        (uuid? x) (integer? x) (decimal? x)) true
    (or (double? x) (float? x)) (Double/isFinite (double x))
    (plain-map? x) (every? (fn [[k v]] (and (portable? k) (portable? v))) x)
    (or (vector? x) (set? x) (list? x)) (every? portable? x)
    :else false))

(declare value-charge)

(defn- collection-charge [x]
  ;; Charge every occurrence, including shared substructure and metadata.
  (+' 64 (*' 48 (count x)) (reduce +' 0 (map value-charge x))
      (if-let [m (meta x)] (value-charge m) 0)))

(defn value-charge [x]
  (cond
    (nil? x) 8
    (or (boolean? x) (instance? Long x) (instance? Integer x)
        (instance? Double x) (instance? Float x)) 32
    (string? x) (+' 64 (*' 2 (count x)))
    (or (keyword? x) (symbol? x))
    (+' 96 (value-charge (namespace x)) (value-charge (name x))
        (if-let [m (meta x)] (value-charge m) 0))
    (instance? UUID x) 48
    (= Date (class x)) 64
    (bytes? x) (+' 32 (alength ^bytes x))
    (instance? BigInteger x) (+' 64 (quot (+ 7 (.bitLength ^BigInteger x)) 8))
    (instance? clojure.lang.BigInt x) (+' 32 (value-charge (biginteger x)))
    (instance? BigDecimal x) (+' 64 (value-charge (.unscaledValue ^BigDecimal x)))
    (plain-map? x) (+' 96 (*' 64 (count x))
                       (reduce +' 0 (map (fn [[k v]] (+' (value-charge k)
                                                        (value-charge v))) x))
                       (if-let [m (meta x)] (value-charge m) 0))
    (or (vector? x) (set? x) (list? x)) (collection-charge x)
    :else (fail! :query-source/unsupported :values)))

(defn matches-type? [t x]
  (if (map? t)
    (case (:kind t)
      :tuple (and (vector? x) (= (count x) (count (:elements t)))
                  (every? true? (map matches-type? (:elements t) x)))
      ;; Descriptor validation is independent of runtime codec availability.
      :custom false
      false)
    (case t
      :boolean (boolean? x)
      :long (instance? Long x)
      :bigint (or (instance? BigInteger x) (instance? clojure.lang.BigInt x))
      :double (instance? Double x)
      :float (instance? Float x)
      :bigdec (instance? BigDecimal x)
      :string (string? x)
      :keyword (keyword? x)
      :symbol (symbol? x)
      :uuid (instance? UUID x)
      :instant (= Date (class x))
      :bytes (bytes? x)
      :data (try (value-charge x) true
                 (catch clojure.lang.ExceptionInfo _ false))
      false)))

(defn rows! [columns rows]
  (when-not (and (vector? rows)
                (every? #(and (vector? %) (= (count columns) (count %))) rows))
    (fail! :query-source/invalid :batch))
  (doseq [row rows, [column x] (map vector columns row)]
    (value-charge x)
    (when (and (some? x) (= :custom (get-in column [:type :kind])))
      (fail! :query-source/unsupported :values))
    (when-not (if (nil? x) (:nullable? column) (matches-type? (:type column) x))
      (fail! :query-source/invalid :batch)))
  rows)

(defn batch-charge-bytes [columns rows]
  (rows! columns rows)
  (if (empty? rows) 0
      (+' 64 (value-charge columns) (value-charge rows))))

