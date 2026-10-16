(ns datalevin.query.source.value
  "Owned logical values and explicit custom-value validation/accounting hooks."
  (:import [clojure.lang Ratio RT Sorted]
           [java.math BigDecimal BigInteger]
           [java.util Date UUID]))

(def scalar-types
  #{:boolean :long :bigint :double :float :bigdec :string :keyword :symbol
    :uuid :instant :bytes :data})

(defprotocol IQuerySourceValue
  "Opt-in contract for immutable custom values. This does not register a storage
  codec or change equality. Implementations must include all retained backing
  storage in their conservative charge and perform no I/O in either method."
  (-logical-type [value] "Return this value's complete custom type descriptor.")
  (-retained-bytes [value] "Return a conservative positive integer heap charge."))

(defn fail!
  ([category phase] (fail! category phase {}))
  ([category phase context]
   ;; All callers supply structural context, never provider values or causes.
   (throw (ex-info "Query-source contract violation"
                   (merge {:error category :phase phase} context)))))

(defn plain-map? [x] (and (map? x) (not (record? x))))

(defn- custom-comparator? [x]
  (and (sorted? x) (not (identical? RT/DEFAULT_COMPARATOR (.comparator ^Sorted x)))))

(defn owned-copy
  "Detach mutable leaves in supported values, including metadata. Custom values
  follow their immutable opt-in contract and are retained as supplied. The
  caller must reserve the value's charge before copying retained query state."
  [x]
  (if (satisfies? IQuerySourceValue x)
    x
    (let [_ (when (custom-comparator? x) (fail! :query-source/unsupported :values))
          copy (cond
                 (bytes? x) (aclone ^bytes x)
                 (= Date (class x)) (Date. (.getTime ^Date x))
                 (vector? x) (mapv owned-copy x)
                 (plain-map? x) (into (if (sorted? x) (empty x) {})
                                      (map (fn [[k v]] [(owned-copy k) (owned-copy v)])) x)
                 (set? x) (into (if (sorted? x) (empty x) #{}) (map owned-copy) x)
                 (list? x) (apply list (map owned-copy x))
                 :else x)]
      (if-let [metadata (when (instance? clojure.lang.IObj copy) (meta x))]
        (with-meta copy (owned-copy metadata))
        copy))))

(defn add-charge
  "Add fixed overhead with primitive dispatch for long charges and exact overflow."
  [^long overhead charge]
  (if (instance? Long charge)
    (+' overhead (long charge))
    (clojure.lang.Numbers/addP overhead charge)))

(defn multiply-charge
  "Scale a charge without truncating an arbitrarily large retained value."
  [^long factor charge]
  (if (instance? Long charge)
    (*' factor (long charge))
    (clojure.lang.Numbers/multiplyP factor charge)))

(defn at-most?
  "Compare against a long bound, preserving exact charges and deadlines beyond it."
  [n ^long maximum]
  (if (instance? Long n)
    (<= (long n) maximum)
    (clojure.lang.Numbers/lte n maximum)))

(defn portable?
  "Metadata, including Clojure metadata, must be immutable EDN data.
  Mutable dates/arrays are row values, not descriptor or plan metadata."
  [x]
  (and
    (cond
      (or (nil? x) (boolean? x) (string? x) (keyword? x) (symbol? x)
          (uuid? x) (integer? x) (decimal? x) (char? x) (ratio? x)) true
      (or (double? x) (float? x)) (Double/isFinite (double x))
      (custom-comparator? x) false
      (plain-map? x) (every? (fn [[k v]] (and (portable? k) (portable? v))) x)
      (or (vector? x) (set? x) (list? x)) (every? portable? x)
      :else false)
    (if-let [metadata (meta x)] (portable? metadata) true)))

(declare value-charge)

(defn- collection-charge [x]
  ;; Charge every occurrence, including shared substructure and metadata.
  (+' 64 (*' 48 (count x)) (reduce +' 0 (map value-charge x))
      (if-let [m (meta x)] (value-charge m) 0)))

(defn value-charge [x]
  (cond
    (satisfies? IQuerySourceValue x)
    (let [n (-retained-bytes x)]
      (when-not (and (integer? n) (<= 64 n Long/MAX_VALUE))
        (fail! :query-source/invalid :values))
      n)
    (nil? x) 8
    (or (boolean? x) (instance? Long x) (instance? Integer x)
        (instance? Byte x) (instance? Short x) (char? x)
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
    (ratio? x) (+' 64 (value-charge (.numerator ^Ratio x))
                   (value-charge (.denominator ^Ratio x)))
    (custom-comparator? x) (fail! :query-source/unsupported :values)
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
      :custom (and (satisfies? IQuerySourceValue x) (= t (-logical-type x)))
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

(defn execution-type!
  "Reject array-identity equality until the engine has a portable byte key.
  Owned byte batches remain valid SPI values for consumers outside d/q."
  [t]
  (when (= :bytes t) (fail! :query-source/unsupported :equality))
  (when (= :tuple (:kind t)) (run! execution-type! (:elements t))))

(defn execution-value! [x]
  (when (bytes? x) (fail! :query-source/unsupported :equality))
  (cond
    (plain-map? x) (doseq [[k v] x] (execution-value! k) (execution-value! v))
    (or (vector? x) (set? x) (list? x)) (run! execution-value! x)))

(defn rows! [columns rows]
  (when-not (and (vector? rows)
                (every? #(and (vector? %) (= (count columns) (count %))) rows))
    (fail! :query-source/invalid :batch))
  (doseq [row rows, [column x] (map vector columns row)]
    (value-charge x)
    (when-not (if (nil? x) (:nullable? column) (matches-type? (:type column) x))
      (fail! :query-source/invalid :batch)))
  rows)

(defn batch-charge-bytes [columns rows]
  (rows! columns rows)
  (if (empty? rows) 0
      (+' 64 (value-charge columns) (value-charge rows))))

