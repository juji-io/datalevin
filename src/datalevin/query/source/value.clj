(ns datalevin.query.source.value
  "Owned logical values and explicit custom-value validation/accounting hooks."
  (:import [clojure.lang Ratio RT Sorted]
           [java.math BigDecimal BigInteger]
           [java.time Duration Instant Period]
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

(defn- charge-part [charge observe!]
  (when observe! (observe! charge))
  charge)

(defn- collection-overhead [x observe!]
  (+' (charge-part (+' 64 (*' 48 (count x))) observe!)
      (if-let [m (meta x)] (value-charge m observe!) 0)))

(defn value-charge
  "Conservative retained size, counting each occurrence and its metadata.
  The internal observer receives incremental charges before child traversal;
  it may throw to stop validation on cancellation or a byte limit."
  ([x] (value-charge x nil))
  ([x observe!]
   (when observe! (observe! 0))
   (cond
     (satisfies? IQuerySourceValue x)
     (let [n (-retained-bytes x)]
       (when-not (and (integer? n) (<= 64 n Long/MAX_VALUE))
         (fail! :query-source/invalid :values))
       (charge-part n observe!))
     (nil? x) (charge-part 8 observe!)
     (or (boolean? x) (instance? Long x) (instance? Integer x)
         (instance? Byte x) (instance? Short x) (char? x)
         (instance? Double x) (instance? Float x)) (charge-part 32 observe!)
     (string? x) (charge-part (+' 64 (*' 2 (count x))) observe!)
     (or (keyword? x) (symbol? x))
     (+' (charge-part 96 observe!) (value-charge (namespace x) observe!)
         (value-charge (name x) observe!)
         (if-let [m (meta x)] (value-charge m observe!) 0))
     (instance? UUID x) (charge-part 48 observe!)
     (= Date (class x)) (charge-part 64 observe!)
     (or (instance? Instant x) (instance? Duration x) (instance? Period x))
     (charge-part 64 observe!)
     (bytes? x) (charge-part (+' 32 (alength ^bytes x)) observe!)
     (instance? BigInteger x)
     (charge-part (+' 64 (quot (+ 7 (.bitLength ^BigInteger x)) 8)) observe!)
     (instance? clojure.lang.BigInt x)
     (+' (charge-part 32 observe!) (value-charge (biginteger x) observe!))
     (instance? BigDecimal x)
     (+' (charge-part 64 observe!) (value-charge (.unscaledValue ^BigDecimal x) observe!))
     (ratio? x) (+' (charge-part 64 observe!) (value-charge (.numerator ^Ratio x) observe!)
                    (value-charge (.denominator ^Ratio x) observe!))
     (custom-comparator? x) (fail! :query-source/unsupported :values)
     (plain-map? x)
     (+' (charge-part (+' 96 (*' 64 (count x))) observe!)
         (reduce (fn [n [k v]] (+' n (value-charge k observe!)
                                    (value-charge v observe!))) 0 x)
         (if-let [m (meta x)] (value-charge m observe!) 0))
     (or (vector? x) (set? x) (list? x))
     (+' (collection-overhead x observe!)
         (reduce (fn [n child] (+' n (value-charge child observe!))) 0 x))
     :else (fail! :query-source/unsupported :values))))

(defn- matches-type*? [t x charged?]
  (if (map? t)
    (case (:kind t)
      :tuple (and (vector? x) (= (count x) (count (:elements t)))
                  (every? true? (map #(matches-type*? %1 %2 charged?) (:elements t) x)))
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
      :data (or charged? (try (value-charge x) true
                             (catch clojure.lang.ExceptionInfo _ false)))
      false)))

(defn matches-type? [t x]
  (matches-type*? t x false))

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
    (when-not (if (nil? x) (:nullable? column) (matches-type*? (:type column) x true))
      (fail! :query-source/invalid :batch)))
  rows)

(defn batch-charge-bytes
  ([columns rows] (batch-charge-bytes columns rows nil))
  ([columns rows observe!]
   (when-not (vector? rows) (fail! :query-source/invalid :batch))
   (if (empty? rows) 0
       (+' (charge-part 64 observe!) (value-charge columns observe!)
           (collection-overhead rows observe!)
           (reduce
             (fn [n row]
               (when-not (and (vector? row) (= (count columns) (count row)))
                 (fail! :query-source/invalid :batch))
               (+' n (collection-overhead row observe!)
                   (reduce
                     (fn [n i]
                       (let [x (nth row i)
                             bytes (value-charge x observe!)
                             column (nth columns i)]
                         ;; The value walk already validated nested :data.
                         (when-not (if (nil? x) (:nullable? column)
                                       (matches-type*? (:type column) x true))
                           (fail! :query-source/invalid :batch))
                         (+' n bytes))) 0 (range (count row))))) 0 rows)))))
