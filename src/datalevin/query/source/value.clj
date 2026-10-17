(ns datalevin.query.source.value
  "Owned logical values and explicit custom-value validation/accounting hooks."
  (:import [clojure.lang APersistentVector$SubVector Ratio RT Sorted]
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
  caller must reserve the value's charge before copying retained query state.
  An internal check callback can stop copying on cancellation."
  ([x] (owned-copy x nil))
  ([x check!]
   (when check! (check!))
   (if (satisfies? IQuerySourceValue x)
     x
     (let [_ (when (custom-comparator? x) (fail! :query-source/unsupported :values))
           copy (cond
                  (bytes? x) (aclone ^bytes x)
                  (= Date (class x)) (Date. (.getTime ^Date x))
                  (vector? x) (mapv #(owned-copy % check!) x)
                  (plain-map? x) (into (if (sorted? x) (empty x) {})
                                       (map (fn [[k v]] [(owned-copy k check!)
                                                        (owned-copy v check!)])) x)
                  (set? x) (into (if (sorted? x) (empty x) #{}) (map #(owned-copy % check!)) x)
                  (list? x) (apply list (map #(owned-copy % check!) x))
                  :else x)]
       (when check! (check!))
       (if-let [metadata (when (instance? clojure.lang.IObj copy) (meta x))]
         (with-meta copy (owned-copy metadata check!))
         copy)))))

(defn diagnostic-copy
  "Copy a validated non-secret portable identifier for diagnostics. Preserve
  its value, detach collection backing, and omit Clojure metadata at every level.
  The caller must reserve copy space and supply its cancellation check."
  [x check!]
  (check!)
  (let [copy #(diagnostic-copy % check!)]
    (cond
      (symbol? x) (with-meta x nil)
      (plain-map? x) (into {} (map (fn [[k v]] [(copy k) (copy v)])) x)
      (vector? x) (mapv copy x)
      (set? x) (into #{} (map copy) x)
      (list? x) (apply list (map copy x))
      :else x)))

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

(defn- metadata-node? [x]
  (cond
    (or (nil? x) (boolean? x) (string? x) (keyword? x) (symbol? x)
        (uuid? x) (integer? x) (decimal? x) (char? x) (ratio? x)) true
    (or (double? x) (float? x)) (Double/isFinite (double x))
    (custom-comparator? x) false
    :else (or (plain-map? x) (vector? x) (set? x) (list? x))))

(defn portable?
  "Metadata, including Clojure metadata, must be immutable EDN data.
  Mutable dates/arrays are row values, not descriptor or plan metadata.
  An internal check callback observes cancellation during validation."
  ([x] (portable? x nil))
  ([x check!]
   (when check! (check!))
   (and (metadata-node? x)
        (cond
          (plain-map? x) (every? (fn [[k v]] (and (portable? k check!) (portable? v check!))) x)
          (or (vector? x) (set? x) (list? x)) (every? #(portable? % check!) x)
          :else true)
        (if-let [metadata (meta x)] (portable? metadata check!) true))))

(declare value-charge*)

(defn- charge-part [charge observe!]
  (when observe! (observe! charge))
  charge)

(defn- collection-overhead [x observe! backing? metadata-phase]
  (+' (charge-part (+' 64 (*' 48 (count x))) observe!)
      (if-let [m (meta x)] (value-charge* m observe! backing? metadata-phase) 0)
      ;; A view pins the entire parent, including elements outside its range
      ;; and the parent's metadata. An owned copy detaches that parent.
      (if (and backing? (instance? APersistentVector$SubVector x))
        (value-charge* (.-v ^APersistentVector$SubVector x) observe! true metadata-phase) 0)))

(defn- value-charge* [x observe! backing? metadata-phase]
  (when observe! (observe! 0))
  (when (and metadata-phase (not (metadata-node? x)))
    (fail! :query-source/invalid metadata-phase))
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
    (+' (charge-part 96 observe!) (value-charge* (namespace x) observe! backing? metadata-phase)
        (value-charge* (name x) observe! backing? metadata-phase)
        (if-let [m (meta x)] (value-charge* m observe! backing? metadata-phase) 0))
    (instance? UUID x) (charge-part 48 observe!)
    (= Date (class x)) (charge-part 64 observe!)
    (or (instance? Instant x) (instance? Duration x) (instance? Period x))
    (charge-part 64 observe!)
    (bytes? x) (charge-part (+' 32 (alength ^bytes x)) observe!)
    (instance? BigInteger x)
    (charge-part (+' 64 (quot (+ 7 (.bitLength ^BigInteger x)) 8)) observe!)
    (instance? clojure.lang.BigInt x)
    (+' (charge-part 32 observe!) (value-charge* (biginteger x) observe! backing? metadata-phase))
    (instance? BigDecimal x)
    (+' (charge-part 64 observe!) (value-charge* (.unscaledValue ^BigDecimal x) observe! backing? metadata-phase))
    (ratio? x) (+' (charge-part 64 observe!) (value-charge* (.numerator ^Ratio x) observe! backing? metadata-phase)
                   (value-charge* (.denominator ^Ratio x) observe! backing? metadata-phase))
    (custom-comparator? x) (fail! :query-source/unsupported :values)
    (plain-map? x)
    (+' (charge-part (+' 96 (*' 64 (count x))) observe!)
        (reduce (fn [n [k v]] (+' n (value-charge* k observe! backing? metadata-phase)
                                   (value-charge* v observe! backing? metadata-phase))) 0 x)
        (if-let [m (meta x)] (value-charge* m observe! backing? metadata-phase) 0))
    (or (vector? x) (set? x) (list? x))
    (+' (collection-overhead x observe! backing? metadata-phase)
        (reduce (fn [n child] (+' n (value-charge* child observe! backing? metadata-phase))) 0 x))
    :else (fail! :query-source/unsupported :values)))

(defn value-charge
  "Conservative retained size, including metadata and vector backing storage.
  Each occurrence is charged; shared backing may be counted more than once.
  The internal observer receives incremental charges before child traversal
  and may throw on cancellation or a byte limit."
  ([x] (value-charge* x nil true nil))
  ([x observe!] (value-charge* x observe! true nil)))

(defn metadata-charge
  "Validate immutable metadata while charging its retained storage. Reject
  custom values without invoking their hooks. Observe limits/cancellation
  before descending into children, including metadata and vector parents."
  [x phase observe!]
  (value-charge* x observe! true phase))

(defn vector-parent
  "Return the retained parent of a vector view, or nil for an owned vector."
  [x]
  (when (instance? APersistentVector$SubVector x)
    (.-v ^APersistentVector$SubVector x)))

(defn copy-charge
  "Charge the detached result of owned-copy without allocating that copy.
  Hidden vector elements are omitted; immutable custom values keep their full
  declared retained charge because owned-copy preserves them."
  ([x] (value-charge* x nil false nil))
  ([x observe!] (value-charge* x observe! false nil)))

(defn- matches-type*? [t x charged? check!]
  (when check! (check!))
  (if (map? t)
    (case (:kind t)
      :tuple (and (vector? x) (= (count x) (count (:elements t)))
                  (every? true? (map #(matches-type*? %1 %2 charged? check!) (:elements t) x)))
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
      :data (or charged?
                (try (copy-charge x (when check! (fn [_] (check!)))) true
                     (catch clojure.lang.ExceptionInfo _
                       ;; Cancellation must not become a type mismatch.
                       (when check! (check!))
                       false)))
      false)))

(defn matches-type?
  "Check a logical type, optionally observing cancellation inside nested data
  and tuple components. The callback also runs after custom type validation."
  ([t x] (matches-type? t x nil))
  ([t x check!]
   (let [matches? (matches-type*? t x false check!)]
     (when check! (check!))
     matches?)))

(defn execution-value!
  ([x] (execution-value! x nil))
  ([x check!]
   (when check! (check!))
   (cond
     (plain-map? x) (doseq [[k v] x] (execution-value! k check!) (execution-value! v check!))
     (or (vector? x) (set? x) (list? x)) (run! #(execution-value! % check!) x))))

(defn rows!
  "Validate logical rows before copying or sizing their retained representation."
  [columns rows]
  (when-not (and (vector? rows)
                (every? #(and (vector? %) (= (count columns) (count %))) rows))
    (fail! :query-source/invalid :batch))
  (doseq [row rows, [column x] (map vector columns row)]
    (copy-charge x)
    (when-not (if (nil? x) (:nullable? column) (matches-type*? (:type column) x true nil))
      (fail! :query-source/invalid :batch)))
  rows)

(defn batch-charge-bytes
  ([columns rows] (batch-charge-bytes columns rows nil))
  ([columns rows observe!]
   (when-not (vector? rows) (fail! :query-source/invalid :batch))
   ;; Ordinary empty batches transfer no row state. Empty views or metadata
   ;; can still pin data, and need a charge and a reservation of their own.
   (if (and (empty? rows) (nil? (meta rows))
            (not (instance? APersistentVector$SubVector rows))) 0
       (+' (charge-part 64 observe!) (value-charge columns observe!)
           (collection-overhead rows observe! true nil)
           (reduce
             (fn [n row]
               (when-not (and (vector? row) (= (count columns) (count row)))
                 (fail! :query-source/invalid :batch))
               (+' n (collection-overhead row observe! true nil)
                   (reduce
                     (fn [n i]
                       (let [x (nth row i)
                             bytes (value-charge x observe!)
                             column (nth columns i)]
                         ;; The value walk already validated nested :data.
                         (when-not (if (nil? x) (:nullable? column)
                                       (matches-type*? (:type column) x true nil))
                           (fail! :query-source/invalid :batch))
                         (+' n bytes))) 0 (range (count row))))) 0 rows)))))
