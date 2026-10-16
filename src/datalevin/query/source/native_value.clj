(ns ^:no-doc datalevin.query.source.native-value
  "Preflight supported native value layouts before allocating decoded values."
  (:require [datalevin.bits :as bits]
            [datalevin.constants :as c]
            [datalevin.query.source.value :refer [fail!]]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.impl :as impl]
            [taoensso.nippy.schema :as schema])
  (:import [java.nio ByteBuffer]))

(def ^:private max-data-depth 64)
(def ^:private max-data-cache 256)

(defn- metadata-capable? [type]
  (or (#{:data :symbol :vector :map :set :list} type)
      (#{:tuple :native-homogeneous-tuple} (:kind type))))

(defn scratch-bytes
  "Bound preflight cache/stack state independently of decoded collections.
  Inline typed encodings can disable metadata, which only Nippy preserves."
  (^long [type encoded-size] (scratch-bytes type encoded-size true))
  (^long [type encoded-size metadata?]
   (let [encoded-size (long encoded-size)
         tuple-scratch (if (= :native-homogeneous-tuple (:kind type))
                         (* 512 (min 255 encoded-size))
                         (* 512 (count (:elements type))))]
     (if (or (= type :data) (and metadata? (metadata-capable? type)))
       (max tuple-scratch
            (+ (* 256 (min (long max-data-cache) encoded-size))
               (* 512 (min (long max-data-depth) encoded-size))))
       tuple-scratch))))

(defn- add-charge ^long [^long a ^long b]
  ;; Saturation still exceeds any available ledger space: the raw-buffer
  ;; reservation is already live. Cached DAGs must never wrap a charge.
  (if (> b (- Long/MAX_VALUE a)) Long/MAX_VALUE (+ a b)))

(def ^:private datom-type-id (impl/coerce-custom-type-id :datalevin/datom))

(defn- need! [^ByteBuffer buffer ^long size]
  (when-not (<= 0 size (.remaining buffer))
    (fail! :query-source/invalid :native-value)))

(defn- skip! [^ByteBuffer buffer ^long size]
  (need! buffer size)
  (.position buffer (int (+ (.position buffer) size))))

(defn- length! ^long [^ByteBuffer buffer ^long width]
  (need! buffer width)
  (case width
    1 (long (.get buffer))
    2 (long (.getShort buffer))
    4 (long (.getInt buffer))))

(defn- scalar-body! [^ByteBuffer buffer expected]
  (need! buffer 1)
  (let [kind (first (schema/types-spec (.get buffer)))
        [type size]
        (case kind
          (:true :false) [:boolean 0]
          :boolean_ [:boolean 1]
          :integer [:integer 4]
          :long-0 [:long 0]
          (:long-pos-sm :long-neg-sm :long-sm_) [:long 1]
          (:long-pos-md :long-neg-md :long-md_) [:long 2]
          (:long-pos-lg :long-neg-lg :long-lg_) [:long 4]
          :long-xl [:long 8]
          :float [:float 4]
          :double-0 [:double 0]
          :double [:double 8]
          :util-date [:instant 8]
          :uuid [:uuid 16]
          :str-0 [:string 0]
          :str-sm* [:string (+ 128 (length! buffer 1))]
          :str-sm_ [:string (length! buffer 1)]
          :str-md [:string (length! buffer 2)]
          :str-lg [:string (length! buffer 4)]
          :kw-sm [:keyword (length! buffer 1)]
          :kw-md [:keyword (length! buffer 2)]
          (:kw-md_ :kw-lg_) [:keyword (length! buffer 4)]
          :sym-sm [:symbol (length! buffer 1)]
          :sym-md [:symbol (length! buffer 2)]
          (:sym-md_ :sym-lg_) [:symbol (length! buffer 4)]
          (:bigint :biginteger) [:bigint (length! buffer 4)]
          :bigdec [:bigdec (length! buffer 4)]
          (fail! :query-source/unsupported :native-value))]
    (when-not (or (= expected :data) (= type expected))
      (fail! :query-source/invalid :native-value))
    (skip! buffer size)
    (when (= kind :bigdec) (skip! buffer 4))
    {:type type :charge (+ 512 (* 8 (long size))) :height 0}))

(declare value!)

(defn- collection-layout [kind ^ByteBuffer buffer]
  (case kind
    :vec-0 [:vector 0] :vec-2 [:vector 2] :vec-3 [:vector 3]
    :vec-sm* [:vector (+ 128 (length! buffer 1))]
    :vec-sm_ [:vector (length! buffer 1)]
    :vec-md [:vector (length! buffer 2)] :vec-lg [:vector (length! buffer 4)]
    :set-0 [:set 0]
    :set-sm* [:set (+ 128 (length! buffer 1))]
    :set-sm_ [:set (length! buffer 1)]
    :set-md [:set (length! buffer 2)] :set-lg [:set (length! buffer 4)]
    :map-0 [:map 0]
    (:map-sm* :pam-sm*_) [:map (* 2 (+ 128 (length! buffer 1)))]
    :map-sm_ [:map (* 2 (length! buffer 1))]
    :map-md [:map (* 2 (length! buffer 2))]
    :map-lg [:map (* 2 (length! buffer 4))]
    :list-0 [:list 0] :list-sm [:list (length! buffer 1)]
    :list-md [:list (length! buffer 2)] :list-lg [:list (length! buffer 4)]
    nil))

(defn- value-body! [^ByteBuffer buffer expected context depth nullable?]
  (need! buffer 1)
  (let [kind (first (schema/types-spec (.get buffer (.position buffer))))]
    (cond
      (= kind :nil)
      (do (when-not nullable? (fail! :query-source/invalid :native-value))
          (skip! buffer 1)
          {:type :nil :charge 512 :height 0})

      (and (metadata-capable? expected) (= kind :meta))
      (do
        (skip! buffer 1)
        (let [depth (inc (long depth))
              metadata (value! buffer :data context depth true)
              _ (when-not (#{:map :nil} (:type metadata))
                  (fail! :query-source/invalid :native-value))
              value (value! buffer expected context depth nullable?)]
          (when-not (metadata-capable? (:type value))
            (fail! :query-source/invalid :native-value))
          (assoc value
                 :metadata? true
                 :height (inc (max (long (:height metadata)) (long (:height value))))
                 :charge (add-charge 512 (add-charge (long (:charge metadata))
                                                    (long (:charge value)))))))

      (or (= expected :data) (#{:tuple :native-homogeneous-tuple} (:kind expected)))
      (let [start (.position buffer)
            _ (skip! buffer 1)]
        (if-let [[kind size] (collection-layout kind buffer)]
          (let [size (long size)
                homogeneous? (= :native-homogeneous-tuple (:kind expected))
                tuple? (or homogeneous? (= :tuple (:kind expected)))]
            ;; At least one tag per element is required, even for a cache ref.
            ;; Validate counts before any Nippy collection allocation.
            (need! buffer size)
            (when (and tuple? (not (and (= kind :vector)
                                        (if homogeneous? (<= 1 size 255)
                                            (= size (count (:elements expected)))))))
              (fail! :query-source/invalid :native-value))
            ((:check! context))
            (loop [i 0, charge (+ 512 (* 128 size)), height 0, metadata? false]
              (if (= i size)
                (cond-> {:type (if tuple? expected kind) :charge charge :height (inc height)}
                  metadata? (assoc :metadata? true))
                (let [_ (when (zero? (bit-and i 1023)) ((:check! context)))
                      child (value! buffer (cond homogeneous? (:element expected)
                                                 tuple? (nth (:elements expected) i)
                                                 :else :data)
                                    context (inc (long depth)) (not homogeneous?))]
                  (recur (inc i) (add-charge charge (long (:charge child)))
                         (max height (long (:height child)))
                         (or metadata? (:metadata? child)))))))
          (do (.position buffer start)
              (scalar-body! buffer expected))))

      :else (scalar-body! buffer expected))))

(defn- compatible! [expected layout nullable?]
  (when-not (if (= :nil (:type layout)) nullable?
                (or (= expected :data) (= expected (:type layout))))
    (fail! :query-source/invalid :native-value))
  layout)

(defn- value! [^ByteBuffer buffer expected {:keys [cache cache-limit] :as context} depth nullable?]
  (when (> (long depth) (long max-data-depth))
    (fail! :query-source/unsupported :native-value))
  (need! buffer 1)
  (let [kind (first (schema/types-spec (.get buffer (.position buffer))))
        cached (case kind
                 :cached-0 0 :cached-1 1 :cached-2 2
                 :cached-3 3 :cached-4 4 :cached-5 5 :cached-6 6 :cached-7 7
                 :cached-sm :small :cached-md :medium nil)
        layout
        (if (nil? cached)
          (value-body! buffer expected context depth nullable?)
          (do
            (skip! buffer 1)
            (let [id (case cached
                       :small (length! buffer 1) :medium (length! buffer 2) cached)]
              (when-not (< -1 id (long cache-limit))
                (fail! :query-source/unsupported :native-value))
              (if-let [layout (@cache id)]
                (do (when (:pending? layout) (fail! :query-source/invalid :native-value))
                    (compatible! expected layout nullable?))
                (do
                  (when-not (= id (count @cache))
                    (fail! :query-source/invalid :native-value))
                  ;; Match Nippy's ordered first occurrences and reject cycles.
                  (vswap! cache assoc id {:pending? true})
                  (let [layout (value-body! buffer expected context depth nullable?)]
                    (vswap! cache assoc id layout)
                    layout))))))]
    ;; Also bound logical depth when cached subtrees occur deeper in the value.
    (when (> (+ (long depth) (long (:height layout))) (long max-data-depth))
      (fail! :query-source/unsupported :native-value))
    layout))

(defn- skip-header! [^ByteBuffer buffer]
  (let [p (.position buffer)]
    (when (and (<= 4 (.remaining buffer))
               (= 0x4e (.get buffer p))
               (= 0x50 (.get buffer (+ p 1)))
               (= 0x59 (.get buffer (+ p 2))))
      ;; Legacy uncompressed Nippy headers are safe. Other envelopes can expand
      ;; independently of the storage envelope and require their own bounds.
      (when-not (zero? (.get buffer (+ p 3)))
        (fail! :query-source/unsupported :native-value))
      (skip! buffer 4))))

(defn- context [type check!]
  {:cache (volatile! {}) :check! check!
   :cache-limit (max (long (if (metadata-capable? type) max-data-cache 0))
                    (if (= :native-homogeneous-tuple (:kind type)) 258
                        (+ 3 (count (:elements type)))))})

(defn- layout-charge? [type]
  (or (= type :data) (= :native-homogeneous-tuple (:kind type))))

(defn prepare-giant
  "Validate a raw Nippy datom without constructing its values. Returns a decoder
  and an additional retained-byte charge, to reserve through consumer handoff.
  The caller must already reserve raw/expanded buffers and scratch-bytes."
  ([buffer type eid attribute] (prepare-giant buffer type eid attribute (fn [])))
  ([^ByteBuffer buffer type eid attribute check!]
   (skip-header! buffer)
   (need! buffer 11)
   (when-not (and (= schema/id-prefixed-custom-md (.get buffer))
                 (= datom-type-id (.getShort buffer))
                 (= eid (.getLong buffer)))
     (fail! :query-source/invalid :native-value))
   (let [start (.position buffer)
         context (context type check!)]
     (value! buffer :keyword context 0 false)
     (let [layout (value! buffer type context 0 false)]
       (value! buffer :long context 0 false)
       (when (.hasRemaining buffer) (fail! :query-source/invalid :native-value))
       {:retained-bytes (if (or (layout-charge? type) (:metadata? layout)) (:charge layout) 0)
        :decode (fn []
                  (.position buffer start)
                  (impl/with-thaw-cache
                    (when-not (= attribute (nippy/thaw-from-bb! buffer))
                      (fail! :query-source/invalid :native-value))
                    (nippy/thaw-from-bb! buffer)))}))))

(defn giant-value
  "Decode a pre-reserved scalar or fixed tuple without metadata. Values needing
  an additional layout charge must use prepare-giant and reserve it first."
  [buffer type eid attribute]
  (when (layout-charge? type) (fail! :query-source/unsupported :native-value))
  (let [{:keys [retained-bytes decode]} (prepare-giant buffer type eid attribute)]
    (when (pos? (long retained-bytes)) (fail! :query-source/unsupported :native-value))
    (decode)))

(defn- scalar-header [^long header]
  (case header
    (-64 -63 -8) :long
    -15 :bigint -14 :bigdec -11 :float -10 :double -9 :instant
    -7 :uuid -6 :string -5 :keyword -4 :symbol -3 :boolean
    (fail! :query-source/unsupported :native-value)))

(defn- inline-tuple! [^ByteBuffer buffer elements]
  (need! buffer 2)
  (when-not (= c/type-hete-tuple (.get buffer))
    (fail! :query-source/unsupported :native-value))
  (let [n (bit-and 0xff (.get buffer))
        sizes (- (.limit buffer) 2 n)
        end (dec sizes)]
    (when-not (and (= n (count elements)) (<= (.position buffer) end)
                   (= c/truncator (.get buffer end)))
      (fail! :query-source/invalid :native-value))
    (dotimes [i n]
      (let [size (bit-and 0xff (.get buffer (int (+ sizes i))))
            start (.position buffer)
            next (+ start size)]
        (when-not (and (pos? size) (<= next end))
          (fail! :query-source/invalid :native-value))
        (let [header (.get buffer)
              type (when-not (= c/type-nil header) (scalar-header header))
              fixed (case type
                      nil 1 :boolean 2 :long 9 :float 5 :double 9
                      :instant 9 :uuid 17 nil)]
          (when-not (and (or (nil? type) (= type (nth elements i)))
                         (if fixed (= size fixed)
                             (and (<= 2 size) (= c/separator (.get buffer (dec next))))))
            (fail! :query-source/invalid :native-value)))
        (.position buffer (int next))))
    (when-not (= (.position buffer) end)
      (fail! :query-source/invalid :native-value))))

(defn- inline-homogeneous-tuple! [^ByteBuffer buffer element]
  (need! buffer 3)
  (when-not (= c/type-homo-tuple (.get buffer))
    (fail! :query-source/invalid :native-value))
  (let [type (scalar-header (.get buffer))
        n (bit-and 0xff (.get buffer))
        fixed (case type
                :boolean 1 :long 8 :float 4 :double 8 :instant 8 :uuid 16 nil)]
    (when-not (and (= type element) (pos? n))
      (fail! :query-source/invalid :native-value))
    (if fixed
      (when-not (= (.remaining buffer) (+ 2 (* n (long fixed))))
        (fail! :query-source/invalid :native-value))
      (let [sizes (- (.limit buffer) 2 n)
            end (dec sizes)]
        (when-not (and (<= (.position buffer) end)
                       (= c/truncator (.get buffer end)))
          (fail! :query-source/invalid :native-value))
        (dotimes [i n]
          (let [size (bit-and 0xff (.get buffer (int (+ sizes i))))
                separator (+ (.position buffer) size)]
            (when-not (and (< separator end)
                           (= c/separator (.get buffer (int separator))))
              (fail! :query-source/invalid :native-value))
            (.position buffer (int (inc separator)))))
        (when-not (= (.position buffer) end)
          (fail! :query-source/invalid :native-value))))))

(defn inline-data-buffer
  "Borrow the Nippy payload of an inline AVG data value without copying it."
  ^ByteBuffer [^ByteBuffer raw]
  (let [buffer (.duplicate raw)]
    (need! buffer 7)
    (.position buffer 4)
    (.limit buffer (- (.limit buffer) 2))
    buffer))

(defn prepare-data
  "Preflight raw Nippy data after any separately reserved envelope expansion."
  [^ByteBuffer buffer check!]
  (skip-header! buffer)
  (let [start (.position buffer)
        layout (value! buffer :data (context :data check!) 0 false)]
    (when (.hasRemaining buffer) (fail! :query-source/invalid :native-value))
    {:retained-bytes (:charge layout)
     :decode (fn []
               (.position buffer start)
               (impl/with-thaw-cache (nippy/thaw-from-bb! buffer)))}))

(defn prepare-inline
  "Validate an inline AVG value, returning a decoder and additional charge.
  The caller must already reserve encoded buffers and scratch-bytes."
  ([raw type] (prepare-inline raw type (fn [])))
  ([^ByteBuffer raw type check!]
   (let [buffer (.duplicate raw)]
     (need! buffer 7)
     (.position buffer 4)
     (if (= type :data)
       (prepare-data (inline-data-buffer raw) check!)
       (do
         (case (:kind type)
           :tuple (inline-tuple! buffer (:elements type))
           :native-homogeneous-tuple (inline-homogeneous-tuple! buffer (:element type))
           (when-not (= type (scalar-header (.get buffer)))
             (fail! :query-source/invalid :native-value)))
         {:retained-bytes 0 :decode #(bits/avg->inline-value raw)})))))

(defn inline-value
  "Decode a pre-reserved scalar or fixed tuple. Other supported values must use
  prepare-inline and reserve its returned charge before invoking the decoder."
  [raw type]
  (when (layout-charge? type) (fail! :query-source/unsupported :native-value))
  ((:decode (prepare-inline raw type))))
