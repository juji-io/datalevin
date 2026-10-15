(ns ^:no-doc datalevin.query.source.native-value
  "Preflight scalar and fixed tuple layouts before allocating decoded values."
  (:require [datalevin.bits :as bits]
            [datalevin.constants :as c]
            [datalevin.query.source.value :refer [fail!]]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.impl :as impl]
            [taoensso.nippy.schema :as schema])
  (:import [java.nio ByteBuffer]))

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
    (when-not (= type expected) (fail! :query-source/invalid :native-value))
    (skip! buffer size)
    (when (= kind :bigdec) (skip! buffer 4))))

(declare value!)

(defn- value-body! [^ByteBuffer buffer expected cache cache-limit nullable?]
  (need! buffer 1)
  (let [kind (first (schema/types-spec (.get buffer (.position buffer))))]
    (cond
      (= kind :nil)
      (do (when-not nullable? (fail! :query-source/invalid :native-value))
          (skip! buffer 1)
          true)

      (= :tuple (:kind expected))
      (let [_ (skip! buffer 1)
            size (case kind
                   :vec-0 0 :vec-2 2 :vec-3 3
                   :vec-sm* (+ 128 (length! buffer 1))
                   :vec-sm_ (length! buffer 1)
                   :vec-md (length! buffer 2)
                   :vec-lg (length! buffer 4)
                   (fail! :query-source/unsupported :native-value))]
        (when-not (= size (count (:elements expected)))
          (fail! :query-source/invalid :native-value))
        (doseq [type (:elements expected)]
          (value! buffer type cache cache-limit true))
        false)

      :else (do (scalar-body! buffer expected) false))))

(defn- value! [^ByteBuffer buffer expected cache cache-limit nullable?]
  (need! buffer 1)
  (let [kind (first (schema/types-spec (.get buffer (.position buffer))))
        cached (case kind
                 :cached-0 0 :cached-1 1 :cached-2 2
                 :cached-3 3 :cached-4 4 :cached-5 5 :cached-6 6 :cached-7 7
                 :cached-sm :small :cached-md :medium nil)]
    (if (nil? cached)
      (value-body! buffer expected cache cache-limit nullable?)
      (do
        (skip! buffer 1)
        (let [id (case cached
                   :small (length! buffer 1) :medium (length! buffer 2) cached)]
          ;; Cache space is bounded by the schema's field count, independently
          ;; of encoded IDs. Nested/generic readers cannot allocate extra state.
          (when-not (< -1 id (long cache-limit))
            (fail! :query-source/unsupported :native-value))
          (if-let [{:keys [type nil?]} (@cache id)]
            (when-not (and (= type expected) (or nullable? (not nil?)))
              (fail! :query-source/invalid :native-value))
            (do
              (when-not (= id (count @cache))
                (fail! :query-source/invalid :native-value))
              ;; Match Nippy's ordered first occurrences. Reserve this slot
              ;; before nested elements, while rejecting references to it.
              (vswap! cache assoc id {:pending? true})
              (let [nil? (value-body! buffer expected cache cache-limit nullable?)]
                (vswap! cache assoc id {:type expected :nil? nil?})))))))))

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

(defn giant-value
  "Read a declared scalar or fixed tuple from a reserved raw Nippy datom buffer.
  Check every tag and length without constructing values first. No metadata,
  generic collection, or custom reader is invoked; caches and tuple counts are
  bounded by the schema. Validate the owning entity/attribute, so a stale giant
  reference cannot emit another fact."
  [^ByteBuffer buffer type eid attribute]
  (skip-header! buffer)
  (need! buffer 11)
  (when-not (and (= schema/id-prefixed-custom-md (.get buffer))
                (= datom-type-id (.getShort buffer))
                (= eid (.getLong buffer)))
    (fail! :query-source/invalid :native-value))
  (let [start (.position buffer)
        cache (volatile! {})
        cache-limit (+ 3 (count (:elements type)))]
    (value! buffer :keyword cache cache-limit false)
    (value! buffer type cache cache-limit false)
    (value! buffer :long cache cache-limit false)
    (when (.hasRemaining buffer) (fail! :query-source/invalid :native-value))
    (.position buffer start)
    (impl/with-thaw-cache
      (when-not (= attribute (nippy/thaw-from-bb! buffer))
        (fail! :query-source/invalid :native-value))
      (nippy/thaw-from-bb! buffer))))

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

(defn inline-value
  "Decode an AVG value only after checking every component's scalar header.
  Tuple trailers bound each component; no fallback to a generic reader is
  permitted. The caller reserves memory before this preflight and decoding."
  [^ByteBuffer raw type]
  (let [buffer (.duplicate raw)]
    (need! buffer 7)
    (.position buffer 4)
    (if (= :tuple (:kind type))
      (inline-tuple! buffer (:elements type))
      (when-not (= type (scalar-header (.get buffer)))
        (fail! :query-source/invalid :native-value)))
    (bits/avg->inline-value raw)))
