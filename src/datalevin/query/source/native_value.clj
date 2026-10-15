(ns ^:no-doc datalevin.query.source.native-value
  "Preflight the flat scalar layout before allowing Nippy to allocate a value."
  (:require [datalevin.query.source.value :refer [fail!]]
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

(defn- scalar! [^ByteBuffer buffer expected cache]
  (need! buffer 1)
  (let [kind (first (schema/types-spec (.get buffer (.position buffer))))
        cached (case kind
                 :cached-0 0 :cached-1 1 :cached-2 2
                 :cached-3 3 :cached-4 4 :cached-5 5 :cached-6 6 :cached-7 7
                 :cached-sm :small :cached-md :medium nil)]
    (if (nil? cached)
      (scalar-body! buffer expected)
      (do
        (skip! buffer 1)
        (let [id (case cached
                   :small (length! buffer 1) :medium (length! buffer 2) cached)]
          ;; Only three scalar fields exist; sparse/recursive caches cannot
          ;; force Nippy to allocate a cache unrelated to the payload size.
          (when-not (<= 0 id 2) (fail! :query-source/unsupported :native-value))
          (if-let [type (@cache id)]
            (when-not (= type expected) (fail! :query-source/invalid :native-value))
            (do (scalar-body! buffer expected) (vswap! cache assoc id expected))))))))

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
  "Read a declared scalar from an already reserved raw Nippy datom buffer.
  Check every tag and length without constructing values first. No generic,
  metadata, nested, or custom reader is invoked; scalar caches are bounded to
  the three fields. Validate the owning entity/attribute as well, so a stale
  giant reference cannot emit another fact."
  [^ByteBuffer buffer type eid attribute]
  (skip-header! buffer)
  (need! buffer 11)
  (when-not (and (= schema/id-prefixed-custom-md (.get buffer))
                (= datom-type-id (.getShort buffer))
                (= eid (.getLong buffer)))
    (fail! :query-source/invalid :native-value))
  (let [start (.position buffer)
        cache (volatile! {})]
    (scalar! buffer :keyword cache)
    (scalar! buffer type cache)
    (scalar! buffer :long cache)
    (when (.hasRemaining buffer) (fail! :query-source/invalid :native-value))
    (.position buffer start)
    (impl/with-thaw-cache
      (when-not (= attribute (nippy/thaw-from-bb! buffer))
        (fail! :query-source/invalid :native-value))
      (nippy/thaw-from-bb! buffer))))
