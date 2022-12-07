(ns ^:no-doc datalevin.spill
  "KV range query results that spills to disk automatically when memory
  pressure is high. Present an `IPersistentVector` API"
  (:require
   [datalevin.constants :as c]
   [datalevin.util :refer [raise]]
   [datalevin.lmdb :as l]
   [clojure.string :as s]
   [clojure.stacktrace :as st]
   [datalevin.util :as u])
  (:import
   [java.lang Runtime IndexOutOfBoundsException]
   [java.lang.management ManagementFactory]
   [javax.management NotificationEmitter NotificationListener Notification]
   [com.sun.management GarbageCollectionNotificationInfo]
   [clojure.lang IPersistentVector MapEntry]))

(defonce memory-pressure (volatile! 0))

(defonce ^Runtime runtime (Runtime/getRuntime))

(defn- set-memory-pressure []
  (let [fm (.freeMemory runtime)
        tm (.totalMemory runtime)
        mm (.maxMemory runtime)]
    (vreset! memory-pressure (int (/ (- tm fm) mm)))))

(defonce listeners (volatile! {}))

(defn install-memory-updater []
  (doseq [^NotificationEmitter gcbean
          (ManagementFactory/getGarbageCollectorMXBeans)]
    (let [^NotificationListener listener
          (reify NotificationListener
            (^void handleNotification [this ^Notification notif _]
             (when (= (.getType notif)
                      GarbageCollectionNotificationInfo/GARBAGE_COLLECTION_NOTIFICATION)
               (set-memory-pressure))))]
      (vswap! listeners assoc gcbean listener)
      (.addNotificationListener gcbean listener nil nil))))

(defn uninstall-memory-updater []
  (doseq [[^NotificationEmitter gcbean listener] @listeners]
    (vswap! listeners dissoc gcbean)
    (.removeNotificationListener gcbean listener nil nil)))

(def memory-updater (memoize install-memory-updater)) ; do it once

(defn- long-inc [^long i] (inc i))

(defprotocol ISpillable
  (memory-count [this] "The number of items reside in memory")
  (disk-count [this] "The number of items reside on disk")
  (spill [this] "Spill to disk"))

(deftype SpillableVector [^long spill-threshold
                          ^String spill-root
                          ^String spill-dir
                          memory-vec
                          disk-lmdb]
  ISpillable

  (memory-count ^long [_]
    (.length ^IPersistentVector @memory-vec))

  (disk-count ^long [_]
    (if @disk-lmdb (l/entries @disk-lmdb c/tmp-dbi) 0))

  (spill [this]
    (let [dir (str spill-root "dtlv-spill-" (random-uuid))]
      (vreset! spill-dir dir)
      (vreset! disk-lmdb (l/open-kv dir))
      (l/open-dbi @disk-lmdb c/tmp-dbi {:key-size (inc Long/BYTES)}))
    this)

  IPersistentVector

  (assocN [this i v]
    (let [mc  (memory-count this)
          cnt (+ ^long mc ^long (disk-count this))]
      (cond
        (or (< 0 i mc) (= i 0))
        (vswap! memory-vec clojure.core/assoc i v)

        (= i cnt) (.cons this v)

        :else (throw (IndexOutOfBoundsException.))))
    this)

  (cons [this v]
    (if (and (not @disk-lmdb) (< ^long @memory-pressure spill-threshold))
      (vswap! memory-vec conj v)
      (do (when-not @disk-lmdb (spill this))
          (l/transact-kv @disk-lmdb [[:put c/tmp-dbi (.length this) v :long]])))
    this)

  (length [this] (+ ^long (memory-count this) ^long (disk-count this)))

  (assoc [this k v]
    (if (integer? k)
      (.assocN this k v)
      (throw (IllegalArgumentException. "Key must be integer"))))

  (containsKey [this k]
    (if (integer? k)
      (or (contains? this k)
          (if (l/get-value @disk-lmdb c/tmp-dbi k :long) true false))
      false))

  (entryAt [this k]
    (when (integer? k)
      (if-let [v (get @memory-vec k)]
        (MapEntry. k v)
        (when-let [v (l/get-value @disk-lmdb c/tmp-dbi k :long)]
          (MapEntry. k v)))))

  (valAt [this k nf]
    (if (integer? k)
      (or (get @memory-vec k)
          (l/get-value @disk-lmdb c/tmp-dbi k :long)
          nf)
      nf))
  (valAt [this k]
    (.valAt this k nil))

  (peek [this]
    (if (zero? ^long (disk-count this))
      (clojure.core/peek @memory-vec)
      (l/get-first @disk-lmdb c/tmp-dbi [:all-back] :long :data true)))

  Object
  (finalize [_]
    (when @disk-lmdb
      (l/close-kv @disk-lmdb)
      (u/delete-files @spill-dir)))

  )

(defn new-spillable-vector
  ([] (new-spillable-vector nil))
  ([{:keys [spill-threshold spill-root]
     :or   {spill-threshold c/+default-spill-threshold+
            spill-root      c/+default-spill-root+}}]
   (when (empty? @listeners) (memory-updater))
   (->SpillableVector spill-threshold
                      spill-root
                      (volatile! nil)
                      (volatile! [])
                      (volatile! nil))))
