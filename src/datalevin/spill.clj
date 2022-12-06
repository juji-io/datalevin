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
   [clojure.lang IPersistentVector]))

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

(defprotocol ISpillable
  (memory-count [this] "The number of items reside in memory")
  (disk-count [this] "The number of items reside on disk")
  (spill [this] "Spill to disk"))

(deftype Spillable [spill-threshold
                    spill-root
                    spill-dir
                    memory-vec
                    disk-lmdb]
  ISpillable

  (memory-count ^long [_] (count @memory-vec))

  (disk-count ^long [_] (l/entries @disk-lmdb c/tmp-dbi))

  (spill [_]
    (let [dir (str spill-root "dtlv-spill-" (random-uuid))]
      (vreset! spill-dir dir)
      (vreset! disk-lmdb (l/open-kv dir))
      (l/open-dbi @disk-lmdb c/tmp-dbi {:key-size (inc Long/BYTES)})))

  IPersistentVector

  (assocN [this i v]
    (let [mc  (memory-count this)
          dc  (disk-count this)
          cnt (+ ^long mc ^long dc)]
      (cond
        (or (< 0 i mc)
            (= i 0)) (vswap! memory-vec assoc i v)
        (<= i cnt)   (l/transact-kv @disk-lmdb [[:put c/tmp-dbi i v :long]])
        :else        (throw (IndexOutOfBoundsException.))))
    this)

  (cons [this v]
    (l/transact-kv
      @disk-lmdb
      [[:put c/tmp-dbi (+ (memory-count this) (disk-count this)) v :long]])
    this)

  Object
  (finalize [_]
    (when @disk-lmdb
      (l/close-kv @disk-lmdb)
      (u/delete-files @spill-dir)))

  )

(defn new-spillable
  ([] (new-spillable nil))
  ([{:keys [spill-threshold spill-root]
     :or   {spill-threshold c/+default-spill-threshold+
            spill-root      c/+default-spill-root+}}]
   (when (empty? @listeners) (memory-updater))
   (->Spillable spill-threshold
                spill-root
                (volatile! nil)
                (volatile! [])
                (volatile! nil))))
