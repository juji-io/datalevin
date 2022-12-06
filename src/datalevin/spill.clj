(ns ^:no-doc datalevin.spill
  "KV range query results that spills to disk automatically when there is
  not enough available free memory. Present an `IPersistentVector` API"
  (:require
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.util :refer [raise]]
   [datalevin.lmdb :as l]
   [clojure.stacktrace :as st])
  (:import
   [java.lang Runtime]
   [java.lang.management ManagementFactory]
   [javax.management NotificationEmitter NotificationListener Notification]
   [com.sun.management GarbageCollectionNotificationInfo]))

(defonce free-memory (volatile! Long/MAX_VALUE))

(defonce runtime (Runtime/getRuntime))

(defn install-memory-updater []
  (doseq [^NotificationEmitter gcbean
          (ManagementFactory/getGarbageCollectorMXBeans)]
    (let [obj (random-uuid)
          ^NotificationListener listener
          (reify NotificationListener
            (^void handleNotification [this ^Notification notif obj]
             (when (= (.getType notif)
                      GarbageCollectionNotificationInfo/GARBAGE_COLLECTION_NOTIFICATION)
               (let [fm (.freeMemory ^Runtime runtime)]
                 (println "free memory:" fm)
                 (vreset! free-memory fm)))))]
      (.addNotificationListener gcbean listener nil obj))))

(defprotocol ISpillable
  (memory-count [this] "The number of items reside in memory")
  (disk-count [this] "The number of items reside on disk"))

(deftype Spillable [spill-threshold
                    spill-interval
                    memory-vec
                    disk-lmdb]
  ISpillable
  (memory-count [_] (count @memory-vec))

  (disk-count [_] (l/entries @disk-lmdb c/tmp-dbi))


  )

(comment

  (install-memory-updater)

  )
