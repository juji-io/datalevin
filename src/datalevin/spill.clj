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
   [java.lang Runtime]
   [java.lang.management ManagementFactory]
   [javax.management NotificationEmitter NotificationListener Notification]
   [com.sun.management GarbageCollectionNotificationInfo]))

(defonce memory-pressure (volatile! 0))

(defonce ^Runtime runtime (Runtime/getRuntime))

(defn- set-memory-pressure []
  (let [fm (.freeMemory runtime)
        tm (.totalMemory runtime)
        mm (.maxMemory runtime)
        pr (int (/ (- tm fm) mm))]
    (vreset! memory-pressure pr)))

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
                    spill-path
                    spill-file
                    memory-vec
                    disk-lmdb]
  ISpillable
  (memory-count [_] (count @memory-vec))

  (disk-count [_] (l/entries @disk-lmdb c/tmp-dbi))

  (spill [_]
    (let [fp (str spill-path "datalevin-spill-" (random-uuid))]
      (vreset! spill-file fp)
      (vreset! disk-lmdb (l/open-kv fp {:flags c/tmp-env-flags}))))

  Object
  (finalize [_]
    (when @disk-lmdb
      (l/close-kv @disk-lmdb)
      (u/delete-files @spill-file)))

  )

(defn new-spillable
  [{:keys [spill-threshold spill-path]
    :or   {spill-threshold c/+default-spill-threshold+
           spill-path      c/+default-spill-path+}}]
  (when (empty? @listeners) (memory-updater))
  (->Spillable spill-threshold
               spill-path
               (volatile! nil)
               (volatile! [])
               (volatile! nil)))
