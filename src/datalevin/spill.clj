(ns ^:no-doc datalevin.spill
  "A mutable vector that spills to disk automatically
  when memory pressure is high. Presents a `IPersistentVector` API
  for compatibility and convenience."
  (:require
   [datalevin.constants :as c]
   [datalevin.util :as u]
   [datalevin.lmdb :as l]
   [taoensso.nippy :as nippy])
  (:import
   [java.util Iterator List UUID NoSuchElementException]
   [java.io DataInput DataOutput]
   [java.lang.management ManagementFactory]
   [javax.management NotificationEmitter NotificationListener Notification]
   [com.sun.management GarbageCollectionNotificationInfo]
   [org.eclipse.collections.impl.list.mutable FastList]
   [clojure.lang ISeq IPersistentVector MapEntry Util Sequential]))

(defonce memory-pressure (volatile! 0))

(defonce ^Runtime runtime (Runtime/getRuntime))

(defn- set-memory-pressure []
  (let [fm (.freeMemory runtime)
        tm (.totalMemory runtime)
        mm (.maxMemory runtime)
        pr (int (/ (- tm fm) mm))]
    ;; (println "used" pr "% of" (int (/ mm (* 1024 1024))))
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

(declare ->Seq ->RSeq)

(deftype SpillableVector [^long spill-threshold
                          ^String spill-root
                          ^String spill-dir
                          ^FastList memory
                          disk
                          total]
  ISpillable

  (memory-count ^long [_] (.size memory))

  (disk-count ^long [_]
    (if @disk (l/entries @disk c/tmp-dbi) 0))

  (spill [this]
    (let [dir (str spill-root "dtlv-spill-" (UUID/randomUUID))]
      (vreset! spill-dir dir)
      (vreset! disk (l/open-kv dir {:temp? true}))
      (l/open-dbi @disk c/tmp-dbi {:key-size (inc Long/BYTES)}))
    this)

  IPersistentVector

  (assocN [this i v]
    (let [mc (memory-count this)]
      (cond
        (= i @total)            (.cons this v)
        (or (< 0 i mc) (= i 0)) (.add memory i v)
        :else                   (throw (IndexOutOfBoundsException.))))
    this)

  (cons [this v]
    (if (and (nil? @disk) (< ^long @memory-pressure spill-threshold))
      (.add memory v)
      (do (when (nil? @disk) (spill this))
          (l/transact-kv @disk [[:put c/tmp-dbi @total v :long]])))
    (vswap! total #(inc ^long %))
    this)

  (length [_] @total)

  (assoc [this k v]
    (if (integer? k)
      (.assocN this k v)
      (throw (IllegalArgumentException. "Key must be integer"))))

  (containsKey [this k]
    (if (integer? k)
      (or (if (get this k) true false)
          (if @disk
            (if (l/get-value @disk c/tmp-dbi k :long) true false)
            false))
      false))

  (entryAt [this k]
    (when (integer? k)
      (if-let [v (.get memory k)]
        (MapEntry. k v)
        (when-let [v (l/get-value @disk c/tmp-dbi k :long)]
          (MapEntry. k v)))))

  (valAt [this k nf]
    (if (integer? k)
      (or (when-not (.isEmpty memory) (.get memory k))
          (when @disk (l/get-value @disk c/tmp-dbi k :long))
          nf)
      nf))
  (valAt [this k]
    (.valAt this k nil))

  (peek [this]
    (if (zero? ^long (disk-count this))
      (.getLast memory)
      (l/get-first @disk c/tmp-dbi [:all-back] :long :data true)))

  (pop [this]
    (cond
      (zero? ^long @total)
      (throw (IllegalStateException. "Can't pop empty vector"))

      (< 0 ^long (disk-count this))
      (let [[lk _] (l/get-first @disk c/tmp-dbi [:all-back]
                                :long :ignore)]
        (l/transact-kv @disk [[:del c/tmp-dbi lk :long]]))

      :else (.remove memory (dec ^long (memory-count this))))
    (vswap! total #(dec ^long %))
    this)

  (count [this] @total)

  (empty [this]
    (.clear memory)
    (when @disk
      (l/close-kv @disk)
      (u/delete-files @spill-dir)
      (vreset! disk nil))
    this)

  (equiv [this other]
    (cond
      (identical? this other) true

      (or (instance? Sequential other) (instance? List other))
      (if (not= (count this) (count other))
        false
        (if (every? true? (map #(Util/equiv %1 %2) this other))
          true
          false))

      :else false))

  (seq ^ISeq [this] (when (< 0 ^long @total) (->Seq this 0)))

  (rseq ^ISeq [this]
    (when (< 0 ^long @total) (->RSeq this (dec ^long @total))))

  (nth [this i]
    (if (and (<= 0 i) (< i ^long @total))
      (.valAt this i)
      (throw (IndexOutOfBoundsException.))))
  (nth [this i nf]
    (if (and (<= 0 i) (< i ^long @total))
      (.nth this i)
      nf))

  Iterable

  (iterator [this]
    (let [i (volatile! 0)]
      (reify
        Iterator
        (hasNext [_] (< ^long @i ^long @total))
        (next [_]
          (if (< ^long @i ^long @total)
            (let [res (.nth this @i)]
              (vswap! i #(inc ^long %))
              res)
            (throw (NoSuchElementException.)))))))

  Object

  (toString [this] (str (into [] this)))

  (finalize ^void [_]
    (when @disk
      (l/close-kv @disk)
      (u/delete-files @spill-dir))))

(deftype Seq [^SpillableVector v
              ^long i]
  ISeq

  (seq ^ISeq [this] this)

  (first ^Object [_] (nth v i))

  (next ^ISeq [_]
    (let [i+1 (inc i)]
      (when (< i+1 (count v)) (->Seq v i+1))))

  (more ^ISeq [this] (let [s (.next this)] (if s s '())))

  (cons [_ _] (throw "Changing SpillableVector.Seq is not supported"))

  (count [_] (- (count v) i))

  (equiv [this other]
    (if (or (instance? List other) (instance? Sequential other))
      (if (not= (count this) (count other))
        false
        (if (every? true? (map #(Util/equiv %1 %2) this other))
          true
          false))
      false)))

(deftype RSeq [^SpillableVector v
               ^long i]
  ISeq

  (seq ^ISeq [this] this)

  (first ^Object [this] (nth v i))

  (next ^ISeq [this] (when (< 0 i) (->RSeq v (dec i))))

  (more ^ISeq [this] (let [s (.next this)] (if s s '())))

  (cons [_ _] (throw "Changing SpillableVector.RSeq is not supported"))

  (count [_] (inc i))

  (equiv [this other]
    (if (or (instance? List other)
            (instance? Sequential other))
      (if (not= (count this) (count other))
        false
        (if (every? true? (map #(Util/equiv %1 %2) this other))
          true
          false))
      false)))

(defn new-spillable-vector
  ([] (new-spillable-vector nil nil))
  ([vs] (new-spillable-vector vs nil))
  ([vs {:keys [spill-threshold spill-root]
        :or   {spill-threshold c/+default-spill-threshold+
               spill-root      c/+default-spill-root+}}]
   (when (empty? @listeners) (memory-updater))
   (let [svec (->SpillableVector spill-threshold
                                 spill-root
                                 (volatile! nil)
                                 (FastList.)
                                 (volatile! nil)
                                 (volatile! 0))]
     (doseq [v vs] (conj svec v))
     svec)))

(nippy/extend-freeze
  SpillableVector :spillable
  [^SpillableVector x ^DataOutput out]
  (let [n (count x)]
    (.writeLong out n)
    (dotimes [i n] (nippy/freeze-to-out! out (nth x i)))))

(nippy/extend-thaw
  :spillable
  [^DataInput in]
  (let [n  (.readLong in)
        vs (new-spillable-vector)]
    (dotimes [_ n] (conj vs (nippy/thaw-from-in! in)))
    vs))
