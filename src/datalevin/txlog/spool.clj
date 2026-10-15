(ns ^:no-doc datalevin.txlog.spool
  "Spillable pending bytes for atomic bulk WAL transactions. Scratch bytes are
  never replayed; commit appends them through the ordinary WAL insertion path."
  (:require [datalevin.txlog.codec :as codec])
  (:import [datalevin.cpp UnsafeAccess]
           [java.io Closeable]
           [java.nio ByteBuffer]
           [java.nio.channels ClosedChannelException FileChannel FileChannel$MapMode]
           [java.nio.file Files Path StandardOpenOption]
           [java.nio.file.attribute FileAttribute]))

(def ^:const default-memory-limit (* 8 1024 1024))

(deftype PendingSpool [^long memory-limit state]
  clojure.lang.Counted
  (count [_] (int (:row-count @state)))
  Closeable
  (close [_]
    (let [{:keys [channel path]} @state]
      (vswap! state assoc :closed? true :memory nil)
      (when channel
        (try
          (.close ^FileChannel channel)
          (finally (Files/deleteIfExists ^Path path)))))))

(defn spool? [x] (instance? PendingSpool x))

(defn check!
  "Reject a failed or closed buffer, including failures before its first row."
  [^PendingSpool spool]
  (let [{:keys [failure closed?]} @(.-state spool)]
    (when failure (throw failure))
    (when closed? (throw (ClosedChannelException.)))))

(defn close-quietly!
  "Cleanup must not prevent the caller from aborting its native transaction."
  [^PendingSpool spool]
  (try (.close spool) (catch Throwable _)))

(defn create!
  "Buffer owned WAL bytes in memory up to memory-limit, then spill to disk.
  Creating an empty buffer does not allocate memory for rows or open a file."
  (^PendingSpool [] (create! default-memory-limit))
  (^PendingSpool [memory-limit]
   (when-not (and (integer? memory-limit)
                  (<= 0 memory-limit
                      (- Integer/MAX_VALUE codec/max-commit-row-header-size)))
     (throw (IllegalArgumentException. "Invalid pending WAL memory limit")))
   (PendingSpool. (long memory-limit)
                  (volatile! {:row-count 0 :bytes 0 :major 1}))))

(defn spilled?
  "Whether this buffer has crossed its memory limit and opened a scratch file."
  [^PendingSpool spool]
  (some? (:channel @(.-state spool))))

(defn memory-byte-count
  "Retained buffer capacity, including the reserved commit header."
  ^long [^PendingSpool spool]
  (if-let [^ByteBuffer memory (:memory @(.-state spool))]
    (.capacity memory)
    0))

(defn- create-scratch-file!
  []
  (let [path (Files/createTempFile "datalevin-wal-pending-" ".tmp"
                                   (make-array FileAttribute 0))]
    (try
      {:path path
       :channel (FileChannel/open path
                  (into-array StandardOpenOption
                    [StandardOpenOption/READ StandardOpenOption/WRITE
                     StandardOpenOption/DELETE_ON_CLOSE]))}
      (catch Throwable failure
        (try (Files/deleteIfExists path)
             (catch Throwable cleanup (.addSuppressed failure cleanup)))
        (throw failure)))))

(defn byte-count
  "Number of buffered row bytes, excluding the reserved header."
  ^long [^PendingSpool spool]
  (:bytes @(.-state spool)))

(defn- write-buffer!
  [^FileChannel channel ^ByteBuffer body ^long offset]
  (loop [offset offset]
    (when (.hasRemaining body)
      (recur (+ offset (.write channel body offset))))))

(defn- spill!
  ^FileChannel [^PendingSpool spool]
  (let [state (.-state spool)
        {:keys [memory bytes]} @state
        {:keys [channel] :as scratch} (create-scratch-file!)]
    ;; Own the channel before writing, so any failed transfer is cleaned up by
    ;; native abort. Until the whole prefix is copied, retain its memory too.
    (vswap! state merge scratch)
    (when memory
      (let [prefix (.duplicate ^ByteBuffer memory)]
        (.limit prefix (int (+ codec/max-commit-row-header-size (long bytes))))
        (.position prefix codec/max-commit-row-header-size)
        (write-buffer! channel prefix codec/max-commit-row-header-size)))
    (vswap! state dissoc :memory)
    channel))

(defn- append-memory!
  [^PendingSpool spool ^ByteBuffer body ^long new-bytes]
  (let [state (.-state spool)
        {:keys [memory bytes]} @state
        needed (+ codec/max-commit-row-header-size new-bytes)
        ^ByteBuffer memory
        (if (and memory (<= needed (.capacity ^ByteBuffer memory)))
          memory
          (let [capacity (min (+ (.-memory-limit spool) codec/max-commit-row-header-size)
                              (max needed (if memory
                                            (* 2 (.capacity ^ByteBuffer memory))
                                            8192)))
                expanded (ByteBuffer/allocate (int capacity))]
            (when memory
              (let [prefix (.duplicate ^ByteBuffer memory)]
                (.clear prefix)
                (.limit prefix (int (+ codec/max-commit-row-header-size (long bytes))))
                (.put expanded prefix)))
            (vswap! state assoc :memory expanded)
            expanded))]
    (.limit memory (.capacity memory))
    (.position memory (int (+ codec/max-commit-row-header-size (long bytes))))
    (.put memory body)))

(defn append!
  "Encode one batch before its row objects or encoding arena can be reused.
  A failed write poisons this spool so a caught error cannot commit a prefix."
  [^PendingSpool spool rows]
  (check! spool)
  (let [state (.-state spool)
        {:keys [row-count bytes major channel]} @state]
    (try
      (let [^ByteBuffer body (codec/encode-commit-row-payload-buffer 0 0 rows)]
        (try
          (let [region (codec/commit-row-payload-region body)
                new-count (+ (long row-count) (long (:op-count region)))
                new-bytes (+ (long bytes) (- (.limit body) (long (:offset region))))]
            (when (or (> new-count Integer/MAX_VALUE)
                      (> (+ new-bytes codec/max-commit-row-header-size)
                         Integer/MAX_VALUE))
              (throw (ex-info "Pending WAL transaction exceeds the record size limit"
                              {:type :txlog/payload-overflow :outcome :not-committed})))
            (.position body (int (:offset region)))
            (when (.hasRemaining body)
              (if (or channel (> new-bytes (.-memory-limit spool)))
                (write-buffer! (or channel (spill! spool)) body
                               (+ codec/max-commit-row-header-size (long bytes)))
                (append-memory! spool body new-bytes)))
            (vswap! state assoc :row-count new-count :bytes new-bytes
                    :major (max (long major) (long (:major region)))))
          (finally (codec/release-commit-row-payload-buffer! body))))
      (catch Throwable failure
        (vswap! state assoc :failure failure)
        (throw failure))))
  spool)

(defn payload-buffer!
  "Return a memory view or root file mapping of the completed payload. No
  combined payload copy is needed. Release it with release-payload-buffer!
  after synchronous insertion; the buffer still lives until commit or abort."
  ^ByteBuffer [^PendingSpool spool opts]
  (check! spool)
  (let [{:keys [major row-count bytes channel memory]} @(.-state spool)
        ^ByteBuffer header (codec/encode-commit-row-header major row-count opts)
        header-size (.remaining header)
        start (- codec/max-commit-row-header-size header-size)]
    (if channel
      (do
        (write-buffer! channel header start)
        (.map ^FileChannel channel FileChannel$MapMode/READ_WRITE
              start (+ header-size (long bytes))))
      (let [body (if memory
                   (.duplicate ^ByteBuffer memory)
                   (ByteBuffer/allocate codec/max-commit-row-header-size))]
        (.position body (int start))
        (.put body header)
        (.limit body (int (+ codec/max-commit-row-header-size (long bytes))))
        (.position body (int start))
        body))))

(defn release-payload-buffer!
  "Release a root file mapping after insertion; memory views need no cleanup."
  [^ByteBuffer body]
  ;; As with other direct-buffer cleanup, fall back to the JVM cleaner. A
  ;; cleanup failure must not turn a completed WAL append into an abort.
  (try (UnsafeAccess/clean body) (catch Throwable _)))
