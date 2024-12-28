(ns ^:no-doc datalevin.async
  "Asynchronous work mechanism that does adaptive batch processing - the higher
  the load, the bigger the batch, up to batch limit"
  (:require
   [clojure.stacktrace :as stt])
  (:import
   [java.util.concurrent.atomic AtomicBoolean AtomicLong]
   [java.util.concurrent Executors ExecutorService LinkedBlockingQueue
    ConcurrentLinkedQueue ConcurrentHashMap Callable TimeUnit]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defprotocol IAsyncWork
  "Work that wishes to be done asynchronously and auto-batched needs to
  implement this protocol"
  (work-key [_]
    "Return a keyword representing this type of work, workloads of the same
    type will be batched")
  (do-work [_]
    "Actually do the work, result will be in the future returned by exec.
    If there's an exception, it will be thrown when deref the future.")
  (pre-batch [_]
    "Preparation before the batch. Should handle its own exception, for
    the system will ignore it.")
  (post-batch [_]
    "Cleanup after the batch. Should handle its own exception.")
  (batch-limit [_] "The upper limit of batch size for this type of work")
  (combine [_]
    "Return a function that takes a collection of this work and combine them
     into one. Or return nil if there's no need to combine.")
  (callback [_]
    "Add a callback for when a work is done. This callback takes as
     input the result of do-work. This could be nil."))

(deftype WorkItem [work promise])

(deftype WorkQueue [^ConcurrentLinkedQueue items ; [WorkItem ...]
                    ^long limit
                    ^AtomicLong size
                    ^FastList stage])  ; for combining work

(defn- do-work*
  [work]
  (try [:ok (do-work work)]
       (catch Exception e
         [:err e])))

(defn- handle-result
  [p cb]
  (let [[status payload] @p]
    (when cb (locking cb (cb payload)))
    (if (identical? status :ok)
      payload
      (throw payload))))

(defn- individual-work
  [^ConcurrentLinkedQueue items ^long limit ^AtomicLong size]
  (loop [i 0]
    (when (and (.peek items) (< i limit))
      (let [^WorkItem item (.poll items)]
        (.decrementAndGet size)
        (deliver (.-promise item) (do-work* (.-work item))))
      (recur (inc i)))))

(defn- combined-work
  [cmb ^ConcurrentLinkedQueue items limit ^AtomicLong size ^FastList stage]
  (.clear stage)
  (loop [i 0]
    (when (and (.peek items) (< i ^long limit))
      (let [^WorkItem item (.poll items)]
        (.decrementAndGet size)
        (.add stage item))
      (recur (inc i))))
  (let [res (do-work* (cmb (mapv #(.-work ^WorkItem %) stage)))]
    (mapv #(deliver (.-promise ^WorkItem %) res) stage)))

(defn- event-handler
  [^LinkedBlockingQueue event-queue k ^WorkQueue wq]
  (let [^ConcurrentLinkedQueue items (.-items wq)
        limit                        (.-limit wq)
        ^AtomicLong size             (.-size wq)]
    (locking items
      (when (or (<= limit (.get size)) (not (.contains event-queue k)))
        (let [^WorkItem first-item (.peek items)
              first-work           (.-work first-item)]
          (try (pre-batch first-work)
               (catch Exception e
                 (stt/print-stack-trace e)))
          (if-let [cmb (combine first-work)]
            (combined-work cmb items limit size (.-stage wq))
            (individual-work items limit size))
          (try (post-batch first-work)
               (catch Exception e
                 (stt/print-stack-trace e))))))))

(defprotocol IAsyncExecutor
  (start [_] "Start the async event loop")
  (stop [_] "Stop the async event loop")
  (running? [_] "Return true if this is running")
  (exec [_ work] "Submit a work, get back a future"))

(deftype AsyncExecutor [^ExecutorService dispatcher
                        ^ExecutorService workers
                        ^LinkedBlockingQueue event-queue
                        ^ConcurrentHashMap work-queues ; work-key -> WorkQueue
                        ^AtomicBoolean running]
  IAsyncExecutor
  (start [_]
    (letfn [(event-loop []
              (when (.get running)
                (let [k  (.take event-queue)
                      wq (.get work-queues k)]
                  (.submit workers ^Callable #(event-handler event-queue k wq)))
                (recur)))
            (init []
              (try (event-loop)
                   (catch Exception _
                     (when (.get running)
                       (.submit dispatcher ^Callable init)))))]
      (when-not (.get running)
        (.submit dispatcher ^Callable init)
        (.set running true))))
  (running? [_] (.get running))
  (stop [_]
    (.set running false)
    (.shutdownNow dispatcher)
    (.shutdownNow workers)
    (.awaitTermination dispatcher 5 TimeUnit/MILLISECONDS)
    (.awaitTermination workers 5 TimeUnit/MILLISECONDS))
  (exec [_ work]
    (let [k     (work-key work)
          limit (batch-limit work)
          cmb   (combine work)
          cb    (callback work)]
      (assert (keyword? k) "work-key should return a keyword")
      (assert (pos-int? limit) "batch-limit should return a positive integer")
      (assert (or (nil? cmb) (ifn? cmb)) "combine should be nil or a function")
      (assert (or (nil? cb) (ifn? cb)) "callback should be nil or a function")
      (.putIfAbsent work-queues k
                    (->WorkQueue (ConcurrentLinkedQueue.)
                                 limit
                                 (AtomicLong.)
                                 (when cmb (FastList. (int limit)))))
      (let [p                        (promise)
            item                     (->WorkItem work p)
            ^WorkQueue wq            (.get work-queues k)
            ^ConcurrentLinkedQueue q (.-items wq)
            ^AtomicLong s            (.-size wq)]
        (.offer q item)
        (.incrementAndGet s)
        (.offer event-queue k)
        (future (handle-result p cb))))))

(defn- new-async-executor
  []
  (->AsyncExecutor (Executors/newSingleThreadExecutor)
                   (Executors/newWorkStealingPool)
                   (LinkedBlockingQueue.)
                   (ConcurrentHashMap.)
                   (AtomicBoolean. false)))

(defonce executor-atom (atom nil))

(defn- new-executor
  []
  (let [new-e (new-async-executor)]
    (reset! executor-atom new-e)
    (start new-e)
    new-e))

(defn get-executor
  "access the async executor"
  []
  (let [e @executor-atom]
    (if (and e (running? e))
      e
      (new-executor))))

(defn shutdown-executor [] (when-let [e @executor-atom] (stop e)))
