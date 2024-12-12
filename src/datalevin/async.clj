(ns ^:no-doc datalevin.async
  "Asynchronous work mechanism that does adaptive batch processing - the higher
  the load, the bigger the batch, up to batch limit"
  (:require
   [datalevin.util :as u])
  (:import
   [java.util.concurrent.atomic AtomicBoolean]
   [java.util.concurrent ExecutorService LinkedBlockingQueue
    ConcurrentLinkedQueue ConcurrentHashMap Callable]))

(defprotocol IAsyncWork
  "Work that wishes to be done asynchronously and auto-batched needs to
  implement this protocol"
  (work-key [_]
    "Return a keyword representing this type of work, workloads of the same
    type will be batched")
  (do-work [_]
    "Actually do the work, result will be in the future returned by exec")
  (pre-batch [_] "Preparation before the batch")
  (post-batch [_] "Cleanup after the batch")
  (batch-size [_] "The batch limit for this type of work"))

(defprotocol IAsyncExecutor
  (start [_] "Start the async event loop")
  (stop [_] "Stop the async event loop")
  (exec [_ work] "Submit a work, get back a future"))

(deftype WorkItem [work promise])

(defn- setup-work
  [^ConcurrentHashMap works ^ConcurrentHashMap limits
   ^LinkedBlockingQueue queue p k work]
  (let [item (->WorkItem work p)]
    (.putIfAbsent limits k (batch-size work))
    (.putIfAbsent works k (ConcurrentLinkedQueue.))
    (let [^ConcurrentLinkedQueue q (.get works k)]
      (locking q
        (.offer q item)
        (.offer queue k)))))

(defn- execute-works
  [^"[Ldatalevin.async.WorkItem;" items]
  (let [n (alength items)]
    (when (< 0 n)
      (let [fw (.-work ^WorkItem (aget items 0))]
        (pre-batch fw)
        (dotimes [i n]
          (let [item ^WorkItem (aget items i)]
            (deliver (.-promise item) (do-work (.-work item)))))
        (post-batch fw)))))

(defn- event-handler
  [^ExecutorService executor ^LinkedBlockingQueue queue ^ConcurrentLinkedQueue q
   k limit]
  (locking q
    (let [size (.size q)]
      (when (and (< 0 size)
                 (or (not (.contains queue k)) ; do nothing when busy
                     (<= ^long limit size)))
        ;; copy into an array, for iterator is only weakly consistent
        (let [items (.toArray q)]
          (.submit executor ^Callable #(execute-works items))
          (.clear q))))))

(deftype AsyncExecutor [^ExecutorService dispatcher ; event dispatcher
                        ^ExecutorService executor   ; workers
                        ^LinkedBlockingQueue queue  ; event queue
                        ^ConcurrentHashMap works  ; work-key -> WorkItem queue
                        ^ConcurrentHashMap limits ; work-key -> batch size limit
                        ^AtomicBoolean running]
  IAsyncExecutor
  (start [_]
    (letfn [(event-loop []
              (when (.get running)
                (let [k (.take queue)
                      q (.get works k)
                      l (.get limits k)]
                  (.submit executor
                           ^Callable (event-handler executor queue q k l)))
                (recur)))
            (init []
              (try (event-loop)
                   (catch Exception _
                     (when (.get running)
                       (.submit dispatcher ^Callable init)))))]
      (when-not (.get running)
        (.submit dispatcher ^Callable init)
        (.set running true))))
  (stop [_]
    (.set running false)
    (.shutdown dispatcher)
    (.shutdown executor))
  (exec [_ work]
    (when-let [k (work-key work)]
      (assert (keyword? k) "work-key should return a keyword")
      (let [p (promise)]
        (setup-work works limits queue p k work)
        (future @p)))))

(defn new-async-executor
  []
  (->AsyncExecutor (u/get-async-event-dispatcher)
                   (u/get-async-worker-pool)
                   (LinkedBlockingQueue.)
                   (ConcurrentHashMap.)
                   (ConcurrentHashMap.)
                   (AtomicBoolean. false)))
