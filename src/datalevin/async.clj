(ns ^:no-doc datalevin.async
  "Asynchronous work mechanism that does adaptive batch processing - the higher
  the load, the bigger the batch, up to batch limit"
  (:import
   [java.util.concurrent.atomic AtomicBoolean]
   [java.util.concurrent Executors ExecutorService LinkedBlockingQueue
    ConcurrentLinkedDeque ConcurrentHashMap Callable TimeUnit]))

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
  (batch-limit [_] "The batch limit for this type of work")
  (last-only? [_]
    "Return true if this type of work cares only about the last one")
  (callback [_]
    "Add a callback function when the work is done. This callback takes as
     input the result of do-work."))

(deftype WorkItem [work promise])

(defprotocol ILastWork
  (get-future [_] "return the last future")
  (set-future [_ fut] "set the last future, return it"))

(deftype WorkQueue [^ConcurrentLinkedDeque items ; [WorkItem ...]
                    limit
                    last?
                    ^:volatile-mutable last-future]
  ILastWork
  (get-future [_] last-future)
  (set-future [_ fut] (set! last-future fut)))

(defn- do-work*
  [work]
  (try [:ok (do-work work)]
       (catch Exception e
         [:err e])))

(defn- execute-works
  [^ConcurrentLinkedDeque items ^WorkQueue wq]
  (let [^WorkItem last-item  (.getLast items)
        ^WorkItem first-item (.getFirst items)
        last-work            (.-work last-item)]
    (try (pre-batch last-work) (catch Exception _))
    (if (.-last? wq)
      (do (deliver (.-promise first-item) (do-work* last-work))
          (set-future wq nil))
      (doseq [^WorkItem item items]
        (deliver (.-promise item) (do-work* (.-work item)))))
    (try (post-batch last-work) (catch Exception _))))

(defn- handle-result
  [p cb]
  (let [[status payload] @p]
    (when cb (locking cb (cb payload)))
    (if (identical? status :ok)
      payload
      (throw payload))))

(defn- event-handler
  [^ExecutorService workers ^LinkedBlockingQueue event-queue k ^WorkQueue wq]
  (let [^ConcurrentLinkedDeque q (.-items wq)]
    (locking q
      (when (and (not (.isEmpty q))
                 (or (not (.contains event-queue k)) ; do nothing when busy
                     (<= ^long (.-limit wq) (.size q))))
        (execute-works q wq)
        (.clear q)))))

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
                  (.submit workers
                           ^Callable #(event-handler workers event-queue k wq)))
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
          last? (last-only? work)
          cb    (callback work)]
      (assert (keyword? k) "work-key should return a keyword")
      (assert (pos-int? limit) "batch-limit should return a positive integer")
      (assert (boolean? last?) "last-only? should return a boolean")
      (assert (or (nil? cb) (ifn? cb)) "callback should be nil or a function")
      (.putIfAbsent work-queues k
                    (->WorkQueue (ConcurrentLinkedDeque.) limit last? nil))
      (let [p                        (promise)
            item                     (->WorkItem work p)
            ^WorkQueue wq            (.get work-queues k)
            ^ConcurrentLinkedDeque q (.-items wq)]
        (locking q
          (.offer q item)
          (.offer event-queue k)
          (if last?
            (if-let [fut (get-future wq)]
              fut
              (set-future wq (future (handle-result p cb))))
            (future (handle-result p cb))))))))

(defn- new-async-executor
  []
  (->AsyncExecutor (Executors/newSingleThreadExecutor)
                   (Executors/newWorkStealingPool)
                   (LinkedBlockingQueue.)
                   (ConcurrentHashMap.)
                   (AtomicBoolean. false)))

(defonce executor-atom (atom nil))

(defn get-executor
  "access the async executor"
  []
  (let [e @executor-atom]
    (if (or (nil? e) (not (running? e)))
      (let [new-e (new-async-executor)]
        (reset! executor-atom new-e)
        (start new-e)
        new-e)
      e)))

(defn shutdown-executor [] (when-let [e @executor-atom] (stop e)))
