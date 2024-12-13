(ns ^:no-doc datalevin.async
  "Asynchronous work mechanism that does adaptive batch processing - the higher
  the load, the bigger the batch, up to batch limit"
  (:import
   [java.util.concurrent.atomic AtomicBoolean]
   [java.util.concurrent Executors ExecutorService LinkedBlockingQueue
    ConcurrentLinkedQueue ConcurrentHashMap Callable]))

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
    "Return true if this type of work cares only about the last one"))

(defprotocol IAsyncExecutor
  (start [_] "Start the async event loop")
  (stop [_] "Stop the async event loop")
  (running? [_] "Return true if this is running")
  (exec [_ work] "Submit a work, get back a future"))

(deftype WorkItem [work promise])

(deftype WorkQueue [^ConcurrentLinkedQueue items ; [WorkItem ...]
                    limit
                    last?])

(defn- do-work*
  [work]
  (try [:ok (do-work work)]
       (catch Exception e
         [:err e])))

(defn- execute-works
  [^"[Ldatalevin.async.WorkItem;" items last?]
  (let [n (alength items)]
    (when (< 0 n)
      (let [lw (.-work ^WorkItem (aget items (dec n)))]
        (try (pre-batch lw) (catch Exception _))
        (if last?
          (let [res (do-work* lw)]
            (dotimes [i n]
              (let [item ^WorkItem (aget items i)]
                (deliver (.-promise item) res))))
          (dotimes [i n]
            (let [item ^WorkItem (aget items i)]
              (deliver (.-promise item) (do-work* (.-work item))))))
        (try (post-batch lw) (catch Exception _))))))

(defn- handle-result
  [p]
  (let [[status payload] @p]
    (if (identical? status :ok)
      payload
      (throw payload))))

(defn- event-handler
  [^ExecutorService workers ^LinkedBlockingQueue event-queue
   ^ConcurrentLinkedQueue q k limit last?]
  (locking q
    (let [size (.size q)]
      (when (and (< 0 size)
                 (or (not (.contains event-queue k)) ; do nothing when busy
                     (<= ^long limit size)))
        ;; copy into an array, for iterator is only weakly consistent
        (let [items (.toArray q)]
          (.submit workers ^Callable #(execute-works items last?))
          (.clear q))))))

(deftype AsyncExecutor [^ExecutorService dispatcher
                        ^ExecutorService workers
                        ^LinkedBlockingQueue event-queue
                        ^ConcurrentHashMap work-queues ; work-key -> WorkQueue
                        ^AtomicBoolean running]
  IAsyncExecutor
  (start [_]
    (letfn [(event-loop []
              (when (.get running)
                (let [k            (.take event-queue)
                      ^WorkQueue q (.get work-queues k)
                      limit        (.-limit q)
                      last?        (.-last? q)
                      items        (.-items q)]
                  (.submit workers
                           ^Callable
                           (event-handler
                             workers event-queue items k limit last?)))
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
    (.shutdown dispatcher)
    (.shutdown workers))
  (exec [_ work]
    (let [k (work-key work)]
      (assert (keyword? k) "work-key should return a keyword")
      (.putIfAbsent work-queues k
                    (let [limit (batch-limit work)
                          last? (last-only? work)]
                      (assert (pos-int? limit)
                              "batch-limit should return a positive integer")
                      (assert (boolean? last?)
                              "last-only? should return a boolean")
                      (->WorkQueue (ConcurrentLinkedQueue.) limit last?)))
      (let [p                        (promise)
            item                     (->WorkItem work p)
            ^ConcurrentLinkedQueue q (.-items ^WorkQueue (.get work-queues k))]
        (locking q (.offer q item))
        (.offer event-queue k)
        (future (handle-result p))))))

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
