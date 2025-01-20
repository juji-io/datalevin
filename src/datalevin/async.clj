(ns ^:no-doc datalevin.async
  "Asynchronous work mechanism that does adaptive batch processing - the higher
  the load, the bigger the batch"
  (:import
   [java.util.concurrent.atomic AtomicBoolean]
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
  (combine [_]
    "Return a function that takes a collection of this work and combine them
     into one. Or return nil if there's no need to combine.")
  (callback [_]
    "Return a callback for when a work is done. This callback takes as
     input the result of do-work. This could be nil."))

(deftype WorkItem [work promise])

(deftype WorkQueue [^ConcurrentLinkedQueue items  ; [WorkItem ...]
                    fw ; first work
                    ^FastList stage ; for combining work
                    cb])

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
  [^ConcurrentLinkedQueue items]
  (loop []
    (when (.peek items)
      (let [^WorkItem item (.poll items)]
        (deliver (.-promise item) (do-work* (.-work item))))
      (recur))))

(defn- combined-work
  [cmb ^ConcurrentLinkedQueue items ^FastList stage]
  (.clear stage)
  (loop []
    (when (.peek items)
      (let [^WorkItem item (.poll items)]
        (.add stage item))
      (recur)))
  (let [res (do-work* (cmb (mapv #(.-work ^WorkItem %) stage)))]
    (dotimes [i (.size stage)]
      (deliver (.-promise ^WorkItem (.get stage i)) res))))

(defn- event-handler
  [^ConcurrentHashMap work-queues k]
  (let [^WorkQueue wq                (.get work-queues k)
        ^ConcurrentLinkedQueue items (.-items wq)
        first-work                   (.-fw wq)]
    (locking items
      (if-let [cmb (combine first-work)]
        (combined-work cmb items (.-stage wq))
        (individual-work items)))))

(defn- new-workqueue
  [work]
  (let [cmb (combine work)
        cb  (callback work)]
    (assert (or (nil? cmb) (ifn? cmb)) "combine should be nil or a function")
    (assert (or (nil? cb) (ifn? cb)) "callback should be nil or a function")
    (->WorkQueue (ConcurrentLinkedQueue.) work (when cmb (FastList.)) cb)))

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
                (let [k (.take event-queue)]
                  (when-not (.contains event-queue k) ; do nothing when busy
                    (.submit workers ^Callable #(event-handler work-queues k))))
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
    (let [k (work-key work)]
      (assert (keyword? k) "work-key should return a keyword")
      (.putIfAbsent work-queues k (new-workqueue work))
      (let [p                        (promise)
            item                     (->WorkItem work p)
            ^WorkQueue wq            (.get work-queues k)
            ^ConcurrentLinkedQueue q (.-items wq)]
        (.offer q item)
        (.offer event-queue k)
        (future (handle-result p (.-cb wq)))))))

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
