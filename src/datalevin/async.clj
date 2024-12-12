(ns ^:no-doc datalevin.async
  "Asynchronous work mechanism that does adaptive batch processing - the higher
  the load, the bigger the batch, up to batch limit"
  (:require
   [datalevin.util :as u])
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
  (batch-limit [_] "The batch limit for this type of work"))

(defprotocol IAsyncExecutor
  (start [_] "Start the async event loop")
  (stop [_] "Stop the async event loop")
  (running? [_] "Return true if this is running")
  (exec [_ work] "Submit a work, get back a future"))

(deftype WorkItem [work promise])

(defn- setup-work
  [^ConcurrentHashMap works ^ConcurrentHashMap limits
   ^LinkedBlockingQueue queue p k work]
  (let [item  (->WorkItem work p)
        limit (batch-limit work)]
    (assert (pos-int? limit) "batch-limit should return a positive integer")
    (.putIfAbsent limits k limit)
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
        (try (pre-batch fw) (catch Exception _))
        (dotimes [i n]
          (let [item ^WorkItem (aget items i)]
            (deliver (.-promise item)
                     (try [:ok (do-work (.-work item))]
                          (catch Exception e
                            [:err e])))))
        (try (post-batch fw) (catch Exception _))))))

(defn- handle-result
  [p]
  (let [[status payload] @p]
    (if (identical? status :ok)
      payload
      (throw payload))))

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
  (running? [_] (.get running))
  (stop [_]
    (.set running false)
    (.shutdown dispatcher)
    (.shutdown executor))
  (exec [_ work]
    (when-let [k (work-key work)]
      (assert (keyword? k) "work-key should return a keyword")
      (let [p (promise)]
        (setup-work works limits queue p k work)
        (future (handle-result p))))))

(defn- new-async-executor
  []
  (->AsyncExecutor (Executors/newSingleThreadExecutor)
                   (Executors/newWorkStealingPool)
                   (LinkedBlockingQueue.)
                   (ConcurrentHashMap.)
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
