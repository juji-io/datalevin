(ns ^:no-doc datalevin.async
  "Asynchronous work mechanism that does adaptive batch processing"
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
    "Actually do the work, result will be in the future returned by exec")
  (pre-batch [_] "Preparation before the batch")
  (post-batch [_] "Cleanup after the batch")
  (batch-size [_] "The maximal allowed batch size for this type of work"))

(defprotocol IAsyncExecutor
  (start [_] "Start the async event loop")
  (stop [_] "Stop the async event loop")
  (exec [_ work] "Submit a work, get back a future"))

(deftype WorkItem [work promise])

(declare setup-work execute-works)

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
                      q ^ConcurrentLinkedQueue (.get works k)]
                  (locking q
                    (let [s (.size q)
                          l ^long (.get limits k)]
                      (when (and (< 0 s)
                                 (or (not (.contains queue k)) (<= l s)))
                        ;; copy into array, for iterator is weakly consistent
                        (let [items (.toArray q)]
                          (.submit executor ^Callable #(execute-works items))
                          (.clear q))))))
                (recur)))
            (init []
              (try (event-loop)
                   (catch Exception e
                     (when (.get running)
                       (.submit dispatcher ^Callable init)))))]
      (when-not (.get running)
        (.submit dispatcher ^Callable init)
        (.set running true))))
  (stop [_]
    (.set running false)
    (.shutdown dispatcher)
    (.shutdown executor))
  (exec [this work]
    (when-let [k (work-key work)]
      (assert (keyword? k) "work-key should return a keyword")
      (let [p (promise)]
        (setup-work this k p work)
        (future @p)))))

(defn- setup-work
  [^AsyncExecutor ae k p work]
  (let [item   (->WorkItem work p)
        works  ^ConcurrentHashMap (.-works ae)
        limits ^ConcurrentHashMap (.-limits ae)
        queue  ^LinkedBlockingQueue (.-queue ae)]
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

(defn new-async-executor
  []
  (AsyncExecutor. (Executors/newSingleThreadExecutor)
                  (Executors/newWorkStealingPool)
                  (LinkedBlockingQueue.)
                  (ConcurrentHashMap.)
                  (ConcurrentHashMap.)
                  (AtomicBoolean. false)))
