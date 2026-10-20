(ns datalevin.client-recovery-chaos
  "Real TCP fault injection for long-lived non-HA clients. No client or remote
  handle is recreated after setup. Seeds choose fault order; failures include
  operation outcomes, proxy events, and the completed recovery history."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [datalevin.client :as cl]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.interpret :as inter]
   [datalevin.protocol :as p]
   [datalevin.server :as srv]
   [datalevin.server.resources :as resources]
   [datalevin.server.session :as session]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [datalevin.client Client ConnectionPool ConnectionSlot IClientInternals]
   [datalevin.db DB]
   [datalevin.remote DatalogStore KVStore]
   [datalevin.server Server]
   [java.io DataInputStream ByteArrayOutputStream DataOutputStream EOFException IOException]
   [java.net InetSocketAddress]
   [java.nio.channels ServerSocketChannel SocketChannel]
   [java.nio.file Files]
   [java.util ArrayList Collections Random]
   [java.util.concurrent Callable ConcurrentHashMap CountDownLatch ExecutorService
    Executors Future ThreadFactory TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicLong]))

(def faults
  [:reset :blackhole :registration-timeout :expire-sessions :restart
   :lost-write-reply :truncated-reply :authentication-timeout
   :copy-in-cut :copy-out-cut])

(def ^:private schema
  {:chaos/id {:db/valueType :db.type/long :db/unique :db.unique/identity}
   :chaos/value {:db/valueType :db.type/string}})

(def ^:private query
  '[:find ?id ?value :where [?e :chaos/id ?id] [?e :chaos/value ?value]])

(def ^:private increment (inter/inter-fn [x] (inc x)))

(defn- ensure! [pred message data]
  (when-not pred (throw (ex-info message (assoc data ::invariant? true)))))

(defn- await-condition! [pred timeout-ms message]
  (let [deadline (+ (System/nanoTime) (* 1000000 (long timeout-ms)))]
    (loop []
      (when-not (pred)
        (ensure! (< (System/nanoTime) deadline) message {})
        (Thread/sleep 5)
        (recur)))))

(defn- executor [label]
  (let [sequence (AtomicInteger.)]
    (Executors/newCachedThreadPool
      (reify ThreadFactory
        (newThread [_ task]
          (doto (Thread. task (str "datalevin-chaos-" label "-" (.incrementAndGet sequence)))
            (.setDaemon true)))))))

(defn- shut-down! [^ExecutorService executor]
  (.shutdownNow executor)
  (ensure! (.awaitTermination executor 10 TimeUnit/SECONDS)
           "Chaos executor leaked a blocked task" {}))

(defn- safe-close! [^java.io.Closeable resource]
  (when resource (try (.close resource) (catch IOException _ nil))))

(defn- event! [proxy event]
  (swap! (:events proxy) #(vec (take-last 100 (conj % event)))))

(defn- progress! [ctx stage & [details]]
  (when-let [report! (:report-progress! ctx)]
    (report! (merge {:seed (:seed ctx) :stage stage :server-root (:root ctx)}
                    @(:current ctx) details))))

(defn- policy! [proxy policy]
  (locking (:policy proxy)
    (reset! (:policy proxy) policy)
    (.notifyAll ^Object (:policy proxy))))

(defn- close-flow! [proxy flow]
  (when (.compareAndSet ^AtomicBoolean (:closed? flow) false true)
    (safe-close! (:front flow))
    (safe-close! (:back flow))
    (.remove ^ConcurrentHashMap (:flows proxy) (:id flow))
    (locking (:policy proxy) (.notifyAll ^Object (:policy proxy)))))

(defn- cut-flows! [proxy]
  (doseq [flow (.values ^ConcurrentHashMap (:flows proxy))]
    (close-flow! proxy flow)))

(defn- read-frame [^DataInputStream input]
  (when-let [fmt (try (.readUnsignedByte input) (catch EOFException _ nil))]
    (let [length (.readInt input)
          _ (ensure! (<= 5 length 67108864) "Invalid proxy frame length" {:length length})
          payload (byte-array (- length 5))
          bytes (ByteArrayOutputStream.)]
      (.readFully input payload)
      (with-open [output (DataOutputStream. bytes)]
        (.writeByte output fmt)
        (.writeInt output length)
        (.write output payload))
      {:bytes (.toByteArray bytes)
       ;; Only request types are inspected. Forward the original frame bytes,
       ;; including negotiated compact/prepared messages and compression.
       :message (try (p/read-value fmt payload)
                     (catch Exception _ nil))})))

(defn- withheld? [mode request-type]
  (or (= :blackhole mode)
      (and (= :registration-timeout mode) (= :set-client-id request-type))
      (and (= :authentication-timeout mode) (= :authentication request-type))))

(defn- hold-reply! [proxy flow request-type]
  (locking (:policy proxy)
    (loop [reported? false]
      (when (and (not (.get ^AtomicBoolean (:closed? flow)))
                 (withheld? (:mode @(:policy proxy)) request-type))
        (when-not reported?
          (swap! (:hits proxy) update (:mode @(:policy proxy)) (fnil inc 0)))
        (.wait ^Object (:policy proxy) 100)
        (recur true)))))

(defn- await-cut! [policy]
  (when-let [^CountDownLatch arrived (:arrived policy)]
    (.countDown arrived)
    (ensure! (.await ^CountDownLatch (:release policy) 5 TimeUnit/SECONDS)
             "The fault's target frame was not released" {})))

(defn- damage-reply! [proxy flow request-type message ^bytes bytes output]
  (let [policy @(:policy proxy)
        mode (:mode policy)
        target? (case mode
                  :lost-write-reply (= :update-kv request-type)
                  :truncated-reply (not (#{:authentication :set-client-id} request-type))
                  :copy-out-cut (and (= :get-range request-type)
                                     @(:copy-out? flow)
                                     (not= :copy-out-response (:type message)))
                  false)]
    (when (and target? (compare-and-set! (:policy proxy) policy {:mode :pass}))
      (swap! (:hits proxy) update mode (fnil inc 0))
      (event! proxy {:fault mode :request-type request-type :flow (:id flow)})
      (await-cut! policy)
      (when (#{:truncated-reply :copy-out-cut} mode)
        ;; Forward a valid header and an incomplete payload, then send EOF.
        (.write ^java.io.OutputStream output bytes 0 (min (dec (alength bytes)) 7))
        (.flush ^java.io.OutputStream output))
      (close-flow! proxy flow)
      true)))

(defn- damage-request! [proxy flow message ^bytes bytes output]
  (let [policy @(:policy proxy)]
    (when (and (= :copy-in-cut (:mode policy)) @(:copy-in? flow)
               (sequential? message)
               (compare-and-set! (:policy proxy) policy {:mode :pass}))
      (swap! (:hits proxy) update :copy-in-cut (fnil inc 0))
      (event! proxy {:fault :copy-in-cut :flow (:id flow)})
      (await-cut! policy)
      (.write ^java.io.OutputStream output bytes 0 7)
      (.flush ^java.io.OutputStream output)
      (close-flow! proxy flow)
      true)))

(defn- pump! [proxy flow direction]
  (let [request? (= direction :request)
        ^SocketChannel source (if request? (:front flow) (:back flow))
        ^SocketChannel target (if request? (:back flow) (:front flow))]
    (try
      (let [input (DataInputStream. (.getInputStream (.socket source)))
            output (.getOutputStream (.socket target))]
        (loop []
          (when-let [{:keys [bytes message]} (read-frame input)]
            (when request?
              (reset! (:request-type flow) (:type message))
              (when (= :copy-in (:mode message)) (reset! (:copy-in? flow) true)))
            (when (and (not request?) (= :copy-out-response (:type message)))
              (reset! (:copy-out? flow) true))
            (let [request-type @(:request-type flow)]
              (when-let [observe (:observe @(:policy proxy))]
                (observe direction request-type message))
              (when-not request? (hold-reply! proxy flow request-type))
              (when-not (if request?
                          (damage-request! proxy flow message bytes output)
                          (damage-reply! proxy flow request-type message bytes output))
                (.write output ^bytes bytes)
                (.flush output)
                (recur))))))
      (catch IOException _ nil)
      (catch InterruptedException _ nil)
      (catch Throwable error
        (swap! (:errors proxy) conj error))
      (finally (close-flow! proxy flow)))))

(defn- start-proxy! [target-port]
  (let [listener (doto (ServerSocketChannel/open)
                   (.bind (InetSocketAddress. "127.0.0.1" 0)))
        proxy {:listener listener
               :port (.getPort ^InetSocketAddress (.getLocalAddress listener))
               :target-port target-port
               :executor (executor "proxy")
               :running? (AtomicBoolean. true)
               :flows (ConcurrentHashMap.)
               :next-id (AtomicInteger.)
               :policy (atom {:mode :pass})
               :hits (atom {}) :events (atom []) :errors (atom [])}]
    (.submit ^ExecutorService (:executor proxy)
             ^Runnable
             (fn []
               (try
                 (while (.get ^AtomicBoolean (:running? proxy))
                   (let [front (.accept listener)]
                     (if (= :reset (:mode @(:policy proxy)))
                       (do (swap! (:hits proxy) update :reset (fnil inc 0))
                           (safe-close! front))
                       (let [back (SocketChannel/open)]
                         (try
                           (.connect back (InetSocketAddress. "127.0.0.1" (int target-port)))
                           (.setTcpNoDelay (.socket front) true)
                           (.setTcpNoDelay (.socket back) true)
                           (let [id (.incrementAndGet ^AtomicInteger (:next-id proxy))
                                 flow {:id id :front front :back back
                                       :closed? (AtomicBoolean. false)
                                       :copy-in? (atom false) :copy-out? (atom false)
                                       :request-type (atom nil)}]
                             (.put ^ConcurrentHashMap (:flows proxy) id flow)
                             (doseq [direction [:request :reply]]
                               (.submit ^ExecutorService (:executor proxy)
                                        ^Runnable #(pump! proxy flow direction))))
                           (catch IOException _
                             (safe-close! front)
                             (safe-close! back)))))))
                 (catch IOException _ nil)
                 (catch Throwable error (swap! (:errors proxy) conj error)))))
    proxy))

(defn- stop-proxy! [proxy]
  (.set ^AtomicBoolean (:running? proxy) false)
  (policy! proxy {:mode :pass})
  (safe-close! (:listener proxy))
  (cut-flows! proxy)
  (shut-down! (:executor proxy))
  (ensure! (.isEmpty ^ConcurrentHashMap (:flows proxy)) "Proxy sockets leaked" {})
  (ensure! (empty? @(:errors proxy)) "Fault injector failed" {:errors @(:errors proxy)}))

(defn- start-child! [ctx]
  (let [ready (io/file (:root ctx) "child.ready")
        _ (Files/deleteIfExists (.toPath ready))
        command [(str (System/getProperty "java.home") "/bin/java")
                 "--enable-native-access=ALL-UNNAMED"
                 "--add-opens=java.base/java.nio=ALL-UNNAMED"
                 "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
                 "-Dclojure.compiler.direct-linking=true"
                 "-cp" (System/getProperty "java.class.path")
                 "clojure.main" "-m" "datalevin.client-recovery-chaos"
                 "child" (:root ctx) (str (:server-port ctx)) (.getPath ready)]
        process (.start (doto (ProcessBuilder. ^java.util.List command)
                          (.redirectErrorStream true)
                          (.redirectOutput (io/file (:root ctx) "child.log"))))]
    (reset! (:server ctx) process)
    (await-condition!
      #(do (ensure! (.isAlive process) "Child server exited during startup"
                    {:log (slurp (io/file (:root ctx) "child.log"))})
           (.exists ready))
      60000 "Child server did not start")))

(defn- start-server! [ctx]
  (progress! ctx :starting-server)
  (if (:child? ctx)
    (start-child! ctx)
    (let [server (srv/create {:root (:root ctx) :port (:server-port ctx)})]
      (reset! (:server ctx) server)
      (srv/start server))))

(defn- stop-server! [ctx]
  (when-let [server @(:server ctx)]
    (if (:child? ctx)
      (let [^Process process server]
        (.destroyForcibly process)
        (ensure! (.waitFor process 10 TimeUnit/SECONDS) "Child server would not die" {}))
      (srv/stop server))
    (reset! (:server ctx) nil)))

(defn- port []
  (with-open [listener (doto (ServerSocketChannel/open)
                        (.bind (InetSocketAddress. "127.0.0.1" 0)))]
    (.getPort ^InetSocketAddress (.getLocalAddress listener))))

(defn- datalog-client [conn]
  (.-client ^DatalogStore (.-store ^DB @conn)))

(defn- clients [ctx]
  (vec (distinct [(:admin ctx)
                  (.-client ^KVStore (:kv ctx))
                  (datalog-client (:conn ctx))
                  (.-tx-client ^KVStore (:kv ctx))
                  (.-tx-client ^DatalogStore (.-store ^DB @(:conn ctx)))])))

(defn- allocated-clients [ctx]
  (keep identity [(:admin ctx)
                  (when-let [kv (:kv ctx)] (.-client ^KVStore kv))
                  (when-let [conn (:conn ctx)] (datalog-client conn))
                  (when-let [kv (:kv ctx)] (.-tx-client ^KVStore kv))
                  (when-let [conn (:conn ctx)]
                    (.-tx-client ^DatalogStore (.-store ^DB @conn)))]))

(defn- assert-clients! [ctx]
  (ensure! (empty? @(:health-errors ctx))
           "A live client reported disconnected during a transient fault"
           {:observations @(:health-errors ctx)})
  (swap! (:pools ctx) into (map cl/get-pool (clients ctx)))
  (doseq [[original current] (map vector (:original-clients ctx) (clients ctx))]
    (ensure! (identical? original current) "Application client was replaced" {})
    (ensure! (not (cl/disconnected? original))
             "Transient fault permanently closed an application client" {})))

(defn- assert-pools! [ctx]
  (doseq [client (clients ctx)]
    ;; A previously idle dedicated transaction client may first discover its
    ;; expired session here. Exercise normal automatic renewal before probing
    ;; the individual sockets through the low-level pool API.
    (cl/list-databases client)
    (let [capacity (.-pool-size ^Client client)
          ^ConnectionPool pool (cl/get-pool client)
          held (atom [])]
      (try
        (dotimes [_ capacity] (swap! held conj (cl/get-connection pool)))
        (ensure! (= capacity (count (set @held))) "Pool capacity was lost" {})
        (doseq [connection @held]
          (ensure! (= :command-complete
                      (:type (cl/send-n-receive connection {:type :list-databases :args []})))
                   "A recovered pool socket was unusable" {}))
        (finally (doseq [connection @held] (cl/release-connection pool connection))))
      (ensure! (zero? (.get ^AtomicInteger (.-waiters pool))) "Pool waiters leaked" {})
      (ensure! (= capacity (.size ^ConcurrentHashMap (.-owners pool)))
               "Pool ownership leaked" {})
      (doseq [^ConnectionSlot slot (.-slots pool)]
        (ensure! (not (.get ^AtomicBoolean (.-borrowed? slot))) "A pool slot stayed borrowed" {}))))
  (let [current-pools (set (map cl/get-pool (clients ctx)))]
    (doseq [^ConnectionPool retired @(:pools ctx)
            :when (not (contains? current-pools retired))]
      (ensure! (cl/closed-pool? retired) "A replaced pool remained open" {})
      (ensure! (zero? (.size ^ConcurrentHashMap (.-owners retired)))
               "A replaced pool retained connection ownership" {}))))

(defn- expire-sessions! [ctx]
  (let [^Server server @(:server ctx)
        deps {:clients-fn (constantly (.-clients server))
              :sys-conn-fn (constantly (.-sys-conn server))}]
    (doseq [client (clients ctx)]
      ;; Use the real persistent deletion, retaining sockets so concurrent
      ;; requests discover :reconnect independently on the same original client.
      (session/remove-client deps server (cl/get-id client)))))

(defn- operation [ctx cycle worker]
  (let [id (+ (* (long cycle) 1000) (long worker))]
    (case worker
      100 {:kind :kv-tx :id id
           :run #(d/with-transaction-kv [tx (:kv ctx)]
                   (d/update-kv tx "counts" id increment :long :long))}
      101 {:kind :datalog-tx :id id :value (str "value-" id)
           :run #(d/with-transaction [tx (:conn ctx)]
                   (d/transact! tx [{:chaos/id id :chaos/value (str "value-" id)}]))}
      102 {:kind :kv-abort :id id
           :run #(d/with-transaction-kv [tx (:kv ctx)]
                   (d/update-kv tx "counts" id increment :long :long)
                   (d/abort-transact-kv tx))}
      103 {:kind :datalog-abort :id id :value (str "value-" id)
           :run #(d/with-transaction [tx (:conn ctx)]
                   (d/transact! tx [{:chaos/id id :chaos/value (str "value-" id)}])
                   (d/abort-transact tx))}
      104 {:kind :bulk-write :id cycle
           :run #(d/transact-kv (:kv ctx) "bulk"
                                (mapv (fn [i] [:put (+ (* (long cycle) 10000) (long i)) i])
                                      (range (inc c/+wire-datom-batch-size+)))
                                :long :long)}
      105 {:kind :bulk-read
           :run #(d/get-range (:kv ctx) "bulk-base" [:all] :long :long)}
      (case (mod worker 5)
      0 {:kind :increment :id id
         :run #(d/update-kv (:kv ctx) "counts" id increment :long :long)}
      1 {:kind :datalog :id id :value (str "value-" id)
         :run #(d/transact! (:conn ctx) [{:chaos/id id :chaos/value (str "value-" id)}])}
      2 {:kind :kv-read :run #(d/get-value (:kv ctx) "counts" -1 :long :long)}
      3 {:kind :query :run #(d/q query @(:conn ctx))}
        4 {:kind :search :run #(vec (d/search (:engine ctx) "sentinel"))}))))

(defn- run-wave! [ctx operations]
  (let [policy @(:policy (:proxy ctx))
        target (when-let [kind (:target-kind policy)]
                 (first (filter #(= kind (:kind %)) operations)))
        gate (CountDownLatch. 1)
        jobs (mapv (fn [op]
                     (reify Callable
                       (call [_]
                         (when-not (identical? op target) (.await gate))
                         (let [start (System/nanoTime)]
                           (try
                             (let [value ((:run op))]
                               (assoc (dissoc op :run) :status :ok :result value))
                             (catch Exception error
                               (assoc (dissoc op :run) :status :error
                                      :message (ex-message error)
                                      :data (ex-data error)
                                      :stack (mapv str (take 8 (.getStackTrace error)))
                                      :elapsed-ms (quot (- (System/nanoTime) start) 1000000))))))))
                   operations)
        futures (mapv #(.submit ^ExecutorService (:workers ctx) ^Callable %) jobs)
        deadline (+ (System/nanoTime) 5000000000)]
    (try
      (when-let [^CountDownLatch arrived (:arrived policy)]
        ;; Let the selected operation reach its exact cut boundary before
        ;; competing callers can consume every pool slot. Release the waiting
        ;; workload and the intercepted frame together, retaining concurrency.
        (ensure! target "No operation matches the scheduled fault" {})
        (ensure! (.await arrived 5 TimeUnit/SECONDS)
                 "The selected operation never reached its fault boundary"
                 {:fault (:mode policy) :kind (:target-kind policy)}))
      (.countDown gate)
      (when-let [^CountDownLatch release (:release policy)] (.countDown release))
      (mapv (fn [^Future task]
              (.get task (max 1 (- deadline (System/nanoTime))) TimeUnit/NANOSECONDS))
            futures)
      (finally
        (.countDown gate)
        (when-let [^CountDownLatch release (:release policy)] (.countDown release))
        (doseq [^Future task futures] (when-not (.isDone task) (.cancel task true)))))))

(defn- verify-data! [ctx outcomes]
  (let [counts (into {} (d/get-range (:kv ctx) "counts" [:all] :long :long))
        bulk (into {} (d/get-range (:kv ctx) "bulk" [:all] :long :long))
        rows (d/q query @(:conn ctx))
        actual (into {} rows)
        attempted (into {-1 "sentinel"}
                        (keep #(when (#{:datalog :datalog-tx} (:kind %)) [(:id %) (:value %)]))
                        outcomes)]
    (ensure! (= (count rows) (count actual)) "A logical Datalog write was duplicated" {})
    (ensure! (= "sentinel" (get actual -1)) "Acknowledged Datalog data was lost" {})
    (doseq [[id value] actual]
      (ensure! (= value (get attempted id)) "Unexpected Datalog data" {:id id :value value}))
    (doseq [{:keys [kind id value status]} outcomes]
      (case kind
        (:increment :kv-tx)
        (let [n (get counts id)]
          (ensure! (#{0 1} n) "A non-idempotent update was replayed" {:id id :count n})
          (when (= :ok status)
            (ensure! (= 1 n) "An acknowledged update was lost" {:id id})))
        (:datalog :datalog-tx)
        (when (= :ok status)
          (ensure! (= value (get actual id)) "An acknowledged transaction was lost" {:id id}))
        :kv-abort (ensure! (= 0 (get counts id)) "An aborted KV transaction committed" {:id id})
        :datalog-abort (ensure! (not (contains? actual id)) "An aborted Datalog transaction committed" {:id id})
        :bulk-write
        (let [n (count (filter #(contains? bulk (+ (* (long id) 10000) (long %)))
                               (range (inc c/+wire-datom-batch-size+))))]
          (ensure! (#{0 (inc c/+wire-datom-batch-size+)} n)
                   "A bulk transaction partially committed" {:cycle id :rows n})
          (when (= :ok status)
            (ensure! (= (inc c/+wire-datom-batch-size+) n)
                     "An acknowledged bulk write was lost" {:cycle id})))
        nil))
    (doseq [{:keys [kind result status]} outcomes :when (= :ok status)]
      (case kind
        :kv-read (ensure! (= 7 result) "A successful read returned the wrong response" {})
        :search (ensure! (= ["baseline"] result) "A search returned the wrong response" {})
        :bulk-read (ensure! (= (mapv #(vector % %) (range (inc c/+wire-datom-batch-size+))) result)
                            "A copy-out read returned incomplete or mismatched data" {})
        :query (doseq [[id value] result]
                 (ensure! (= value (get attempted id)) "A query returned unexpected data" {:id id}))
        nil))
    (ensure! (= ["baseline"] (vec (d/search (:engine ctx) "sentinel")))
             "The original search handle did not recover" {})
    (ensure! (= 7 (get counts -1)) "Acknowledged KV data was lost" {})))

(defn- recover! [ctx outcomes]
  (let [deadline (+ (System/nanoTime) 10000000000)]
    (loop []
      (assert-clients! ctx)
      (let [error (try
                    (cl/list-databases (:admin ctx))
                    (verify-data! ctx outcomes)
                    (assert-pools! ctx)
                    nil
                    (catch Exception error error))]
        (when error
          ;; Only regular operations on original handles are retried. A data
          ;; invariant failure must never disappear behind a later good probe.
          (when (or (::invariant? (ex-data error)) (>= (System/nanoTime) deadline))
            (throw error))
          (Thread/sleep 10)
          (recur))))))

(defn- stage-transactions-before-kill! [ctx cycle]
  (let [kv-id (+ (* (long cycle) 1000) 996)
        dt-id (inc kv-id)
        entered (CountDownLatch. 2)
        release (CountDownLatch. 1)
        _ (d/transact-kv (:kv ctx) "counts" [[:put kv-id 0]] :long :long)
        jobs [(fn []
                (d/with-transaction-kv [tx (:kv ctx)]
                  (d/update-kv tx "counts" kv-id increment :long :long)
                  (.countDown entered)
                  (.await release)
                  (d/abort-transact-kv tx)))
              (fn []
                (d/with-transaction [tx (:conn ctx)]
                  (d/transact! tx [{:chaos/id dt-id :chaos/value "must-abort"}])
                  (.countDown entered)
                  (.await release)
                  (d/abort-transact tx)))]
        tasks (mapv (fn [f]
                      (.submit ^ExecutorService (:workers ctx)
                               ^Callable (reify Callable
                                           (call [_] (try (f) (catch Exception e e))))))
                    jobs)]
    (try
      (ensure! (.await entered 5 TimeUnit/SECONDS)
               "Pending transactions did not reach the crash boundary"
               {:completed (mapv #(.isDone ^Future %) tasks)})
      {:release release :tasks tasks
       :outcomes [{:kind :kv-abort :id kv-id :status :error}
                  {:kind :datalog-abort :id dt-id :value "must-abort" :status :error}]}
      (catch Throwable error
        (.countDown release)
        (doseq [^Future task tasks] (.cancel task true))
        (throw error)))))

(defn- exercise-authentication-timeout! [ctx hits-before]
  ;; Password verification can itself consume a short budget under CPU load.
  ;; Certify a genuinely withheld reply before releasing the concurrent wave;
  ;; expiry before the proxy sees a reply is not evidence of this fault.
  (let [proxy (:proxy ctx)]
    (loop [attempt 0]
      (when (and (< attempt 4)
                 (<= (long (get @(:hits proxy) :authentication-timeout 0))
                     (long hits-before)))
        (let [start (System/nanoTime)]
          (try (cl/list-databases (:admin ctx)) (catch Exception _ nil))
          (ensure! (< (- (System/nanoTime) start) 1500000000)
                   "Withheld authentication exceeded the short request budget" {})
          (event! proxy {:fault :authentication-timeout :certification-attempt attempt}))
        (recur (inc attempt))))))

(defn- fault-cycle! [ctx cycle fault history all-outcomes]
  (let [proxy (:proxy ctx)
        target-kind (case fault
                      :copy-in-cut :bulk-write
                      :copy-out-cut :bulk-read
                      :lost-write-reply :increment
                      :truncated-reply :kv-read
                      nil)
        arrived (when target-kind (CountDownLatch. 1))
        release (when target-kind (CountDownLatch. 1))
        operations (mapv #(operation ctx cycle %)
                         (concat (range (max 15 (* 5 (long (:pool-size ctx)))))
                                 (range 100 106)))
        increment-ops (filter #(#{:increment :kv-tx :kv-abort} (:kind %)) operations)
        hits-before (get @(:hits proxy) fault 0)
        _ (reset! (:current ctx) {:cycle cycle :fault fault})
        _ (progress! ctx :fault)
        _ (d/transact-kv (:kv ctx) "counts"
                         (mapv (fn [op] [:put (:id op) 0]) increment-ops) :long :long)
        staged (when (= :kill fault) (stage-transactions-before-kill! ctx cycle))]
    (try
      (when staged (event! proxy {:fault :kill :staged-transactions 2}))
      (case fault
        (:restart :kill) (stop-server! ctx)
        (:expire-sessions :authentication-timeout) (expire-sessions! ctx)
        nil)
      (when staged
        ;; The process is already dead, with native transactions still open.
        ;; Releasing the bodies now must never make their staged writes durable.
        (.countDown ^CountDownLatch (:release staged))
        (doseq [^Future task (:tasks staged)] (.get task 5 TimeUnit/SECONDS))
        (swap! all-outcomes into (:outcomes staged)))
      (policy! proxy (cond-> {:mode (if (#{:reset :blackhole :registration-timeout
                                          :authentication-timeout :lost-write-reply
                                          :truncated-reply :copy-in-cut :copy-out-cut} fault)
                                     fault :pass)}
                      target-kind (assoc :target-kind target-kind
                                         :arrived arrived :release release)))
      (when (#{:reset :registration-timeout} fault) (cut-flows! proxy))
      (when (= :authentication-timeout fault)
        (exercise-authentication-timeout! ctx hits-before))
      (let [outcomes (run-wave! ctx operations)]
        (swap! all-outcomes into outcomes)
        (doseq [outcome outcomes]
          (ensure! (not (str/includes? (or (:message outcome) "") "This client is closed"))
                   "A live client reported terminal closure during recovery" outcome))
        (when (#{:reset :blackhole :registration-timeout :authentication-timeout
                  :lost-write-reply :truncated-reply :copy-in-cut :copy-out-cut} fault)
          (ensure! (< (long hits-before) (long (get @(:hits proxy) fault 0)))
                   "The scheduled transport fault was never exercised" {:fault fault}))
        (policy! proxy {:mode :pass})
        (when (#{:restart :kill} fault) (start-server! ctx))
        (progress! ctx :recovering)
        (recover! ctx @all-outcomes)
        ;; Write once after every healing, proving continued service through
        ;; the original application handles instead of read-only recovery.
        (let [op (operation ctx cycle 990)
              _ (d/transact-kv (:kv ctx) "counts" [[:put (:id op) 0]] :long :long)
              result ((:run op))]
          (swap! all-outcomes conj (assoc (dissoc op :run) :status :ok :result result)))
        (let [op (operation ctx cycle 991)
              result ((:run op))]
          (swap! all-outcomes conj (assoc (dissoc op :run) :status :ok :result result)))
        (let [id (str "healed-" cycle)]
          (d/add-doc (:engine ctx) id "fresh healthy document")
          (ensure! (d/doc-indexed? (:engine ctx) id) "Search writes did not recover" {}))
        (verify-data! ctx @all-outcomes)
        (progress! ctx :cycle-complete)
        (swap! history conj {:cycle cycle :fault fault
                             :staged-transactions (if staged 2 0)
                             :successes (count (filter #(= :ok (:status %)) outcomes))
                             :failures (count (filter #(= :error (:status %)) outcomes))
                             :hits @(:hits proxy)}))
      (finally
        (when release (.countDown ^CountDownLatch release))
        (when staged (.countDown ^CountDownLatch (:release staged)))
        (policy! proxy {:mode :pass})))))

(defn- start-health-monitor! [ctx]
  (.submit ^ExecutorService (:workers ctx)
           ^Runnable
           (fn []
             (try
               (while (.get ^AtomicBoolean (:monitor-running? ctx))
                 (doseq [client (:original-clients ctx)]
                   (.incrementAndGet ^AtomicLong (:health-checks ctx))
                   (when (cl/disconnected? client)
                     (swap! (:health-errors ctx) conj
                            {:id (cl/get-id client) :cycle @(:current ctx)
                             :current-pool-closed? (cl/closed-pool? (cl/get-pool client))})))
                 (Thread/sleep 1))
               (catch InterruptedException _ nil)))))

(defn- close-during-renewal! [ctx]
  (let [proxy (:proxy ctx)
        clients (clients ctx)
        before (long (get @(:hits proxy) :authentication-timeout 0))
        closing (CountDownLatch. (count clients))
        submit! (fn [f]
                  (.submit ^ExecutorService (:workers ctx)
                           ^Callable (reify Callable
                                       (call [_] (try (f) (catch Exception e e))))))]
    (expire-sessions! ctx)
    (policy! proxy {:mode :authentication-timeout})
    ;; The ordinary-request timeout is deliberately short in the fault waves.
    ;; This separate publication/closure race uses the same real renewal path
    ;; with enough time to capture authentication replies before releasing them.
    (let [renewals (mapv #(submit! (fn []
                                    (.renewSession ^IClientInternals % (cl/get-pool %)
                                                   (+ (System/nanoTime) 10000000000))))
                         clients)]
      (try
        (await-condition! #(or (>= (long (get @(:hits proxy) :authentication-timeout 0))
                                    (+ before (count clients)))
                               (every? (fn [^Future task] (.isDone task)) renewals))
                          5000 "Session renewals did not reach the fault proxy")
        (ensure! (>= (long (get @(:hits proxy) :authentication-timeout 0))
                      (+ before (count clients)))
                 "Renewals finished before the controlled disconnect race"
                 {:results (mapv (fn [^Future task]
                                   (when (.isDone task) (str (.get task)))) renewals)})
        (let [closures (mapv #(submit! (fn [] (.countDown closing) (cl/disconnect %))) clients)]
          (ensure! (.await closing 5 TimeUnit/SECONDS) "Disconnect calls did not start" {})
          ;; Give a disconnect that incorrectly ignores the renewal lock a
          ;; chance to finish before the withheld authentication is delivered.
          (doseq [^Future task closures]
            (try (.get task 20 TimeUnit/MILLISECONDS)
                 (catch java.util.concurrent.TimeoutException _ nil)))
          (policy! proxy {:mode :pass})
          (doseq [^Future task (concat renewals closures)] (.get task 5 TimeUnit/SECONDS)))
        (finally (policy! proxy {:mode :pass}))))
    (doseq [client clients]
      (ensure! (cl/disconnected? client) "Renewal resurrected an explicitly closed client" {})
      (ensure! (try (cl/list-databases client) false (catch Exception _ true))
               "Explicitly disconnected client accepted a request" {}))))

(defn run-chaos!
  "Keep the same KV, Datalog, search and admin handles through every fault.
  :child? runs a real server JVM; :kill forcibly kills it without shutdown."
  [{:keys [seed pool-size cycles child? fault-order report-progress!]
    :or {seed 17 pool-size 3 cycles 10 child? false}}]
  (log/set-min-level! :report)
  (binding [c/*db-background-sampling?* false]
    (let [wire-count (.size ^ConcurrentHashMap @#'cl/connection-wire-opts)
          root (u/tmp-dir (str "client-recovery-chaos-" seed "-" (random-uuid)))
          _ (.mkdirs (io/file root))
          proxy (start-proxy! (port))
          ctx-v (atom {:root root :seed seed :report-progress! report-progress!
                       :server-port (:target-port proxy) :server (atom nil)
                       :proxy proxy :child? child? :pool-size pool-size
                       :pools (atom #{}) :current (atom nil)
                       :monitor-running? (AtomicBoolean. true)
                       :health-errors (atom []) :health-checks (AtomicLong.)
                       :workers (executor "workers")})
          history (atom [])
          outcomes (atom [])
          failure (volatile! nil)
          order (or fault-order
                    (let [items (ArrayList. ^java.util.Collection faults)]
                      (Collections/shuffle items (Random. (long seed)))
                      (vec items)))]
      (try
        (start-server! @ctx-v)
        (let [uri (str "dtlv://datalevin:datalevin@127.0.0.1:" (:port proxy))
              opts {:client-opts {:pool-size pool-size :time-out 500}
                    :background-sampling? false}
              admin (cl/new-client uri (:client-opts opts))
              _ (swap! ctx-v assoc :admin admin)
              kv (d/open-kv (str uri "/kv") opts)
              _ (swap! ctx-v assoc :kv kv)
              conn (d/create-conn (str uri "/datalog") schema opts)
              _ (swap! ctx-v assoc :conn conn)
              engine (d/new-search-engine kv {:include-text? true})
              ctx (swap! ctx-v assoc :engine engine :original-clients (clients @ctx-v))]
          (d/open-dbi kv "counts")
          (d/open-dbi kv "bulk")
          (d/open-dbi kv "bulk-base")
          (d/transact-kv kv "bulk-base"
                         (mapv #(vector :put % %) (range (inc c/+wire-datom-batch-size+)))
                         :long :long)
          (d/transact-kv kv "counts" [[:put -1 7]] :long :long)
          (d/transact! conn [{:chaos/id -1 :chaos/value "sentinel"}])
          (d/add-doc engine "baseline" "durable recovery sentinel")
          (let [monitor (start-health-monitor! ctx)]
            (try
              (dotimes [cycle cycles]
                (fault-cycle! ctx cycle (nth order (mod cycle (count order))) history outcomes))
              (finally
                (.set ^AtomicBoolean (:monitor-running? ctx) false)
                (.get ^Future monitor 5 TimeUnit/SECONDS))))
          (assert-clients! ctx)
          (progress! ctx :disconnect-race)
          (if child?
            (doseq [client (clients ctx)]
              (cl/disconnect client)
              (ensure! (cl/disconnected? client) "Explicit disconnect was ignored" {}))
            (close-during-renewal! ctx))
          {:seed seed :pool-size pool-size :cycles cycles :child? child?
           :coverage (set (map :fault @history)) :history @history
           :operations (count @outcomes) :clients-created 3
           :health-checks (.get ^AtomicLong (:health-checks ctx))
           :transport-clients (count (:original-clients ctx))})
        (catch Throwable error
          (progress! @ctx-v :failed {:message (ex-message error)
                                    :data (pr-str (ex-data error))})
          (let [wrapped (ex-info "Long-lived client recovery chaos failed"
                                 {:seed seed :pool-size pool-size :cycles cycles :child? child?
                                  :fault-order order :history @history
                                  :current @(:current @ctx-v)
                                  :outcomes (mapv #(dissoc % :result) (take-last 60 @outcomes))
                                  :proxy-events @(:events proxy) :fault-hits @(:hits proxy)}
                                 error)]
            (vreset! failure wrapped)
            (throw wrapped)))
        (finally
          (when-not @failure (progress! @ctx-v :cleanup))
          (policy! proxy {:mode :pass})
          (cut-flows! proxy)
          (let [cleanup!
                #(resources/close-all!
                   [(fn [] (doseq [client (allocated-clients @ctx-v)]
                             (cl/close-pool (cl/get-pool client))))
                    (fn [] (shut-down! (:workers @ctx-v)))
                    ;; Explicit disconnects above already closed the transports;
                    ;; release the handles' local caches and lifecycle state too.
                    (fn [] (when-let [conn (:conn @ctx-v)] (d/close conn)))
                    (fn [] (when-let [kv (:kv @ctx-v)] (d/close-kv kv)))
                    (fn [] (stop-server! @ctx-v))
                    (fn [] (stop-proxy! proxy))
                    (fn [] (ensure!
                             (= wire-count (.size ^ConcurrentHashMap @#'cl/connection-wire-opts))
                             "Client codec registrations leaked"
                             {:before wire-count
                              :after (.size ^ConcurrentHashMap @#'cl/connection-wire-opts)}))
                    (fn [] (u/delete-files root))])]
            (if-let [error @failure]
              (resources/close-suppressing! error cleanup!)
              (cleanup!))))))))

(defn -main [operation root port ready]
  (ensure! (= "child" operation) "Unknown chaos child command" {:operation operation})
  (log/set-min-level! :report)
  (binding [c/*db-background-sampling?* false]
    (let [server (srv/create {:root root :port (Integer/parseInt port)})]
      (srv/start server)
      (spit ready "ready")
      (.await (CountDownLatch. 1)))))
