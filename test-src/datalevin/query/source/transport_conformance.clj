(ns datalevin.query.source.transport-conformance
  "Deterministic transport fixtures. SQL, exported files and paged HTTP are
  explicitly materialized under the query ledger during session acquisition;
  the supplied adapter then owns and executes that complete observation."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.test :refer [is]]
            [datalevin.core :as d]
            [datalevin.query.source :as spi]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.fake :as fake]
            [datalevin.query.source.value :as value]
            [datalevin.util :as u]
            [jsonista.core :as json])
  (:import [com.sun.net.httpserver HttpServer HttpHandler]
           [java.io ByteArrayOutputStream Closeable InputStream]
           [java.net InetSocketAddress URI]
           [java.net.http HttpClient HttpRequest HttpResponse$BodyHandlers]
           [java.nio.charset StandardCharsets]))

(def ^:private constraints {:max-concurrent-requests 1 :rate-limit nil})
(def ^:private facts
  [[1 :node/name "Ada"] [1 :node/tag "a"] [1 :node/tag "b"]
   [2 :node/name "Grace"] [2 :node/parent 1] [2 :node/tag "b"]
   [3 :node/parent 1] [4 :node/name "No parent"]])
(defn- descriptor [kind]
  (let [info (fake/info {:node/name {:entity-type :long :value-type :string}
                    :node/tag {:entity-type :long :value-type :string}
                    :node/parent {:entity-type :long :value-type :long}})]
    (-> info
        (assoc :source-instance-id (random-uuid) :read-capabilities #{kind})
        (assoc-in [:logical-schema :attributes :node/parent :reference-domain]
                  (get-in info [:logical-schema :attributes :node/name :entity-domain])))))
(defn- error [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(defn- read-owned!
  "Admit encoding, parser expansion and growing buffers before reading chunks.
  Every chunk stays charged until the adapter has pinned its own copy."
  [request keep! ^InputStream stream]
  (let [ctl (:control request)]
    (keep! (spi/reserve! ctl {:retained-bytes 8192}))
    (let [buffer (byte-array 4096) out (ByteArrayOutputStream.)]
      (loop []
        (spi/check-active! ctl)
        (keep! (spi/reserve! ctl {:retained-bytes 65536}))
        (let [n (.read stream buffer)]
          (if (neg? n) (.toString out StandardCharsets/UTF_8)
              (do (.write out buffer 0 n) (recur))))))))

(defn- json-facts [text]
  (mapv (fn [row] [(long (get row "e")) (keyword (get row "a"))
                  (let [v (get row "v")] (if (integer? v) (long v) v))])
        (json/read-value text)))
(defn- wire-facts [rows]
  (mapv (fn [[e a v]] {:e e :a (subs (str a) 1) :v v}) rows))

(defrecord TransportSession [provider delegate limiter kind events]
  spi/IQuerySourceSession
  (-session-info [_]
    (cond-> (assoc (spi/-session-info delegate) :consistency kind :request-constraints constraints)
      (not= kind :snapshot) (assoc :data-version nil)))
  (-open-cursor [_ request]
    (swap! events conj [:open request])
    (spi/-open-cursor delegate request))
  (-cancel-session! [_ reason] (spi/-cancel-session! delegate reason))
  (-close-session! [_]
    (try (spi/-close-session! delegate)
         (finally (.close ^Closeable limiter) (swap! events conj [:close])))))

(defn- transport-source [adapt backend mode events]
  (let [kind (:kind backend) info (descriptor kind)]
    (reify spi/IQuerySource
      (-source-info [_] info)
      (-open-session [_ request]
        (let [limiter (spi/request-limiter (:control request) constraints)
              tokens (volatile! []) keep! #(vswap! tokens conj %)]
          (swap! events conj [:acquire request])
          (try
            (let [before (count @(:requests backend))
                  rows ((:load! backend) request limiter keep!)
                  _ (swap! events conj [:load {:actual (- (count @(:requests backend)) before)
                                               :ledger (:external-requests (control/usage (:control request)))}])
                  ;; Source construction copies once before session acquisition.
                  bytes (value/copy-charge rows (fn [_] (spi/check-active! (:control request))))
                  _ (keep! (spi/reserve! (:control request) {:retained-bytes (+ 4096 (* 2 bytes))}))
                  provider (adapt info rows {:mode mode})
                  delegate (spi/-open-session provider request)]
              (->TransportSession provider delegate limiter kind events))
            (catch Throwable e (.close ^Closeable limiter) (throw e))
            (finally (doseq [token @tokens] (spi/release! token))))))
      (-plan-source [_ session request]
        (spi/-plan-source (:provider session) (:delegate session) request)))))

(defn- sql-backend [path requests]
  (let [file (str path "/fixture.sqlite")
        sql (str "CREATE TABLE facts(e INTEGER,a TEXT,v);"
                 (apply str (map (fn [[e a v]]
                                   (str "INSERT INTO facts VALUES(" e ",'" (subs (str a) 1) "',"
                                        (if (string? v) (str "'" v "'") v) ");")) facts)))
        result (shell/sh "sqlite3" file sql)]
    (assert (zero? (:exit result)) (:err result))
    {:kind :statement
     :load! (fn [request limiter keep!]
              (with-open [_ (spi/acquire-request! limiter)]
                (swap! requests conj :sql-statement)
                (let [process (.start (ProcessBuilder. ^java.util.List
                                       ["sqlite3" "-json" file "SELECT e,a,v FROM facts ORDER BY rowid"]))]
                  (try
                    (with-open [stream (.getInputStream process)]
                      (let [rows (json-facts (read-owned! request keep! stream))]
                        (when-not (zero? (.waitFor process)) (throw (Exception. "fixture SQL failed")))
                        rows))
                    (finally (.destroyForcibly process))))))}))

(defn- file-backend [path requests]
  (let [file (str path "/export.json")]
    (spit file (json/write-value-as-string (wire-facts facts)))
    {:kind :snapshot
     :load! (fn [request limiter keep!]
              (with-open [_ (spi/acquire-request! limiter) stream (io/input-stream file)]
                (swap! requests conj :export-read)
                (json-facts (read-owned! request keep! stream))))}))

(defn- api-backend [requests]
  (let [retry? (atom true) on-page (atom nil)
        server (HttpServer/create (InetSocketAddress. "127.0.0.1" 0) 0)
        pages (vec (partition-all 3 facts))]
    (.createContext server "/facts"
      (reify HttpHandler
        (handle [_ exchange]
          (let [page (Long/parseLong (.getQuery (.getRequestURI exchange)))
                fail? (and (zero? page) (compare-and-set! retry? true false))
                body (.getBytes (if fail? "retry" (json/write-value-as-string (wire-facts (get pages page []))))
                                StandardCharsets/UTF_8)]
            (.sendResponseHeaders exchange (if fail? 503 200) (alength body))
            (with-open [stream (.getResponseBody exchange)] (.write stream body))))))
    (.start server)
    (let [base (str "http://127.0.0.1:" (.getPort (.getAddress server)) "/facts?")
          client (HttpClient/newHttpClient)]
      {:kind :best-effort :close! #(.stop server 0) :on-page on-page
       :load! (fn [request limiter keep!]
                (loop [page 0 rows [] retried? false]
                  (spi/check-active! (:control request))
                  (let [[status text] (with-open [_ (spi/acquire-request! limiter)]
                                   (swap! requests conj [:api-page page retried?])
                                   (let [response (.send client (.build (HttpRequest/newBuilder (URI. (str base page))))
                                                         (HttpResponse$BodyHandlers/ofInputStream))]
                                     (with-open [stream (.body response)]
                                       [(.statusCode response) (read-owned! request keep! stream)])))]
                    (when-let [observe @on-page] (observe request page))
                    (spi/check-active! (:control request))
                    (cond
                      (and (= 503 status) (not retried?)) (recur page rows true)
                      (not= 200 status) (throw (Exception. "fixture API failed"))
                      :else (let [next (json-facts text)]
                              (if (empty? next) rows
                                  (recur (inc page) (into rows next) false)))))))})))

(defn matrix!
  "Exercise actual SQLite, file and HTTP reads through the supplied v2 adapter,
  shared query engine and a native DB. No transport is advertised for production."
  [adapt]
  (let [path (u/tmp-dir (str "spi-v2-transports-" (random-uuid)))
        conn (d/create-conn nil {:native/key {:db/valueType :db.type/long}}
                            {:kv-opts {:inmemory? true} :background-sampling? false})]
    (.mkdirs (io/file path))
    (try
      (d/transact! conn [{:db/id 1 :native/key 1} {:db/id 2 :native/key 2}])
      (doseq [build [sql-backend file-backend (fn [_ requests] (api-backend requests))]]
        (let [requests (atom []) backend (assoc (build path requests) :requests requests)]
          (try
            (doseq [mode [:scan :lookup :value :both]]
              (let [events (atom []) provider (transport-source adapt backend mode events)
                    query '{:find [?id ?tag ?label] :in [$s [[?id ?label ?tag]]]
                            :where [[$s ?id :node/name ?label]]
                            :query-source {:consistency :best-effort :limits {:batch-rows 1 :binding-rows 1}}}
                    input [[1 "Ada" :a] [1 "Ada" :b] [2 "Grace" :c] [99 "Absent" :missing]]]
                (is (= (d/q (dissoc query :query-source) facts input) (d/q query provider input)))
                (let [plan (d/explain {:run? false} query provider input)
                      run (d/explain {:run? true :intermediate-counts? false} query provider input)]
                  (is (= [] (:cursors plan)))
                  (is (= 3 (:result-rows run)))
                  (is (every? #(= (:kind backend) (:consistency %)) (:sources run)))
                  (is (every? #(= (= :snapshot (:kind backend)) (some? (:data-version %))) (:sources run)))
                  (is (not (.contains (pr-str run) "SELECT"))))
                (is (= 3 (count (filter #(= :close (first %)) @events))))
                (doseq [[_ counts] (filter #(= :load (first %)) @events)]
                  (is (= (:actual counts) (:ledger counts)))
                  (is (pos? (:actual counts))))
                (doseq [[_ request] (filter #(= :acquire (first %)) @events)]
                  (is (every? zero? (vals (dissoc (control/usage (:control request)) :external-requests)))))))
            (let [events (atom []) provider (transport-source adapt backend :both events)
                  query '{:find [?child ?parent ?name ?tag] :in [$n $s % [[?child]]]
                          :where [[$n ?native :native/key ?child] ($s named-parent ?child ?parent ?name)
                                  [$s ?parent :node/tag ?tag]]
                          :query-source {:consistency :best-effort :limits {:batch-rows 1}}}
                  rules '[[(named-parent [?child] ?parent ?name)
                           [?child :node/parent ?parent] [?parent :node/name ?name]]]]
              (is (= (d/q (dissoc query :query-source) @conn facts rules [[2] [2] [3] [99]])
                     (d/q query @conn provider rules [[2] [2] [3] [99]])))
              (is (= :query-source/limit
                     (:error (error #(d/q (assoc-in query [:query-source :limits :external-requests] 0)
                                         @conn provider rules [[2]]))))))
            (when-let [on-page (:on-page backend)]
              (is (some #{[:api-page 0 true]} @requests))
              (is (some #(= 1 (second %)) @requests))
              (doseq [category [:query-source/cancelled :query-source/incomplete]]
                (let [before (count @requests) events (atom [])
                      provider (transport-source adapt backend :scan events)]
                  (reset! on-page
                    (fn [request page]
                      (when (zero? page)
                        (if (= category :query-source/cancelled)
                          (control/cancel! (:control request) {:error category :phase :execution})
                          (throw (ex-info "private page cap" {:error category :phase :fetch}))))))
                  (is (= category
                         (:error (error #(d/q '{:find [?id] :in [$s] :where [[$s ?id :node/name _]]
                                               :query-source {:consistency :best-effort}} provider)))))
                  (is (= 1 (- (count @requests) before)))
                  (doseq [[_ request] (filter #(= :acquire (first %)) @events)]
                    (is (every? zero? (vals (dissoc (control/usage (:control request)) :external-requests))))))))
            (finally (when-let [close (:close! backend)] (close))))))
      (finally (d/close conn) (u/delete-files path)))))
