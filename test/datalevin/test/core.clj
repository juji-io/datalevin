(ns datalevin.test.core
  (:require
   [clojure.test :as t]
   [clojure.walk :as walk]
   [datalevin.client :as cl]
   [datalevin.core :as d]
   [datalevin.entity :as de]
   [datalevin.server :as srv]
   [taoensso.timbre :as log]
   [datalevin.util :as u])
  (:import
   [datalevin.server Server]
   [java.net ServerSocket]
   [java.nio.channels ServerSocketChannel]))

(defn wrap-res [f]
  (let [res (f)]
    (when (pos? ^long (+ ^long (:fail res) ^long (:error res)))
      (System/exit 1))))

;; utils
(defmethod t/assert-expr 'thrown-msg? [msg form]
  (let [[_ match & body] form]
    `(try ~@body
          (t/do-report {:type :fail, :message ~msg, :expected '~form, :actual nil})
          (catch Throwable e#
            (let [m# (.getMessage e#)]
              (if (= ~match m#)
                (t/do-report {:type :pass, :message ~msg, :expected '~form, :actual e#})
                (t/do-report {:type :fail, :message ~msg, :expected '~form, :actual e#})))
            e#))))

(defn entity-map [db e]
  (when-let [entity (d/entity db e)]
    (->> (assoc (into {} entity) :db/id (:db/id entity))
         (walk/prewalk #(if (de/entity? %)
                          {:db/id (:db/id %)}
                          %)))))

(defn all-datoms [db]
  (into #{} (map (juxt :e :a :v)) (d/datoms db :eav)))

(defn no-namespace-maps [t]
  (binding [*print-namespace-maps* false]
    (t)))

(defn allocate-port
  []
  (with-open [s (ServerSocket. 0)]
    (.getLocalPort s)))

(defn server-port
  "Return the TCP port already bound by a server, including a :port 0 server."
  [^Server server]
  (.getLocalPort (.socket ^ServerSocketChannel (.-server-socket server))))

(defn server-fixture
  "Run a remote test against a temporary server on an available port. Both
  test projects share this namespace, so either copy must provide the fixture."
  [f]
  (let [dir (u/tmp-dir (str "server-test-" (random-uuid)))
        server (srv/create {:port 0 :root dir})
        port (server-port server)]
    (log/set-min-level! :report)
    (binding [cl/*default-port* port]
      (try
        (srv/start server)
        (f)
        (finally
          (srv/stop server)
          (u/delete-files dir))))))

(defn db-fixture
  [f]
  (log/set-min-level! :report)
  (f))
