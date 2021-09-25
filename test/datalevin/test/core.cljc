(ns datalevin.test.core
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [clojure.string :as str]
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [datalevin.entity :as de]
   [datalevin.util :as u #?@(:cljs [:refer-macros [defrecord-updatable]]
                             :clj  [:refer [defrecord-updatable]])]
   #?(:clj [datalevin.server :as srv])
   #?(:cljs [datalevin.test.cljs]))
  #?(:clj (:import [java.util UUID])))

#?(:cljs
   (enable-console-print!))

;; Added special case for printing ex-data of ExceptionInfo
#?(:cljs
  (defmethod t/report [::t/default :error] [m]
    (t/inc-report-counter! :error)
    (println "\nERROR in" (t/testing-vars-str m))
    (when (seq (:testing-contexts (t/get-current-env)))
      (println (t/testing-contexts-str)))
    (when-let [message (:message m)] (println message))
    (println "expected:" (pr-str (:expected m)))
    (print "  actual: ")
    (let [actual (:actual m)]
      (cond
        (instance? ExceptionInfo actual)
          (println (.-stack actual) "\n" (pr-str (ex-data actual)))
        (instance? js/Error actual)
          (println (.-stack actual))
        :else
          (prn actual)))))

#?(:cljs (def test-summary (atom nil)))
#?(:cljs (defmethod t/report [::t/default :end-run-tests] [m]
           (reset! test-summary (dissoc m :type))))

(defn wrap-res [f]
  #?(:cljs (do (f) (clj->js @test-summary))
     :clj  (let [res (f)]
             (when (pos? (+ (:fail res) (:error res)))
               (System/exit 1)))))

;; utils
#?(:clj
(defmethod t/assert-expr 'thrown-msg? [msg form]
  (let [[_ match & body] form]
    `(try ~@body
          (t/do-report {:type :fail, :message ~msg, :expected '~form, :actual nil})
          (catch Throwable e#
            (let [m# (.getMessage e#)]
              (if (= ~match m#)
                (t/do-report {:type :pass, :message ~msg, :expected '~form, :actual e#})
                (t/do-report {:type :fail, :message ~msg, :expected '~form, :actual e#})))
            e#)))))

(defn entity-map [db e]
  (when-let [entity (d/entity db e)]
    (->> (assoc (into {} entity) :db/id (:db/id entity))
         (clojure.walk/prewalk #(if (de/entity? %)
                                  {:db/id (:db/id %)}
                                  %)))))

(defn all-datoms [db]
  (into #{} (map (juxt :e :a :v)) (d/datoms db :eavt)))

(defn no-namespace-maps [t]
  (binding [*print-namespace-maps* false]
    (t)))

#?(:clj
   (defn server-fixture
     [f]
     (let [dir    (u/tmp-dir (str "server-test-" (UUID/randomUUID)))
           server (srv/create {:port    c/default-port
                               :root    dir
                               :verbose true})]
       (try
         (srv/start server)
         (f)
         (catch Exception e (throw e))
         (finally
           (srv/stop server)
           (u/delete-files dir))))))
