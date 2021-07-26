(ns datalevin.interpret
  "Code string interpreter"
  (:require
   [clojure.string :as s]
   [clojure.walk :as w]
   [sci.core :as sci]
   [sci.impl.vars :as vars]
   [datalevin.query :as q]
   [datalevin.binding.graal]
   [datalevin.binding.java])
  (:import [java.io BufferedReader]))

(def user-facing-ns #{'datalevin.core})

(defn- user-facing? [v]
  (let [m (meta v)]
    (and (:doc m)
         (if-let [p (:protocol m)]
           (and (not (:no-doc (meta p)))
                (not (:no-doc m)))
           (not (:no-doc m))))))

(defn user-facing-map [ns var-map]
  (let [sci-ns (vars/->SciNamespace ns nil)]
    (reduce
      (fn [m [k v]]
        (assoc m k (vars/->SciVar v
                                  (symbol v)
                                  (assoc (meta v)
                                         :sci.impl/built-in true
                                         :ns sci-ns)
                                  false)))
      {}
      (select-keys var-map
                   (keep (fn [[k v]] (when (user-facing? v) k)) var-map)))))

(defn- user-facing-vars []
  (reduce
    (fn [m ns]
      (assoc m ns (user-facing-map ns (ns-publics ns))))
    {}
    user-facing-ns))

(defn resolve-var [s]
  (when (symbol? s)
    (some #(ns-resolve % s) user-facing-ns)))

(defn- qualify-fn [x]
  (if (list? x)
    (let [[f & args] x]
      (if-let [var (when-not (q/rule-head f) (resolve-var f))]
        (apply list (symbol var) args)
        x))
    x))

(defn eval-fn [ctx form]
  (sci/eval-form ctx (if (coll? form)
                       (w/postwalk qualify-fn form)
                       form)))

(def sci-opts {:namespaces (user-facing-vars)})

(defn exec-code
  "Execute code and print results. `code` is a string. Acceptable code includes
  Datalevin functions and Clojure core functions."
  [code]
  (let [reader (sci/reader code)
        ctx    (sci/init sci-opts)]
    (sci/with-bindings {sci/ns @sci/ns}
      (loop []
        (let [next-form (sci/parse-next ctx reader)]
          (when-not (= ::sci/eof next-form)
            (prn (eval-fn ctx next-form))
            (recur)))))))
