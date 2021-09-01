(ns datalevin.interpret
  "Code interpreter"
  (:require [clojure.walk :as w]
            [clojure.set :as set]
            [sci.core :as sci]
            [sci.impl.vars :as vars]
            [taoensso.nippy :as nippy]
            [datalevin.query :as q]
            [datalevin.util :as u]
            [datalevin.binding.graal]
            [datalevin.binding.java])
  (:import [clojure.lang AFn]
           [datalevin.datom Datom]
           [java.io DataInput DataOutput]))

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
    (some #(ns-resolve % s) (conj user-facing-ns *ns*))))

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

(def sci-opts {:namespaces (user-facing-vars)
               :classes    {'datalevin.datom.Datom datalevin.datom.Datom}})

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

;; inter-fn

(defn- filter-used
  "Only keep referred locals in the form"
  [locals [args & body]]
  (let [args (set args)
        used (reduce (fn [coll s]
                       (if-not (or (qualified-symbol? s) (args s))
                         (conj coll s)
                         coll))
                     #{}
                     (filter symbol? (flatten body)))]
    (set/intersection (set locals) used)))

(defn- save-env
  "Borrowed some pieces from https://github.com/technomancy/serializable-fn"
  [locals form]
  (let [form        (cons 'fn (w/postwalk qualify-fn (rest form)))
        quoted-form `(quote ~form)]
    (if locals
      `(list `let [~@(for [local   (filter-used locals (rest form)),
                           let-arg [`(quote ~local)
                                    `(list `quote ~local)]]
                       let-arg)]
             ~quoted-form)
      quoted-form)))

(defmacro inter-fn
  "Create a function that can be used in Datalevin queries. This function
  can be sent over the wire after frozen by nippy, and runs in a
  sandboxed interpreter."
  [args & body]
  `(with-meta
     (sci/eval-form (sci/init sci-opts) (fn ~args (do ~@body)))
     {:type   :datalevin/inter-fn
      :source ~(save-env (keys &env) &form)}))

(defn inter-fn?
  "return true if `x` is an `inter-fn`"
  [x]
  (and (instance? clojure.lang.AFn x)
       (= (:type (meta x)) :datalevin/inter-fn)))

(defn- source->inter-fn
  "Convert a source form to get an inter-fn"
  [src]
  (with-meta
    (sci/eval-form (sci/init sci-opts) src)
    {:type   :datalevin/inter-fn
     :source src}))

(nippy/extend-freeze AFn :datalevin/inter-fn
                     [^AFn x ^DataOutput out]
                     (if (inter-fn? x)
                       (nippy/freeze-to-out! out (:source (meta x)))
                       (u/raise "Can only freeze an inter-fn" {})))

(nippy/extend-thaw :datalevin/inter-fn
                   [^DataInput in]
                   (let [src (nippy/thaw-from-in! in)]
                     (source->inter-fn src)))
