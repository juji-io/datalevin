(ns datalevin.interpret
  "Code interpreter, including functions and macros useful for command line
  and query/transaction functions."
  (:require
   [clojure.walk :as w]
   [clojure.set :as set]
   [clojure.pprint :as p]
   [clojure.java.io :as io]
   [sci.core :as sci]
   [taoensso.nippy :as nippy]
   [datalevin.query :as q]
   [datalevin.util :as u]
   [datalevin.core]
   [datalevin.search]
   [datalevin.stem]
   [datalevin.client]
   [datalevin.constants]
   [clojure.string :as s])
  (:import
   [clojure.lang AFn]
   [datalevin.datom Datom]
   [org.tartarus.snowball SnowballStemmer]
   [java.text Normalizer Normalizer$Form]
   [java.io DataInput DataOutput Writer]))

(if (u/graal?)
  (require 'datalevin.binding.graal)
  (require 'datalevin.binding.java))

(def ^:no-doc user-facing-ns
  #{'datalevin.core 'datalevin.client 'datalevin.interpret 'datalevin.constants})

(defn- user-facing? [v]
  (let [m (meta v)
        d (m :doc)]
    (and d
         (if-let [p (:protocol m)]
           (and (not (:no-doc (meta p)))
                (not (:no-doc m)))
           (not (:no-doc m)))
         (not (s/starts-with? d "Positional factory function for class")))))

(defn ^:no-doc user-facing-map [ns var-map]
  (let [sci-ns (sci/create-ns ns)]
    (reduce
      (fn [m [k v]]
        (assoc m k (sci/new-var (symbol v) v (assoc (meta v)
                                                    :sci.impl/built-in true
                                                    :ns sci-ns))))
      {}
      (select-keys var-map
                   (keep (fn [[k v]] (when (user-facing? v) k)) var-map)))))

(defn- user-facing-vars []
  (reduce
    (fn [m ns]
      (assoc m ns (user-facing-map ns (ns-publics ns))))
    {}
    user-facing-ns))

(defn ^:no-doc resolve-var [s]
  (when (symbol? s)
    (some #(ns-resolve % s) (conj user-facing-ns *ns*))))

(defn- qualify-fn [x]
  (if (list? x)
    (let [[f & args] x]
      (if-let [var (when-not (q/rule-head f) (resolve-var f))]
        (apply list (symbol var) args)
        x))
    x))

(declare ctx)

(defn ^:no-doc eval-fn [ctx form]
  (sci/eval-form ctx (if (coll? form)
                       (w/postwalk qualify-fn form)
                       form)))

(defn load-edn
  "Same as [`clojure.core/load-file`](https://clojuredocs.org/clojure.core/load-file),
   useful for e.g. loading schema from a file"
  [f]
  (let [f (io/file f)
        s (slurp f)]
    (sci/with-bindings {sci/ns   @sci/ns
                        sci/file (.getAbsolutePath f)}
      (sci/eval-string* ctx s))))

(defn exec-code
  "Execute code and print results. `code` is a string. Acceptable code includes
  Datalevin functions and some Clojure core functions."
  [code]
  (let [reader (sci/reader code)]
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
  "Same signature as `fn`. Create a function that can be used as an input in
  Datalevin queries or transactions, e.g. as a filtering predicate or as a
  transaction function.

  This function will be sent over the wire if the database
  is on a remote server. It runs in a sandboxed interpreter whether the
  database is remote or local.

  The symbols used in the inter-fn definition needs to be fully-qualified
  to be resolved correctly."
  [args & body]
  `(with-meta
     (sci/eval-form ctx (fn ~args (do ~@body)))
     {:type   :datalevin/inter-fn
      :source ~(save-env (keys &env) &form)}))

(defn inter-fn?
  "Return true if `x` is an `inter-fn`"
  [x]
  (= (:type (meta x)) :datalevin/inter-fn))

(defmacro definterfn
  "Create a named `inter-fn`"
  [fn-name args & body]
  `(def ~fn-name (inter-fn ~args ~@body)))

(defn- source->inter-fn
  "Convert a source form to get an inter-fn"
  [src]
  (with-meta
    (sci/eval-form ctx src)
    {:type   :datalevin/inter-fn
     :source src}))

(nippy/extend-freeze AFn :datalevin/inter-fn
                     [^AFn x ^DataOutput out]
                     (if (inter-fn? x)
                       (nippy/freeze-to-out! out (:source (meta x)))
                       (u/raise "Can only freeze an inter-fn" {:x x})))

(nippy/extend-thaw :datalevin/inter-fn
                   [^DataInput in]
                   (let [src (nippy/thaw-from-in! in)]
                     (source->inter-fn src)))

(defmethod print-method :datalevin/inter-fn [f, ^Writer w]
  (.write w "#datalevin/inter-fn ")
  (binding [*out* w] (p/pprint (:source (meta f)))))

(defn inter-fn-from-reader
  "Read a printed `inter-fn` back in."
  [x]
  (source->inter-fn x))

(defn ^:no-doc additional-vars
  []
  {'datalevin.search
   (user-facing-map 'datalevin.search
                    {'en-analyzer #'datalevin.search/en-analyzer})
   'datalevin.stem
   (user-facing-map 'datalevin.stem
                    {'get-stemmer #'datalevin.stem/get-stemmer})
   'datalevin.constants
   (user-facing-map 'datalevin.constants
                    {'en-stop-words? #'datalevin.constants/en-stop-words?})})

(def ^:no-doc sci-opts
  {:namespaces (merge (user-facing-vars) (additional-vars))
   :classes    {:allow                     :all
                'java.text.Normalizer      java.text.Normalizer
                'java.text.Normalizer$Form java.text.Normalizer$Form
                'org.tartarus.snowball.SnowballStemmer
                org.tartarus.snowball.SnowballStemmer}})

(def ^:no-doc ctx (sci/init sci-opts))
