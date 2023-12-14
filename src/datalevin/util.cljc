(ns ^:no-doc datalevin.util
  (:refer-clojure :exclude [seqable?])
  (:require
   [clojure.walk]
   [clojure.string :as s]
   [clojure.java.io :as io])
  #?(:clj
     (:import
      [java.io File]
      [java.nio.file Files Paths LinkOption AccessDeniedException]
      [java.nio.file.attribute PosixFilePermissions FileAttribute])))

;; For when we need to use datalevin specific print method
(def ^:dynamic *datalevin-print* false)

(defn #?@(:clj  [^Boolean seqable?]
          :cljs [^boolean seqable?])
  [x]
  (and (not (string? x))
       #?(:cljs (cljs.core/seqable? x)
          :clj  (or (seq? x)
                    (instance? clojure.lang.Seqable x)
                    (nil? x)
                    (instance? Iterable x)
                    (instance? java.util.Map x)))))

(defn unchunk
  "Unchunked lazy seq, one item at a time"
  [s]
  (when (seq s)
    (lazy-seq
      (cons (first s)
            (unchunk (next s))))))

#?(:clj
   (defmacro cond+ [& clauses]
     (when-some [[test expr & rest] clauses]
       (case test
         :do   `(do ~expr (cond+ ~@rest))
         :let  `(let ~expr (cond+ ~@rest))
         :some `(or ~expr (cond+ ~@rest))
         `(if ~test ~expr (cond+ ~@rest))))))

#?(:clj
   (defmacro some-of
     ([] nil)
     ([x] x)
     ([x & more]
      `(let [x# ~x] (if (nil? x#) (some-of ~@more) x#)))))

#?(:clj
   (defmacro raise [& fragments]
     (let [msgs (butlast fragments)
           data (last fragments)]
       `(throw (ex-info
                 (str ~@(map (fn [m#] (if (string? m#) m# (list 'pr-str m#))) msgs))
                 ~data)))))

;; files

#?(:clj
   (defn windows? []
     (s/starts-with? (System/getProperty "os.name") "Windows")))

#?(:clj
   (defn apple-silicon? []
     (and (= (System/getProperty "os.name") "Mac OS X")
          (= (System/getProperty "os.arch") "aarch64"))))

(defn delete-files
  "Recursively delete file"
  [& fs]
  (when-let [f (first fs)]
    (if-let [cs (seq (.listFiles (io/file f)))]
      (recur (concat cs fs))
      (do (io/delete-file f)
          (recur (rest fs))))))

(defn delete-on-exit
  "Recursively register file to be deleted. NB. new files in
  a directory will prevent it from deletion."
  [^File dir]
  (.deleteOnExit dir)
  (doseq [^File f (.listFiles dir)]
    (if (.isDirectory f)
      (delete-on-exit f)
      (.deleteOnExit f))))

(defn file-exists
  "check if a file exists"
  [fname]
  (.exists (io/file fname)))

(defn create-dirs
  "create all parent directories"
  [^String path]
  (if (windows?)
    (.mkdirs ^File (io/file path))
    (Files/createDirectories
      (Paths/get path (into-array String []))
      (into-array FileAttribute
                  [(PosixFilePermissions/asFileAttribute
                     (PosixFilePermissions/fromString "rwxr-x---"))]))))

(defn file
  "Return directory path as File, create it if missing"
  [^String path]
  (try
    (let [path' (Paths/get path (into-array String []))]
      (when-not (Files/exists path' (into-array LinkOption []))
        (create-dirs path))
      (io/file path))
    (catch AccessDeniedException e
      (raise "Access denied " (ex-message e) {:path path}))
    (catch Exception e
      (raise "Error openning file " (ex-message e) {:path path}))))

(defn empty-dir?
  "test if the given File is an empty directory"
  [^File file]
  (and (.isDirectory file) (-> file .list empty?)))

(def +separator+
  #?(:clj  java.io.File/separator
     ;;this is faulty, since we could be in node.js
     ;;on windows...but it will work for now!
     :cljs "/"))

(defn dir-size
  "get the size of a directory or file, in bytes"
  ([^File file]
   (dir-size 0 file))
  ([^long total ^File file]
   (if (.isDirectory file)
     (if-let [lst (seq (.listFiles file))]
       (reduce dir-size total lst)
       total)
     (+ total (.length file)))))

(def +tmp+
  (s/escape (let [path #?(:clj (System/getProperty "java.io.tmpdir")
                          :default "/tmp/")]
              (if-not (s/ends-with? path +separator+)
                (str path +separator+)
                path))
            char-escape-string))

(defn tmp-dir
  "Given a directory name as a string, returns an platform
   neutral temporary directory path."
  ([] +tmp+)
  ([dir] (str +tmp+ (s/escape dir char-escape-string))))


;; ----------------------------------------------------------------------------

#?(:cljs
   (do
     (def Exception js/Error)
     (def IllegalArgumentException js/Error)
     (def UnsupportedOperationException js/Error)))

;; ----------------------------------------------------------------------------

;; ----------------------------------------------------------------------------
;; macros and funcs to support writing defrecords and updating
;; (replacing) builtins, i.e., Object/hashCode, IHashEq hasheq, etc.
;; code taken from prismatic:
;;  https://github.com/Prismatic/schema/commit/e31c419c56555c83ef9ee834801e13ef3c112597
;;

(defn- cljs-env?
  "Take the &env from a macro, and tell whether we are expanding into cljs."
  [env]
  (boolean (:ns env)))

#?(:clj
   (defmacro if-cljs
     "Return then if we are generating cljs code and else for Clojure code.
     https://groups.google.com/d/msg/clojurescript/iBY5HaQda4A/w1lAQi9_AwsJ"
     [then else]
     (if (cljs-env? &env) then else)))

#?(:clj
   (defn graal? []
     (System/getProperty "org.graalvm.nativeimage.kind")))

#?(:clj
   (defn- get-sig [method]
     ;; expects something like '(method-symbol [arg arg arg] ...)
     ;; if the thing matches, returns [fully-qualified-symbol arity], otherwise nil
     (and (sequential? method)
          (symbol? (first method))
          (vector? (second method))
          (let [sym (first method)
                ns  (or (some->> sym resolve meta :ns str) "clojure.core")]
            [(symbol ns (name sym)) (-> method second count)]))))

#?(:clj
   (defn- dedupe-interfaces [deftype-form]
     ;; get the interfaces list, remove any duplicates, similar to
     ;; remove-nil-implements in potemkin, verified w/ deftype impl in compiler:
     ;; (deftype* tagname classname [fields]
     ;;   :implements [interfaces] :tag tagname methods*)
     (let [[deftype* tagname classname fields implements interfaces & rest]
           deftype-form]
       (when (or (not= deftype* 'deftype*) (not= implements :implements))
         (throw (IllegalArgumentException. "deftype-form mismatch")))
       (list* deftype* tagname classname fields implements
              (vec (distinct interfaces)) rest))))

#?(:clj
   (defn- make-record-updatable-clj [name fields & impls]
     (let [impl-map (->> impls (map (juxt get-sig identity)) (filter first) (into {}))
           body     (macroexpand-1 (list* 'defrecord name fields impls))]
       (clojure.walk/postwalk
        (fn [form]
          (if (and (sequential? form) (= 'deftype* (first form)))
            (->> form
                 dedupe-interfaces
                 (remove (fn [method]
                           (when-some [impl (-> method get-sig impl-map)]
                             (not= method impl)))))
            form))
        body))))

#?(:clj
   (defn- make-record-updatable-cljs [name fields & impls]
     `(do
        (defrecord ~name ~fields)
        (extend-type ~name ~@impls))))

#?(:clj
   (defmacro defrecord-updatable [name fields & impls]
     `(if-cljs
          ~(apply make-record-updatable-cljs name fields impls)
        ~(apply make-record-updatable-clj  name fields impls))))

;; ----------------------------------------------------------------------------

(defmacro repeat-try-catch
  "Keeps executing body up to n times when an exception is caught and
  the repeat condition function that takes the exception is true,
  otherwise throw."
  [n condition-fn & body]
  (let [helper-fn (gensym "helper")]
    `(letfn [(~helper-fn [^long attempts#]
              (if (pos? attempts#)
                (try
                  ~@body
                  (catch Exception e#
                    (if (and (> attempts# 1) (~condition-fn e#))
                      (~helper-fn (dec attempts#))
                      (throw e#))))
                (raise "Invalid number of attempts" {:num ~n})))]
       (~helper-fn ~n))))

(defmacro combine-cmp [& comps]
  (loop [comps (reverse comps)
         res   (num 0)]
    (if (not-empty comps)
      (recur
        (next comps)
        `(let [c# ~(first comps)]
           (if (= 0 c#)
             ~res
             c#)))
      res)))

(defn combine-hashes [x y]
  #?(:clj  (clojure.lang.Util/hashCombine x y)
     :cljs (hash-combine x y)))

#?(:clj
   (defn- -case-tree [queries variants]
     (if queries
       (let [n  (/ (count variants) 2)
             v1 (take n variants)
             v2 (drop n variants)]
         (list 'if (first queries)
               (-case-tree (next queries) v1)
               (-case-tree (next queries) v2)))
       (first variants))))

#?(:clj
   (defmacro case-tree [qs vs]
     (-case-tree qs vs)))

(defn sym-name-eqs [sym str]
  (and (symbol? sym) (= (name sym) str)))

(defn ensure-vec [x]
  (cond
    (vector? x) x
    (nil? x) []
    (sequential? x) (vec x)
    :else [x]))

(defn memoize-1 [f]
  "Like clojure.core/memoize but only caches the last invocation.
  Effectively dedupes invocations with same args."
  (let [cache (atom {})]
    (fn [& args]
      (or (get @cache args)
          (let [ret (apply f args)]
            (reset! cache {args ret})
            ret)))))

(defn- split-words [s]
  (remove
    empty?
    (-> s
        (s/replace #"_|-" " ")
        (s/replace
          #"(\p{javaUpperCase})((\p{javaUpperCase})[(\p{javaLowerCase})0-9])"
          "$1 $2")
        (s/replace #"(\p{javaLowerCase})(\p{javaUpperCase})" "$1 $2")
        (s/split #"[^\w0-9]+"))))

(defn lisp-case
  ^String [^String s]
  {:pre  [(string? s)]
   :post [(string? %)]}
  (s/join "-" (map s/lower-case (split-words s))))

(defn keyword->string [k] (subs (str k) 1))

(defn lazy-concat
  [colls]
  (lazy-seq
    (when-first [c colls]
      (lazy-cat c (lazy-concat (rest colls))))))

(defn reduce-indexed
  "Same as reduce, but `f` takes [acc el idx]"
  [f init xs]
  (first
    (reduce
      (fn [[acc ^long idx] x]
        (let [res (f acc x idx)]
          (if (reduced? res)
            (reduced [res idx])
            [res (inc idx)])))
      [init 0]
      xs)))

(defn long-inc ^long [^long x] (inc x))

(defn index-of
  [pred xs]
  (some (fn [[x idx]] (when (pred x) idx)) (map vector xs (range))))

(defn array? [^Object x]
  (some-> x .getClass .isArray))

(defn link-vars
  "Makes sure that all changes to `src` are reflected in `dst`."
  [src dst]
  (add-watch src dst
             (fn [_ src old new]
               (alter-var-root dst (constantly @src))
               (alter-meta! dst merge (dissoc (meta src) :name)))))

;; lifted from https://github.com/clj-commons/potemkin
(defmacro import-macro
  "Given a macro in another namespace, defines a macro with the same
   name in the current namespace.  Argument lists, doc-strings, and
   original line-numbers are preserved."
  ([sym]
   `(import-macro ~sym nil))
  ([sym name]
   (let [vr       (resolve sym)
         m        (meta vr)
         n        (or name (with-meta (:name m) {}))
         arglists (:arglists m)]
     (when-not vr
       (throw (IllegalArgumentException. (str "Don't recognize " sym))))
     (when-not (:macro m)
       (throw (IllegalArgumentException.
                (str "Calling import-macro on a non-macro: " sym))))
     `(do
        (def ~n ~(resolve sym))
        (alter-meta! (var ~n) merge (dissoc (meta ~vr) :name))
        (.setMacro (var ~n))
        (link-vars ~vr (var ~n))
        ~vr))))
