(ns ^:no-doc datalevin.util
  (:refer-clojure :exclude [seqable?])
  (:require
   [clojure.walk]
   [clojure.string :as s]
   [clojure.java.io :as io])
  (:import
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.util Random Arrays]
   [java.io File]
   [java.nio.file Files Paths LinkOption AccessDeniedException]
   [java.nio.file.attribute PosixFilePermissions FileAttribute]))

;; For when we need to use datalevin specific print method
(def ^:dynamic *datalevin-print* false)

(defn seqable?
  ^Boolean [x]
  (and (not (string? x))
       (or (seq? x)
           (instance? clojure.lang.Seqable x)
           (nil? x)
           (instance? Iterable x)
           (instance? java.util.Map x))))

(defn unchunk
  "Unchunked lazy seq, one item at a time"
  [s]
  (when (seq s)
    (lazy-seq
      (cons (first s)
            (unchunk (next s))))))

(defmacro cond+ [& clauses]
  (when-some [[test expr & rest] clauses]
    (case test
      :do   `(do ~expr (cond+ ~@rest))
      :let  `(let ~expr (cond+ ~@rest))
      :some `(or ~expr (cond+ ~@rest))
      `(if ~test ~expr (cond+ ~@rest)))))

(defmacro some-of
  ([] nil)
  ([x] x)
  ([x & more]
   `(let [x# ~x] (if (nil? x#) (some-of ~@more) x#))))

(defmacro raise [& fragments]
  (let [msgs (butlast fragments)
        data (last fragments)]
    `(throw
       (ex-info
         (str ~@(map (fn [m#] (if (string? m#) m# (list 'pr-str m#))) msgs))
         ~data))))

;; files

(defn windows? [] (s/starts-with? (System/getProperty "os.name") "Windows"))

(defn apple-silicon?
  []
  (and (= (System/getProperty "os.name") "Mac OS X")
       (= (System/getProperty "os.arch") "aarch64")))

(defn concatv [& xs] (into [] cat xs))

(defn delete-files
  "Recursively delete file"
  [& fs]
  (when-let [f (first fs)]
    (if-let [cs (seq (.listFiles (io/file f)))]
      (recur (concatv cs fs))
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
      (raise "Access denied " e {:path path}))
    (catch Exception e
      (raise "Error openning file " e {:path path}))))

(defn empty-dir?
  "test if the given File is an empty directory"
  [^File file]
  (and (.isDirectory file) (-> file .list empty?)))

(def +separator+ java.io.File/separator)

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
  (s/escape (let [path (System/getProperty "java.io.tmpdir")]
              (if-not (s/ends-with? path +separator+)
                (str path +separator+)
                path))
            char-escape-string))

(defn tmp-dir
  "Given a directory name as a string, returns an platform
   neutral temporary directory path."
  ([] +tmp+)
  ([dir] (str +tmp+ (s/escape dir char-escape-string))))

(defn graal? [] (System/getProperty "org.graalvm.nativeimage.kind"))

;; ----------------------------------------------------------------------------
;; macros and funcs to support writing defrecords and updating
;; (replacing) builtins, i.e., Object/hashCode, IHashEq hasheq, etc.
;; code taken from prismatic:
;;  https://github.com/Prismatic/schema/commit/e31c419c56555c83ef9ee834801e13ef3c112597
;;

(defn- get-sig
  [method]
  ;; expects something like '(method-symbol [arg arg arg] ...)
  ;; if the thing matches, returns [fully-qualified-symbol arity], otherwise nil
  (and (sequential? method)
       (symbol? (first method))
       (vector? (second method))
       (let [sym (first method)
             ns  (or (some->> sym resolve meta :ns str) "clojure.core")]
         [(symbol ns (name sym)) (-> method second count)])))

(defn- dedupe-interfaces
  [deftype-form]
  ;; get the interfaces list, remove any duplicates, similar to
  ;; remove-nil-implements in potemkin, verified w/ deftype impl in compiler:
  ;; (deftype* tagname classname [fields]
  ;;   :implements [interfaces] :tag tagname methods*)
  (let [[deftype* tagname classname fields implements interfaces & rest]
        deftype-form]
    (when (or (not= deftype* 'deftype*) (not= implements :implements))
      (throw (IllegalArgumentException. "deftype-form mismatch")))
    (list* deftype* tagname classname fields implements
           (vec (distinct interfaces)) rest)))

(defn- make-record-updatable-clj [name fields & impls]
  (let [impl-map (->> impls (map (juxt get-sig identity))
                      (filter first) (into {}))
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
      body)))

(defmacro defrecord-updatable [name fields & impls]
  (apply make-record-updatable-clj  name fields impls))

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

(defmacro combine-cmp
  "Combine multiple comparisons"
  [& comps]
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

(defn combine-hashes [x y] (clojure.lang.Util/hashCombine x y))

(defn- -case-tree [queries variants]
  (if queries
    (let [n  (/ (count variants) 2)
          v1 (take n variants)
          v2 (drop n variants)]
      (list 'if (first queries)
             (-case-tree (next queries) v1)
             (-case-tree (next queries) v2)))
    (first variants)))

(defmacro case-tree [qs vs] (-case-tree qs vs))

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

(defn distinct-by
  [f coll]
  (peek (reduce (fn [[seen ^FastList res :as acc] el]
                  (let [key (f el)]
                    (if (contains? seen key)
                      acc
                      (do (.add res el)
                          [(conj! seen key) res]))))
                [(transient #{}) (FastList.)]
                coll)))

(defn map-fl
  [f coll]
  (reduce (fn [^FastList acc e]
            (.add acc (f e))
            acc)
          (FastList.)
          coll))

(defn long-inc ^long [^long x] (inc x))

(defn index-of
  [pred xs]
  (some (fn [[x idx]] (when (pred x) idx)) (map vector xs (range))))

(defn array? [^Object x] (some-> x .getClass .isArray))

(defn same-sign?
  [^long x ^long y]
  (or (= x y 0)
      (and (< x 0) (< y 0))
      (and (> x 0) (> y 0))))

(defn szudzik
  "Szudzik's paring function"
  [x y]
  (let [x (int x) y (int y)]
    (if (> y x)
      (+ x (* y y))
      (+ x y (* x x)))))

(defn n-bits-mask ^long [^long n] (dec (bit-shift-left 1 n)))

(def hex [\0 \1 \2 \3 \4 \5 \6 \7 \8 \9 \A \B \C \D \E \F])

(defn hexify-byte
  "Convert a byte to a hex pair"
  [b]
  (let [v (bit-and ^byte b 0xFF)]
    [(hex (bit-shift-right v 4)) (hex (bit-and v 0x0F))]))

(defn hexify
  "Convert bytes to hex string"
  [bs]
  (apply str (mapcat hexify-byte bs)))

(defn unhexify-2c
  "Convert two hex characters to a byte"
  [c1 c2]
  (unchecked-byte
    (+ (bit-shift-left (Character/digit ^char c1 16) 4)
       (Character/digit ^char c2 16))))

(defn unhexify
  "Convert hex string to byte sequence"
  [s]
  (map #(apply unhexify-2c %) (partition 2 s)))

(defn hexify-string [^String s] (hexify (.getBytes s)))

(defn unhexify-string [s] (String. (byte-array (unhexify s))))

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

(defn reservoir-sampling
  "optimized reservoir sampling, random sample n out of m items, returns a
  sorted array of sampled indices, or returns nil if n >= m"
  ^longs [^long m ^long n]
  (when (< n m)
    (let [indices (long-array (range n))
          r       (Random.)
          p       (Math/log (- 1.0 (double (/ n m))))]
      (loop [i n]
        (when (< i m)
          (aset indices (.nextInt r n) i)
          (recur (+ i (long (/ (Math/log (- 1.0 (.nextDouble r))) p)) 1))))
      (Arrays/sort indices)
      indices)))
