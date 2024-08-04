(ns ^:no-doc datalevin.util
  (:refer-clojure :exclude [seqable? merge-with])
  (:require
   [clojure.walk :as walk]
   [clojure.string :as s]
   [clojure.java.io :as io])
  (:import
   [datalevin.utl LRUCache]
   [clojure.lang IEditableCollection IPersistentSet ITransientSet
    Associative IKVReduce]
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.util Random Arrays Iterator List Collection Deque]
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
    (walk/postwalk
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

(defn memoize-1
  "Like clojure.core/memoize but only caches the last invocation.
  Effectively dedupes invocations with same args."
  [f]
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

;;

(defn walk-collect
  [form pred]
  (let [res (volatile! (transient []))]
    (walk/postwalk
      #(do (when (pred %) (vswap! res conj! %)) %)
      form)
    (persistent! @res)))

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
      [init 0] xs)))

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

(defn list-add [^FastList lst item] (.add lst item) lst)

(defn map-fl
  [f coll]
  (reduce (fn [^FastList acc e] (.add acc (f e)) acc)
          (FastList.)
          coll))

(defn long-inc ^long [^long x] (unchecked-inc x))
(defn long-dec ^long [^long x] (unchecked-dec x))

(def conjv (fnil conj []))
(def conjs (fnil conj #{}))

(defn bit-count
  ^long [^long v]
  (let [v (- v (bit-and (bit-shift-right v 1) 0x55555555))
        v (+ (bit-and v 0x33333333)
             (bit-and (bit-shift-right v 2) 0x33333333))]
    (bit-shift-right
      (* (bit-and (+ v (bit-shift-right v 4)) 0xF0F0F0F) 0x1010101)
      24)))

(defn combinations
  [coll ^long n]
  (let [arr    (object-array coll)
        len    (count coll)
        result (volatile! [])]
    (when (<= n len)
      (dotimes [i (bit-shift-left 1 len)] ;; max-index
        (when (== n (bit-count i))
          (vswap! result conj
                  (for [^long j (range 0 len)
                        :when   (not (zero? (bit-and (bit-shift-left 1 j)
                                                     i)))]
                    (aget arr j))))))
    @result))

(defn index-of
  [pred xs]
  (loop [i 0 xs xs]
    (when (seq xs)
      (if (pred (first xs))
        i
        (recur (inc i) (rest xs))))))

;; lifted from https://github.com/bsless/clj-fast/blob/master/src/clj_fast/core.clj
(defmacro as
  [tag sym]
  (if (symbol? sym)
    (let [tag (if (class? tag) (.getName ^Class tag) (str tag))]
      `(with-meta ~sym {:tag ~tag}))
    sym))

(definline fast-assoc
  "Like assoc but only takes one kv pair. Slightly faster."
  [a k v]
  (if (symbol? a)
    (let [a (as clojure.lang.Associative a)]
      `(.assoc ~a ~k ~v))
    `(let [a# ~a] (fast-assoc a# ~k ~v))))

(defn kvreduce
  {:inline
   (fn kvreduce [f init amap]
     (if (symbol? amap)
       (let [amap (as IKVReduce amap)]
         `(.kvreduce ~amap ~f ~init))
       `(let [amap# ~amap] (kvreduce ~f ~init amap#))))}
  [f init IKVReduce amap]
  (kvreduce amap f init))
;;

(defn merge-with
  [f & maps]
  (when (some identity maps)
    (let [merge-entry (fn [m k v]
			                  (if (contains? m k)
			                    (fast-assoc m k (f (m k) v))
			                    (fast-assoc m k v)))
          merge2      (fn [m1 m2]
		                    (kvreduce merge-entry m1 m2))]
      (reduce merge2 maps))))


(defn idxs-of
  [pred coll]
  (sequence (comp (map #(when (pred %1) %2))
               (remove nil?))
            coll (range)))

(defn keep-idxs
  "take a set of idxs to keep"
  [kp-idxs-set coll]
  (into []
        (comp (map-indexed #(when (kp-idxs-set %1) %2))
           (remove nil?))
        coll))

(defn remove-idxs
  "take a set of idxs to remove"
  [rm-idxs-set coll]
  (into []
        (comp (map-indexed #(when-not (rm-idxs-set %1) %2))
           (remove nil?))
        coll))

(defn intersection
  [^IEditableCollection s1 ^IPersistentSet s2]
  (if (< (count s2) (count s1))
    (recur s2 s1)
    (let [^Iterator items (.iterator ^Iterable s1)]
      (if (instance? IEditableCollection s1)
        (loop [^ITransientSet out (.asTransient s1)]
          (if (.hasNext items)
            (let [item (.next items)]
              (if (.contains s2 item)
                (recur out)
                (recur (.disjoin out item))))
            (.persistent out)))
        (loop [^IPersistentSet out s1]
          (if (.hasNext items)
            (let [item (.next items)]
              (if (.contains s2 item)
                (recur out)
                (recur (.disjoin out item))))
            out))))))

(defn array? [^Object x] (some-> x .getClass .isArray))

(defn same-sign?
  [^long x ^long y]
  (or (= x y 0)
      (and (< x 0) (< y 0))
      (and (> x 0) (> y 0))))

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
             (fn [_ src _ _]
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
   (let [vr (resolve sym)
         m  (meta vr)
         n  (or name (with-meta (:name m) {}))]
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

(def sample-cache (LRUCache. 256))

(defn reservoir-sampling
  "optimized reservoir sampling, random sample n out of m items, returns a
  sorted array of sampled indices, or returns nil if n > m"
  ^longs [^long m ^long n]
  (or (.get ^LRUCache sample-cache [m n])
      (let [res (cond
                  (< n m)
                  (let [indices (long-array (range n))
                        r       (Random.)
                        p       (Math/log (- 1.0 (double (/ n m))))]
                    (loop [i n]
                      (when (< i m)
                        (aset indices (.nextInt r n) i)
                        (recur (+ i
                                  (long (/ (Math/log (- 1.0 (.nextDouble r))) p))
                                  1))))
                    (Arrays/sort indices)
                    indices)
                  (= n m) (long-array (range n))
                  :else   nil)]
        (.put ^LRUCache sample-cache [m n] res)
        res)))
