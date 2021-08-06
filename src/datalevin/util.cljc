(ns ^:no-doc datalevin.util
  (:refer-clojure :exclude [seqable?])
  (:require [clojure.walk]
            [clojure.string :as s]
            [clojure.java.io :as io]
            [cognitect.transit :as transit])
  #?(:clj
     (:import
      [java.io ByteArrayInputStream ByteArrayOutputStream
       BufferedReader PrintWriter File]
      [java.nio ByteBuffer]
      [java.nio.file Files Path Paths LinkOption AccessDeniedException]
      [java.nio.file.attribute PosixFilePermissions FileAttribute]
      [java.net URI]
      [java.util Base64 Base64$Decoder Base64$Encoder])))

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

#?(:clj
   (defmacro cond+ [& clauses]
     (when-some [[test expr & rest] clauses]
       (case test
         :let `(let ~expr (cond+ ~@rest))
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

(defn delete-files
  "Recursively delete file"
  [& fs]
  (when-let [f (first fs)]
    (if-let [cs (seq (.listFiles (io/file f)))]
      (recur (concat cs fs))
      (do (io/delete-file f)
          (recur (rest fs))))))

(defn file
  "Return directory path as File, create it if missing"
  [^String path]
  (try
    (let [path' (Paths/get path (into-array String []))]
      (if (Files/exists path' (into-array LinkOption []))
        (io/file path)
        (do (Files/createDirectories
              path'
              (into-array FileAttribute
                          [(PosixFilePermissions/asFileAttribute
                             (PosixFilePermissions/fromString "rwxr-x---"))]))
            (io/file path))))
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

;; en/decode

(defn read-transit-string
  "Read a transit+json encoded string into a Clojure value"
  [^String s]
  (try
    (transit/read
      (transit/reader
        (ByteArrayInputStream. (.getBytes s "utf-8")) :json))
    (catch Exception e
      (raise "Unable to read transit:" (ex-message e) {:string s}))))

(defn write-transit-string
  "Write a Clojure value as a transit+json encoded string"
  [v]
  (try
    (let [baos (ByteArrayOutputStream.)]
      (transit/write (transit/writer baos :json) v)
      (.toString baos "utf-8"))
    (catch Exception e
      (raise "Unable to write transit:" (ex-message e) {:value v}))))

(def base64-encoder (.withoutPadding (Base64/getEncoder)))

(def base64-decoder (Base64/getDecoder))

(defn encode-base64
  "encode bytes into a base64 string"
  [bs]
  (.encodeToString ^Base64$Encoder base64-encoder bs))

(defn decode-base64
  "decode a base64 string to return the bytes"
  [^String s]
  (.decode ^Base64$Decoder base64-decoder s))


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
   (defn windows? []
     (s/starts-with? (System/getProperty "os.name") "Windows")))

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
