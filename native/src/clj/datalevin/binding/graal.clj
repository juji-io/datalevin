(ns datalevin.binding.graal
  "LMDB binding for GraalVM native image"
  (:require [datalevin.bits :as b]
            [datalevin.util :refer [raise]]
            [datalevin.constants :as c]
            [datalevin.lmdb :refer [open-lmdb IBuffer IRange IRtx IKV ILMDB]]
            [clojure.string :as s])
  (:import [clojure.lang IMapEntry]
           [java.util Iterator]
           [java.util.concurrent ConcurrentHashMap]
           [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer]
           [java.lang.annotation Retention RetentionPolicy Target ElementType]
           [datalevin.ni LMDB LMDB$Directives]
           [com.oracle.svm.core.c CGlobalData CGlobalDataFactory]
           [org.graalvm.nativeimage PinnedObject StackValue]
           [org.graalvm.nativeimage.c CContext]
           [org.graalvm.nativeimage.c.type CTypeConversion WordPointer
            CTypeConversion$CCharPointerHolder]
           ))

(deftype ^{;Retention RetentionPolicy/RUNTIME
           CContext {:value LMDB$Directives}}
    Library [env dir dbis])

(defmethod open-lmdb :graal
  [dir]
  #_(try
      (let [file          (b/file dir)
            builder       (doto (Env/create)
                            (.setMapSize (* ^long c/+init-db-size+ 1024 1024))
                            (.setMaxReaders c/+max-readers+)
                            (.setMaxDbs c/+max-dbs+))
            ^Env env      (.open builder
                                 file
                                 (into-array EnvFlags default-env-flags))
            ^RtxPool pool (->RtxPool env (ConcurrentHashMap.) 0)]
        (LMDB. env dir pool (ConcurrentHashMap.)))
      (catch Exception e
        (raise
          "Fail to open LMDB database: " (ex-message e)
          {:dir dir}))))
