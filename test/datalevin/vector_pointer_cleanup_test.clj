(ns datalevin.vector-pointer-cleanup-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.util :as u])
  (:import [datalevin.cpp VecIdx VecIdx$IndexInfo VecIdx$SearchResult]
           [datalevin.dtlvnative DTLV]
           [org.bytedeco.javacpp BytePointer Pointer]))

(defn- create-index []
  (VecIdx/create 8 DTLV/usearch_metric_l2sq_k DTLV/usearch_scalar_f32_k
                 16 64 64))

(use-fixtures
  :once
  (fn [f]
    ;; Load JNI and its allocation bookkeeping before taking any baselines.
    (VecIdx/free (create-index))
    (System/gc)
    (Pointer/deallocateReferences)
    (f)))

(defn- balanced-call [label f]
  (let [bytes (Pointer/totalBytes)
        pointers (Pointer/totalCount)]
    (try
      (f)
      (finally
        ;; Check immediately, without GC: these are the actual JavaCPP-owned
        ;; native allocations, not RSS or the index's intentionally retained data.
        (is (= bytes (Pointer/totalBytes)) (str label " retained native bytes"))
        (is (= pointers (Pointer/totalCount)) (str label " retained native pointers"))))))

(defn- scalar-cases []
  (let [doubles (double-array [1 0 0 0 0 0 0 0])
        floats (float-array [1 0 0 0 0 0 0 0])
        ;; IEEE half-precision 1.0, represented as its raw 16-bit value.
        shorts (short-array [0x3c00 0 0 0 0 0 0 0])
        int8s (byte-array [1 0 0 0 0 0 0 0])
        bits (byte-array [85])]
    [{:name :double :kind DTLV/usearch_scalar_f64_k :input doubles
      :add #(VecIdx/addDouble % 1 doubles)
      :get #(VecIdx/getDouble % 1 8)
      :missing #(VecIdx/getDouble % 2 8)
      :search #(VecIdx/searchDouble % doubles 1)}
     {:name :float :kind DTLV/usearch_scalar_f32_k :input floats
      :add #(VecIdx/addFloat % 1 floats)
      :get #(VecIdx/getFloat % 1 8)
      :missing #(VecIdx/getFloat % 2 8)
      :search #(VecIdx/searchFloat % floats 1)}
     {:name :float16 :kind DTLV/usearch_scalar_f16_k :input shorts
      :add #(VecIdx/addShort % 1 shorts)
      :get #(VecIdx/getShort % 1 8)
      :missing #(VecIdx/getShort % 2 8)
      :search #(VecIdx/searchShort % shorts 1)}
     {:name :int8 :kind DTLV/usearch_scalar_i8_k :input int8s
      :add #(VecIdx/addInt8 % 1 int8s)
      :get #(VecIdx/getInt8 % 1 8)
      :missing #(VecIdx/getInt8 % 2 8)
      :search #(VecIdx/searchInt8 % int8s 1)}
     {:name :bits :kind DTLV/usearch_scalar_b1_k :input bits
      :add #(VecIdx/addByte % 1 bits)
      :get #(VecIdx/getByte % 1 1)
      :missing #(VecIdx/getByte % 2 1)
      :search #(VecIdx/searchByte % bits 1)}]))

(deftest scalar-operations-release-temporary-native-pointers
  (doseq [{:keys [name kind input add get missing search]} (scalar-cases)]
    (testing (str name)
      (let [metric (if (= name :bits)
                     DTLV/usearch_metric_hamming_k
                     DTLV/usearch_metric_l2sq_k)
            index (VecIdx/create 8 metric kind 16 64 64)]
        (try
          (dotimes [_ 20]
            (balanced-call :add #(add index))
            (let [result (balanced-call :get #(get index))
                  ^VecIdx$SearchResult found (balanced-call :search #(search index))]
              ;; Results must remain valid after the temporary native buffers close.
              (is (= (vec input) (vec result)))
              (is (= [1] (vec (.getKeys found))))
              (is (= [0.0] (vec (.getDists found)))))
            (is (nil? (balanced-call :missing #(missing index))))
            (balanced-call :remove #(VecIdx/remove index 1)))
          (finally (VecIdx/free index)))))))

(deftest native-add-errors-release-input-and-error-pointers
  (doseq [{:keys [name kind add]} (scalar-cases)]
    (testing (str name)
      (let [metric (if (= name :bits)
                     DTLV/usearch_metric_hamming_k
                     DTLV/usearch_metric_l2sq_k)
            index (VecIdx/create 8 metric kind 16 64 64)]
        (try
          (add index)
          (is (thrown-with-msg? RuntimeException #"Duplicate keys"
                               (balanced-call :failed-add #(add index))))
          (finally (VecIdx/free index)))))))

(deftest index-lifecycle-releases-options-and-error-pointers
  (let [index (balanced-call :create create-index)]
    (try
      (let [^VecIdx$IndexInfo info (balanced-call :info #(VecIdx/info index))]
        (is (= 0 (.getSize info))))
      (finally (balanced-call :free #(VecIdx/free index)))))
  (is (thrown-with-msg?
        RuntimeException #"Unknown metric"
        (balanced-call :failed-create
                       #(VecIdx/create 8 DTLV/usearch_metric_l2sq_k
                                       DTLV/usearch_scalar_unknown_k 16 64 64)))))

(deftest index-persistence-releases-temporary-pointers
  (let [dir (u/tmp-dir (str "vector-pointer-cleanup-" (random-uuid)))
        index (create-index)
        loaded (create-index)
        input (float-array [1 0 0 0 0 0 0 0])]
    (u/create-dirs dir)
    (try
      (VecIdx/addFloat index 1 input)
      (let [filename (str dir "/index.usearch")
            missing (str dir "/missing.usearch")
            length (balanced-call :serialized-length #(VecIdx/serializedLength index))]
        (balanced-call :save #(VecIdx/save index filename))
        (balanced-call :load #(VecIdx/load loaded filename))
        (is (= (vec input) (vec (VecIdx/getFloat loaded 1 8))))
        (with-open [buffer (BytePointer. (long length))]
          (balanced-call :save-buffer #(VecIdx/saveBuffer index buffer length))
          (balanced-call :load-buffer #(VecIdx/loadBuffer loaded buffer length))
          (is (not (.isNull buffer)) "Serialization buffers remain caller-owned")
          (is (= (vec input) (vec (VecIdx/getFloat loaded 1 8)))))
        (is (thrown-with-msg? RuntimeException #"Fail to load index"
                             (balanced-call :failed-load #(VecIdx/load loaded missing))))
        (is (thrown? RuntimeException
                     (balanced-call :failed-save #(VecIdx/save index dir)))))
      (finally
        (VecIdx/free loaded)
        (VecIdx/free index)
        (u/delete-files dir)))))
