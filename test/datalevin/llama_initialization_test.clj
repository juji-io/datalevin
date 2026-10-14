(ns datalevin.llama-initialization-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.util :as u])
  (:import [datalevin.dtlvnative DTLV DTLV$dtlv_llama_embedder
            DTLV$dtlv_llama_generator]
           [datalevin.llm LlamaEmbedder LlamaGenerator]))

(defn- with-invalid-models [f]
  (let [dir (u/tmp-dir (str "llama-initialization-" (random-uuid)))
        missing (str dir "/missing.gguf")
        malformed (str dir "/malformed.gguf")]
    (u/create-dirs dir)
    (try
      (spit malformed "not a GGUF model")
      (f [missing malformed])
      (finally (u/delete-files dir)))))

(deftest failed-native-creation-allows-destruction
  (with-invalid-models
    (fn [paths]
      (doseq [^String path paths]
        (testing path
          ;; Verify the shipped JNI library's failure contract. These calls
          ;; allocate internally before failing to load the requested model.
          (let [handle (DTLV$dtlv_llama_embedder.)]
            (try
              (is (not (zero? (DTLV/dtlv_llama_embedder_create
                               handle path 0 0 0 0))))
              (is (.isNull handle))
              (finally (DTLV/dtlv_llama_embedder_destroy handle))))
          (let [handle (DTLV$dtlv_llama_generator.)]
            (try
              (is (not (zero? (DTLV/dtlv_llama_generator_create
                               handle path 0 0 0))))
              (is (.isNull handle))
              (finally (DTLV/dtlv_llama_generator_destroy handle)))))))))

(deftest failed-constructors-preserve-the-native-error
  (with-invalid-models
    (fn [paths]
      (doseq [^String path paths]
        (testing path
          (is (thrown-with-msg? IllegalStateException
                               #"^Failed to initialize llama embedder \(rc=[1-9][0-9]*\)$"
                               (LlamaEmbedder. path)))
          (is (thrown-with-msg? IllegalStateException
                               #"^Failed to initialize llama generator \(rc=[1-9][0-9]*\)$"
                               (LlamaGenerator. path))))))))
