(ns datalevin.tx-group-batch-error-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.embedded :as embedded]
            [datalevin.tx-group.failure :as failure]))

(deftest cancellation-adaptation-is-independent-of-wrapper-wording
  (doseq [user-body? [false true]
          message ["Cancellation wording changed" ""]]
    (testing (str "user body: " user-body? ", wrapper: " (pr-str message))
      (let [original (ex-info "Application rejected" {:error :application/rejected})
            cause (if user-body? (failure/user-body-failure original) original)
            cancellation (batch/cancellation-error cause)
            wrapper (ex-cause cancellation)
            renamed (ex-info message (ex-data wrapper) (ex-cause wrapper))]
        ;; The collector delivers the outcome wrapper after removing its
        ;; pre-dispatch control exception. Public adaptation preserves identity.
        (is (identical? original (#'embedded/public-error renamed)))))))

(deftest public-errors-do-not-unwrap-unmarked-outcome-errors
  (let [cause (ex-info "Original cause" {})
        original (ex-info "Batch aborted before WAL append"
                          {:error :application/rejected :outcome :not-committed}
                          cause)
        cancellation (batch/cancellation-error original)]
    ;; An application's own error can use the old wrapper wording and already
    ;; carry its outcome. Neither its data nor its cause makes it a wrapper.
    (is (identical? original (#'embedded/public-error original)))
    (is (identical? original (#'embedded/public-error (ex-cause cancellation))))))
