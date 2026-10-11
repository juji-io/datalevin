(ns datalevin.tx-group-batch-public-encoding-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.bits :as bits]
            [datalevin.tx-group.batch.public :as public])
  (:import [java.nio ByteBuffer]))

(defn- bounded-charge [allowance]
  (let [used (volatile! 0)]
    (fn [n]
      (let [next (+ (long @used) (long n))]
        (when (> next allowance)
          (throw (ex-info "Encoding exceeds allowance"
                          {:error :txlog/pending-budget-exceeded})))
        (vreset! used next)))))

(deftest encoding-growth-fits-a-peak-scratch-allowance
  (doseq [[size allowance] [[90000 300000] [200000 700000]]
          reuse? [false true]]
    (testing (str size " bytes, reusable scratch: " reuse?)
      ;; These allowances cover overlapping old/new buffers and detached
      ;; output, but not all discarded buffers from the geometric retries.
      (let [value (byte-array size)
            scratch (when reuse? (object-array 1))
            encoded (#'public/encode (bounded-charge allowance)
                                     value :data false scratch)]
        (is (java.util.Arrays/equals
              value ^bytes (bits/read-buffer (ByteBuffer/wrap encoded) :data)))
        (when scratch
          (is (<= (.capacity ^ByteBuffer (aget scratch 0)) 65536)))))))

(deftest encoding-growth-still-charges-overlap-and-detached-output
  (doseq [allowance [180000 250000]]
    ;; 180,000 bytes cannot hold the final growth overlap; 250,000 can hold that
    ;; overlap but cannot also cover the detached serialized output.
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"exceeds allowance"
                         (#'public/encode (bounded-charge allowance)
                                          (byte-array 90000) :data false)))))
