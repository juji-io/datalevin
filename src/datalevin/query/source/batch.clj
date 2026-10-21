(ns ^:no-doc datalevin.query.source.batch
  "Shared validation and reservation transfer for provider cursor batches."
  (:require [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]))

(defn- ensure! [ok]
  (when-not ok (fail! :query-source/invalid :batch)))

(defn- nonnegative? [x]
  (and (integer? x) (<= 0 x Long/MAX_VALUE)))

(defn- limit! [category actual maximum]
  (when-not (value/at-most? actual (long maximum))
    (fail! :query-source/limit :batch
           {:category category :maximum maximum :attempted actual})))

(defn accept!
  "Validate an input-free or dependent batch before transferring its reservation
  to consume!. The consumer owns release, including when consumption throws.
  Temporary validation charges remain live until consume! returns."
  [runtime columns dependent? request batch consume!]
  (let [ctl (:control runtime) tokens (volatile! [])
        keep! #(vswap! tokens conj %)]
    (try
      (keep! (control/reserve! ctl {:retained-bytes 512}))
      (when (nil? batch) (fail! :query-source/incomplete :batch))
      (ensure! (value/plain-map? batch))
      (let [required (cond-> #{:columns :rows :row-count :byte-count :exhausted?}
                       dependent? (conj :binding-ids))]
        (ensure! (and (every? #(contains? batch %) required)
                      (every? #(do (control/check-active! ctl)
                                   (or (required %) (= :reservation %) (qualified-keyword? %)))
                              (keys batch))
                      (vector? (:columns batch)) (= columns (:columns batch)) (vector? (:rows batch))
                      (nonnegative? (:row-count batch)) (nonnegative? (:byte-count batch))
                      (boolean? (:exhausted? batch))
                      (= (:row-count batch) (count (:rows batch)))
                      (or (seq (:rows batch)) (:exhausted? batch))))
        ;; Reject oversized envelopes before walking any rows or annotations.
        (limit! :batch-rows (:row-count batch) (:max-rows request))
        (limit! :batch-bytes (:byte-count batch) (:max-bytes request))
        (keep! (rt/charge-metadata! runtime (meta batch) :batch))
        ;; The empty input-free charge is zero; its columns can still carry
        ;; metadata/backing, so admit those separately even for an empty final.
        (keep! (rt/charge-metadata! runtime (:columns batch) :batch))
        (doseq [[k x] batch :when (qualified-keyword? k)]
          (keep! (rt/charge-metadata! runtime [k x] :batch)))
        (let [charged (volatile! 0)
              observe! (fn [n]
                         (control/check-active! ctl)
                         (limit! :batch-bytes (vswap! charged value/add-charge n) (:max-bytes request)))
              bytes (if dependent?
                      (value/dependent-batch-charge-bytes (:columns batch) (:rows batch) (:binding-ids batch) observe!)
                      (value/batch-charge-bytes (:columns batch) (:rows batch) observe!))]
          (ensure! (= bytes (:byte-count batch)))
          (when (or (not (value/at-most? bytes 0)) (:reservation batch))
            (control/accept! ctl (:reservation batch) bytes)))
        (consume! batch))
      (finally
        (doseq [token @tokens] (control/release! token))))))
