;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.failure
  "Failure provenance at user callback boundaries."
  (:import [datalevin.cpp Util$DTLVException]))

(defn user-body-failure? [t]
  (true? (::body-failure (ex-data t))))

(defn user-body-failure [^Exception t]
  (if (user-body-failure? t)
    t
    (ex-info (ex-message t) (assoc (ex-data t) ::body-failure true) t)))

(defmacro with-user-body
  "Tag exceptions from an actual user callback in a native batch. Outside that
  path, preserve the original exception. Errors are never application failures."
  [batch? & body]
  `(let [batch?# ~batch?]
     (try ~@body
          ;; The explicit transaction watchdog recognizes the interrupted
          ;; callback directly; retain that cause and owner-interrupt handling.
          (catch InterruptedException t# (throw t#))
          (catch Exception t#
            (throw (if batch?# (user-body-failure t#) t#))))))

(defn unwrap-user-body-failure [t]
  (if (user-body-failure? t) (ex-cause t) t))

(defn expected-native-rejection? [^Throwable t]
  (boolean
    (or (and (instance? Util$DTLVException t)
             (let [message (.getMessage t)]
               (or (.startsWith message "MDB_KEYEXIST")
                   (.startsWith message "MDB_BAD_VALSIZE"))))
        (when-let [cause (ex-cause t)] (expected-native-rejection? cause)))))
