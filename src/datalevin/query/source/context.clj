(ns ^:no-doc datalevin.query.source.context
  "Bounded invocation context extraction before copying query/input values."
  (:require [datalevin.parser :as parser]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.input-program :as input-program]
            [datalevin.query.source.inputs :as inputs]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.timeout :as timeout]))

(defn- header!
  "Borrow only top-level context fields. Do not traverse query constants or
  materialize an unbounded sequence before discovering invocation limits."
  [runtime query]
  (let [ctl (:control runtime)]
    (control/check-active! ctl)
    (cond
      (value/plain-map? query) (select-keys query [:query-source :timeout])
      (sequential? query)
      (let [tokens (volatile! []) read! (inputs/item-reader query)]
        (try
          (loop [section nil header {}]
            (control/check-active! ctl)
            ;; Retain a conservative work charge until this walk completes;
            ;; an infinite top-level query cannot bypass the invocation quota.
            (vswap! tokens conj (control/reserve! ctl {:retained-bytes 512}))
            (if-let [[form] (read!)]
              (cond
                (not form) header ; ordinary query->map stops at nil/false
                (keyword? form) (recur form header)
                (not (#{:query-source :timeout} section)) (recur section header)
                (contains? header section)
                (if (= :query-source section) (fail! :query-source/invalid :context)
                    (recur section header))
                :else (recur section (assoc header section form)))
              header))
          (finally (doseq [token @tokens] (control/release! token)))))
      :else (fail! :query-source/invalid :syntax))))

(defn- timeout! [runtime form]
  ;; Ordinary timeout syntax follows the first item of a sequential form.
  ;; Bound that descent without copying or walking an unused timeout tail.
  (loop [form form depth 0]
    (control/check-active! (:control runtime))
    (when (> depth 128) (fail! :query-source/limit :syntax))
    (if (sequential? form) (recur (first form) (inc depth))
        (input-program/parse! parser/parse-timeout form))))

(defn- deadline [millis]
  (let [local (when millis (long (min Long/MAX_VALUE (+' (System/currentTimeMillis) millis))))]
    (cond (nil? local) timeout/*deadline*
          (nil? timeout/*deadline*) local
          :else (min (long timeout/*deadline*) local))))

(defn open!
  "Create the invocation's child control before copying query values. Explicit
  source options take precedence over the embedded :query-source map. All
  later work, including parsing and shared key workspaces, uses this control."
  [runtime query options]
  (let [header (header! runtime query)
        options (or (if (nil? options) (:query-source header) options) {})
        _ (when-not (value/plain-map? options) (fail! :query-source/invalid :context))
        millis (timeout! runtime (:timeout header))
        inherited (timeout/time-left)
        duration (cond (nil? millis) inherited (nil? inherited) millis :else (min millis inherited))
        ctl (control/create {:parent (:control runtime) :limits (:limits options) :timeout-ms duration})
        deadline (deadline millis)]
    (rt/own! runtime #(control/close! ctl))
    {:runtime (assoc runtime :control ctl :deadline deadline) :options options :deadline deadline}))
