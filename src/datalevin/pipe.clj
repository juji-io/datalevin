;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.pipe
  "Tuple pipes"
  (:refer-clojure :exclude [update assoc])
  (:require
   [datalevin.util :as u])
  (:import
   [java.util List Collection]
   [java.util.concurrent LinkedBlockingQueue TimeUnit]))

(def ^:dynamic *pipe-take-timeout-ms*
  "Max millis to wait for a pipe element before failing; nil to wait forever."
  300000)

(def ^:dynamic *pipe-capacity*
  "Maximum queue size for a tuple pipe; producers block when full."
  4096)

(defn- enqueue!
  [^LinkedBlockingQueue queue o]
  (try
    (.put queue o)
    true
    (catch InterruptedException e
      (.interrupt (Thread/currentThread))
      (u/raise "Interrupted while enqueuing to pipe" {:object o}))))

(defprotocol ITuplePipe
  (pipe? [this] "test if implements this protocol")
  (finish [this] "send a sentinel to indicate end of this pipe")
  (produce [this]
    "take a tuple from the pipe, block if there is nothing to take (up to
     *pipe-take-timeout-ms*), if encounter :datalevin/end-scan, return nil")
  (drain-to [this sink] "pour all remaining content into sink")
  (reset [this] "reset the pipe for next round of operation")
  (total [this] "return the total number of tuples pass through the pipe"))

(extend-type Object ITuplePipe (pipe? [_] false))
(extend-type nil ITuplePipe (pipe? [_] false))

(deftype TuplePipe [^LinkedBlockingQueue queue]
  ITuplePipe
  (pipe? [_] true)
  (finish [_] (enqueue! queue :datalevin/end-scan))
  (produce [_]
    (let [o (if *pipe-take-timeout-ms*
              (.poll queue ^long *pipe-take-timeout-ms*
                     TimeUnit/MILLISECONDS)
              (.take queue))]
      (when (nil? o)
        (u/raise "Pipe take timed out waiting for producer"
                 {:timeout-ms *pipe-take-timeout-ms*}))
      (when-not (identical? :datalevin/end-scan o) o)))
  (drain-to [_ sink] (.drainTo queue sink))
  (reset [_] (.clear queue))
  (total [_] total)

  Collection
  (add [_ o] (enqueue! queue o))
  (addAll [_ o] (doseq [e o] (enqueue! queue e)) true))

(deftype CountedTuplePipe [^LinkedBlockingQueue queue
                           ^:unsynchronized-mutable total]
  ITuplePipe
  (pipe? [_] true)
  (finish [_] (enqueue! queue :datalevin/end-scan))
  (produce [_]
    (let [o (if *pipe-take-timeout-ms*
              (.poll queue ^long *pipe-take-timeout-ms*
                     TimeUnit/MILLISECONDS)
              (.take queue))]
      (when (nil? o)
        (u/raise "Pipe take timed out waiting for producer"
                 {:timeout-ms *pipe-take-timeout-ms*}))
      (when-not (identical? :datalevin/end-scan o)
        (set! total (u/long-inc total))
        o)))
  (drain-to [_ sink] (.drainTo queue sink))
  (reset [_] (.clear queue))
  (total [_] total)

  Collection
  (add [_ o] (enqueue! queue o))
  (addAll [_ o] (doseq [e o] (enqueue! queue e)) true))

(defn tuple-pipe
  []
  (->TuplePipe (LinkedBlockingQueue. ^long *pipe-capacity*)))

(defn counted-tuple-pipe
  []
  (->CountedTuplePipe (LinkedBlockingQueue. ^long *pipe-capacity*) 0))

(defn remove-end-scan
  [tuples]
  (if (.isEmpty ^Collection tuples)
    tuples
    (let [size (.size ^List tuples)
          s-1  (dec size)
          l    (.get ^List tuples s-1)]
      (if (identical? :datalevin/end-scan l)
        (do (.remove ^List tuples s-1)
            (recur tuples))
        tuples))))
