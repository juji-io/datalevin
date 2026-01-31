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
  "Tuple pipes for query execution"
  (:refer-clojure :exclude [update assoc])
  (:require
   [datalevin.constants :as c]
   [datalevin.util :as u])
  (:import
   [java.util List Collection HashMap]
   [java.util.concurrent LinkedBlockingQueue TimeUnit]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private ^ThreadLocal batch-buffer-tl
  (ThreadLocal.))

(defn batch-buffer
  "Returns a pre-allocated, thread-local FastList for batching.
   The buffer is cleared before returning. Caller should not hold
   references to it across batch operations."
  ^FastList []
  (let [^FastList buf (.get batch-buffer-tl)]
    (if buf
      (do (.clear buf) buf)
      (let [buf (FastList. (int c/query-pipe-batch-size))]
        (.set batch-buffer-tl buf)
        buf))))

(defn- enqueue
  [^LinkedBlockingQueue queue o]
  (try
    (.put queue o) ;; block when full
    true
    (catch InterruptedException e
      (.interrupt (Thread/currentThread))
      (u/raise "Interrupted while enqueuing to pipe" e {:object o}))))

(defprotocol ITuplePipe
  (pipe? [this] "test if implements this protocol")
  (finish [this] "send a sentinel to indicate end of this pipe")
  (produce [this]
    "take a tuple from the pipe, block if there is nothing to take (up to
     c/query-pipe-timeout), if encounter :datalevin/end-scan, return nil")
  (drain-to [this sink] "pour all remaining content into sink")
  (reset [this] "reset the pipe for next round of operation")
  (total [this] "return the total number of tuples pass through the pipe"))

(extend-type Object ITuplePipe (pipe? [_] false))
(extend-type nil ITuplePipe (pipe? [_] false))

(deftype TuplePipe [^LinkedBlockingQueue queue]
  ITuplePipe
  (pipe? [_] true)
  (finish [_] (enqueue queue :datalevin/end-scan))
  (produce [_]
    (let [o (.poll queue ^long c/query-pipe-timeout TimeUnit/MILLISECONDS)]
      (when (nil? o)
        (u/raise "Pipe take timed out waiting for producer"
                 {:timeout c/query-pipe-timeout}))
      (when-not (identical? :datalevin/end-scan o) o)))
  (drain-to [_ sink] (.drainTo queue sink))
  (reset [_] (.clear queue))
  (total [_] 0)

  Collection
  (add [_ o] (enqueue queue o))
  (addAll [_ l]
    (dotimes [i (.size ^List l)]
      (enqueue queue (.get ^List l i)))
    true))

(deftype CountedTuplePipe [^LinkedBlockingQueue queue
                           ^:unsynchronized-mutable total]
  ITuplePipe
  (pipe? [_] true)
  (finish [_] (enqueue queue :datalevin/end-scan))
  (produce [_]
    (let [o (.poll queue ^long c/query-pipe-timeout TimeUnit/MILLISECONDS)]
      (when (nil? o)
        (u/raise "Pipe take timed out waiting for producer"
                 {:timeout c/query-pipe-timeout}))
      (when-not (identical? :datalevin/end-scan o)
        (set! total (u/long-inc total))
        o)))
  (drain-to [_ sink] (.drainTo queue sink))
  (reset [_] (.clear queue))
  (total [_] total)

  Collection
  (add [_ o] (enqueue queue o))
  (addAll [_ o]
    (dotimes [i (.size ^List o)]
      (enqueue queue (.get ^List o i)))
    true))

(defn tuple-pipe
  []
  (->TuplePipe (LinkedBlockingQueue. ^long c/query-pipe-capacity)))

(defn counted-tuple-pipe
  []
  (->CountedTuplePipe (LinkedBlockingQueue. ^long c/query-pipe-capacity) 0))

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

(deftype OrJoinTuplePipe [^List tuples
                          ^long bound-idx
                          ^HashMap or-by-bound
                          ^long free-var-idx
                          ^long tuple-len
                          ^:unsynchronized-mutable ^long i
                          ^:unsynchronized-mutable ^objects current
                          ^:unsynchronized-mutable ^List matches
                          ^:unsynchronized-mutable ^long j]
  ITuplePipe
  (pipe? [_] true)
  (finish [_] nil)
  (produce [_]
    (loop []
      (if (and matches (< j (.size ^List matches)))
        (let [^objects or-tuple (.get ^List matches j)
              fv                (aget or-tuple free-var-idx)
              ^objects joined   (object-array (inc tuple-len))]
          (System/arraycopy current 0 joined 0 tuple-len)
          (aset joined tuple-len fv)
          (set! j (inc j))
          joined)
        (when (< i (.size ^List tuples))
          (let [^objects in-tuple (.get ^List tuples i)
                bv                (aget in-tuple bound-idx)
                ^List m           (.get ^HashMap or-by-bound bv)]
            (set! i (inc i))
            (if (and m (pos? (.size m)))
              (do (set! current in-tuple)
                  (set! matches m)
                  (set! j 0)
                  (recur))
              (do (set! current nil)
                  (set! matches nil)
                  (set! j 0)
                  (recur))))))))
  (drain-to [this sink]
    (loop [t (produce this)]
      (when t
        (.add ^Collection sink t)
        (recur (produce this)))))
  (reset [_]
    (set! i 0)
    (set! current nil)
    (set! matches nil)
    (set! j 0))
  (total [_] 0))

(defn or-join-tuple-pipe
  [tuples bound-idx or-by-bound free-var-idx tuple-len]
  (OrJoinTuplePipe. tuples bound-idx or-by-bound free-var-idx
                    tuple-len 0 nil nil 0))
