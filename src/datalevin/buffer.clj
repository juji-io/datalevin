;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.buffer
  "ByteBuffer operations"
  (:require
   [datalevin.constants :as c]
   [datalevin.util :as u])
  (:import
   [java.util.concurrent ArrayBlockingQueue]
   [java.nio ByteBuffer]
   [datalevin.utl BufOps]
   [datalevin.io ScratchBufferPool]))

(defn buffer-transfer
  "Transfer content from one bytebuffer to another"
  ([^ByteBuffer src ^ByteBuffer dst]
   (.put dst src))
  ([^ByteBuffer src ^ByteBuffer dst n]
   (dotimes [_ n] (.put dst (.get src)))))

(defn compare-buffer
  ^long [^ByteBuffer b1 ^ByteBuffer b2]
  (BufOps/compareByteBuf b1 b2))

;; Bound both the number of idle buffers and each buffer's capacity, limiting
;; retained heap/direct storage to 32 MiB per pool after a burst of large reads.
(def ^:private ^:const buffer-pool-size 32)
(def ^:private ^:const max-pooled-buffer-capacity (* 1024 1024))

(defonce ^ArrayBlockingQueue array-buffers
  (ArrayBlockingQueue. buffer-pool-size))

(defn- allocate-array-buffer
  ^ByteBuffer [size]
  (ByteBuffer/wrap (byte-array size)))

(defn- take-buffer!
  ^ByteBuffer [^ArrayBlockingQueue buffers allocate ^long size]
  (let [cap (+ size Integer/BYTES)] ;; for storing length
    ;; An unpoolable request must not drain buffers useful to smaller callers.
    (if (> cap max-pooled-buffer-capacity)
      (allocate cap)
      (loop []
        (if-let [^ByteBuffer bf (.poll buffers)]
          (if (<= cap (.capacity bf))
            (do (.clear bf) bf)
            ;; Drop undersized buffers instead of scanning them on every call.
            (recur))
          (allocate cap))))))

(defn- return-buffer!
  [^ArrayBlockingQueue buffers ^ByteBuffer bf]
  (and (<= (.capacity bf) max-pooled-buffer-capacity)
       (.offer buffers bf)))

(defn get-array-buffer
  ([] (get-array-buffer (+ c/+max-key-size+ Integer/BYTES)))
  (^ByteBuffer [^long size]
   (take-buffer! array-buffers allocate-array-buffer size)))

(defn return-array-buffer [bf] (return-buffer! array-buffers bf))

(defonce ^ArrayBlockingQueue direct-buffers
  (ArrayBlockingQueue. buffer-pool-size))

(defn allocate-buffer
  "Allocate JVM off-heap ByteBuffer in the Datalevin expected endian order"
  ^ByteBuffer [size]
  (.acquireBuffer (ScratchBufferPool/shared) (int size)))

(def ^:const max-retained-buffer-capacity
  "Maximum capacity retained by an idle connection or value scratch owner."
  (* 1024 1024))

(defn release-buffer!
  "Return an exclusively owned allocation to the shared byte-budgeted cache,
  or release it. Never pass a slice or borrowed view."
  [^ByteBuffer buffer]
  (.releaseBuffer (ScratchBufferPool/shared) buffer)
  nil)

(defn grown-capacity
  "Geometric buffer growth with checked JVM capacity bounds."
  ^long [^long capacity ^long required]
  (when (or (neg? required) (> required Integer/MAX_VALUE))
    (throw (ex-info "Buffer size is too large" {:size required})))
  (min (long Integer/MAX_VALUE) (max required (* 2 capacity))))

(defn resize-buffer!
  "Replace an owned buffer in write mode, preserving its buffered bytes and
  releasing its old allocation. No views of the old buffer may remain in use."
  ^ByteBuffer [^ByteBuffer buffer ^long capacity]
  (let [replacement (or (when (> capacity (.capacity buffer))
                          (.cachedBuffer (ScratchBufferPool/shared) (int capacity)))
                        (allocate-buffer capacity))]
    (.flip buffer)
    (.put replacement buffer)
    (release-buffer! buffer)
    replacement))

(defn trim-buffer!
  "Bound idle retention, preserving any unread bytes in a write-mode buffer.
  Large pending frames keep their allocation until they have been consumed."
  ^ByteBuffer [^ByteBuffer buffer ^long initial-capacity]
  (if (and (> (.capacity buffer) max-retained-buffer-capacity)
           (<= (.position buffer) max-retained-buffer-capacity))
    (resize-buffer! buffer (max initial-capacity (.position buffer)))
    buffer))

(defn get-direct-buffer
  ([] (get-direct-buffer (+ c/+max-key-size+ Integer/BYTES)))
  (^ByteBuffer [^long size]
   (take-buffer! direct-buffers allocate-buffer size)))

(defn return-direct-buffer [^ByteBuffer bf]
  (if (> (.capacity bf) max-pooled-buffer-capacity)
    (do (release-buffer! bf) false)
    (return-buffer! direct-buffers bf)))

(defn hex-buffer [^ByteBuffer sb]
  (let [sa (byte-array (.remaining sb))]
    (.get sb sa)
    (u/hexify sa)))
