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
   [datalevin.constants :as c])
  (:import
   [java.util.concurrent ConcurrentLinkedDeque]
   [java.nio ByteBuffer]
   [datalevin.utl BufOps]))

(defn buffer-transfer
  "Transfer content from one bytebuffer to another"
  ([^ByteBuffer src ^ByteBuffer dst]
   (.put dst src))
  ([^ByteBuffer src ^ByteBuffer dst n]
   (dotimes [_ n] (.put dst (.get src)))))

(defn compare-buffer
  ^long [^ByteBuffer b1 ^ByteBuffer b2]
  (BufOps/compareByteBuf b1 b2))

(defonce ^ConcurrentLinkedDeque array-buffers (ConcurrentLinkedDeque.))

(defn- allocate-array-buffer
  ^ByteBuffer [size]
  (ByteBuffer/wrap (byte-array size)))

(defn get-array-buffer
  ([] (get-array-buffer (+ c/+max-key-size+ Integer/BYTES)))
  (^ByteBuffer [^long size]
   (let [cap (+ size Integer/BYTES)] ;; for storing length
     (or (some (fn [^ByteBuffer bf]
                 (when (<= cap (.capacity bf))
                   (.remove array-buffers bf)
                   (.clear bf)
                   bf))
               array-buffers)
         (allocate-array-buffer cap)))))

(defn return-array-buffer [bf] (.offer array-buffers bf))

(defonce ^ConcurrentLinkedDeque direct-buffers (ConcurrentLinkedDeque.))

(defn allocate-buffer
  "Allocate JVM off-heap ByteBuffer in the Datalevin expected endian order"
  ^ByteBuffer [size]
  (ByteBuffer/allocateDirect size))

(defn get-direct-buffer
  ([] (get-array-buffer (+ c/+max-key-size+ Integer/BYTES)))
  (^ByteBuffer [^long size]
   (let [cap (+ size Integer/BYTES)] ;; for storing length
     (or (some (fn [^ByteBuffer bf]
                 (when (<= cap (.capacity bf))
                   (.remove direct-buffers bf)
                   (.clear bf)
                   bf))
               direct-buffers)
         (allocate-buffer cap)))))

(defn return-direct-buffer [bf] (.offer direct-buffers bf))
