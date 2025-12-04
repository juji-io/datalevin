;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.ints
  "ints compressors"
  (:require
   [datalevin.interface :as i
    :refer [ICompressor compress uncompress]])
  (:import
   [java.nio ByteBuffer]
   [me.lemire.integercompression IntCompressor]
   [me.lemire.integercompression.differential IntegratedIntCompressor]))

(defonce int-compressor
  (let [^IntCompressor compressor (IntCompressor.)]
    (reify
      ICompressor
      (method [_] :int)
      (compress [_ ar] (.compress compressor ar))
      (uncompress [_ ar] (.uncompress compressor ar)))) )

(defonce sorted-int-compressor
  (let [^IntegratedIntCompressor sorted-compressor (IntegratedIntCompressor.)]
    (reify
      ICompressor
      (method [_] :sorted-int)
      (compress [_ ar] (.compress sorted-compressor ar))
      (uncompress [_ ar] (.uncompress sorted-compressor ar)))) )

(defn- get-ints*
  [compressor ^ByteBuffer bf]
  (let [csize (.getInt bf)
        comp? (neg? csize)
        size  (if comp? (- csize) csize)
        car   (int-array size)]
    (dotimes [i size] (aset car i (.getInt bf)))
    (if comp?
      (uncompress ^ICompressor compressor car)
      car)))

(defn- put-ints*
  [compressor ^ByteBuffer bf ^ints ar]
  (let [osize     (alength ar)
        comp?     (< 3 osize) ;; don't compress small array
        ^ints car (if comp?
                    (compress ^ICompressor compressor ar)
                    ar)
        size      (alength car)]
    (.putInt bf (if comp? (- size) size))
    (dotimes [i size] (.putInt bf (aget car i)))))

(defn get-ints [bf] (get-ints* int-compressor bf))

(defn put-ints [bf ar] (put-ints* int-compressor bf ar))

(defn get-sorted-ints [bf] (get-ints* sorted-int-compressor bf))

(defn put-sorted-ints [bf ar] (put-ints* sorted-int-compressor bf ar))
