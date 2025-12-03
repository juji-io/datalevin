;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.compress
  "Various data compressors"
  (:require
   [clojure.java.io :as io]
   [datalevin.constants :as c]
   [datalevin.hu :as hu])
  (:import
   [java.nio ByteBuffer]
   [java.nio.file Files Path Paths]
   [datalevin.hu HuTucker]
   [me.lemire.integercompression IntCompressor]
   [me.lemire.integercompression.differential IntegratedIntCompressor]
   [com.github.luben.zstd Zstd ZstdDictCompress ZstdDictDecompress
    ZstdDictTrainer]))

(defprotocol ICompressor
  (method [this] "compression method, a keyword")
  (compress [this obj] "compress into byte array")
  (uncompress [this obj] "takes a byte array")
  (bf-compress [this src-bf dst-bf] "compress between byte buffers")
  (bf-uncompress [this src-bf dst-bf] "decompress between byte buffers"))

;; int compressors

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

;; Value compressor using zstd

(defn train-zstd
  "given a sample of bytes, return the zstd dictionary bytes"
  [samples]
  (let [sample-size (transduce (map alength) + samples)
        dict-size   (* c/+value-compress-dict-size+ 1024)
        trainer     (ZstdDictTrainer. sample-size dict-size)]
    (doseq [sample samples]
      (.addSample trainer sample))
    (.trainSamples trainer)))

(defn- zstd-compressor
  [^bytes dict level]
  (let [dict-compress   (ZstdDictCompress. dict (int level))
        dict-decompress (ZstdDictDecompress. dict)]
    (reify
      ICompressor
      (method [_] :zstd)
      (bf-compress [_  src dst]
        (Zstd/compress ^ByteBuffer dst ^ByteBuffer src dict-compress))
      (bf-uncompress [_ src dst]
        (Zstd/decompress ^ByteBuffer dst ^ByteBuffer src dict-decompress)))))

(defn val-compressor
  [^bytes dict]
  (zstd-compressor dict 3))

(defn load-val-compressor
  [^String path]
  (let [dict (Files/readAllBytes (Paths/get path))]
    (zstd-compressor dict 3)))

;; key compressor using Hu-Tucker coding

(defn init-key-freqs
  "We assume a Dirichlet prior"
  ^longs []
  (long-array c/+key-compress-num-symbols+ (repeat 1)))

(defn- hu-compressor
  [^HuTucker ht]
  (reify
    ICompressor
    (method [_] :hu)
    (bf-compress [_ src dst] (.encode ht src dst))
    (bf-uncompress [_ src dst] (.decode ht src dst))))

(defn key-compressor
  ([^longs freqs]
   (hu-compressor (hu/new-hu-tucker freqs)))
  ([^bytes lens ^ints codes]
   (hu-compressor (hu/codes->hu-tucker lens codes))))

(defn load-key-compressor
  [^String path]
  (hu-compressor (hu/load-hu-tucker (io/input-stream path))))
