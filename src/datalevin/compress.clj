(ns ^:no-doc datalevin.compress
  "Various data compressors"
  (:require
   [datalevin.constants :as c]
   [datalevin.buffer :as bf]
   [datalevin.hu :as hu])
  (:import
   [datalevin.hu HuTucker]
   [java.nio ByteBuffer]
   [me.lemire.integercompression IntCompressor]
   [me.lemire.integercompression.differential IntegratedIntCompressor]
   ;; [com.github.luben.zstd ZstdDictCompress ZstdDictDecompress
   ;;  ZstdDictTrainer Zstd]
   [net.jpountz.lz4 LZ4Factory LZ4Compressor LZ4FastDecompressor]))

(defprotocol ICompressor
  (compress [this obj])
  (uncompress [this obj])
  (bf-compress [this src-bf dst-bf])
  (bf-uncompress [this src-bf dst-bf]))

;; int compressors

(defonce int-compressor
  (let [^IntCompressor compressor (IntCompressor.)]
    (reify
      ICompressor
      (compress [_ ar]
        (.compress compressor ar))
      (uncompress [_ ar]
        (.uncompress compressor ar)))) )

(defonce sorted-int-compressor
  (let [^IntegratedIntCompressor sorted-compressor (IntegratedIntCompressor.)]
    (reify
      ICompressor
      (compress [_ ar]
        (.compress sorted-compressor ar))
      (uncompress [_ ar]
        (.uncompress sorted-compressor ar)))) )

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

;; dictionary-less compressor

(defn- create-dict-less-compressor
  []
  (let [^LZ4Factory factory               (LZ4Factory/fastestInstance)
        ^LZ4Compressor compressor         (.fastCompressor factory)
        ^LZ4FastDecompressor decompressor (.fastDecompressor factory)]
    (reify
      ICompressor
      (bf-compress [_ src dst]
        (let [src   ^ByteBuffer src
              dst   ^ByteBuffer dst
              total (.remaining src)]
          (if (< total ^long c/value-compress-threshold)
            (do (.putInt dst (int 0))
                (bf/buffer-transfer src dst))
            (do (.putInt dst (int (.limit src)))
                (.compress compressor src dst)))))
      (bf-uncompress [_ src dst]
        (let [src   ^ByteBuffer src
              dst   ^ByteBuffer dst
              total (.getInt src)]
          (if (zero? total)
            (bf/buffer-transfer src dst)
            (do (.limit dst total)
                (.decompress decompressor src dst))))))))

(defonce dict-less-compressor (atom nil))

(defn get-dict-less-compressor
  []
  (or @dict-less-compressor
      (do (reset! dict-less-compressor (create-dict-less-compressor))
          @dict-less-compressor)))

;; TODO not used at the moment, as zstd-jni doesn't support graal yet
;; value compressor
#_(defn- value-dictionary
    ^bytes [sample-bas]
    (let [trainer (ZstdDictTrainer. c/compress-sample-size (* 16 1024))]
      (doseq [ba sample-bas] (.addSample trainer ^bytes ba))
      (.trainSamples trainer)))

#_(defn value-compressor
    "take a seq of byte array samples"
    [sample-bas]
    (let [dict  (value-dictionary sample-bas)
          cmp   (ZstdDictCompress. dict (Zstd/defaultCompressionLevel))
          decmp (ZstdDictDecompress. dict)]
      (reify
        ICompressor
        (bf-compress [_ src dst]
          (Zstd/compress ^ByteBuffer dst ^ByteBuffer src cmp))
        (bf-uncompress [_ src dst]
          (Zstd/decompress ^ByteBuffer dst ^ByteBuffer src decmp)))))

;; key compressor

(defn init-key-freqs [] (long-array c/compress-sample-size (repeat 1)))

(defn key-compressor
  [^longs freqs]
  (let [^HuTucker ht (hu/new-hu-tucker freqs)]
    (reify
      ICompressor
      (bf-compress [_ src dst] (.encode ht src dst))
      (bf-uncompress [_ src dst] (.decode ht src dst)))))
