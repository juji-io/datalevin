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
   [io.airlift.compress.lz4 Lz4Compressor Lz4Decompressor]))

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

;; dictionary-less compressor
;; using lz4

(defn- create-dict-less-compressor
  []
  (let [^Lz4Compressor compressor     (Lz4Compressor.)
        ^Lz4Decompressor decompressor (Lz4Decompressor.)]
    (reify
      ICompressor
      (method [_] :lz4)
      (bf-compress [_ src dst]
        (let [src   ^ByteBuffer src
              dst   ^ByteBuffer dst
              total (.remaining src)]
          (if (< total ^long c/+value-compress-threshold+)
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

;; key compressor

(defn init-key-freqs ^longs []
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
