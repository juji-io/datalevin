(ns ^:no-doc datalevin.compress
  "Data compressors"
  (:require
   [datalevin.hu :as hu])
  (:import
   [datalevin.hu HuTucker]
   [java.nio ByteBuffer]
   [me.lemire.integercompression IntCompressor]
   [me.lemire.integercompression.differential IntegratedIntCompressor]
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

;; key value compressors

(defonce value-compressor
  (let [^LZ4Factory factory               (LZ4Factory/fastestInstance)
        ^LZ4Compressor compressor         (.fastCompressor factory)
        ^LZ4FastDecompressor decompressor (.fastDecompressor factory)]
    (reify
      ICompressor
      (bf-compress [_ src dst]
        (.putInt dst (int (.limit ^ByteBuffer src)))
        (.compress compressor ^ByteBuffer src ^ByteBuffer dst))
      (bf-uncompress [_ src dst]
        (.limit dst (.getInt src))
        (.decompress decompressor ^ByteBuffer src ^ByteBuffer dst)))))

(defn key-compressor
  [^longs freqs]
  (let [^HuTucker ht (hu/new-hu-tucker freqs)]
    (reify
      ICompressor
      (bf-compress [_ src dst]
        (.encode ht ^ByteBuffer src ^ByteBuffer dst))
      (bf-uncompress [_ src dst]
        (.decode ht ^ByteBuffer src ^ByteBuffer dst)))))
