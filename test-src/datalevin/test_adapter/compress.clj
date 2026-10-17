(ns ^:no-doc datalevin.test-adapter.compress
  "Selected-backend compressors implementing the existing buffer protocol."
  (:require [datalevin.interface :as i]
            [datalevin.test-adapter :as adapter]
            [datalevin.test-adapter.hu :as hu]))

(deftype HuCompressor [dictionary]
  i/ICompressor
  (method [_] :hu)
  (bf-compress [_ source destination]
    (hu/encode dictionary source destination))
  (bf-uncompress [_ source destination]
    (hu/decode dictionary source destination)))

(defn key-compressor [lengths codes]
  (adapter/call! :compression/key-compressor [lengths codes]))

(defn load-key-compressor [source]
  (adapter/call! :compression/load-key-compressor [source]))

(deftype ZstdCompressor [dictionary]
  i/ICompressor
  (method [_] :zstd)
  (bf-compress [_ source destination]
    (adapter/call! :compression/zstd-compress [dictionary source destination]))
  (bf-uncompress [_ source destination]
    (adapter/call! :compression/zstd-decompress [dictionary source destination])))

(defn val-compressor [dictionary]
  (adapter/call! :compression/val-compressor [dictionary]))

(defn load-val-compressor [path]
  (adapter/call! :compression/load-val-compressor [path]))
