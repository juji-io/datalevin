(ns ^:no-doc datalevin.test-adapter.compress
  "Selected-backend value compressors implementing the existing buffer protocol."
  (:require [datalevin.interface :as i]
            [datalevin.test-adapter :as adapter]))

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
