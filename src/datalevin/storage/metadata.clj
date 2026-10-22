;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.storage.metadata
  "Version timestamps for persisted store metadata."
  (:require
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [datalevin.lmdb :as lmdb]))

(defn schema-revision
  "Read the schema concurrency token, including schemas predating revisions."
  ^long [lmdb]
  (long (or (i/get-value lmdb c/meta :schema-revision :attr :long) 0)))

(defn schema-revision-tx
  "Advance the schema token in the caller's serialized metadata transaction."
  [lmdb]
  (lmdb/kv-tx :put c/meta :schema-revision (inc (schema-revision lmdb)) :attr :long))

(defn next-last-modified
  "Advance the metadata version even within one millisecond or after clock
  rollback. Call while serializing the mutation with the LMDB write lock."
  ^long [lmdb]
  (max (inc (long (or (i/get-value lmdb c/meta :last-modified :attr :long) 0)))
       (System/currentTimeMillis)))
