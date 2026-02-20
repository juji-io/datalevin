;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.overlay
  "WAL overlay merge logic for KV operations"
  (:require
   [datalevin.bits :as b]
   [datalevin.buffer :as bf]
   [datalevin.interface :as i
    :refer [get-value get-range key-range list-range]]
   [datalevin.lmdb :as l]
   [datalevin.scan :as scan]
   [datalevin.util :as u :refer [raise]]
   [datalevin.wal :as wal])
  (:import
   [datalevin.cpp BufVal Cursor Txn Dbi]
   [datalevin.dtlvnative DTLV DTLV$MDB_val]
   [datalevin.lmdb RangeContext IKV IListRandKeyValIterable
    IListRandKeyValIterator]
   [org.bytedeco.javacpp LongPointer]
   [java.lang AutoCloseable]
   [java.nio ByteBuffer BufferOverflowException]
   [java.util Comparator Iterator TreeMap Map$Entry NavigableMap SortedMap
    Collections]
   [java.util.concurrent ConcurrentSkipListMap]))

(defn normalize-kv-type [t] (or t :data))

(defn encode-kv-bytes
  [x t]
  (let [t (normalize-kv-type t)]
    (loop [^ByteBuffer buf (bf/get-tl-buffer)]
      (let [result (try
                     (b/put-bf buf x t)
                     (b/get-bytes buf)
                     (catch BufferOverflowException _
                       ::overflow))]
        (if (identical? result ::overflow)
          (let [new-buf (ByteBuffer/allocate (* 2 (.capacity buf)))]
            (bf/set-tl-buffer new-buf)
            (recur new-buf))
          result)))))

(defn decode-kv-bytes [^bytes bs t] (b/read-buffer (ByteBuffer/wrap bs) t))

(defn empty-overlay-keys-map
  ^ConcurrentSkipListMap []
  (ConcurrentSkipListMap. ^Comparator b/bytes-cmp))

(defn empty-overlay-committed-map
  ^ConcurrentSkipListMap []
  (ConcurrentSkipListMap.))

(defn empty-sorted-byte-map ^TreeMap [] (TreeMap. ^Comparator b/bytes-cmp))

;; ---- Types ----

(deftype ListOverlayEntry [^boolean wiped? ^TreeMap vals])

(deftype OverlayKV [^ByteBuffer kb ^ByteBuffer vb]
  IKV
  (k [_] kb)
  (v [_] vb))

;; ---- Key range predicate ----

(defn key-in-k-range?
  [^bytes kbs range-type ^bytes low ^bytes high]
  (let [c-low (when low (long (b/compare-bytes kbs low)))
        c-high (when high (long (b/compare-bytes kbs high)))
        c1 (or c-low c-high)]
    (case range-type
      (:all :all-back) true
      (:at-least :at-least-back) (>= ^long c1 0)
      (:at-most :at-most-back) (<= ^long c1 0)
      (:closed :closed-back) (and (>= ^long c-low 0)
                                  (<= ^long c-high 0))
      (:closed-open :closed-open-back) (and (>= ^long c-low 0)
                                            (< ^long c-high 0))
      (:greater-than :greater-than-back) (> ^long c1 0)
      (:less-than :less-than-back) (< ^long c1 0)
      (:open :open-back) (and (> ^long c-low 0)
                              (< ^long c-high 0))
      (:open-closed :open-closed-back) (and (> ^long c-low 0)
                                            (<= ^long c-high 0))
      false)))

;; ---- Core overlay data structure operations ----

(defn list-overlay-entry? [entry] (instance? ListOverlayEntry entry))

(defn merge-list-overlay-entry
  [existing incoming]
  (let [^ListOverlayEntry inc-loe incoming]
    (if (.-wiped? inc-loe)
      ;; wipe -> fresh start with incoming's vals
      (let [^TreeMap tm (empty-sorted-byte-map)]
        (.putAll tm (.-vals inc-loe))
        (->ListOverlayEntry true tm))
      ;; non-wipe -> merge into existing
      (if (list-overlay-entry? existing)
        (let [^ListOverlayEntry ex-loe existing
              ^TreeMap tm (empty-sorted-byte-map)]
          (.putAll tm (.-vals ex-loe))
          (.putAll tm (.-vals inc-loe))
          (->ListOverlayEntry (.-wiped? ex-loe) tm))
        ;; existing is not a list overlay (e.g. nil or tombstone)
        (let [^TreeMap tm (empty-sorted-byte-map)]
          (.putAll tm (.-vals inc-loe))
          (->ListOverlayEntry false tm))))))

(defn merge-overlay-entry
  [existing incoming]
  (if (list-overlay-entry? incoming)
    (merge-list-overlay-entry existing incoming)
    incoming))

(defn- resolve-op-dupsort?
  [dbis dbi dupsort?]
  (if (some? dupsort?)
    (boolean dupsort?)
    (boolean (get-in dbis [dbi :flags :dupsort]))))

(defn wal-op->overlay-entry
  ([op]
   (wal-op->overlay-entry nil op))
  ([dbis {:keys [op dbi k v kt vt dupsort? k-bytes raw?]}]
   (when dbi
     (let [kbs (if raw?
                 k
                 (or k-bytes (encode-kv-bytes k (normalize-kv-type kt))))
           dupsort? (resolve-op-dupsort? dbis dbi dupsort?)]
       (case op
         :put (if dupsort?
                (let [tm ^TreeMap (empty-sorted-byte-map)
                      vbs (if raw? v
                              (encode-kv-bytes v (normalize-kv-type vt)))]
                  (.put tm vbs :overlay-sentinel)
                  [dbi kbs (->ListOverlayEntry false tm)])
                [dbi kbs (if raw? v
                             (encode-kv-bytes v (normalize-kv-type vt)))])
         :del (if dupsort?
                [dbi kbs (->ListOverlayEntry true (empty-sorted-byte-map))]
                [dbi kbs :overlay-deleted])
         :put-list (let [tm ^TreeMap (empty-sorted-byte-map)
                         vt' (normalize-kv-type vt)
                         vs (if raw? (b/deserialize v) v)]
                     (doseq [vi vs]
                       (.put tm (encode-kv-bytes vi vt') :overlay-sentinel))
                     [dbi kbs (->ListOverlayEntry false tm)])
         :del-list (let [tm ^TreeMap (empty-sorted-byte-map)
                         vt' (normalize-kv-type vt)
                         vs (if raw? (b/deserialize v) v)]
                     (doseq [vi vs]
                       (.put tm (encode-kv-bytes vi vt') :overlay-tombstone-val))
                     [dbi kbs (->ListOverlayEntry false tm)])
         nil)))))

(defn overlay-delta-by-dbi
  ([ops]
   (overlay-delta-by-dbi nil ops))
  ([dbis ops]
   (reduce
    (fn [m op]
      (if-let [[dbi kbs entry] (wal-op->overlay-entry dbis op)]
        (let [^ConcurrentSkipListMap dbi-delta
              (or (get m dbi) (empty-overlay-keys-map))]
          (.put dbi-delta kbs
                (merge-overlay-entry (.get dbi-delta kbs) entry))
          (if (contains? m dbi)
            m
            (assoc m dbi dbi-delta)))
        m))
    {} ops)))

(defn merge-overlay-delta
  "Create a new overlay by copying existing maps and merging delta."
  [overlay delta]
  (reduce-kv
   (fn [m dbi dbi-delta]
     (let [^ConcurrentSkipListMap merged
           (if-let [^ConcurrentSkipListMap dbi-overlay (get m dbi)]
             (ConcurrentSkipListMap. dbi-overlay)
             (empty-overlay-keys-map))]
       (doseq [[k entry] dbi-delta]
         (.put merged k (merge-overlay-entry (.get merged k) entry)))
       (assoc m dbi merged)))
   (or overlay {}) delta))

(defn rebuild-overlay-by-dbi
  [committed-by-tx]
  (reduce
   (fn [m [_ delta]]
     (merge-overlay-delta m delta))
   {} committed-by-tx))

;; ---- Overlay lifecycle functions ----

(defn- overlay-entry-count
  [overlay]
  (reduce
   (fn [^long acc ^ConcurrentSkipListMap m]
     (if m
       (+ acc (.size m))
       acc))
   0
   (vals (or overlay {}))))

(defn update-private-overlay-entries!
  [wtxn info-vol overlay]
  (vswap! info-vol assoc
          :kv-overlay-private-entries
          (overlay-entry-count overlay)))

(defn reset-private-overlay!
  "Reset the private overlay on a write-txn. Takes the write-txn (Rtx)
   and the info volatile of the lmdb."
  [wtxn info-vol]
  (when-let [ov-ref (l/private-overlay wtxn)]
    (vreset! ov-ref {})
    (vswap! info-vol assoc :kv-overlay-private-entries 0)))

(defn publish-kv-private-overlay!
  [wtxn info-vol ops]
  (let [delta (overlay-delta-by-dbi (:dbis @info-vol) ops)]
    (when (seq delta)
      (when-let [ov-ref (l/private-overlay wtxn)]
        (let [merged (merge-overlay-delta (or @ov-ref {}) delta)]
          (vreset! ov-ref merged)
          (update-private-overlay-entries! wtxn info-vol merged))))))

(defn merge-single-overlay-maps
  "Merge private overlay map on top of committed for a single DBI.
  Returns a new ConcurrentSkipListMap with private entries winning."
  ^ConcurrentSkipListMap
  [^ConcurrentSkipListMap committed ^ConcurrentSkipListMap private]
  (let [merged (ConcurrentSkipListMap. ^SortedMap committed)]
    (doseq [^Map$Entry e private]
      (let [k (.getKey e)
            v (.getValue e)]
        (.put merged k (merge-overlay-entry (.get merged k) v))))
    merged))

(defn publish-kv-committed-overlay!
  "Publish committed overlay delta. Takes the info volatile and the
   write-txn atom for locking."
  [info-vol write-txn-atom wal-id ops]
  (locking write-txn-atom
    (let [info @info-vol
          delta (overlay-delta-by-dbi (:dbis info) ops)]
      (when (seq delta)
        (let [^ConcurrentSkipListMap committed-by-tx
              (or (:kv-overlay-committed-by-tx info)
                  (empty-overlay-committed-map))
              by-dbi (merge-overlay-delta
                      (:kv-overlay-by-dbi info) delta)]
          (.put committed-by-tx wal-id delta)
          (vswap! info-vol assoc
                  :kv-overlay-committed-by-tx committed-by-tx
                  :kv-overlay-by-dbi by-dbi))))))

(defn publish-kv-overlay-watermark!
  [info-vol wal-id]
  (vswap! info-vol assoc :overlay-published-wal-tx-id wal-id))

(defn recover-kv-overlay!
  "Rebuild committed overlay from un-indexed WAL records."
  [info-vol write-txn-atom dir]
  (locking write-txn-atom
    (let [inf @info-vol
          committed (long (or (:last-committed-wal-tx-id inf) 0))
          indexed (long (or (:last-indexed-wal-tx-id inf) 0))]
      (when (> committed indexed)
        (try
          (let [dbis (:dbis inf)
                committed-by-tx (empty-overlay-committed-map)
                by-dbi (volatile! {})]
            (doseq [rec (wal/read-wal-records dir indexed committed)]
              (let [wal-id (long (:wal/tx-id rec))
                    delta (overlay-delta-by-dbi dbis (:wal/ops rec))]
                (when (seq delta)
                  (.put committed-by-tx wal-id delta)
                  (vreset! by-dbi (merge-overlay-delta @by-dbi delta)))))
            (vswap! info-vol assoc
                    :kv-overlay-committed-by-tx committed-by-tx
                    :kv-overlay-by-dbi @by-dbi))
          (catch Exception e
            (raise "Fail to recover KV overlay from WAL: " e
                   {:error :kv-overlay/recover-failed
                    :dir dir
                    :last-indexed-wal-tx-id indexed
                    :last-committed-wal-tx-id committed})))))))

(defn prune-kv-committed-overlay!
  [info-vol upto-wal-id]
  (let [upto (or upto-wal-id 0)]
    (when (pos? ^long upto)
      (vswap! info-vol
              (fn [m]
                (let [^ConcurrentSkipListMap committed-by-tx
                      (or (:kv-overlay-committed-by-tx m)
                          (empty-overlay-committed-map))]
                  (cond
                    (.isEmpty committed-by-tx)
                    m

                    (> ^long (.firstKey committed-by-tx) ^long upto)
                    m

                    (<= ^long (.lastKey committed-by-tx) ^long upto)
                    (assoc m
                           :kv-overlay-committed-by-tx
                           (empty-overlay-committed-map)
                           :kv-overlay-by-dbi {})

                    :else
                    (let [^ConcurrentSkipListMap remaining
                          (empty-overlay-committed-map)]
                      (.putAll remaining
                               (.tailMap committed-by-tx upto false))
                      (assoc m
                             :kv-overlay-committed-by-tx remaining
                             :kv-overlay-by-dbi
                             (rebuild-overlay-by-dbi remaining))))))))))

(defn private-overlay-by-dbi
  "Return the private overlay map {dbi-name -> ConcurrentSkipListMap} or nil."
  [info-map write-txn-atom]
  (when (:kv-wal? info-map)
    (when-let [wtxn @write-txn-atom]
      (when-let [v (l/private-overlay wtxn)]
        @v))))

(defn overlay-for-dbi
  "Return NavigableMap overlay for the given DBI, merging committed + private.
  Returns nil immediately when WAL is not enabled (fast path for non-WAL mode).
  Returns committed map directly when no private overlay exists."
  ^NavigableMap [info-map write-txn-atom dbi-name]
  (when (:kv-wal? info-map)
    (let [committed (get (:kv-overlay-by-dbi info-map) dbi-name)
          priv-map (get (private-overlay-by-dbi info-map write-txn-atom)
                        dbi-name)]
      (cond
        (and committed priv-map) (merge-single-overlay-maps committed priv-map)
        committed committed
        priv-map priv-map
        :else nil))))

(defn kv-overlay-active?
  [info-map write-txn-atom dbi-name]
  (and (:kv-wal? info-map)
       (or (some? (get (:kv-overlay-by-dbi info-map) dbi-name))
           (seq (get (private-overlay-by-dbi info-map write-txn-atom)
                     dbi-name)))))

;; ---- Overlay read helpers ----

(defn overlay-kv
  "Create an OverlayKV from byte arrays."
  [^bytes kbs ^bytes vbs]
  (->OverlayKV (ByteBuffer/wrap kbs) (when vbs (ByteBuffer/wrap vbs))))

(defn compute-ov-submap
  "Return a NavigableMap submap view for the given key range."
  ^NavigableMap
  [^NavigableMap m range-type ^bytes low-bs ^bytes high-bs]
  (case range-type
    (:all :all-back)
    m

    (:at-least :at-least-back)
    (.tailMap m (or low-bs high-bs) true)

    (:at-most :at-most-back)
    (.headMap m (or low-bs high-bs) true)

    (:closed :closed-back)
    (.subMap m low-bs true high-bs true)

    (:closed-open :closed-open-back)
    (.subMap m low-bs true high-bs false)

    (:greater-than :greater-than-back)
    (.tailMap m (or low-bs high-bs) false)

    (:less-than :less-than-back)
    (.headMap m (or low-bs high-bs) false)

    (:open :open-back)
    (.subMap m low-bs false high-bs false)

    (:open-closed :open-closed-back)
    (.subMap m low-bs false high-bs true)

    m))

;; ---- Overlay iteration wrappers ----

(defn wrap-key-iterable
  "Wrap a KeyIterable with NavigableMap overlay merge logic."
  [^Iterable base ^NavigableMap ov-submap desc?]
  (let [^NavigableMap view (if desc? (.descendingMap ov-submap) ov-submap)]
    (reify Iterable
      (iterator [_]
        (let [base-iter (.iterator base)
              base-ready (volatile! (.hasNext base-iter))
              bkv (when @base-ready (.next base-iter))
              ov-iter (.iterator (.entrySet view))
              ov-entry (volatile! (when (.hasNext ov-iter) (.next ov-iter)))
              next-kv (volatile! nil)
              adv-base (volatile! false)
              resolved (volatile! false)]
          (letfn [(advance-base! []
                    (vreset! base-ready (.hasNext base-iter)))
                  (advance-ov! []
                    (vreset! ov-entry
                             (when (.hasNext ov-iter) (.next ov-iter))))
                  (ov-kv []
                    (when-let [^Map$Entry e @ov-entry]
                      (let [^bytes kbs (.getKey e)
                            entry (.getValue e)]
                        (cond
                          (identical? entry :overlay-deleted) nil
                          (instance? ListOverlayEntry entry)
                          (let [^ListOverlayEntry loe entry
                                ^TreeMap tm (.-vals loe)]
                            (loop [ents (seq tm)]
                              (when-let [^Map$Entry ve (first ents)]
                                (if (identical? (.getValue ve)
                                                :overlay-tombstone-val)
                                  (recur (next ents))
                                  (overlay-kv kbs (.getKey ve))))))
                          (instance? (Class/forName "[B") entry)
                          (overlay-kv kbs ^bytes entry)
                          :else nil))))
                  (compute! []
                    (when-not @resolved
                      (when @adv-base
                        (advance-base!)
                        (vreset! adv-base false))
                      (loop []
                        (let [bhas @base-ready
                              ohas (some? @ov-entry)]
                          (cond
                            (and (not bhas) (not ohas))
                            (vreset! next-kv nil)

                            (not ohas)
                            (do (vreset! adv-base true)
                                (vreset! next-kv bkv))

                            (not bhas)
                            (let [okv (ov-kv)]
                              (advance-ov!)
                              (if okv
                                (vreset! next-kv okv)
                                (recur)))

                            :else
                            (let [okey ^bytes (.getKey ^Map$Entry @ov-entry)
                                  cmp (b/compare-bf-bytes (l/k bkv) okey)
                                  cmp (if desc? (- cmp) cmp)]
                              (cond
                                (neg? cmp)
                                (do (vreset! adv-base true)
                                    (vreset! next-kv bkv))

                                (pos? cmp)
                                (let [okv (ov-kv)]
                                  (advance-ov!)
                                  (if okv
                                    (vreset! next-kv okv)
                                    (recur)))

                                :else
                                (let [okv (ov-kv)]
                                  (advance-ov!)
                                  (advance-base!)
                                  (if okv
                                    (vreset! next-kv okv)
                                    (recur))))))))
                      (vreset! resolved true)))]
            (reify
              Iterator
              (hasNext [_] (compute!) (some? @next-kv))
              (next [_] (vreset! resolved false) @next-kv)

              AutoCloseable
              (close [_]
                (when (instance? AutoCloseable base-iter)
                  (.close ^AutoCloseable base-iter))))))))))

(defn loe-val-iterator
  "Return an Iterator<Map.Entry> over a ListOverlayEntry's values."
  ^Iterator [^ListOverlayEntry loe desc?]
  (let [^TreeMap tm (.-vals loe)]
    (if desc?
      (.iterator (.entrySet (.descendingMap tm)))
      (.iterator (.entrySet tm)))))

(defn lazy-overlay-val-iterator
  "Returns a Iterator<OverlayKV> over TreeMap entries, filtering tombstones."
  ^Iterator [^TreeMap tm ^bytes kbs desc-v? v-range-type ^bytes v-lo-bs
             ^bytes v-hi-bs]
  (let [entries-iter (if desc-v?
                       (.iterator (.entrySet (.descendingMap tm)))
                       (.iterator (.entrySet tm)))
        next-entry (volatile! nil)]
    (letfn [(vbs-in-range? [^bytes vbs]
              (key-in-k-range? vbs v-range-type v-lo-bs v-hi-bs))
            (find-next! []
              (loop []
                (if (.hasNext entries-iter)
                  (let [^Map$Entry ve (.next entries-iter)]
                    (if (identical? (.getValue ve) :overlay-tombstone-val)
                      (recur)
                      (let [^bytes vbs (.getKey ve)]
                        (if (vbs-in-range? vbs)
                          (do (vreset! next-entry ve)
                              true)
                          (recur)))))
                  false)))]
      (reify Iterator
        (hasNext [_]
          (or (some? @next-entry)
              (find-next!)))
        (next [_]
          (if-let [^Map$Entry ve @next-entry]
            (do (vreset! next-entry nil)
                (overlay-kv kbs (.getKey ve)))
            ;; Fallback: should not happen if hasNext is called first
            (let [^Map$Entry ve (.next entries-iter)]
              (overlay-kv kbs (.getKey ve)))))))))

(defn wrap-list-iterable
  "Wrap a ListIterable with NavigableMap overlay merge logic (key + value level)."
  [^Iterable base ^NavigableMap ov-submap desc-k?
   v-range-type desc-v? ^bytes v-lo-bs ^bytes v-hi-bs]
  (let [^NavigableMap kview (if desc-k? (.descendingMap ov-submap) ov-submap)]
    (reify Iterable
      (iterator [_]
        (let [base-iter (.iterator base)
              base-ready (volatile! (.hasNext base-iter))
              bkv (when @base-ready (.next base-iter))
              merge-key (volatile! nil)
              ov-iter (.iterator (.entrySet kview))
              ov-entry (volatile! (when (.hasNext ov-iter) (.next ov-iter)))
              vm-active (volatile! false)
              vm-iter (volatile! nil)
              vm-wiped (volatile! false)
              ov-sub-iter (volatile! nil)
              next-kv (volatile! nil)
              adv-base (volatile! false)
              resolved (volatile! false)]
          (letfn [(advance-base! []
                    (vreset! base-ready (.hasNext base-iter)))
                  (advance-ov! []
                    (vreset! ov-entry
                             (when (.hasNext ov-iter) (.next ov-iter))))
                  (vbs-in-range? [^bytes vbs]
                    (key-in-k-range? vbs v-range-type v-lo-bs v-hi-bs))
                  (emit-ov-vals! []
                    (when-let [^Map$Entry e @ov-entry]
                      (let [^bytes kbs (.getKey e)
                            entry (.getValue e)]
                        (advance-ov!)
                        (cond
                          (= entry :overlay-deleted) nil
                          (instance? ListOverlayEntry entry)
                          (let [^ListOverlayEntry loe entry]
                            (lazy-overlay-val-iterator (.-vals loe) kbs
                                                       desc-v? v-range-type
                                                       v-lo-bs v-hi-bs))
                          :else nil))))
                  (begin-val-merge! [^ListOverlayEntry loe]
                    (vreset! vm-active true)
                    (vreset! vm-iter (loe-val-iterator loe desc-v?))
                    (vreset! vm-wiped (.-wiped? loe)))
                  (drain-base-key! []
                    (loop []
                      (when (and @base-ready
                                 (some? @merge-key)
                                 (zero? (b/compare-bf-bytes
                                         (l/k bkv) ^bytes @merge-key)))
                        (advance-base!)
                        (recur))))
                  (val-merge-next! []
                    (when @adv-base
                      (advance-base!)
                      (vreset! adv-base false))
                    (let [wiped? @vm-wiped
                          ^Iterator vi @vm-iter
                          base-same (and (not wiped?) @base-ready
                                         (some? @merge-key)
                                         (zero? (b/compare-bf-bytes
                                                 (l/k bkv)
                                                 ^bytes @merge-key)))
                          ov-has (.hasNext vi)]
                      (cond
                        (and (not base-same) (not ov-has))
                        (do (vreset! vm-active false)
                            (when wiped? (drain-base-key!))
                            nil)

                        (not ov-has) ;; only base
                        (do (vreset! adv-base true) bkv)

                        (not base-same) ;; only overlay
                        (loop []
                          (if (.hasNext vi)
                            (let [^Map$Entry ve (.next vi)
                                  ^bytes vbs (.getKey ve)
                                  tomb? (identical?
                                         (.getValue ve)
                                         :overlay-tombstone-val)]
                              (if tomb?
                                (recur)
                                (if (vbs-in-range? vbs)
                                  (overlay-kv ^bytes @merge-key vbs)
                                  (recur))))
                            (do (vreset! vm-active false)
                                (when wiped? (drain-base-key!))
                                nil)))

                        :else ;; both base and overlay
                        (let [^Map$Entry ve (.next vi)
                              ^bytes ovbs (.getKey ve)
                              tomb? (identical? (.getValue ve)
                                                :overlay-tombstone-val)
                              cmp (b/compare-bf-bytes (l/v bkv) ovbs)
                              cmp (if desc-v? (- cmp) cmp)]
                          (cond
                            (neg? cmp) ;; base val first
                            (do
                              (vreset! vm-iter
                                       (let [used (volatile! false)]
                                         (reify Iterator
                                           (hasNext [_]
                                             (or (not @used) (.hasNext vi)))
                                           (next [_]
                                             (if @used
                                               (.next vi)
                                               (do (vreset! used true) ve))))))
                              (vreset! adv-base true) bkv)

                            (pos? cmp) ;; overlay val first
                            (if tomb?
                              (recur)
                              (if (vbs-in-range? ovbs)
                                (overlay-kv ^bytes @merge-key ovbs)
                                (recur)))

                            :else ;; equal -> overlay wins
                            (do (vreset! adv-base true)
                                (if tomb?
                                  (recur)
                                  (if (vbs-in-range? ovbs)
                                    (overlay-kv ^bytes @merge-key ovbs)
                                    (recur)))))))))
                  (compute! []
                    (when-not @resolved
                      (when @adv-base
                        (advance-base!)
                        (vreset! adv-base false))
                      (loop []
                        (cond
                          (some? @ov-sub-iter)
                          (let [^Iterator si @ov-sub-iter]
                            (if (.hasNext si)
                              (vreset! next-kv (.next si))
                              (do (vreset! ov-sub-iter nil) (recur))))

                          @vm-active
                          (if-let [r (val-merge-next!)]
                            (vreset! next-kv r)
                            (recur))

                          :else
                          (let [bhas @base-ready
                                ohas (some? @ov-entry)]
                            (cond
                              (and (not bhas) (not ohas))
                              (vreset! next-kv nil)

                              (not ohas)
                              (do (vreset! adv-base true)
                                  (vreset! next-kv bkv))

                              (not bhas)
                              (if-let [si (emit-ov-vals!)]
                                (do (vreset! ov-sub-iter si) (recur))
                                (recur))

                              :else
                              (let [okey ^bytes (.getKey ^Map$Entry @ov-entry)
                                    cmp (b/compare-bf-bytes (l/k bkv) okey)
                                    cmp (if desc-k? (- cmp) cmp)]
                                (cond
                                  (neg? cmp) ;; base key first
                                  (do (vreset! adv-base true)
                                      (vreset! next-kv bkv))

                                  (pos? cmp) ;; overlay key first
                                  (if-let [si (emit-ov-vals!)]
                                    (do (vreset! ov-sub-iter si) (recur))
                                    (recur))

                                  :else ;; same key -> value merge
                                  (let [entry (.getValue ^Map$Entry @ov-entry)]
                                    (advance-ov!)
                                    (vreset! merge-key
                                             (b/get-bytes (l/k bkv)))
                                    (cond
                                      (= entry :overlay-deleted)
                                      (do (drain-base-key!) (recur))

                                      (instance? ListOverlayEntry entry)
                                      (do (begin-val-merge! entry)
                                          (recur))

                                      :else
                                      (do (advance-base!)
                                          (recur))))))))))
                      (vreset! resolved true)))]
            (reify
              Iterator
              (hasNext [_] (compute!) (some? @next-kv))
              (next [_] (vreset! resolved false) @next-kv)

              AutoCloseable
              (close [_]
                (when (instance? AutoCloseable base-iter)
                  (.close ^AutoCloseable base-iter))))))))))

(defn wrap-list-key-range-full-val-iterable
  "Wrap a ListKeyRangeFullValIterable with NavigableMap overlay merge."
  [^Iterable base ^NavigableMap ov-submap]
  (reify Iterable
    (iterator [_]
      (let [base-iter (.iterator base)
            base-ready (volatile! (.hasNext base-iter))
            bkv (when @base-ready (.next base-iter))
            merge-key (volatile! nil)
            ov-iter (.iterator (.entrySet ov-submap))
            ov-entry (volatile! (when (.hasNext ov-iter) (.next ov-iter)))
            vm-active (volatile! false)
            vm-iter (volatile! nil)
            vm-wiped (volatile! false)
            ov-sub-iter (volatile! nil)
            next-kv (volatile! nil)
            adv-base (volatile! false)
            resolved (volatile! false)]
        (letfn [(advance-base! []
                  (vreset! base-ready (.hasNext base-iter)))
                (advance-ov! []
                  (vreset! ov-entry
                           (when (.hasNext ov-iter) (.next ov-iter))))
                (emit-ov-vals! []
                  (when-let [^Map$Entry e @ov-entry]
                    (let [^bytes kbs (.getKey e)
                          entry (.getValue e)]
                      (advance-ov!)
                      (cond
                        (= entry :overlay-deleted) nil
                        (instance? ListOverlayEntry entry)
                        (let [^ListOverlayEntry loe entry
                              ^TreeMap tm (.-vals loe)
                              out (java.util.ArrayList.)]
                          (doseq [^Map$Entry ve (.entrySet tm)]
                            (when-not (identical? (.getValue ve)
                                                  :overlay-tombstone-val)
                              (.add out (overlay-kv kbs (.getKey ve)))))
                          (when (pos? (.size out))
                            (.iterator out)))
                        :else nil))))
                (begin-val-merge! [^ListOverlayEntry loe]
                  (vreset! vm-active true)
                  (vreset! vm-iter (loe-val-iterator loe false))
                  (vreset! vm-wiped (.-wiped? loe)))
                (drain-base-key! []
                  (loop []
                    (when (and @base-ready
                               (some? @merge-key)
                               (zero? (b/compare-bf-bytes
                                       (l/k bkv) ^bytes @merge-key)))
                      (advance-base!)
                      (recur))))
                (val-merge-next! []
                  (when @adv-base
                    (advance-base!)
                    (vreset! adv-base false))
                  (let [wiped? @vm-wiped
                        ^Iterator vi @vm-iter
                        base-same (and (not wiped?) @base-ready
                                       (some? @merge-key)
                                       (zero? (b/compare-bf-bytes
                                               (l/k bkv)
                                               ^bytes @merge-key)))
                        ov-has (.hasNext vi)]
                    (cond
                      (and (not base-same) (not ov-has))
                      (do (vreset! vm-active false)
                          (when wiped? (drain-base-key!))
                          nil)

                      (not ov-has) ;; only base
                      (do (vreset! adv-base true) bkv)

                      (not base-same) ;; only overlay
                      (loop []
                        (if (.hasNext vi)
                          (let [^Map$Entry ve (.next vi)
                                ^bytes vbs (.getKey ve)
                                tomb? (identical?
                                       (.getValue ve)
                                       :overlay-tombstone-val)]
                            (if tomb?
                              (recur)
                              (overlay-kv ^bytes @merge-key vbs)))
                          (do (vreset! vm-active false)
                              (when wiped? (drain-base-key!))
                              nil)))

                      :else ;; both
                      (let [^Map$Entry ve (.next vi)
                            ^bytes ovbs (.getKey ve)
                            tomb? (identical? (.getValue ve)
                                              :overlay-tombstone-val)
                            cmp (b/compare-bf-bytes (l/v bkv) ovbs)]
                        (cond
                          (neg? cmp) ;; base val first
                          (do (vreset! vm-iter
                                       (let [used (volatile! false)]
                                         (reify Iterator
                                           (hasNext [_]
                                             (or (not @used) (.hasNext vi)))
                                           (next [_]
                                             (if @used
                                               (.next vi)
                                               (do (vreset! used true) ve))))))
                              (vreset! adv-base true) bkv)

                          (pos? cmp)
                          (do (if tomb?
                                (recur)
                                (overlay-kv ^bytes @merge-key ovbs)))

                          :else ;; equal -> overlay wins
                          (do (vreset! adv-base true)
                              (if tomb?
                                (recur)
                                (overlay-kv ^bytes @merge-key ovbs))))))))
                (compute! []
                  (when-not @resolved
                    (when @adv-base
                      (advance-base!)
                      (vreset! adv-base false))
                    (loop []
                      (cond
                        (some? @ov-sub-iter)
                        (let [^Iterator si @ov-sub-iter]
                          (if (.hasNext si)
                            (vreset! next-kv (.next si))
                            (do (vreset! ov-sub-iter nil) (recur))))

                        @vm-active
                        (if-let [r (val-merge-next!)]
                          (vreset! next-kv r)
                          (recur))

                        :else
                        (let [bhas @base-ready
                              ohas (some? @ov-entry)]
                          (cond
                            (and (not bhas) (not ohas))
                            (vreset! next-kv nil)

                            (not ohas)
                            (do (vreset! adv-base true)
                                (vreset! next-kv bkv))

                            (not bhas)
                            (if-let [si (emit-ov-vals!)]
                              (do (vreset! ov-sub-iter si) (recur))
                              (recur))

                            :else
                            (let [okey ^bytes (.getKey ^Map$Entry @ov-entry)
                                  cmp (b/compare-bf-bytes (l/k bkv) okey)]
                              (cond
                                (neg? cmp)
                                (do (vreset! adv-base true)
                                    (vreset! next-kv bkv))

                                (pos? cmp)
                                (if-let [si (emit-ov-vals!)]
                                  (do (vreset! ov-sub-iter si) (recur))
                                  (recur))

                                :else
                                (let [entry (.getValue ^Map$Entry @ov-entry)]
                                  (advance-ov!)
                                  (vreset! merge-key
                                           (b/get-bytes (l/k bkv)))
                                  (cond
                                    (= entry :overlay-deleted)
                                    (do (drain-base-key!) (recur))

                                    (instance? ListOverlayEntry entry)
                                    (do (begin-val-merge! entry)
                                        (recur))

                                    :else
                                    (do (advance-base!)
                                        (recur))))))))))
                    (vreset! resolved true)))]
          (reify
            Iterator
            (hasNext [_] (compute!) (some? @next-kv))
            (next [_] (vreset! resolved false) @next-kv)

            AutoCloseable
            (close [_]
              (when (instance? AutoCloseable base-iter)
                (.close ^AutoCloseable base-iter)))))))))

(defn wrap-list-full-val-iterable
  "Wrap a ListFullValIterable with NavigableMap overlay (seek-key pattern)."
  [base-iterable ^NavigableMap ov-map]
  (reify IListRandKeyValIterable
    (val-iterator [_]
      (let [base-iter (l/val-iterator base-iterable)
            ;; Per seek-key state
            current-loe (volatile! nil)
            current-iter (volatile! nil)
            base-found (volatile! false)
            wiped (volatile! false)
            ;; Value merge state
            base-val-bb (volatile! nil)
            base-has-val (volatile! false)
            next-val-bb (volatile! nil)]
        (letfn [(copy-val! [^ByteBuffer vb]
                  (let [bs (byte-array (.remaining vb))]
                    (.get vb bs)
                    (ByteBuffer/wrap bs)))
                (read-first-base-val! []
                  (let [vb (l/next-val base-iter)]
                    (vreset! base-val-bb (copy-val! vb))
                    (vreset! base-has-val true)))
                (advance-base-val! []
                  (if (l/has-next-val base-iter)
                    (let [vb (l/next-val base-iter)]
                      (vreset! base-val-bb (copy-val! vb))
                      (vreset! base-has-val true))
                    (vreset! base-has-val false)))
                (compute-next-val! []
                  (let [^Iterator vi @current-iter
                        bhas @base-has-val
                        ohas (and vi (.hasNext vi))]
                    (cond
                      (and (not bhas) (not ohas))
                      (vreset! next-val-bb nil)

                      (not ohas)
                      (do (vreset! next-val-bb @base-val-bb)
                          (advance-base-val!))

                      (not bhas)
                      (loop []
                        (if (.hasNext vi)
                          (let [^Map$Entry ve (.next vi)
                                ^bytes vbs (.getKey ve)
                                tomb? (identical?
                                       (.getValue ve)
                                       :overlay-tombstone-val)]
                            (if tomb?
                              (recur)
                              (vreset! next-val-bb (ByteBuffer/wrap vbs))))
                          (vreset! next-val-bb nil)))

                      :else
                      (let [^Map$Entry ve (.next vi)
                            ^bytes ovbs (.getKey ve)
                            tomb? (identical? (.getValue ve)
                                              :overlay-tombstone-val)
                            ^ByteBuffer bvb @base-val-bb
                            cmp (b/compare-bf-bytes bvb ovbs)]
                        (cond
                          (neg? cmp)
                          ;; base < overlay -- put overlay entry back
                          (do (vreset! current-iter
                                       (let [used (volatile! false)]
                                         (reify Iterator
                                           (hasNext [_]
                                             (or (not @used) (.hasNext vi)))
                                           (next [_]
                                             (if @used
                                               (.next vi)
                                               (do (vreset! used true) ve))))))
                              (vreset! next-val-bb bvb)
                              (advance-base-val!))

                          (pos? cmp)
                          ;; overlay < base
                          (if tomb?
                            (compute-next-val!)
                            (vreset! next-val-bb (ByteBuffer/wrap ovbs)))

                          :else
                          ;; equal -> overlay wins
                          (do (advance-base-val!)
                              (if tomb?
                                (compute-next-val!)
                                (vreset! next-val-bb
                                         (ByteBuffer/wrap ovbs)))))))))]
          (reify
            IListRandKeyValIterator
            (seek-key [_ k-value k-type]
              (let [base-ok (l/seek-key base-iter k-value k-type)]
                (vreset! base-found base-ok)
                ;; Look up in overlay
                (let [kbs (encode-kv-bytes k-value (normalize-kv-type k-type))
                      entry (.get ov-map kbs)]
                  (cond
                    (instance? ListOverlayEntry entry)
                    (let [^ListOverlayEntry loe entry]
                      (vreset! current-loe loe)
                      (vreset! current-iter (loe-val-iterator loe false))
                      (vreset! wiped (.-wiped? loe))
                      (when (.-wiped? loe)
                        (vreset! base-found false)
                        (vreset! base-has-val false))
                      (when (and base-ok (not (.-wiped? loe)))
                        (read-first-base-val!))
                      (compute-next-val!)
                      (some? @next-val-bb))

                    (= entry :overlay-deleted)
                    (do (vreset! current-loe nil)
                        (vreset! current-iter nil)
                        (vreset! next-val-bb nil)
                        false)

                    :else
                    (do (vreset! current-loe nil)
                        (vreset! current-iter nil)
                        (vreset! wiped false)
                        (when base-ok
                          (read-first-base-val!)
                          (compute-next-val!))
                        (some? @next-val-bb))))))
            (has-next-val [_] (some? @next-val-bb))
            (next-val [_]
              (let [result @next-val-bb]
                (compute-next-val!)
                result))

            AutoCloseable
            (close [_]
              (when (instance? AutoCloseable base-iter)
                (.close ^AutoCloseable base-iter)))))))))

;; ---- Overlay lookup/get functions ----

(defn overlay-lookup-entry
  "Look up an entry in overlay NavigableMap by key bytes."
  [^NavigableMap m ^bytes kbs]
  (when m
    (.get m kbs)))

(defn overlay-get-value
  [lmdb dbi-name k k-type v-type ignore-key?]
  (let [info-map @(l/kv-info lmdb)
        write-txn (l/write-txn lmdb)
        ^NavigableMap ov-map (overlay-for-dbi info-map write-txn dbi-name)]
    (if-not ov-map
      ::overlay-miss
      (let [kbs (encode-kv-bytes k (normalize-kv-type k-type))
            entry (overlay-lookup-entry ov-map kbs)]
        (cond
          (nil? entry)
          ::overlay-miss

          (= entry :overlay-deleted)
          ::overlay-tombstone

          (instance? ListOverlayEntry entry)
          (let [^ListOverlayEntry loe entry

                kt (normalize-kv-type k-type)
                base-v-type (if (= v-type :ignore) :data v-type)
                base-vals (or (scan/get-list lmdb dbi-name k kt
                                             base-v-type)
                              [])
                ;; Merge base vals with overlay list entry
                ^TreeMap merged (empty-sorted-byte-map)]
            ;; Add base values unless wiped
            (when-not (.-wiped? loe)
              (doseq [bv base-vals]
                (.put merged (encode-kv-bytes bv base-v-type)
                      :overlay-sentinel)))
            ;; Apply overlay values from ListOverlayEntry's TreeMap
            (let [^TreeMap tm (.-vals loe)]
              (doseq [^Map$Entry ve (.entrySet tm)]
                (let [^bytes vbs (.getKey ve)]
                  (if (identical? (.getValue ve) :overlay-tombstone-val)
                    (.remove merged vbs)
                    (.put merged vbs :overlay-sentinel)))))
            (if-let [first-ent (.firstEntry merged)]
              (let [v0 (if (identical? v-type :ignore)
                         nil
                         (decode-kv-bytes (.getKey ^Map$Entry first-ent)
                                          base-v-type))]
                (if ignore-key?
                  v0
                  [(b/expected-return k k-type) v0]))
              ::overlay-tombstone))

          :else
          ;; Regular DBI: entry is byte[] of encoded value
          (let [^bytes vbs entry
                v (decode-kv-bytes vbs (normalize-kv-type v-type))]
            (if ignore-key?
              v
              [(b/expected-return k k-type) v])))))))

(defn overlay-out-value [v v-type] (if (identical? v-type :ignore) nil v))

(defn overlay-raw-key
  ^ByteBuffer [k k-type]
  (ByteBuffer/wrap (encode-kv-bytes k k-type)))

(defn overlay-raw-kv
  [k v k-type v-type]
  (let [^ByteBuffer kb (ByteBuffer/wrap (encode-kv-bytes k k-type))
        ^ByteBuffer vb (when (some? v)
                         (ByteBuffer/wrap (encode-kv-bytes v v-type)))]
    (reify IKV
      (k [_] kb)
      (v [_] (when vb vb)))))

;; ---- LMDB rank helpers (used by overlay rank computation) ----

(defn lmdb-key-exists?
  "Check if raw key bytes exist in LMDB (bypassing overlay). O(log n)."
  [dbi rtx ^bytes kbs]
  (let [^BufVal kp (l/kp rtx)
        ^ByteBuffer bf (.inBuf kp)]
    (.clear bf)
    (b/put-buffer bf kbs :raw)
    (.flip bf)
    (.reset kp)
    (some? (l/get-kv dbi rtx))))

(defn lmdb-rank-of-raw-key
  "Get LMDB B-tree rank for raw key bytes. O(log n). Returns nil if not found."
  [dbi rtx ^bytes kbs]
  (let [^BufVal kp (l/kp rtx)
        ^ByteBuffer bf (.inBuf kp)]
    (.clear bf)
    (b/put-buffer bf kbs :raw)
    (.flip bf)
    (.reset kp)
    (l/get-key-rank dbi rtx)))

(defn lmdb-key-range-count
  "Count LMDB keys strictly less than k. O(log n)."
  [dbi rtx ^Cursor cur k k-type]
  (let [^RangeContext ctx (l/range-info rtx :less-than k nil k-type)]
    (DTLV/dtlv_key_range_count
     (.ptr cur)
     (.ptr ^BufVal (l/kp rtx))
     (.ptr ^BufVal (l/vp rtx))
     DTLV/DTLV_TRUE DTLV/DTLV_TRUE DTLV/DTLV_TRUE
     (when-let [^BufVal bf (.-start-bf ctx)] (.ptr bf))
     (when-let [^BufVal bf (.-stop-bf ctx)] (.ptr bf)))))

(defn- dtlv-bool [x] (if x (int DTLV/DTLV_TRUE) (int DTLV/DTLV_FALSE)))

(defn- dtlv-val ^DTLV$MDB_val [^BufVal x] (when x (.ptr x)))

(defn- checked-native-count
  [op count data]
  (if (neg? ^long count)
    (raise "Native count failure in " op ": " count data)
    count))

(defn- range-bound-bytes
  [^BufVal bf]
  (when bf
    (let [bs (b/get-bytes (.outBuf bf))]
      (.reset bf)
      bs)))

(defn- range-bounds-bytes
  [^RangeContext ctx]
  [(range-bound-bytes (.-start-bf ctx))
   (range-bound-bytes (.-stop-bf ctx))])

(defn- cap-count
  [^long n cap]
  (if (nil? cap)
    n
    (let [cap (long cap)]
      (cond
        (neg? cap) cap
        (zero? cap) 0
        :else (min n cap)))))

(defn- range-count-flag
  [^RangeContext ctx]
  (int
   (bit-or
    (if (.-include-start? ctx)
      (int DTLV/MDB_COUNT_LOWER_INCL)
      0)
    (if (.-include-stop? ctx)
      (int DTLV/MDB_COUNT_UPPER_INCL)
      0))))

(defn- mdb-range-count-keys
  [dbi rtx ^RangeContext ctx]
  (let [^Txn txn (l/txn rtx)
        ^Dbi db-handle (l/dbi dbi)
        ^int flag (range-count-flag ctx)
        ^DTLV$MDB_val sk (dtlv-val (.-start-bf ctx))
        ^DTLV$MDB_val ek (dtlv-val (.-stop-bf ctx))]
    (with-open [total (LongPointer. 1)]
      (DTLV/mdb_range_count_keys
       (.get txn)
       (.get db-handle)
       sk
       ek
       flag
       total)
      (.get ^LongPointer total))))

(defn- mdb-range-count-values
  [dbi rtx ^RangeContext ctx]
  (let [^Txn txn (l/txn rtx)
        ^Dbi db-handle (l/dbi dbi)
        ^int flag (range-count-flag ctx)
        ^DTLV$MDB_val sk (dtlv-val (.-start-bf ctx))
        ^DTLV$MDB_val ek (dtlv-val (.-stop-bf ctx))]
    (with-open [total (LongPointer. 1)]
      (DTLV/mdb_range_count_values
       (.get txn)
       (.get db-handle)
       sk
       ek
       flag
       total)
      (.get ^LongPointer total))))

(defn- native-key-range-count
  [dbi rtx ^Cursor cur [range-type k1 k2] k-type cap]
  (let [^RangeContext ctx (l/range-info rtx range-type k1 k2 k-type)
        forward (dtlv-bool (.-forward? ctx))
        start (dtlv-bool (.-include-start? ctx))
        end (dtlv-bool (.-include-stop? ctx))
        ^DTLV$MDB_val sk (dtlv-val (.-start-bf ctx))
        ^DTLV$MDB_val ek (dtlv-val (.-stop-bf ctx))]
    (checked-native-count
     :key-range-count
     (if (l/dlmdb?)
       (cap-count (long (mdb-range-count-keys dbi rtx ctx)) cap)
       (if cap
         (DTLV/dtlv_key_range_count_cap
          (.ptr cur) cap
          (.ptr ^BufVal (l/kp rtx)) (.ptr ^BufVal (l/vp rtx))
          forward start end sk ek)
         (DTLV/dtlv_key_range_count
          (.ptr cur)
          (.ptr ^BufVal (l/kp rtx)) (.ptr ^BufVal (l/vp rtx))
          forward start end sk ek)))
     {:k-range [range-type k1 k2] :k-type k-type :cap cap})))

(defn- native-list-range-count
  [dbi rtx ^Cursor cur [k-range-type k1 k2] k-type
   [v-range-type v1 v2] v-type cap]
  (let [[^RangeContext kctx ^RangeContext vctx]
        (l/list-range-info rtx k-range-type k1 k2 k-type
                           v-range-type v1 v2 v-type)
        kforward (dtlv-bool (.-forward? kctx))
        kstart (dtlv-bool (.-include-start? kctx))
        kend (dtlv-bool (.-include-stop? kctx))
        ^DTLV$MDB_val sk (dtlv-val (.-start-bf kctx))
        ^DTLV$MDB_val ek (dtlv-val (.-stop-bf kctx))
        vforward (dtlv-bool (.-forward? vctx))
        vstart (dtlv-bool (.-include-start? vctx))
        vend (dtlv-bool (.-include-stop? vctx))
        ^DTLV$MDB_val sv (dtlv-val (.-start-bf vctx))
        ^DTLV$MDB_val ev (dtlv-val (.-stop-bf vctx))]
    (checked-native-count
     :list-range-count
     (if (and (l/dlmdb?) (contains? #{:all :all-back} v-range-type))
       (cap-count (long (mdb-range-count-values dbi rtx kctx)) cap)
       (if cap
         (DTLV/dtlv_list_range_count_cap
          (.ptr cur) cap
          (.ptr ^BufVal (l/kp rtx)) (.ptr ^BufVal (l/vp rtx))
          kforward kstart kend sk ek vforward vstart vend sv ev)
         (DTLV/dtlv_list_range_count
          (.ptr cur)
          (.ptr ^BufVal (l/kp rtx)) (.ptr ^BufVal (l/vp rtx))
          kforward kstart kend sk ek vforward vstart vend sv ev)))
     {:k-range [k-range-type k1 k2]
      :v-range [v-range-type v1 v2]
      :k-type k-type
      :v-type v-type
      :cap cap})))

(defn- overlay-submap-for-k-range
  [^NavigableMap ov rtx [k-range-type k1 k2] k-type]
  (when (and ov (not (.isEmpty ov)))
    (let [^RangeContext kctx (l/range-info rtx k-range-type k1 k2 k-type)
          [k-lo-bs k-hi-bs] (range-bounds-bytes kctx)
          ^NavigableMap sub (compute-ov-submap ov k-range-type
                                               k-lo-bs k-hi-bs)]
      (when-not (.isEmpty sub)
        sub))))

(defn- lmdb-list-value-exists?
  [dbi rtx cur ^bytes kbs ^bytes vbs]
  (pos? ^long
   (native-list-range-count dbi rtx cur
                            [:closed kbs kbs] :raw
                            [:closed vbs vbs] :raw
                            1)))

(defn- overlay-list-count-for-key
  [^ListOverlayEntry loe dbi rtx ^Cursor cur ^bytes kbs
   [v-range-type _ _ :as v-range] v-type
   ^bytes v-lo-bs ^bytes v-hi-bs]
  (let [wiped? (.-wiped? loe)
        ^TreeMap tm (.-vals loe)]
    (if wiped?
      (loop [it (.iterator (.entrySet tm))
             total (long 0)]
        (if (.hasNext it)
          (let [^Map$Entry ve (.next it)
                ^bytes vbs (.getKey ve)
                tomb? (identical? (.getValue ve)
                                  :overlay-tombstone-val)]
            (if (or tomb?
                    (not (key-in-k-range? vbs v-range-type
                                          v-lo-bs v-hi-bs)))
              (recur it total)
              (recur it (u/long-inc total))))
          total))
      (loop [it (.iterator (.entrySet tm))
             total (long (native-list-range-count dbi rtx cur
                                                  [:closed kbs kbs] :raw
                                                  v-range v-type nil))]
        (if (.hasNext it)
          (let [^Map$Entry ve (.next it)
                ^bytes vbs (.getKey ve)]
            (if (key-in-k-range? vbs v-range-type v-lo-bs v-hi-bs)
              (let [tomb? (identical? (.getValue ve)
                                      :overlay-tombstone-val)
                    in-db? (lmdb-list-value-exists? dbi rtx cur kbs vbs)]
                (recur it
                       (long
                        (cond
                          (and tomb? in-db?) (u/long-dec total)
                          (and (not tomb?) (not in-db?)) (u/long-inc total)
                          :else total))))
              (recur it total)))
          total)))))

(defn- overlay-key-count-delta
  [^NavigableMap ov-sub dbi rtx ^Cursor cur]
  (if (or (nil? ov-sub) (.isEmpty ov-sub))
    0
    (let [iter (.iterator (.entrySet ov-sub))]
      (loop [delta (long 0)]
        (if (.hasNext iter)
          (let [^Map$Entry e (.next iter)
                ^bytes kbs (.getKey e)
                entry (.getValue e)
                in-db? (lmdb-key-exists? dbi rtx kbs)]
            (cond
              (identical? entry :overlay-deleted)
              (recur (if in-db? (u/long-dec delta) delta))

              (instance? ListOverlayEntry entry)
              (let [present? (pos? ^long (overlay-list-count-for-key
                                          entry dbi rtx cur kbs
                                          [:all] :raw nil nil))]
                (recur (long
                        (cond
                          (and present? (not in-db?)) (u/long-inc delta)
                          (and (not present?) in-db?) (u/long-dec delta)
                          :else delta))))

              :else
              (recur (if in-db? delta (u/long-inc delta)))))
          delta)))))

(defn- overlay-list-range-delta
  [^NavigableMap ov-sub dbi rtx ^Cursor cur [v-range-type _ _ :as v-range]
   v-type ^bytes v-lo-bs ^bytes v-hi-bs]
  (if (or (nil? ov-sub) (.isEmpty ov-sub))
    0
    (let [iter (.iterator (.entrySet ov-sub))]
      (loop [delta (long 0)]
        (if (.hasNext iter)
          (let [^Map$Entry e (.next iter)
                ^bytes kbs (.getKey e)
                entry (.getValue e)
                base-count (long (native-list-range-count
                                  dbi rtx cur
                                  [:closed kbs kbs] :raw
                                  v-range v-type nil))
                final-count (cond
                              (identical? entry :overlay-deleted)
                              (long 0)

                              (instance? ListOverlayEntry entry)
                              (long (overlay-list-count-for-key
                                     entry dbi rtx cur kbs
                                     v-range v-type
                                     v-lo-bs v-hi-bs))

                              :else
                              (let [^bytes vbs entry]
                                (if (key-in-k-range? vbs v-range-type
                                                     v-lo-bs v-hi-bs)
                                  1
                                  0)))]
            (recur (long (+ ^long delta (- ^long final-count ^long base-count)))))
          delta)))))

;; ---- Overlay rank functions ----

(defn overlay-rank-adj
  "Compute rank adjustment from overlay entries below target.
  Returns the net change: +1 per addition, -1 per deletion of LMDB keys.
  O(m log n) where m = overlay entries below target."
  [^NavigableMap ov ^bytes target dbi rtx]
  (if (or (nil? ov) (.isEmpty ov))
    0
    (let [^NavigableMap head (.headMap ov target false)]
      (if (.isEmpty head)
        0
        (let [iter (.iterator (.entrySet head))]
          (loop [adj (long 0)]
            (if (.hasNext iter)
              (let [^Map$Entry e (.next iter)
                    obs (.getKey e)
                    oval (.getValue e)
                    del? (identical? oval :overlay-deleted)
                    in-db? (lmdb-key-exists? dbi rtx obs)]
                (recur (long (cond
                               (and (not del?) (not in-db?)) (u/long-inc adj)
                               (and del? in-db?) (u/long-dec adj)
                               :else adj))))
              adj)))))))

(defn overlay-rank
  "O(m log n) rank computation for WAL overlay mode."
  [lmdb dbi-name k k-type]
  (let [k-type (normalize-kv-type k-type)
        target (encode-kv-bytes k k-type)
        info-map @(l/kv-info lmdb)
        ^NavigableMap ov (overlay-for-dbi info-map (l/write-txn lmdb) dbi-name)]
    (scan/scan
     (let [ov-entry (when ov (.get ov target))
           deleted? (identical? ov-entry :overlay-deleted)
           in-ov? (and (some? ov-entry) (not deleted?))]
       (when-not deleted?
         (let [lmdb-rank (lmdb-rank-of-raw-key dbi rtx target)
               in-lmdb? (some? lmdb-rank)]
           (when (or in-lmdb? in-ov?)
             (let [base (long (if in-lmdb?
                                (long lmdb-rank)
                                (lmdb-key-range-count dbi rtx cur k k-type)))
                   adj (long (overlay-rank-adj ov target dbi rtx))]
               (+ base adj))))))
     (raise "Fail to get overlay rank: " e
            {:dbi dbi-name :k k :k-type k-type}))))

(defn overlay-get-by-rank
  "O(m log n + log n * log m) rank-based lookup for WAL overlay mode."
  [lmdb dbi-name rank k-type v-type ignore-key?]
  (when-not (neg? ^long rank)
    (let [k-type (normalize-kv-type k-type)
          info-map @(l/kv-info lmdb)
          ^NavigableMap ov (overlay-for-dbi info-map (l/write-txn lmdb)
                                            dbi-name)]
      (scan/scan
       (let [;; Classify overlay entries -- O(m log n)
             additions (java.util.TreeMap. ^Comparator b/bytes-cmp)
             del-ranks (java.util.ArrayList.)
             _
             (when (and ov (not (.isEmpty ov)))
               (let [iter (.iterator (.entrySet ov))]
                 (while (.hasNext iter)
                   (let [^Map$Entry e (.next iter)
                         kbs (.getKey e)
                         oval (.getValue e)]
                     (if (identical? oval :overlay-deleted)
                       (when-let [lr (lmdb-rank-of-raw-key dbi rtx kbs)]
                         (.add del-ranks (long lr)))
                       (when-not (lmdb-key-exists? dbi rtx kbs)
                         (.put additions kbs true)))))))
             _ (Collections/sort del-ranks)
             add-entries
             (let [iter (.iterator (.entrySet additions))]
               (loop [ai (long 0)
                      acc (transient [])]
                 (if (.hasNext iter)
                   (let [^Map$Entry e (.next iter)
                         akbs (.getKey e)
                         lmdb-below (long (lmdb-key-range-count
                                           dbi rtx cur
                                           (b/read-buffer
                                            (ByteBuffer/wrap akbs) k-type)
                                           k-type))
                         idx (Collections/binarySearch
                              del-ranks lmdb-below)
                         dels-below (long (if (neg? idx)
                                            (- (- idx) 1)
                                            idx))
                         m-rank (+ (- lmdb-below dels-below) ai)]
                     (recur (u/long-inc ai)
                            (conj! acc [m-rank akbs])))
                   (persistent! acc))))
             target-rank (long rank)]
          ;; Check additions first -- O(m)
         (or (some (fn [[mr akbs]]
                     (when (= (long mr) target-rank)
                       (let [k (b/read-buffer (ByteBuffer/wrap akbs) k-type)]
                         (get-value lmdb dbi-name k k-type v-type
                                    ignore-key?))))
                   add-entries)
              ;; Binary search LMDB ranks -- O(log n * log m)
             (let [n-add (count add-entries)
                   lmdb-n (long (i/entries lmdb dbi-name))
                   dels-below-lr
                   (fn [^long lr]
                     (let [idx (Collections/binarySearch
                                del-ranks lr)]
                       (if (neg? idx) (- (- idx) 1) idx)))
                   adds-below-mr
                   (fn [^long mr]
                     (loop [lo (int 0) hi (int n-add)]
                       (if (< lo hi)
                         (let [mid (unchecked-int
                                    (quot (+ lo hi) 2))
                               [^long emr] (nth add-entries mid)]
                           (if (< emr mr)
                             (recur (unchecked-inc-int mid) hi)
                             (recur lo mid)))
                         (long lo))))]
               (loop [lo (long 0)
                      hi (u/long-dec lmdb-n)]
                 (when (<= lo hi)
                   (let [mid (quot (+ lo hi) 2)
                         pair (l/get-key-by-rank dbi rtx mid)]
                     (if (nil? pair)
                       (recur (u/long-inc mid) hi)
                       (let [[kbuf _] pair
                             kbs (b/get-bytes kbuf)
                             del? (and ov (identical? (.get ov kbs)
                                                      :overlay-deleted))
                             db (dels-below-lr mid)
                             base (- mid ^long db)
                             ab (adds-below-mr base)
                             mr (+ base ^long ab)]
                         (cond
                           (and (not del?) (= mr target-rank))
                           (let [k (b/read-buffer
                                    (ByteBuffer/wrap kbs) k-type)]
                             (get-value lmdb dbi-name k k-type
                                        v-type ignore-key?))

                           (< mr target-rank)
                           (recur (u/long-inc mid) hi)

                           :else
                           (recur lo (u/long-dec mid)))))))))))
       (raise "Fail to get overlay value by rank: " e
              {:dbi dbi-name :rank rank :k-type k-type})))))

;; ---- Overlay sampling ----

(defn overlay-sample-kv
  [lmdb dbi-name n k-type v-type ignore-key?]
  (let [k-type (normalize-kv-type k-type)
        sample-vt (if (identical? v-type :ignore) :raw v-type)
        kvs (vec (get-range lmdb dbi-name [:all] k-type sample-vt
                            false))
        total (count kvs)
        indices (u/reservoir-sampling total n)
        out-value (fn [v] (overlay-out-value v v-type))
        format-item (fn [[k v]]
                      (let [v' (out-value v)]
                        (if ignore-key? v' [k v'])))]
    (when indices
      (loop [xs (seq indices)
             out (transient [])]
        (if-let [^long idx0 (first xs)]
          (let [idx idx0]
            (if (or (neg? idx) (>= idx total))
              (recur (next xs) out)
              (recur (next xs) (conj! out (format-item (nth kvs idx))))))
          (persistent! out))))))

(defn overlay-visit-key-sample
  [lmdb dbi-name indices visitor k-range k-type raw-pred?]
  (when indices
    (let [k-type (normalize-kv-type k-type)
          ks (vec (key-range lmdb dbi-name k-range k-type))
          total (count ks)]
      (loop [xs (seq indices)]
        (when-let [^long idx0 (first xs)]
          (let [idx idx0]
            (if (or (neg? idx) (>= idx total))
              (recur (next xs))
              (let [k (nth ks idx)
                    res (if raw-pred?
                          (visitor (overlay-raw-key k k-type))
                          (visitor k))]
                (when-not (identical? res :datalevin/terminate-visit)
                  (recur (next xs)))))))))))

(defn overlay-visit-list-sample
  [lmdb dbi-name indices visitor k-range k-type v-type raw-pred?]
  (when indices
    (let [k-type (normalize-kv-type k-type)
          v-type (normalize-kv-type v-type)
          kvs (vec (list-range lmdb dbi-name k-range k-type [:all] v-type))
          total (count kvs)]
      (loop [xs (seq indices)]
        (when-let [^long idx0 (first xs)]
          (let [idx idx0]
            (if (or (neg? idx) (>= idx total))
              (recur (next xs))
              (let [[k v] (nth kvs idx)
                    res (if raw-pred?
                          (visitor (overlay-raw-kv k v k-type v-type))
                          (visitor k v))]
                (when-not (identical? res :datalevin/terminate-visit)
                  (recur (next xs)))))))))))

;; ---- Overlay iterate dispatch (called from DBI methods) ----

(defn overlay-iterate-key
  "WAL overlay path for iterate-key."
  [base ^RangeContext ctx ^NavigableMap ov range-type]
  (let [^BufVal start-bf (.-start-bf ctx)
        ^BufVal stop-bf (.-stop-bf ctx)
        low-bs (when start-bf
                 (let [bs (b/get-bytes (.outBuf start-bf))]
                   (.reset start-bf) bs))
        high-bs (when stop-bf
                  (let [bs (b/get-bytes (.outBuf stop-bf))]
                    (.reset stop-bf) bs))
        submap (compute-ov-submap ov range-type low-bs high-bs)]
    (wrap-key-iterable base submap (not (.-forward? ctx)))))

(defn overlay-iterate-list
  "WAL overlay path for iterate-list."
  [base ctx ^NavigableMap ov k-range-type v-range-type]
  (let [[^RangeContext kctx ^RangeContext vctx] ctx

        ^BufVal k-start-bf (.-start-bf kctx)
        ^BufVal k-stop-bf (.-stop-bf kctx)
        k-lo-bs (when k-start-bf
                  (let [bs (b/get-bytes (.outBuf k-start-bf))]
                    (.reset k-start-bf) bs))
        k-hi-bs (when k-stop-bf
                  (let [bs (b/get-bytes (.outBuf k-stop-bf))]
                    (.reset k-stop-bf) bs))
        submap (compute-ov-submap ov k-range-type k-lo-bs k-hi-bs)
        ^BufVal v-start-bf (.-start-bf vctx)
        ^BufVal v-stop-bf (.-stop-bf vctx)
        v-lo-bs (when v-start-bf
                  (let [bs (b/get-bytes (.outBuf v-start-bf))]
                    (.reset v-start-bf) bs))
        v-hi-bs (when v-stop-bf
                  (let [bs (b/get-bytes (.outBuf v-stop-bf))]
                    (.reset v-stop-bf) bs))]
    (wrap-list-iterable base submap
                        (not (.-forward? kctx))
                        v-range-type (not (.-forward? vctx))
                        v-lo-bs v-hi-bs)))

(defn overlay-iterate-list-key-range-val-full
  "WAL overlay path for iterate-list-key-range-val-full."
  [base ^RangeContext ctx ^NavigableMap ov range-type]
  (let [^BufVal start-bf (.-start-bf ctx)
        ^BufVal stop-bf (.-stop-bf ctx)
        low-bs (when start-bf
                 (let [bs (b/get-bytes (.outBuf start-bf))]
                   (.reset start-bf) bs))
        high-bs (when stop-bf
                  (let [bs (b/get-bytes (.outBuf stop-bf))]
                    (.reset stop-bf) bs))
        submap (compute-ov-submap ov range-type low-bs high-bs)]
    (wrap-list-key-range-full-val-iterable base submap)))

;; ---- WAL range count functions ----

(defn wal-key-range-count
  "Count keys in range for WAL mode."
  [lmdb dbi-name [k-range-type k1 k2 :as k-range] k-type cap]
  (let [cap (when (some? cap) (long cap))]
    (cond
      (and cap (neg? cap)) cap
      (and cap (zero? cap)) 0
      :else
      (let [k-type (normalize-kv-type k-type)
            info-map @(l/kv-info lmdb)
            write-txn (l/write-txn lmdb)
            ^NavigableMap ov (overlay-for-dbi info-map write-txn dbi-name)]
        (scan/scan
         (let [base-count (long (native-key-range-count
                                 dbi rtx cur k-range k-type nil))]
           (if-let [^NavigableMap ov-sub
                    (overlay-submap-for-k-range ov rtx
                                                [k-range-type k1 k2]
                                                k-type)]
             (cap-count (+ base-count
                           (overlay-key-count-delta ov-sub dbi rtx cur))
                        cap)
             (cap-count base-count cap)))
         (raise "Fail to count key range in WAL overlay mode: " e
                {:dbi dbi-name :k-range k-range :k-type k-type :cap cap}))))))

(defn wal-list-range-count
  "Count list-range entries for WAL mode."
  [lmdb dbi-name [k-range-type k1 k2 :as k-range] k-type
   [v-range-type v1 v2 :as v-range] v-type cap]
  (let [cap (when (some? cap) (long cap))]
    (cond
      (and cap (neg? cap)) cap
      (and cap (zero? cap)) 0
      :else
      (let [k-type (normalize-kv-type k-type)
            v-type (if (or (nil? v-type) (identical? v-type :ignore))
                     :raw
                     (normalize-kv-type v-type))
            info-map @(l/kv-info lmdb)
            write-txn (l/write-txn lmdb)
            ^NavigableMap ov (overlay-for-dbi info-map write-txn dbi-name)]
        (scan/scan
         (let [base-count (long (native-list-range-count
                                 dbi rtx cur
                                 k-range k-type
                                 v-range v-type
                                 nil))]
           (if-let [^NavigableMap ov-sub
                    (overlay-submap-for-k-range ov rtx
                                                [k-range-type k1 k2]
                                                k-type)]
             (let [^RangeContext vctx (l/range-info rtx v-range-type v1 v2
                                                    v-type)
                   [v-lo-bs v-hi-bs] (range-bounds-bytes vctx)]
               (cap-count (+ base-count
                             (overlay-list-range-delta ov-sub dbi rtx cur
                                                       [v-range-type v1 v2]
                                                       v-type
                                                       v-lo-bs v-hi-bs))
                          cap))
             (cap-count base-count cap)))
         (raise "Fail to count list range in WAL overlay mode: " e
                {:dbi dbi-name
                 :k-range k-range
                 :k-type k-type
                 :v-range v-range
                 :v-type v-type
                 :cap cap}))))))
