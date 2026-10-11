;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.txlog.weights
  "Reusable primitive storage for logical WAL weights not yet durable.
  Callers serialize access under the sync manager's monitor.")

(definterface IWeightQueue
  (^void addWeight [^long lsn ^long extra])
  (^long dropThrough [^long durable])
  (^long size [])
  (^void clear []))

(deftype WeightQueue [^longs ^:unsynchronized-mutable entries
                     ^long ^:unsynchronized-mutable head
                     ^long ^:unsynchronized-mutable length]
  IWeightQueue
  (addWeight [_ lsn extra]
    (let [capacity (quot (alength entries) 2)]
      (when (== length capacity)
        (let [expanded (long-array (max 2 (* 2 (alength entries))))
              first-count (- capacity head)]
          (System/arraycopy entries (* 2 head) expanded 0 (* 2 first-count))
          (System/arraycopy entries 0 expanded (* 2 first-count) (* 2 head))
          (set! entries expanded)
          (set! head 0))))
    (let [capacity (quot (alength entries) 2)
          tail (rem (+ head length) capacity)]
      (aset entries (int (* 2 tail)) lsn)
      (aset entries (int (inc (* 2 tail))) extra)
      (set! length (inc length))))
  (dropThrough [_ durable]
    (let [capacity (quot (alength entries) 2)]
      (loop [removed 0]
        (if (and (pos? length) (<= (aget entries (* 2 head)) durable))
          (let [extra (aget entries (inc (* 2 head)))]
            (set! head (if (== (inc head) capacity) 0 (inc head)))
            (set! length (dec length))
            (recur (+ removed extra)))
          removed))))
  (size [_] length)
  (clear [_]
    (set! head 0)
    (set! length 0)))

(defn create
  "Create a weight queue, retaining its storage across force and reset cycles."
  ^WeightQueue [full-prefix?]
  (WeightQueue. (long-array (if full-prefix? 0 128)) 0 0))
