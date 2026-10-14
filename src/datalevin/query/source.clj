(ns datalevin.query.source
  "Experimental embedded query-source SPI v1. Not yet integrated with d/q.

  Sources are caller-owned. Sessions and owned-batch cursors belong to one
  invocation. See doc/query-source-spi.md for the complete contract."
  (:require [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.value :as value]))

(defprotocol IQuerySource
  (-source-info [source] "Return immutable metadata without provider I/O.")
  (-open-session [source request] "Authorize and acquire one read view.")
  (-plan-source [source session request] "Plan all eligible clauses without reads."))

(defprotocol IQuerySourceSession
  (-session-info [session] "Return the acquired read-view metadata.")
  (-open-cursor [session request] "Open a selected plan in this session's view.")
  (-cancel-session! [session reason] "Signal cancellation promptly; return nil.")
  (-close-session! [session] "Release this session and its cursors; return nil."))

(defprotocol IQuerySourceCursor
  (-next-batch! [cursor request] "Return one owned, bounded batch.")
  (-cursor-metrics [cursor] "Return cumulative metrics, including after close.")
  (-close-cursor! [cursor] "Release cursor resources idempotently; return nil."))

(defn validate-source-info!
  "Validate a descriptor without acquisition; return it unchanged."
  [info]
  (contract/source-info! info))

(defn validate-type!
  "Validate a logical type descriptor; return it unchanged."
  [descriptor]
  (contract/type! descriptor))

(defn cancelled?
  "Return whether this invocation has been cancelled."
  [control] (control/cancelled? control))
(defn remaining-time-ms
  "Return nonnegative remaining time, or nil when there is no deadline."
  [control] (control/remaining-time-ms control))
(defn check-active!
  "Throw on cancellation, timeout, or a closed invocation; otherwise return nil."
  [control] (control/check-active! control))
(defn register-cancel!
  "Register a prompt cancellation callback, returning an opaque registration."
  [control cancel-fn]
  (control/register-cancel! control cancel-fn))
(defn unregister-cancel!
  "Remove a registration idempotently. An already dispatched callback may run."
  [registration]
  (control/unregister-cancel! registration))
(defn reserve!
  "Atomically reserve all requested resource charges, or throw without charging."
  [control charges] (control/reserve! control charges))
(defn release!
  "Release live charges idempotently; cumulative external requests are not refunded."
  [reservation] (control/release! reservation))
(defn batch-charge-bytes
  "Conservative retained charge for owned values. Unknown/custom values reject
  until a conservative custom-value sizing contract is installed in F1."
  [columns rows]
  (value/batch-charge-bytes columns rows))
