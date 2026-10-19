(ns datalevin.query.source.harness
  "Small v2 provider lifecycle checks without an alternate query evaluator."
  (:refer-clojure :exclude [run!])
  (:require [datalevin.query.source :as source]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.descriptor :as descriptor]
            [datalevin.query.source.envelope :as envelope]
            [datalevin.query.source.permissions :as permissions]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.session :as session]
            [datalevin.query.source.value :as value :refer [fail!]])
  (:import [java.io Closeable]))

(defn run!
  ([provider clauses] (run! provider clauses {}))
  ([provider clauses {:keys [consume limits timeout-ms bindings required-vars]
                      :or {bindings {} consume (fn [_])}}]
   (rt/with-runtime {:limits limits :timeout-ms timeout-ms}
     (fn [runtime]
       (let [ctl (:control runtime)
             info (descriptor/capture! runtime provider)
             prepared (session/prepare! runtime [{:symbol '$test :source provider :descriptor info}] {})
             request (merge (select-keys (:request (first (:groups prepared)))
                                        [:spi-version :features :query-id :authorization :consistency :control :limits])
                       {:source-symbol '$test :source-descriptor info :region-id 0 :scope-id (get-in info [:scope :id])
                        :clauses clauses :bindings bindings :available-inputs []
                        :required-vars (or required-vars (into #{} (mapcat contract/variables) clauses))
                        :demand {:quality :exact :ordering [] :offset 0 :limit nil} :max-alternatives 32})
             _ (envelope/request! runtime request)
             [group] (session/acquire! runtime prepared)
             captured (envelope/capture-response! runtime request
                        (rt/invoke runtime :planning #(source/-plan-source provider (:session group) request))
                        (:consumption group))
             [alternative :as choices] (envelope/alternatives runtime captured)]
         (when-not (and (= 1 (count choices)) (empty? (:requires alternative))
                        (= (set (map :id clauses)) (:covers alternative)))
           (fail! :query-source/unsupported :planning))
         (envelope/select! runtime captured [(:id alternative)])
         (let [permission (permissions/reserve-open! (:consumption group) alternative request)
               close-permission (rt/own! runtime #(.close ^Closeable permission))
               cursor (rt/invoke runtime :open #(source/-open-cursor (:session group)
                                                  (contract/cursor-request request '$test (:definition-fingerprint info) alternative)))
               _ (when-not (satisfies? source/IQuerySourceCursor cursor) (fail! :query-source/invalid :open))
               close (rt/own! runtime #(try (source/-close-cursor! cursor) (finally (close-permission))))
               fetch {:control ctl :max-rows (:batch-rows (control/limits ctl))
                      :max-bytes (:batch-bytes (control/limits ctl))}]
           (loop [rows []]
             (source/check-active! ctl)
             (let [batch (rt/invoke runtime :fetch #(source/-next-batch! cursor fetch))]
               (contract/batch! (:columns alternative) fetch batch)
               (let [owned (try
                             (let [token (rt/charge-copy! runtime (:rows batch))]
                               (rt/own! runtime #(source/release! token))
                               (let [owned (value/owned-copy (:rows batch))]
                                 (consume batch)
                                 owned))
                             (finally (when-let [token (:reservation batch)] (source/release! token))))
                     rows (into rows owned)]
                 (if (:exhausted? batch)
                   (let [metrics (source/-cursor-metrics cursor)]
                     (close)
                     {:rows rows :columns (:columns alternative) :metrics metrics})
                   (recur rows)))))))))))
