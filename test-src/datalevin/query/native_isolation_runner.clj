(ns datalevin.query.native-isolation-runner
  "Run in a fresh JVM: ordinary queries must leave source execution unloaded.
  Load the public SPI before this namespace to also exercise the opposite load
  order. Protocol extensions below belong only to this isolated process."
  (:require [clojure.test :as test :refer [is testing]]
            [datalevin.core :as d]
            [datalevin.query :as query]))

(deftype ExtendedSource [delegate])

(def ^:private execution-namespaces
  '[datalevin.query.source.execute datalevin.query.source.plan
    datalevin.query.source.runtime datalevin.query.source.native
    datalevin.query.source.inputs datalevin.query.source.join
    datalevin.query.source.binding datalevin.query.source.access
    datalevin.query.source.session datalevin.query.source.cursor
    datalevin.query.source.lowering datalevin.query.source.region
    datalevin.query.source.scope datalevin.query.source.descriptor
    datalevin.query.source.envelope datalevin.query.source.planning
    datalevin.query.source.program datalevin.query.source.native-plan
    datalevin.query.source.input-program datalevin.query.source.collection
    datalevin.query.source.recursion datalevin.query.source.native-rules])

(defn- unloaded! []
  (is (empty? (filter find-ns execution-namespaces))))

(defn- native-queries! [conn]
  (let [db @conn
        point '[:find ?value . :in $ ?id :where [?id :value ?value]]
        many '{:find [?id] :in [$ [?id ...]] :where [[?id :value _]]}
        rule '[:find ?id :in $ % :where (large ?id)]
        rules '[[(large ?id) [?id :value ?v] [(> ?v 10)]]]]
    (doseq [cache? [false true]]
      (binding [query/*cache?* cache?]
        (dotimes [_ 2]
          (is (= 10 (d/q point db 1)))
          (doseq [inputs [(list db 1) (map identity [db 1])
                          (next (seq (object-array [:unused db 1])))]]
            (is (= 10 (apply d/q point inputs))))
          (is (= #{[1] [2]} (d/q many db [1 2 2])))
          (is (= 30 (d/q '[:find (sum ?v) . :where [_ :value ?v]] db)))
          (is (= #{[1 2]} (d/q '[:find ?a ?b :where [?a :next ?b] [?b :value _]] db)))
          (is (= #{[2]} (d/q rule db rules)))
          (is (= #{[2]} (d/q '[:find ?id :in $a $b
                               :where [$a ?id :value ?v] [$b ?id :value ?v] [(> ?v 10)]] db db)))
          (is (= #{[2]} (d/q '[:find ?id :in $rows :where [$rows ?id "two"]]
                             [[1 "one"] [2 "two"]])))
          (is (= #{[2]} (d/q '[:find ?id :in $rows ?v :where [$rows ?id ?v]]
                             [[1 "one"] [2 "two"]] "two")))
          (is (not (:query-source? (d/explain {:run? true} point db 1))))
          (is (not (:query-source? (d/explain {:run? false} point db 1)))))))
    (let [prepared (d/prepare-q db point)
          reader (query/query-reader point)]
      (is (= 10 (prepared [1])))
      (is (= 20 (prepared [2])))
      (is (= 10 (reader db [db 1] false))))
    (is (= 2 (query/count-plan '[:find ?id :where [?id :value _]] db)))))

(defn- source-checks! [conn]
  (require 'datalevin.query.source 'datalevin.query.source.fake)
  (unloaded!)
  (let [protocol @(resolve 'datalevin.query.source/IQuerySource)
        info (resolve 'datalevin.query.source/-source-info)
        open (resolve 'datalevin.query.source/-open-session)
        plan (resolve 'datalevin.query.source/-plan-source)
        make (resolve 'datalevin.query.source.fake/source)
        descriptor {:spi-versions #{1} :source-instance-id :isolation :compute-context-id :local
                    :definition-id :rows :definition-fingerprint "isolation:v1"
                    :features #{} :required-features #{}
                    :pattern-contract {:kind :tuple :columns [{:type :long :nullable? false}
                                                               {:type :string :nullable? false}]}}
        provider (make descriptor [[1 "one"] [2 "two"]])]
    ;; Exercise the execution-mode check before optional resolution. This JVM
    ;; check does not certify a native-image build or provider support there.
    (let [key "org.graalvm.nativeimage.kind"
          previous (System/getProperty key)]
      (try
        (System/setProperty key "runtime")
        (doseq [call [#(d/q '[:find ?id :in $s :where [$s ?id _]] provider)
                      #(d/explain {} '[:find ?id :in $s :where [$s ?id _]] provider)]]
          (let [data (try (call) nil (catch clojure.lang.ExceptionInfo e (ex-data e)))]
            (is (= {:error :query-source/unsupported :phase :execution-mode}
                   (select-keys data [:error :phase])))
            (is (some? (:query-id data)))))
        (unloaded!)
        (is (empty? @(:events provider)))
        (finally
          (if previous (System/setProperty key previous) (System/clearProperty key)))))
    ;; Native precedence must hold in q, explain, prepared reads and mixed
    ;; planning, even in the presence of an overly broad protocol extension.
    (extend datalevin.db.DB protocol
      {:-source-info (fn [_] (throw (Exception. "native DB entered source descriptor path")))
       :-open-session (fn [_ _] (throw (Exception. "native DB entered source session path")))
       :-plan-source (fn [_ _ _] (throw (Exception. "native DB entered source planning path")))})
    ;; Cache a negative class lookup, then install an extension. Detection must
    ;; observe it without requiring the executor or reloading core/the SPI.
    (let [value (ExtendedSource. provider)]
      (is (identical? value (d/q '[:find ?x . :in ?x] value))))
    (extend ExtendedSource protocol
      {:-source-info (fn [^ExtendedSource x] (info (.-delegate x)))
       :-open-session (fn [^ExtendedSource x request] (open (.-delegate x) request))
       :-plan-source (fn [^ExtendedSource x session request] (plan (.-delegate x) session request))})
    (native-queries! conn)
    (unloaded!)
    (doseq [source [provider (ExtendedSource. provider)]]
      (testing (str "source implementation " (class source))
        (is (= #{[1 "one"] [2 "two"]}
               (d/q '[:find ?id ?label :in $s :where [$s ?id ?label]] source)))
        (doseq [inputs [(list nil source) (map identity [nil source])
                        (next (seq (object-array [:unused nil source])))]]
          (is (= #{[1 "one"] [2 "two"]}
                 (apply d/q '[:find ?id ?label :in ?unused $s
                              :where [$s ?id ?label]] inputs))))
        (doseq [[q inputs]
                [['[:find ?label :in $ $s :where [?id :value 20] [$s ?id ?label]] [@conn source]]
                 ['[:find ?label :in $s $ :where [?id :value 20] [$s ?id ?label]] [source @conn]]]]
          (is (= #{["two"]} (apply d/q q inputs)))
          (is (= 1 (:result-rows (apply d/explain {:run? true} q inputs))))
          (is (false? (:executed? (apply d/explain nil q inputs)))))
        (doseq [f [#(query/count-plan '[:find ?id :in $s :where [$s ?id _]] source)
                   #((d/prepare-q @conn '[:find ?v :in $ ?v]) [source])
                   #((query/query-reader '[:find ?v :in $ ?v]) @conn [@conn source] false)]]
          (is (= :query-source/unsupported
                 (try (f) nil (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))))))
    (is (find-ns 'datalevin.query.source.execute))
    ;; Native work remains independent after a previous source invocation and
    ;; even inside a cancelled source budget. A new source ledger would fail.
    (let [create (resolve 'datalevin.query.source.control/create)
          cancel (resolve 'datalevin.query.source.control/cancel!)
          close (resolve 'datalevin.query.source.control/close!)
          ctl (create)]
      (try
        (cancel ctl {:error :query-source/cancelled :phase :execution})
        (with-bindings {(resolve 'datalevin.query.source.runtime/*runtime*) {:control ctl}}
          (native-queries! conn))
        (finally (close ctl))))
    ;; The explicit internal v2 bridge must also keep DB precedence after that
    ;; broad protocol extension. Load it only after all public isolation checks.
    (require 'datalevin.query.source.program)
    (let [run (resolve 'datalevin.query.source.runtime/with-runtime)
          execute (resolve 'datalevin.query.source.program/query!)]
      (is (= #{[10] [20]}
             (run {} (fn [runtime]
                       (execute runtime '[:find ?v :in $native [?id ...]
                                          :where [$native ?id :value ?v]]
                                [@conn [1 2]]))))))))

(defn -main [& _]
  (binding [test/*report-counters* (ref test/*initial-report-counters*)]
    (unloaded!)
    (let [conn (d/create-conn nil {:value {:db/valueType :db.type/long}
                                 :next {:db/valueType :db.type/ref}}
                              {:kv-opts {:inmemory? true} :background-sampling? false})]
      (try
        (d/transact! conn [{:db/id 1 :value 10 :next 2} {:db/id 2 :value 20}])
        (native-queries! conn)
        (unloaded!)
        (source-checks! conn)
        (finally (d/close conn))))
    (let [{:keys [fail error] :as result} @test/*report-counters*]
      (prn result)
      (System/exit (if (zero? (+ fail error)) 0 1)))))
