(ns datalevin.query.source-session-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.permissions :as permissions]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.session :as session])
  (:import [java.io Closeable]))

(def descriptor
  {:spi-versions #{2} :source-instance-id :test/store :compute-context-id :local
   :definition-id :rows :definition-fingerprint "rows:v2" :features #{} :required-features #{}
   :pattern-contract {:kind :eav :attributes {:row/value {:entity-type :long :value-type :string}}}
   :logical-schema {:domains {:row {:entity-type :long}}
                    :attributes {:row/value {:entity-domain :row :cardinality :one
                                              :absence :no-fact :reference-domain nil}}}
   :scope {:id "scope:rows" :kind :full :parameters {}}
   :completion {:domain :finite :extent :all-matches}
   :read-capabilities #{:snapshot :statement :best-effort}})

(defn- error [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(defn- entry
  ([alias d events] (entry alias d events {}))
  ([alias d events config]
   {:symbol alias :descriptor d
    :source
    (reify source/IQuerySource
      (-source-info [_] (throw (Exception. "descriptor was already captured")))
      (-plan-source [_ _ _] (throw (Exception. "no planning during acquisition")))
      (-open-session [_ request]
        (swap! events conj [:acquire alias request])
        (when-let [open (:open config)] (open request))
        (reify source/IQuerySourceSession
          (-session-info [_]
            (swap! events conj [:info alias])
            (let [actual (get config :actual (:consistency request))
                  info {:spi-version 2 :features (:features request) :session-id [alias :session]
                        :source-instance-id (:source-instance-id d) :compute-context-id (:compute-context-id d)
                        :authorization-token :effective-policy :consistency actual :acquired-at-ms 0
                        :data-version (when (= :snapshot actual) :data-v1)
                        :consumption {:opens :many :max-open-cursors 1}
                        :request-constraints {:max-concurrent-requests 2 :rate-limit nil}}]
              (if-let [info! (:info config)] (info! request info) info)))
          (-open-cursor [_ _] (throw (Exception. "no execution cursor during acquisition")))
          (-cancel-session! [_ reason]
            (swap! events conj [:cancel alias reason])
            (when-let [cancel (:cancel config)] (cancel reason)))
          (-close-session! [_]
            (swap! events conj [:close alias])
            (when-let [close (:close config)] (close))))))}))

(defn- events-of [events kind]
  (filterv #(= kind (first %)) @events))

(deftest alias-minima-are-grouped-and-stronger-actual-guarantees-are-retained
  (let [events (atom []) saved (atom nil)]
    (rt/with-runtime {}
      (fn [runtime]
        (reset! saved runtime)
        (let [entries [(entry '$a descriptor events {:actual :snapshot})
                       (entry '$b descriptor events) {:symbol '$native :native (Object.)}]
              prepared (session/prepare! runtime entries
                                         {:consistency :best-effort
                                          :sources {'$a {:consistency :statement}
                                                    '$native {:consistency :snapshot}}})
              [group :as groups] (session/acquire! runtime prepared)
              request (:request group)
              explanation (session/explain! runtime groups)]
          (is (= {'$a :statement '$b :best-effort '$native :snapshot} (:requirements prepared)))
          (is (= {'$native :snapshot} (:native-requirements prepared)))
          (is (= ['$a '$b] (:aliases group)))
          (is (= :statement (:consistency request)))
          (is (= {'$a :statement '$b :best-effort} (:read-requirements request)))
          (is (= [descriptor] (:source-descriptors request)))
          (is (= 2 (:spi-version request)))
          (is (identical? (:control runtime) (:control request)))
          (is (= (:query-id runtime) (:query-id request)))
          (is (= (select-keys request [:spi-version :features :query-id :authorization
                                       :consistency :control :limits]) (:context group)))
          (is (= :snapshot (:actual-consistency group)))
          (is (= :data-v1 (:data-version group)))
          (is (= [{:source '$a :requested-consistency :statement :effective-consistency :statement
                   :consistency :snapshot :data-version :data-v1}
                  {:source '$b :requested-consistency :best-effort :effective-consistency :statement
                   :consistency :snapshot :data-version :data-v1}] explanation))
          (is (= 1 (count (events-of events :acquire))))
          (is (= :acquired @(:state prepared)))
          (is (= :query-source/invalid (:error (error #(session/acquire! runtime prepared))))))))
    (is (= [[:close '$a]] (events-of events :close)))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest unsupported-later-groups-fail-before-any-acquisition
  (let [events (atom []) saved (atom nil)
        result (error #(rt/with-runtime {}
                         (fn [runtime]
                           (reset! saved runtime)
                           (session/prepare! runtime
                                             [(entry '$first descriptor events)
                                              (entry '$weak (assoc descriptor :source-instance-id :other
                                                                              :read-capabilities #{:statement}) events)]
                                             {:sources {'$first {:consistency :best-effort}}}))))]
    (is (= :query-source/unsupported (:error result)))
    (is (empty? @events))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest actual-must-be-supported-by-every-alias-and-weak-views-have-no-pinned-token
  (doseq [[caps actual minimum expected]
          [[#{:snapshot} :snapshot :best-effort :success]
           [#{:statement} :statement :best-effort :success]
           [#{:best-effort} :best-effort :best-effort :success]
           [#{:statement} :snapshot :best-effort :query-source/unsupported]
           [#{:snapshot} :statement :snapshot :query-source/unsupported]
           [#{:snapshot} :best-effort :statement :query-source/unsupported]]]
    (let [events (atom [])
          result (error #(rt/with-runtime {}
                           (fn [runtime]
                             (let [prepared (session/prepare! runtime
                                               [(entry '$a descriptor events {:actual actual})
                                                (entry '$b (assoc descriptor :read-capabilities caps) events)]
                                               {:consistency minimum})
                                   [group] (session/acquire! runtime prepared)]
                               (is (= actual (:actual-consistency group)))
                               (when-not (= actual :snapshot) (is (nil? (:data-version group))))))))]
      (if (= expected :success) (is (nil? result)) (is (= expected (:error result))))
      (is (= 1 (count (events-of events :close)))))))

(deftest independent-groups-keep-their-own-minima-and-views
  (let [events (atom [])]
    (rt/with-runtime {}
      (fn [runtime]
        (let [prepared (session/prepare! runtime
                                         [(entry '$weak (assoc descriptor :read-capabilities #{:best-effort}) events)
                                          (entry '$strong (assoc descriptor :source-instance-id :other) events)]
                                         {:sources {'$weak {:consistency :best-effort}}})
              groups (session/acquire! runtime prepared)]
          (is (= [:best-effort :snapshot] (mapv :actual-consistency groups)))
          (is (= [:best-effort :snapshot] (mapv #(get-in % [:request :consistency]) groups)))
          (is (= [nil :data-v1] (mapv :data-version groups))))))
    (is (= [[:close '$strong] [:close '$weak]] (events-of events :close)))))

(deftest malformed-options-reject-before-provider-work-and-release-preparation
  (doseq [options [false [] {:sources nil} {:sources {'$unknown {:consistency :snapshot}}}
                   {:sources {'$s {}}} {:sources {'$s {:consistency :eventual}}}
                   {:sources {'$s {:consistency :snapshot :authorization :different-principal}}}
                   {:consistency nil} {:features #{}} {:version 2}
                   {:limits false} {:limits {:binding-rows 0}} {:limits {:result-rows -1}}
                   {:limits {:not-a-limit 1}} {:planning nil} {:planning {:max-alternatives 0}}
                   {:planning {:estimate-max-age-ms -1}} {:planning {:rounds 2}}]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [events (atom []) before (control/usage (:control runtime))]
          (is (= :query-source/invalid
                 (:error (error #(session/prepare! runtime [(entry '$s descriptor events)] options)))))
          (is (= before (control/usage (:control runtime))))
          (is (empty? @events)))))))

(deftest preparation-preserves-defaults-and-allows-only-stronger-runtime-caps
  (rt/with-runtime {:limits {:binding-rows 4}}
    (fn [runtime]
      (let [events (atom [])
            prepared (session/prepare! runtime [(entry '$s descriptor events)]
                                       {:limits {:binding-rows 8} :private/annotation "ignored"
                                        :sources {'$s {:consistency :statement :private/annotation "ignored"}}
                                        :planning {:max-alternatives 64 :estimate-max-age-ms 0}})]
        (is (= {:max-alternatives 64 :estimate-max-age-ms 0} (:planning prepared)))
        (is (= 4 (get-in prepared [:groups 0 :request :limits :binding-rows])))
        (is (= :statement (get-in prepared [:groups 0 :request :consistency])))
        (is (= {:principal-id :application} (get-in prepared [:groups 0 :request :authorization])))
        (is (= :query-source/invalid
               (:error (error #(session/prepare! runtime [(entry '$s descriptor events)] {:limits {:binding-rows 2}})))))
        (is (empty? @events))))))

(deftest obsolete-revisions-capabilities-features-and-incompatible-views-reject
  (doseq [[d expected] [[(assoc descriptor :spi-versions #{1}) :query-source/version]
                        [(assoc descriptor :spi-versions #{1 2}) :query-source/version]
                        [(assoc descriptor :pattern-contract {:kind :tuple :columns []}) :query-source/unsupported]
                        [(assoc descriptor :read-capabilities #{}) :query-source/invalid]
                        [(assoc descriptor :read-capabilities #{:serializable}) :query-source/invalid]
                        [(assoc descriptor :features #{:new} :required-features #{:new}) :query-source/unsupported]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [events (atom [])]
          (is (= expected (:error (error #(session/prepare! runtime [(entry '$s d events)] nil)))))
          (is (empty? @events))))))
  (doseq [[mutate expected] [[#(assoc % :definition-fingerprint "rows:other") :query-source/stale-definition]
                             [#(assoc-in % [:scope :parameters] {:tenant :other}) :query-source/invalid]
                             [#(assoc-in % [:logical-schema :attributes :row/value :cardinality] :many)
                              :query-source/invalid]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [events (atom [])]
          (is (= expected
                 (:error (error #(session/prepare! runtime [(entry '$a descriptor events)
                                                            (entry '$b (mutate descriptor) events)] nil)))))
          (is (empty? @events)))))))

(deftest same-accepted-view-ignores-inert-annotations-and-distinct-scopes-stay-distinct
  (rt/with-runtime {}
    (fn [runtime]
      (let [events (atom [])
            annotated (-> descriptor (assoc :private/annotation "ignored")
                          (assoc-in [:scope :private/annotation] "ignored")
                          (assoc-in [:logical-schema :attributes :row/value :private/annotation] "ignored"))
            other-view (assoc descriptor :definition-id :tenant-rows :definition-fingerprint "tenant:v2"
                                         :scope {:id "scope:tenant" :kind :restricted :parameters {:tenant :a}})
            prepared (session/prepare! runtime [(entry '$a descriptor events)
                                                (entry '$b annotated events)
                                                (entry '$tenant other-view events)] nil)
            [group] (session/acquire! runtime prepared)]
        (is (= [descriptor annotated other-view] (get-in group [:request :source-descriptors])))
        (is (= ['$a '$b '$tenant] (:aliases group)))
        (is (= {:tenant :a} (get-in group [:request :source-descriptors 2 :scope :parameters])))
        (is (= 1 (count (events-of events :acquire))))))))

(deftest all-groups-are-acquired-eagerly-in-input-order
  (let [events (atom []) aliases (mapv #(symbol (str "$s" %)) (range 40))]
    (rt/with-runtime {}
      (fn [runtime]
        (let [entries (mapv #(entry %1 (assoc descriptor :source-instance-id %2) events) aliases (range 40))
              prepared (session/prepare! runtime entries nil)
              groups (session/acquire! runtime prepared)]
          (is (= aliases (mapv second (events-of events :acquire))))
          (is (= aliases (mapv #(first (:aliases %)) groups)))
          (is (= 40 (count (events-of events :info)))))))
    (is (= (vec (reverse aliases)) (mapv second (events-of events :close))))))

(deftest malformed-session-envelopes-close-the-returned-session
  (doseq [mutate [#(assoc % :spi-version 1) #(assoc % :features #{:unexpected})
                  #(assoc % :session-id nil) #(assoc % :authorization-token nil)
                  #(assoc % :acquired-at-ms -1) #(assoc % :source-instance-id :foreign)
                  #(assoc % :consistency :eventual) #(assoc % :unknown "private")
                  #(assoc % :request-constraints {:max-concurrent-requests 0 :rate-limit nil})
                  #(assoc % :consumption-groups [{:id :a :opens :once :max-open-cursors 1}
                                                 {:id :b :opens :once :max-open-cursors 1}])
                  #(assoc % :data-version (java.util.Date.))]]
    (let [events (atom []) saved (atom nil)
          result (error #(rt/with-runtime {}
                           (fn [runtime]
                             (reset! saved runtime)
                             (let [prepared (session/prepare! runtime
                                                              [(entry '$s descriptor events {:info (fn [_ info] (mutate info))})] nil)]
                               (session/acquire! runtime prepared)))))]
      (is (= :query-source/invalid (:error result)))
      (is (= [[:close '$s]] (events-of events :close)))
      (is (every? zero? (vals (control/usage (:control @saved))))))))

(deftest weak-sessions-cannot-report-pinned-data-versions
  (doseq [actual [:statement :best-effort]]
    (let [events (atom [])
          result (error #(rt/with-runtime {}
                           (fn [runtime]
                             (session/acquire! runtime
                               (session/prepare! runtime
                                 [(entry '$s descriptor events {:actual actual :info (fn [_ info] (assoc info :data-version :illegal))})]
                                 {:consistency :best-effort})))))]
      (is (= :query-source/invalid (:error result)))
      (is (= [[:close '$s]] (events-of events :close))))))

(deftest partial-acquisition-failure-unwinds-all-resources-and-preserves-the-primary-error
  (let [events (atom []) saved (atom nil)
        failure (atom nil)
        result (error #(rt/with-runtime {}
                         (fn [runtime]
                           (reset! saved runtime)
                           (let [entries [(entry '$a descriptor events {:close (fn [] (throw (Exception. "private close")))})
                                          (entry '$b (assoc descriptor :source-instance-id :other) events
                                                 {:info (fn [_ _] (throw (ex-info "private info"
                                                                                 {:error :query-source/snapshot-lost
                                                                                  :source-symbol "private source"})))})]
                                 prepared (session/prepare! runtime entries nil)]
                             (try (session/acquire! runtime prepared)
                                  (catch Throwable e (reset! failure e) (throw e)))))))]
    (is (= :query-source/snapshot-lost (:error result)))
    (is (= '$b (:source-symbol result)))
    (is (= [[:close '$b] [:close '$a]] (events-of events :close)))
    (is (= 1 (count (.getSuppressed ^Throwable @failure))))
    (is (nil? (.getCause ^Throwable @failure)))
    (is (not (.contains (pr-str result) "private")))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest session-permissions-are-shared-by-all-aliases-and-independent-of-consistency
  (rt/with-runtime {}
    (fn [runtime]
      (let [events (atom [])
            prepared (session/prepare! runtime
                                       [(entry '$a descriptor events
                                               {:actual :best-effort
                                                :info (fn [_ info] (assoc info :consumption {:opens :once :max-open-cursors 1}))})
                                        (entry '$b descriptor events)] {:consistency :best-effort})
            [group] (session/acquire! runtime prepared)
            alternative {:properties {:replay :identical}}]
        (with-open [_open (permissions/reserve-open! (:consumption group) alternative {})] (is true))
        (is (= :query-source/unsupported
               (:error (error #(permissions/reserve-open! (:consumption group) alternative {})))))
        (is (= :best-effort (:actual-consistency group)))))))

(deftest all-capability-subsets-and-alias-minima-obey-the-guarantee-order
  (let [levels [:best-effort :statement :snapshot]
        rank (zipmap levels (range))
        capabilities [#{:best-effort} #{:statement} #{:snapshot}
                      #{:best-effort :statement} #{:best-effort :snapshot}
                      #{:statement :snapshot} (set levels)]]
    (doseq [left capabilities right capabilities minimum-left levels minimum-right levels actual levels]
      (let [events (atom [])
            effective (max (rank minimum-left) (rank minimum-right))
            preflight? (and (some #(>= (rank %) effective) left)
                            (some #(>= (rank %) effective) right))
            valid? (and preflight? (>= (rank actual) effective)
                        (some #(>= (rank %) (rank actual)) left)
                        (some #(>= (rank %) (rank actual)) right))
            result (error #(rt/with-runtime {}
                             (fn [runtime]
                               (let [prepared (session/prepare! runtime
                                                 [(entry '$left (assoc descriptor :read-capabilities left) events {:actual actual})
                                                  (entry '$right (assoc descriptor :read-capabilities right) events)]
                                                 {:sources {'$left {:consistency minimum-left}
                                                            '$right {:consistency minimum-right}}})
                                     [group] (session/acquire! runtime prepared)]
                                 (is (= actual (:actual-consistency group)))
                                 (is (= (nth levels effective) (get-in group [:context :consistency])))))))]
        (is (= (when-not valid? :query-source/unsupported) (:error result)))
        (is (= (if preflight? 1 0) (count (events-of events :acquire))))))))

(deftest authorization-and-data-versions-are-fresh-per-invocation-and-explanation-is-owned
  (let [events (atom []) version (atom 0)
        e (entry '$s descriptor events
                 {:info (fn [_ info]
                          (assoc info :authorization-token "private-effective-policy"
                                      :private/annotation "private-session"
                                      :data-version (with-meta {:version (swap! version inc)}
                                                      {:private/annotation "private-version-metadata"})))})
        run (fn []
              (rt/with-runtime {}
                (fn [runtime]
                  (session/explain! runtime
                    (session/acquire! runtime
                      (session/prepare! runtime [e] {:authorization {:principal-id :app
                                                                    :policy-token "private-credential"}}))))))
        first-result (run)
        second-result (run)]
    (is (= {:version 1} (get-in first-result [0 :data-version])))
    (is (= {:version 2} (get-in second-result [0 :data-version])))
    (is (nil? (meta (get-in first-result [0 :data-version]))))
    (is (= 2 (count (events-of events :acquire))))
    (is (= 2 (count (events-of events :close))))
    (is (not= (get-in (first (events-of events :acquire)) [2 :query-id])
              (get-in (second (events-of events :acquire)) [2 :query-id])))
    (is (every? #(= {:principal-id :app :policy-token "private-credential"} (get-in % [2 :authorization]))
                (events-of events :acquire)))
    (is (not (.contains (binding [*print-meta* true] (pr-str first-result second-result)) "private")))))

(deftest request-constraints-are-validated-without-duplicating-provider-setup-charges
  (let [events (atom []) limiter (atom nil) saved (atom nil)
        constraints {:max-concurrent-requests 2 :rate-limit {:requests 4 :per-ms 1000}}]
    (rt/with-runtime {}
      (fn [runtime]
        (reset! saved runtime)
        (let [e (entry '$s descriptor events
                       {:open (fn [request]
                                (reset! limiter (source/request-limiter (:control request) constraints))
                                (with-open [_permit (source/acquire-request! @limiter)]
                                  (swap! events conj [:setup])))
                        :info (fn [_ info]
                                (assoc info :request-constraints
                                            (assoc constraints :rate-limit
                                                   (with-meta (:rate-limit constraints)
                                                     {:private/annotation (apply str (repeat 1000 "private"))}))))
                        :close (fn [] (.close ^Closeable @limiter))})
              [group] (session/acquire! runtime (session/prepare! runtime [e (assoc e :symbol '$alias)] nil))]
          (is (= constraints (:request-constraints group)))
          (is (nil? (meta (get-in group [:request-constraints :rate-limit]))))
          (is (= 1 (:external-requests (control/usage (:control runtime)))))
          (is (zero? (:concurrent-requests (control/usage (:control runtime)))))
          (is (= 1 (count (events-of events :setup)))))))
    (is (= 1 (:external-requests (control/usage (:control @saved)))))
    (is (every? zero? (vals (dissoc (control/usage (:control @saved)) :external-requests))))))

(deftest late-acquisition-handoff-is-owned-before-cancellation-is-observed
  (let [events (atom []) cancelled (promise) saved (atom nil)
        result (error #(rt/with-runtime {}
                         (fn [runtime]
                           (reset! saved runtime)
                           (session/acquire! runtime
                             (session/prepare! runtime
                               [(entry '$s descriptor events
                                       {:open (fn [request]
                                                (control/cancel! (:control request)
                                                  {:error :query-source/cancelled :phase :acquisition}))
                                        :cancel (fn [_] (deliver cancelled true))})] nil)))))]
    (is (= :query-source/cancelled (:error result)))
    (is (= true (deref cancelled 3000 :hung)))
    (is (empty? (events-of events :info)))
    (is (= [[:close '$s]] (events-of events :close)))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest blocking-acquisition-observes-cancellation-and-real-deadlines
  (doseq [deadline? [false true]]
    (let [events (atom []) entered (promise) signalled (promise) release (promise)
          saved (atom nil)
          worker (future
                   (error #(rt/with-runtime (if deadline? {:timeout-ms 1000} {})
                             (fn [runtime]
                               (reset! saved runtime)
                               (session/acquire! runtime
                                 (session/prepare! runtime
                                   [(entry '$s descriptor events
                                           {:open (fn [request]
                                                    ;; Before a session exists its provider must register
                                                    ;; cancellation for checkout/snapshot setup itself.
                                                    (let [registration
                                                          (source/register-cancel! (:control request)
                                                            (fn [_] (deliver signalled true) (deliver release true)))]
                                                      (try (deliver entered true) @release
                                                           (finally (source/unregister-cancel! registration)))))})] nil))))))]
      (try
        (is (= true (deref entered 3000 :hung)))
        (when-not deadline?
          (control/cancel! (:control @saved) {:error :query-source/cancelled :phase :acquisition}))
        (is (= true (deref signalled 3000 :hung)))
        (is (= (if deadline? :query/timeout :query-source/cancelled)
               (:error (deref worker 3000 {}))))
        (is (= [[:close '$s]] (events-of events :close)))
        (is (every? zero? (vals (control/usage (:control @saved)))))
        (finally (deliver release true) (future-cancel worker))))))

(deftest cancellation-stays-registered-until-session-close-completes
  (let [events (atom []) closing (promise) release (promise) saved (atom nil)
        worker (future
                 (error #(rt/with-runtime {}
                           (fn [runtime]
                             (reset! saved runtime)
                             (session/acquire! runtime
                               (session/prepare! runtime
                                 [(entry '$s descriptor events
                                         {:close (fn [] (deliver closing true) @release)
                                          :cancel (fn [_] (deliver release true))})] nil))
                             :finished))))]
    (try
      (is (= true (deref closing 3000 :hung)))
      (control/cancel! (:control @saved) {:error :query-source/cancelled :phase :execution})
      (is (= :query-source/cancelled (:error (deref worker 3000 {}))))
      (is (= [[:close '$s]] (events-of events :close)))
      (is (every? zero? (vals (control/usage (:control @saved)))))
      (finally (deliver release true) (future-cancel worker)))))

(deftest failed-acquisition-releases-snapshots-and-prefix-state-before-rethrowing
  (let [events (atom []) saved (atom nil) before (atom nil) after (atom nil)
        result (error #(rt/with-runtime {:limits {:retained-bytes 65536}}
                         (fn [runtime]
                           (reset! saved runtime)
                           (let [prepared (session/prepare! runtime
                                             [(entry '$first descriptor events)
                                              (entry '$later (assoc descriptor :source-instance-id :other) events
                                                     {:info (fn [_ info]
                                                              (assoc info :private/annotation
                                                                          [(apply str (repeat 100000 "x")) (Object.)]))})] nil)]
                             (reset! before (control/usage (:control runtime)))
                             (try (session/acquire! runtime prepared)
                                  (catch Throwable e
                                    (reset! after (control/usage (:control runtime)))
                                    (throw e)))))))]
    (is (= :query-source/limit (:error result)))
    (is (= @before @after))
    (is (= [[:close '$later] [:close '$first]] (events-of events :close)))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest incompatible-runtime-and-mutable-option-metadata-reject-before-acquisition
  (testing "prepared acquisitions belong to one invocation"
    (rt/with-runtime {}
      (fn [runtime]
        (let [events (atom [])
              prepared (session/prepare! runtime [(entry '$s descriptor events)] nil)
              child (control/create {:parent (:control runtime)})]
          (try
            (is (= :query-source/invalid
                   (:error (error #(session/acquire! (assoc runtime :control child) prepared)))))
            (is (= :prepared @(:state prepared)))
            (is (empty? @events))
            (finally (control/close! child)))))))
  (testing "metadata remains immutable even when its annotations are unused"
    (rt/with-runtime {}
      (fn [runtime]
        (let [events (atom []) before (control/usage (:control runtime))]
          (is (= :query-source/invalid
                 (:error (error #(session/prepare! runtime [(entry '$s descriptor events)]
                                                  {:private/annotation (java.util.Date.)})))))
          (is (= before (control/usage (:control runtime))))
          (is (empty? @events)))))))
