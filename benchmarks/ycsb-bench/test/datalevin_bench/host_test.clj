(ns datalevin-bench.host-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin-bench.host :as host]))

(deftest monitor-stops-replacements-and-joins-before-resuming
  (let [calls (atom 0)
        replacement (promise)
        resumed (atom nil)
        session (atom nil)
        control {:enabled? true
                 :pause (fn []
                          (case (swap! calls inc)
                            1 [101]
                            2 (do (deliver replacement true) [202])
                            []))
                 :resume (fn [pids]
                           (is (not (.isAlive ^Thread (:thread @session))))
                           (reset! resumed pids))}]
    (reset! session (host/start-pause-monitor! control))
    (try
      (is (= true (deref replacement 5000 :timeout)))
      (finally (host/stop-pause-monitor! @session)))
    (is (= [101 202] @resumed))))

(deftest callback-cleans-up-on-failure
  (let [resumed (atom nil)
        failure (ex-info "body failed" {})
        control {:enabled? true :pause (constantly [303])
                 :resume #(reset! resumed %)}]
    (is (identical? failure
                    (try (host/call-with-paused-media control #(throw failure))
                         (catch Exception e e))))
    (is (= [303] @resumed))))

(deftest cleanup-preserves-interruption-after-resuming
  (let [resumed (atom nil)
        control {:enabled? true :pause (constantly [404])
                 :resume (fn [pids]
                           (is (not (.isInterrupted (Thread/currentThread))))
                           (reset! resumed pids))}
        session (host/start-pause-monitor! control)]
    (.interrupt (Thread/currentThread))
    (try
      (let [result (host/stop-pause-monitor! session)
            interrupted? (.isInterrupted (Thread/currentThread))]
        ;; Reporting assertions can clear interruption through the STM counters.
        (Thread/interrupted)
        (is (= [404] result))
        (is interrupted?)
        (is (= [404] @resumed)))
      (finally (Thread/interrupted)))))

(deftest disabled-control-does-not-start-monitor
  (let [control {:enabled? false
                 :pause #(throw (ex-info "Unexpected pause" {}))
                 :resume #(is (= [] %))}
        session (host/start-pause-monitor! control)]
    (is (nil? (:thread session)))
    (is (= [] (host/stop-pause-monitor! session)))))

(deftest already-stopped-daemons-are-not-owned-by-monitor
  (let [commands (atom [])
        run-command (fn [& command]
                      (swap! commands conj (vec command))
                      {:exit 0 :out ""})
        paused (host/pause! [505 606] #(= 505 %) run-command)]
    (is (= [606] paused))
    (host/resume! paused run-command)
    (is (= [["kill" "-STOP" "606"] ["kill" "-CONT" "606"]] @commands))))
