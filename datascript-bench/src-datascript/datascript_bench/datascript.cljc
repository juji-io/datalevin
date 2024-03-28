(ns datascript-bench.datascript
  (:require
   [clojure.string :as str]
   [datascript.core :as d]
   [datascript-bench.core :as core])
  (:import
   [java.util UUID]))


#?(:cljs
   (enable-console-print!))


(def schema
  {:follows {:db/valueType   :db.type/ref
             :db/cardinality :db.cardinality/many }
   :name    {:db/index true}
   :salary  {:db/index true}})


(defn- wide-db
  ([depth width] (d/db-with (d/empty-db schema) (wide-db 1 depth width)))
  ([id depth width]
    (if (pos? depth)
      (let [children (map #(+ (* id width) %) (range width))]
        (concat
          (map #(array-map
                  :db/add  id
                  :name    "Ivan"
                  :follows %) children)
          (mapcat #(wide-db % (dec depth) width) children)))
      [{:db/id id :name "Ivan"}])))


(defn- long-db [depth width]
  (d/db-with (d/empty-db schema)
             (apply concat
                    (for [x    (range width)
                          y    (range depth)
                          :let [from (+ (* x (inc depth)) y)
                                to   (+ (* x (inc depth)) y 1)]]
                      [{:db/id   (inc from)
                        :name    "Ivan"
                        :follows (inc to)}
                       {:db/id (inc to)
                        :name  "Ivan"}]))))

(def db100k
  (d/db-with
    (d/empty-db schema)
    core/people20k))


(defn ^:export add-1 []
  (core/bench-10
    (reduce
      (fn [db p]
        (-> db
            (d/db-with [[:db/add (:db/id p) :name      (:name p)]])
            (d/db-with [[:db/add (:db/id p) :last-name (:last-name p)]])
            (d/db-with [[:db/add (:db/id p) :sex       (:sex p)]])
            (d/db-with [[:db/add (:db/id p) :age       (:age p)]])
            (d/db-with [[:db/add (:db/id p) :salary    (:salary p)]])))
      (d/empty-db schema)
      core/people20k)))


(defn ^:export add-5 []
  (core/bench-10
    (reduce (fn [db p] (-> db (d/db-with [p])))
            (d/empty-db schema)
            core/people20k)))


(defn ^:export add-all []
  (core/bench-10
    (-> (d/db-with
          (d/empty-db schema)
          core/people20k))))


(defn ^:export init []
  (let [datoms (into []
                     (for [p     core/people20k
                           :let  [id (#?(:clj Integer/parseInt :cljs js/parseInt) (:db/id p))]
                           [k v] p
                           :when (not= k :db/id)]
                       (d/datom id k v)))]
    (core/bench-10
      (d/init-db datoms schema))))


(defn ^:export retract-5 []
  (let [db   (-> (d/empty-db schema)
                 (d/db-with core/people20k))
        eids (->> (d/datoms db :aevt :name) (map :e) (shuffle))]
    (core/bench-10
      (reduce (fn [db eid]
                (-> db
                    (d/db-with [[:db.fn/retractEntity eid]])))
              db eids))))


(defn ^:export q1 []
  (core/bench
    (d/q '[:find ?e
           :where [?e :name "Ivan"]]
         db100k)))

(defn ^:export q2 []
  (core/bench
    (d/q '[:find ?e ?a
           :where
           [?e :name "Ivan"]
           [?e :age ?a]]
         db100k)))

(defn ^:export q2-switch []
  (core/bench
    (d/q '[:find ?e ?a
           :where
           [?e :age ?a]
           [?e :name "Ivan"]]
         db100k)))

(defn ^:export q3 []
  (core/bench
    (d/q '[:find ?e ?a
           :where [?e :name "Ivan"]
           [?e :age ?a]
           [?e :sex :male]]
         db100k)))

(defn ^:export q4 []
  (core/bench
    (d/q '[:find ?e ?l ?a
           :where [?e :name "Ivan"]
           [?e :last-name ?l]
           [?e :age ?a]
           [?e :sex :male]]
         db100k)))

(defn ^:export q5 []
  (core/bench
   (d/q '[:find ?e1 ?l ?a
          :where [?e :name "Ivan"]
          [?e :age ?a]
          [?e1 :age ?a]
          [?e1 :last-name ?l]]
        db100k)))


(defn ^:export qpred1 []
  (core/bench
    (d/q '[:find ?e ?s
           :where [?e :salary ?s]
                  [(> ?s 50000)]]
      db100k)))


(defn ^:export qpred2 []
  (core/bench
    (d/q '[:find ?e ?s
           :in   $ ?min_s
           :where [?e :salary ?s]
           [(> ?s ?min_s)]]
         db100k 50000)))

(defn bench-rules [db]
  (d/q '[:find ?e ?e2
         :in   $ %
         :where (follows ?e ?e2)]
       db
       '[[(follows ?x ?y)
          [?x :follows ?y]]
         [(follows ?x ?y)
          [?x :follows ?t]
          (follows ?t ?y)]]))

(defn ^:export rules-wide-3x3 []
  (let [db (wide-db 3 3)]
    (core/bench (bench-rules db))))

(defn ^:export rules-wide-5x3 []
  (let [db (wide-db 5 3)]
    (core/bench (bench-rules db))))

(defn ^:export rules-wide-7x3 []
  (let [db (wide-db 7 3)]
    (core/bench (bench-rules db))))

(defn ^:export rules-wide-4x6 []
  (let [db (wide-db 4 6)]
    (core/bench (bench-rules db))))

(defn ^:export rules-long-10x3 []
  (let [db (long-db 10 3)]
    (core/bench (bench-rules db))))

(defn ^:export rules-long-30x3 []
  (let [db (long-db 30 3)]
    (core/bench (bench-rules db))))

(defn ^:export rules-long-30x5 []
  (let [db (long-db 30 5)]
    (core/bench (bench-rules db))))

#?(:clj
   (defn ^:export -main [& names]
     (doseq [n names]
       (if-some [benchmark (ns-resolve 'datascript-bench.datascript (symbol n))]
         (let [perf (benchmark)]
           (print (core/round perf) "\t")
           (flush))
         (do
           (print "---" "\t")
           (flush))))
     (println)))
