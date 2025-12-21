(ns datalevin-bench.core
  (:require
   [math-bench.core :as core]
   [datalevin.core :as d]
   [datalevin.query :as q]
   [jsonista.core :as json])
  (:import
   [java.util UUID]))

(def schema
  {:dissertation/cid     {:db/valueType :db.type/ref}
   :dissertation/title   {:db/valueType :db.type/string}
   :dissertation/univ    {:db/valueType :db.type/string}
   :dissertation/country {:db/valueType :db.type/string}
   :dissertation/year    {:db/valueType :db.type/long}
   :dissertation/area    {:db/valueType :db.type/string}
   :person/name          {:db/valueType :db.type/string}
   :person/advised       {:db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/many}})

(def datoms
  (let [nodes    (:nodes (json/read-value (slurp "data.json")
                                          json/keyword-keys-object-mapper))
        dcounter (volatile! 1000000)
        cids     (into #{} (map :id) nodes)
        clean    #(remove (complement cids) %)]
    (sequence
      (comp (remove #(nil? (:name %)))
         (map #(update % :advisors clean))
         (mapcat
           (fn [{:keys [id name thesis school country year subject advisors]}]
             (let [did (vswap! dcounter inc)]
               (into
                 (cond-> [(d/datom did :dissertation/cid id)]
                   name    (conj (d/datom id :person/name name))
                   thesis  (conj (d/datom did :dissertation/title thesis))
                   school  (conj (d/datom did :dissertation/univ school))
                   country (conj (d/datom did :dissertation/country country))
                   year    (conj (d/datom did :dissertation/year year))
                   subject (conj (d/datom did :dissertation/area subject)))
                 (map #(d/datom % :person/advised did) advisors))))))
      nodes)))

(defn load-data
  [dir]
  (d/conn-from-datoms datoms dir schema {:kv-opts {:mapsize 300}}))

(def q1-conn (load-data (str "/tmp/math-q1-" (UUID/randomUUID))))
(def q2-conn (load-data (str "/tmp/math-q2-" (UUID/randomUUID))))
(def q3-conn (load-data (str "/tmp/math-q3-" (UUID/randomUUID))))
(def q4-conn (load-data (str "/tmp/math-q4-" (UUID/randomUUID))))

(defn run-q1 [db]
  (d/q '[:find [?n ...]
         :in $ %
         :where
         [?d :person/name "David Scott Warren"]
         (adv ?x ?d)
         (adv ?y ?x)
         [?y :person/name ?n]]
       db core/rule-q1))

(defn q1 [] (core/bench (run-q1 (d/db q1-conn))))

(defn run-q2 [db]
  (d/q '[:find [?n ...]
         :in $ %
         :where
         (adv ?x ?y)
         (univ ?x ?u)
         (univ ?y ?u)
         [?y :person/name ?n]]
       db core/rule-q2))

(defn q2 [] (core/bench (run-q2 (d/db q2-conn))))

(defn run-q3 [db]
  (d/q '[:find [?n ...]
         :in $ %
         :where
         (adv ?x ?y)
         (area ?x ?a1)
         (area ?y ?a2)
         [(!= ?a1 ?a2)]
         [?y :person/name ?n]]
       db core/rule-q3))

(defn q3 [] (core/bench (run-q3 (d/db q3-conn))))

(defn run-q4 [db]
  (d/q '[:find [?n ...]
         :in $ %
         :where
         [?x :person/name "David Scott Warren"]
         (anc ?y ?x)
         [?y :person/name ?n]]
       db core/rule-q4))

(defn q4 [] (core/bench (run-q4 (d/db q4-conn))))

(defn ^:export -main [& names]
  (doseq [n names]
    (if-some [benchmark (ns-resolve 'datalevin-bench.core (symbol n))]
      (let [perf (binding [q/*cache?* false] (benchmark))]
        (print (core/round perf) "\t")
        (flush))
      (do
        (print "---" "\t")
        (flush))))
  (println))

(comment

  (d/explain {:run? false}
             '[:find [?n ...]
               :in $ %
               :where
               (adv ?x ?y)
               (univ ?x ?u)
               (univ ?y ?u)
               [?y :person/name ?n]]
             (d/db q2-conn) core/rule-q2)
;; => {:opt-clauses [[?x :person/advised ?d34025] [?d34025 :dissertation/cid ?y] [?d34026 :dissertation/cid ?x] [?d34026 :dissertation/univ ?u] [?d34027 :dissertation/cid ?y] [?d34027 :dissertation/univ ?u] [?y :person/name ?n]], :prepare-time "75.784", :late-clauses [], :parsing-time "2.344", :query-graph {$ {?x {:links [{:type :ref, :tgt ?d34025, :attr :person/advised} {:type :_ref, :tgt ?d34026, :attr :dissertation/cid}], :free [{:attr :person/advised, :var ?d34025}]}, ?d34025 {:links [{:type :_ref, :tgt ?x, :attr :person/advised} {:type :ref, :tgt ?y, :attr :dissertation/cid} {:type :val-eq, :tgt ?d34027, :var ?y, :attrs {?d34025 :dissertation/cid, ?d34027 :dissertation/cid}}], :free [{:attr :dissertation/cid, :var ?y}]}, ?d34026 {:links [{:type :ref, :tgt ?x, :attr :dissertation/cid} {:type :val-eq, :tgt ?d34027, :var ?u, :attrs {?d34026 :dissertation/univ, ?d34027 :dissertation/univ}}], :free [{:attr :dissertation/cid, :var ?x} {:attr :dissertation/univ, :var ?u}]}, ?d34027 {:links [{:type :ref, :tgt ?y, :attr :dissertation/cid} {:type :val-eq, :tgt ?d34025, :var ?y, :attrs {?d34025 :dissertation/cid, ?d34027 :dissertation/cid}} {:type :val-eq, :tgt ?d34026, :var ?u, :attrs {?d34026 :dissertation/univ, ?d34027 :dissertation/univ}}], :free [{:attr :dissertation/cid, :var ?y} {:attr :dissertation/univ, :var ?u}]}, ?y {:links [{:type :_ref, :tgt ?d34025, :attr :dissertation/cid} {:type :_ref, :tgt ?d34027, :attr :dissertation/cid}], :free [{:attr :person/name, :var ?n}]}}}, :planning-time "68.414", :building-time "5.026", :plan {$ [(#datalevin.query.Plan{:steps ["Initialize [?d34026 ?u] by :dissertation/univ." "Merge [?x] by scanning [:dissertation/cid]."], :cost 431468, :size 215734} #datalevin.query.Plan{:steps ["Merge [?d34025] by scanning [:person/advised]."], :cost 647202, :size 215734} #datalevin.query.Plan{:steps ["Merge [?y] by scanning [:dissertation/cid]."], :cost 862936, :size 215734} #datalevin.query.Plan{:steps ["Merge [?n] by scanning [:person/name]."], :cost 1078670, :size 215734} #datalevin.query.Plan{:steps ["Obtain ?d34027 by reverse reference of :dissertation/cid." "Filter by predicates on [:dissertation/univ :dissertation/cid]."], :cost 1725872, :size 215950})]}}


  (d/explain {:run? false}
             '[:find [?n ...]
               :in $
               :where
               (?x :person/advised ?a)
               (?a :dissertation/cid ?y)
               (?b :dissertation/cid ?x)
               (?b :dissertation/univ ?u)
               (?c :dissertation/cid ?y)
               (?c :dissertation/univ ?u)
               [?y :person/name ?n]]
             (d/db q2-conn))
;; => {:opt-clauses [(?x :person/advised ?a) (?a :dissertation/cid ?y) (?b :dissertation/cid ?x) (?b :dissertation/univ ?u) (?c :dissertation/cid ?y) (?c :dissertation/univ ?u) [?y :person/name ?n]], :prepare-time "36.341", :late-clauses [], :parsing-time "0.713", :query-graph {$ {?x {:links [{:type :ref, :tgt ?a, :attr :person/advised} {:type :_ref, :tgt ?b, :attr :dissertation/cid}], :free [{:attr :person/advised, :var ?a}]}, ?a {:links [{:type :_ref, :tgt ?x, :attr :person/advised} {:type :ref, :tgt ?y, :attr :dissertation/cid} {:type :val-eq, :tgt ?c, :var ?y, :attrs {?a :dissertation/cid, ?c :dissertation/cid}}], :free [{:attr :dissertation/cid, :var ?y}]}, ?b {:links [{:type :ref, :tgt ?x, :attr :dissertation/cid} {:type :val-eq, :tgt ?c, :var ?u, :attrs {?b :dissertation/univ, ?c :dissertation/univ}}], :free [{:attr :dissertation/cid, :var ?x} {:attr :dissertation/univ, :var ?u}]}, ?c {:links [{:type :ref, :tgt ?y, :attr :dissertation/cid} {:type :val-eq, :tgt ?a, :var ?y, :attrs {?a :dissertation/cid, ?c :dissertation/cid}} {:type :val-eq, :tgt ?b, :var ?u, :attrs {?b :dissertation/univ, ?c :dissertation/univ}}], :free [{:attr :dissertation/cid, :var ?y} {:attr :dissertation/univ, :var ?u}]}, ?y {:links [{:type :_ref, :tgt ?a, :attr :dissertation/cid} {:type :_ref, :tgt ?c, :attr :dissertation/cid}], :free [{:attr :person/name, :var ?n}]}}}, :planning-time "34.671", :building-time "0.956", :plan {$ [(#datalevin.query.Plan{:steps ["Initialize [?b ?u] by :dissertation/univ." "Merge [?x] by scanning [:dissertation/cid]."], :cost 431468, :size 215734} #datalevin.query.Plan{:steps ["Merge [?a] by scanning [:person/advised]."], :cost 647202, :size 215734} #datalevin.query.Plan{:steps ["Merge [?y] by scanning [:dissertation/cid]."], :cost 862936, :size 215734} #datalevin.query.Plan{:steps ["Merge [?n] by scanning [:person/name]."], :cost 1078670, :size 215734} #datalevin.query.Plan{:steps ["Obtain ?c by equal values of :dissertation/cid." "Filter by predicates on [:dissertation/univ :dissertation/cid]."], :cost 1725872, :size 215950})]}}

  )
