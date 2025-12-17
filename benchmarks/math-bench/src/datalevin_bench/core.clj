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
;; => {:opt-clauses [[?x :person/advised ?d38910] [?d38910 :dissertation/cid ?y] [?d38911 :dissertation/cid ?x] [?d38911 :dissertation/univ ?u] [?d38912 :dissertation/cid ?y] [?d38912 :dissertation/univ ?u] [?y :person/name ?n]], :prepare-time "40.616", :late-clauses [], :parsing-time "0.956", :query-graph {$ {?x {:links [{:type :ref, :tgt ?d38910, :attr :person/advised} {:type :_ref, :tgt ?d38911, :attr :dissertation/cid}], :free [{:attr :person/advised, :var ?d38910}]}, ?d38910 {:links [{:type :_ref, :tgt ?x, :attr :person/advised} {:type :ref, :tgt ?y, :attr :dissertation/cid} {:type :val-eq, :tgt ?d38912, :var ?y, :attrs {?d38910 :dissertation/cid, ?d38912 :dissertation/cid}}], :free [{:attr :dissertation/cid, :var ?y}]}, ?d38911 {:links [{:type :ref, :tgt ?x, :attr :dissertation/cid} {:type :val-eq, :tgt ?d38912, :var ?u, :attrs {?d38911 :dissertation/univ, ?d38912 :dissertation/univ}}], :free [{:attr :dissertation/cid, :var ?x} {:attr :dissertation/univ, :var ?u}]}, ?d38912 {:links [{:type :ref, :tgt ?y, :attr :dissertation/cid} {:type :val-eq, :tgt ?d38910, :var ?y, :attrs {?d38910 :dissertation/cid, ?d38912 :dissertation/cid}} {:type :val-eq, :tgt ?d38911, :var ?u, :attrs {?d38911 :dissertation/univ, ?d38912 :dissertation/univ}}], :free [{:attr :dissertation/cid, :var ?y} {:attr :dissertation/univ, :var ?u}]}, ?y {:links [{:type :_ref, :tgt ?d38910, :attr :dissertation/cid} {:type :_ref, :tgt ?d38912, :attr :dissertation/cid}], :free [{:attr :person/name, :var ?n}]}}}, :planning-time "33.038", :building-time "6.623", :plan {$ [(#datalevin.query.Plan{:steps ["Initialize [?d38912 ?u] by :dissertation/univ." "Merge [?y] by scanning [:dissertation/cid]."], :cost 9492296, :size 215734} #datalevin.query.Plan{:steps ["Merge [?n] by scanning [:person/name]."], :cost 18553124, :size 215734} #datalevin.query.Plan{:steps ["Merge ?d38910 by reverse reference of :dissertation/cid."], :cost 19362127, :size 215950} #datalevin.query.Plan{:steps ["Merge ?x by reverse reference of :person/advised."], :cost 20171940, :size 223725} #datalevin.query.Plan{:steps ["Merge ?d38911 by reverse reference of :dissertation/cid." "Filter by predicates on [:dissertation/univ :dissertation/cid]."], :cost 27275209, :size 223733})]}}

  ;; => {:opt-clauses [[?x :person/advised ?d34987] [?d34987 :dissertation/cid ?y] [?d34988 :dissertation/cid ?x] [?d34988 :dissertation/univ ?u] [?d34989 :dissertation/cid ?y] [?d34989 :dissertation/univ ?u] [?y :person/name ?n]], :prepare-time "35.031", :late-clauses [], :parsing-time "0.092", :query-graph {$ {?x {:links [{:type :ref, :tgt ?d34987, :attr :person/advised} {:type :_ref, :tgt ?d34988, :attr :dissertation/cid}], :free [{:attr :person/advised, :var ?d34987}]}, ?d34987 {:links [{:type :_ref, :tgt ?x, :attr :person/advised} {:type :ref, :tgt ?y, :attr :dissertation/cid} {:type :val-eq, :tgt ?d34989, :var ?y, :attrs {?d34987 :dissertation/cid, ?d34989 :dissertation/cid}}], :free [{:attr :dissertation/cid, :var ?y}]}, ?d34988 {:links [{:type :ref, :tgt ?x, :attr :dissertation/cid} {:type :val-eq, :tgt ?d34989, :var ?u, :attrs {?d34988 :dissertation/univ, ?d34989 :dissertation/univ}}], :free [{:attr :dissertation/cid, :var ?x} {:attr :dissertation/univ, :var ?u}]}, ?d34989 {:links [{:type :ref, :tgt ?y, :attr :dissertation/cid} {:type :val-eq, :tgt ?d34987, :var ?y, :attrs {?d34987 :dissertation/cid, ?d34989 :dissertation/cid}} {:type :val-eq, :tgt ?d34988, :var ?u, :attrs {?d34988 :dissertation/univ, ?d34989 :dissertation/univ}}], :free [{:attr :dissertation/cid, :var ?y} {:attr :dissertation/univ, :var ?u}]}, ?y {:links [{:type :_ref, :tgt ?d34987, :attr :dissertation/cid} {:type :_ref, :tgt ?d34989, :attr :dissertation/cid}], :free [{:attr :person/name, :var ?n}]}}}, :planning-time "32.527", :building-time "2.412", :plan {$ [(#datalevin.query.Plan{:steps ["Initialize [?d34989 ?u] by :dissertation/univ." "Merge [?y] by scanning [:dissertation/cid]."], :cost 9492296, :size 215734} #datalevin.query.Plan{:steps ["Merge [?n] by scanning [:person/name]."], :cost 18553124, :size 215734} #datalevin.query.Plan{:steps ["Merge ?d34987 by reverse reference of :dissertation/cid."], :cost 19092459, :size 215950} #datalevin.query.Plan{:steps ["Merge ?x by reverse reference of :person/advised."], :cost 19632334, :size 223725} #datalevin.query.Plan{:steps ["Merge ?d34988 by equal values of :dissertation/univ." "Filter by predicates on [:dissertation/univ :dissertation/cid]."], :cost 26455947, :size 682911390})]}}

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
;; => {:opt-clauses [(?x :person/advised ?a) (?a :dissertation/cid ?y) (?b :dissertation/cid ?x) (?b :dissertation/univ ?u) (?c :dissertation/cid ?y) (?c :dissertation/univ ?u) [?y :person/name ?n]], :prepare-time "32.442", :late-clauses [], :parsing-time "0.671", :query-graph {$ {?x {:links [{:type :ref, :tgt ?a, :attr :person/advised} {:type :_ref, :tgt ?b, :attr :dissertation/cid}], :free [{:attr :person/advised, :var ?a}]}, ?a {:links [{:type :_ref, :tgt ?x, :attr :person/advised} {:type :ref, :tgt ?y, :attr :dissertation/cid} {:type :val-eq, :tgt ?c, :var ?y, :attrs {?a :dissertation/cid, ?c :dissertation/cid}}], :free [{:attr :dissertation/cid, :var ?y}]}, ?b {:links [{:type :ref, :tgt ?x, :attr :dissertation/cid} {:type :val-eq, :tgt ?c, :var ?u, :attrs {?b :dissertation/univ, ?c :dissertation/univ}}], :free [{:attr :dissertation/cid, :var ?x} {:attr :dissertation/univ, :var ?u}]}, ?c {:links [{:type :ref, :tgt ?y, :attr :dissertation/cid} {:type :val-eq, :tgt ?a, :var ?y, :attrs {?a :dissertation/cid, ?c :dissertation/cid}} {:type :val-eq, :tgt ?b, :var ?u, :attrs {?b :dissertation/univ, ?c :dissertation/univ}}], :free [{:attr :dissertation/cid, :var ?y} {:attr :dissertation/univ, :var ?u}]}, ?y {:links [{:type :_ref, :tgt ?a, :attr :dissertation/cid} {:type :_ref, :tgt ?c, :attr :dissertation/cid}], :free [{:attr :person/name, :var ?n}]}}}, :planning-time "30.808", :building-time "0.964", :plan {$ [(#datalevin.query.Plan{:steps ["Initialize [?c ?u] by :dissertation/univ." "Merge [?y] by scanning [:dissertation/cid]."], :cost 9492296, :size 215734} #datalevin.query.Plan{:steps ["Merge [?n] by scanning [:person/name]."], :cost 18553124, :size 215734} #datalevin.query.Plan{:steps ["Merge ?a by equal values of :dissertation/cid."], :cost 19092459, :size 215950} #datalevin.query.Plan{:steps ["Merge ?x by reverse reference of :person/advised."], :cost 19632334, :size 223725} #datalevin.query.Plan{:steps ["Merge ?b by reverse reference of :dissertation/cid." "Filter by predicates on [:dissertation/univ :dissertation/cid]."], :cost 26455947, :size 223733})]}}

  )
