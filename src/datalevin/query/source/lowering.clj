(ns ^:no-doc datalevin.query.source.lowering
  "Finite attribute lowering for an owned logical EAV occurrence. Descriptors
  are already captured/validated; this compiler helper never calls a source."
  (:require [datalevin.query.source.access :as access]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]])
  (:import [datalevin.utl QueryKey]))

(defn- ensure! [ok]
  (when-not ok (fail! :query-source/invalid :clauses)))

(defn- variable? [x]
  (and (symbol? x) (.startsWith (name x) "?")))

(defn- term! [term]
  (ensure! (and (vector? term)
                (case (first term)
                  :ignore (= 1 (count term))
                  :var (and (= 2 (count term)) (variable? (second term)))
                  :const (= 2 (count term))
                  false))))

(defn- disjoint-types? [a b check!]
  ;; :data includes other logical values. Custom protocol implementations can
  ;; also extend ordinary classes, so custom/scalar overlap is not disproved by
  ;; different descriptors alone. Never turn an unsupported overlap into empty.
  (check!)
  (cond
    (or (= a b) (= :data a) (= :data b)) false
    (and (= :custom (:kind a)) (= :custom (:kind b))) true
    (or (= :custom (:kind a)) (= :custom (:kind b))) false
    (and (= :tuple (:kind a)) (= :tuple (:kind b)))
    (or (not= (count (:elements a)) (count (:elements b)))
        (some true? (map #(disjoint-types? %1 %2 check!) (:elements a) (:elements b))))
    :else true))

(defn- branch [descriptor clause bindings attribute check!]
  (let [[entity attr value] (:terms clause)
        variable (when (= :var (first attr)) (second attr))
        restore (if variable {variable attribute} {})
        rewrite (fn [[tag x :as term]]
                  (check!)
                  (if (and (= :var tag) (= variable x)) [:const attribute] term))
        terms [(rewrite entity) [:const attribute] (rewrite value)]
        types (get-in descriptor [:pattern-contract :attributes attribute])
        columns (map vector [(first terms) (nth terms 2)] [(:entity-type types) (:value-type types)])
        by-variable (reduce (fn [m [[tag x] type]]
                              (when m
                                (if (= :var tag)
                                  (if (and (contains? m x) (not= type (get-in m [x :type])))
                                    (if (disjoint-types? type (get-in m [x :type]) check!) nil
                                        (fail! :query-source/unsupported :types))
                                    (assoc m x {:variable x :type type :nullable? false}))
                                  m))) {} columns)
        ;; Restoring the attribute also unifies its repeated occurrences in E/V.
        ;; A declared type excluding that keyword is an exact no-match proof.
        compatible? (and by-variable
                         ;; Constants and captured scalar constraints can prove
                         ;; no fact only under ordinary equality, not merely a
                         ;; different Java numeric/collection representation.
                         (not-any? (fn [[[tag x] type]]
                                     (check!)
                                     (cond (= :const tag) (value/disjoint-constant? type x check!)
                                           (and (= :var tag) (contains? bindings x))
                                           (value/disjoint-constant? type (get bindings x) check!)
                                           :else false)) columns)
                         (every? (fn [[original [_ x] type]]
                                   (or (not= [:var variable] original)
                                       (value/matches-type? type x check!)))
                                 [[entity (first terms) (:entity-type types)]
                                  [value (nth terms 2) (:value-type types)]]))]
    (when compatible?
      {:attribute attribute :clause (assoc clause :terms terms)
       :bindings (select-keys bindings (contract/variables (assoc clause :terms terms)))
       :restore restore
       :columns (into [] (comp (filter #(= :var (ffirst %)))
                               (map #(get by-variable (second (first %)))) (distinct)) columns)
       :guarantees (select-keys (get-in descriptor [:logical-schema :attributes attribute])
                               [:entity-domain :reference-domain :cardinality :absence])})))

(defn lower!
  "Own and lower one EAV pattern. A known attribute keeps its region; an
  unknown attribute yields canonical fixed-attribute children. Each child owns
  its type contract, restored attribute binding and no-fact guarantees. No
  type coercion, estimates, provider capabilities or outer rows narrow it."
  [runtime descriptor clause bindings available-vars]
  (let [ctl (:control runtime) check! #(control/check-active! ctl)
        tokens (volatile! []) success? (volatile! false)
        keep! #(do (vswap! tokens conj %) %)]
    (try
      (keep! (rt/charge-copy! runtime [clause bindings available-vars]))
      (ensure! (and (value/plain-map? clause) (= :pattern (:kind clause))
                    (integer? (:id clause)) (<= 0 (:id clause) Long/MAX_VALUE)
                    (vector? (:terms clause)) (= 3 (count (:terms clause)))
                    (value/plain-map? bindings) (every? variable? (keys bindings))
                    (set? available-vars) (every? variable? available-vars)))
      (doseq [term (:terms clause)] (term! term))
      (let [attributes (get-in descriptor [:pattern-contract :attributes])]
        (ensure! (and (= :eav (get-in descriptor [:pattern-contract :kind]))
                      (value/plain-map? attributes) (seq attributes)))
        (keep! (control/reserve! ctl {:retained-bytes (+ 2048 (* 512 (count attributes)))}))
        (doseq [attribute (keys attributes)] (check!) (ensure! (keyword? attribute)))
        (let [clause (value/owned-copy clause check!)
              bindings (value/owned-copy bindings check!)
              [tag x] (second (:terms clause))
              known? (or (= :const tag) (and (= :var tag) (contains? bindings x)))
              attribute (if (= :const tag) x (when known? (get bindings x)))
              _ (when known? (ensure! (keyword? attribute)))
              mode (cond known? :fixed
                         (and (= :var tag) (contains? available-vars x)) :dispatch
                         :else :union)
              candidates (if known? (if (contains? attributes attribute) [attribute] [])
                             (sort #(access/compare-keys %1 %2 check!) (keys attributes)))
              branches
              (into [] (keep (fn [attribute]
                               (check!)
                               (keep! (control/reserve! ctl {:retained-bytes 4096}))
                               (branch descriptor clause bindings attribute check!))) candidates)
              closed? (atom false)
              output {:mode mode :attribute-variable (when (= :var tag) x)
                      :branches branches
                      :empty-proof (when (empty? branches)
                                     (if (and known? (not (contains? attributes attribute)))
                                       :undeclared-attribute
                                       (if (some (fn [[tag x]]
                                                   (or (= :const tag) (and (= :var tag) (contains? bindings x))))
                                                 [(first (:terms clause)) (nth (:terms clause) 2)])
                                         :incompatible-constant :incompatible-repeated-variable)))}
              held @tokens
              close (rt/own! runtime #(when (compare-and-set! closed? false true)
                                       (doseq [token held] (control/release! token))))]
          (vreset! success? true)
          (assoc output :close close :closed? closed?)))
      (finally (when-not @success? (doseq [token @tokens] (control/release! token)))))))

(defn visit!
  "Visit compatible preplanned branches for one borrowed outer binding. The
  callback borrows its restored binding map only during the call. Dispatch is
  equality against captured keywords, never replanning or provider execution."
  [runtime lowered outer emit!]
  (let [ctl (:control runtime) variable (:attribute-variable lowered)]
    (control/check-active! ctl)
    (when-not (and (not @(:closed? lowered)) (value/plain-map? outer)
                  (or (not= :dispatch (:mode lowered)) (contains? outer variable)))
      (fail! :query-source/invalid :execution))
    (doseq [branch (:branches lowered)]
      (control/check-active! ctl)
      (when @(:closed? lowered) (fail! :query-source/invalid :execution))
      (when (every? (fn [[k v]]
                      (or (not (contains? outer k)) (QueryKey/equals v (get outer k))))
                    (:restore branch))
        (let [token (control/reserve! ctl
                      {:retained-bytes (+ 256 (* 96 (+ (count outer) (count (:restore branch)))))})]
          (try (emit! branch (merge outer (:restore branch)))
               (control/check-active! ctl)
               (when @(:closed? lowered) (fail! :query-source/invalid :execution))
               (finally (control/release! token))))))
    nil))
