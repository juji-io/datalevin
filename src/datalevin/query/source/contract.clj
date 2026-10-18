(ns ^:no-doc datalevin.query.source.contract
  "Structural checks shared by F0 fixtures and the embedded executor.
  Request validation observes the invocation control when one is supplied."
  (:require [clojure.set :as set]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.value :as v :refer [fail!]]))

(defn- ensure! [ok phase]
  (when-not ok (fail! :query-source/invalid phase)))

(defn- fields! [m required optional phase]
  (ensure! (and (v/plain-map? m) (every? #(contains? m %) required)
                (every? #(or (contains? required %) (contains? optional %)
                             (qualified-keyword? %)) (keys m))) phase))

(defn- nonempty-string? [x] (and (string? x) (pos? (count x))))
(defn- identifier? [x check!] (and (some? x) (v/portable? x check!)))
(defn- variable? [x] (and (symbol? x) (.startsWith (name x) "?")))
(defn- keyword-set? [x] (and (set? x) (every? keyword? x)))
(defn- nonnegative? [x] (and (integer? x) (<= 0 x Long/MAX_VALUE)))

(defn cursor-request
  "Build the baseline open envelope from captured context and a selected plan.
  Planning inputs belong to the session's handle, not the cursor request."
  [context source-symbol definition-fingerprint alternative]
  (assoc (select-keys context [:spi-version :features :query-id :authorization
                               :consistency :control :limits])
         :source-symbol source-symbol
         :definition-fingerprint definition-fingerprint
         :alternative-id (:id alternative)
         :plan (:plan alternative)))

(defn type!
  ([t] (type! t nil))
  ([t check!]
   (ensure! (v/portable? t check!) :descriptor)
   (if (keyword? t)
     (ensure! (v/scalar-types t) :descriptor)
     (case (:kind t)
       :tuple (do (fields! t #{:kind :elements} #{} :descriptor)
                  (ensure! (and (vector? (:elements t)) (seq (:elements t))) :descriptor)
                  (run! #(type! % check!) (:elements t)))
       :custom (do (fields! t #{:kind :name :version :fingerprint} #{:parameters} :descriptor)
                   (ensure! (and (qualified-keyword? (:name t))
                                 (pos-int? (:version t))
                                 (nonempty-string? (:fingerprint t))
                                 (or (not (contains? t :parameters))
                                     (and (v/plain-map? (:parameters t))
                                          (v/portable? (:parameters t) check!)))) :descriptor))
       (fail! :query-source/invalid :descriptor)))
   t))

(defn- column! [column variable-required? check!]
  (fields! column (cond-> #{:type :nullable?} variable-required? (conj :variable))
           #{} :descriptor)
  (type! (:type column) check!)
  (ensure! (and (boolean? (:nullable? column))
                (or (not variable-required?) (variable? (:variable column)))) :descriptor))

(defn binding-columns!
  "Validate the nonempty ordered typed inputs of a dependent access. This pure
  helper does not admit a source descriptor or negotiate an SPI revision."
  ([columns] (binding-columns! columns nil))
  ([columns check!]
   (ensure! (and (vector? columns) (seq columns) (v/portable? columns check!)) :batch)
   (doseq [column columns] (column! column true check!))
   (ensure! (= (count columns) (count (set (map :variable columns)))) :batch)
   columns))

(defn pattern-contract!
  ([contract] (pattern-contract! contract nil))
  ([contract check!]
   (case (:kind contract)
     :tuple (do (fields! contract #{:kind :columns} #{} :descriptor)
                (ensure! (vector? (:columns contract)) :descriptor)
                (doseq [column (:columns contract)] (column! column false check!)))
     :eav (do (fields! contract #{:kind :attributes} #{} :descriptor)
              (ensure! (and (v/plain-map? (:attributes contract))
                            (seq (:attributes contract))) :descriptor)
              (doseq [[attribute types] (:attributes contract)]
                (ensure! (keyword? attribute) :descriptor)
                (fields! types #{:entity-type :value-type} #{} :descriptor)
                (type! (:entity-type types) check!)
                (type! (:value-type types) check!)))
     (fail! :query-source/invalid :descriptor))
   contract))

(defn source-info!
  ([info] (source-info! info nil))
  ([info check!]
   (fields! info #{:spi-versions :source-instance-id :compute-context-id
                   :definition-id :definition-fingerprint :features
                   :required-features :pattern-contract} #{} :descriptor)
   (ensure! (and (set? (:spi-versions info)) (seq (:spi-versions info))
                 (every? pos-int? (:spi-versions info))
                 (every? #(identifier? % check!) (map info [:source-instance-id :compute-context-id
                                                :definition-id]))
                 (nonempty-string? (:definition-fingerprint info))
                 (keyword-set? (:features info))
                 (keyword-set? (:required-features info))
                 (set/subset? (:required-features info) (:features info))
                 (v/portable? info check!)) :descriptor)
   (pattern-contract! (:pattern-contract info) check!)
   info))

(defn negotiate
  ([info] (negotiate info #{1} #{}))
  ([info versions features] (negotiate info versions features nil))
  ([info versions features check!]
   (source-info! info check!)
   (let [version (last (sort (set/intersection versions (:spi-versions info))))
         features (set/intersection features (:features info))]
     (when-not version (fail! :query-source/version :descriptor))
     (when-not (set/subset? (:required-features info) features)
       (fail! :query-source/unsupported :descriptor))
     {:spi-version version :features features})))

(defn- term! [term check!]
  (when check! (check!))
  (ensure! (and (vector? term)
                (case (first term)
                  :var (and (= 2 (count term)) (variable? (second term)))
                  :const (and (= 2 (count term))
                              (try (v/value-charge (second term) (when check! (fn [_] (check!)))) true
                                   (catch clojure.lang.ExceptionInfo _
                                     (when check! (check!))
                                     false)))
                  :ignore (= 1 (count term))
                  false)) :clauses))

(defn pattern-columns
  ([descriptor clause] (pattern-columns descriptor clause nil))
  ([descriptor clause check!]
   (let [contract (:pattern-contract descriptor)
         terms (:terms clause)]
     (ensure! (vector? terms) :clauses)
     (run! #(term! % check!) terms)
     (case (:kind contract)
       :tuple (do (ensure! (= (count terms) (count (:columns contract))) :clauses)
                  (:columns contract))
       :eav (do
              (ensure! (= 3 (count terms)) :clauses)
              (let [[tag attribute] (nth terms 1)
                    types (get (:attributes contract) attribute)]
                (when-not (and (= :const tag) types)
                  (fail! :query-source/unsupported :clauses {:clause-id (:id clause)}))
                [{:type (:entity-type types) :nullable? false}
                 {:type :keyword :nullable? false}
                 {:type (:value-type types) :nullable? false}]))))))

(defn clauses!
  ([descriptor clauses] (clauses! descriptor clauses nil))
  ([descriptor clauses check!]
   (when check! (check!))
   (source-info! descriptor check!)
   (ensure! (and (vector? clauses)
                 (every? #(identifier? (:id %) check!) clauses)
                 (= (count clauses) (count (set (map :id clauses))))) :clauses)
   (doseq [clause clauses]
     (when check! (check!))
     (case (:kind clause)
       :pattern (do
                  (fields! clause #{:id :kind :terms} #{} :clauses)
                  (doseq [[term column] (map vector (:terms clause)
                                            (pattern-columns descriptor clause check!))]
                    (when (= :const (first term))
                      (let [value (second term)]
                        (ensure! (if (nil? value) (:nullable? column)
                                     (v/matches-type? (:type column) value check!)) :clauses)))))
       :predicate (do
                    (fields! clause #{:id :kind :operator :semantics-version :args}
                             #{} :clauses)
                    (when-not (and (#{:= :not= :< :<= :> :>=} (:operator clause))
                                   (= 1 (:semantics-version clause)))
                      (fail! :query-source/unsupported :clauses {:clause-id (:id clause)}))
                    (ensure! (and (vector? (:args clause)) (= 2 (count (:args clause)))) :clauses)
                    (run! #(term! % check!) (:args clause))
                    (ensure! (not-any? #(= [:ignore] %) (:args clause)) :clauses))
       (fail! :query-source/unsupported :clauses {:clause-id (:id clause)})))
   clauses))

(defn variables [clause]
  (into #{} (keep #(when (= :var (first %)) (second %)))
        (or (:terms clause) (:args clause))))

(defn- request-check [request]
  (when-let [ctl (:control request)] #(control/check-active! ctl)))

(defn planning-request! [request]
  (let [check! (request-check request)]
    (clauses! (:source-descriptor request) (:clauses request) check!)
    (ensure! (and (symbol? (:source-symbol request))
                  (.startsWith (name (:source-symbol request)) "$")
                  (map? (:bindings request)) (every? variable? (keys (:bindings request)))
                  (set? (:required-vars request)) (every? variable? (:required-vars request))
                  (pos-int? (:max-alternatives request))
                  (= :exact (get-in request [:demand :quality]))
                  (= [] (get-in request [:demand :ordering]))
                  (nonnegative? (get-in request [:demand :offset]))
                  (or (nil? (get-in request [:demand :limit]))
                      (nonnegative? (get-in request [:demand :limit]))))
             :planning)
    (doseq [value (vals (:bindings request))]
      (v/value-charge value (when check! (fn [_] (check!)))))
    (let [bound (into (set (keys (:bindings request)))
                      (mapcat variables) (filter #(= :pattern (:kind %)) (:clauses request)))]
      (ensure! (every? #(set/subset? (variables %) bound) (:clauses request)) :planning))
    request))

(defn- variable-types [request]
  (reduce
    (fn [types clause]
      (if (= :pattern (:kind clause))
        (reduce (fn [types [term column]]
                  (if (= :var (first term))
                    (let [variable (second term)]
                      (when-let [previous (get types variable)]
                        (when-not (= (:type previous) (:type column))
                          (fail! :query-source/unsupported :planning)))
                      (assoc types variable
                             (update column :nullable?
                                     #(and % (get-in types [variable :nullable?] true)))))
                    types))
                types (map vector (:terms clause)
                           (pattern-columns (:source-descriptor request) clause (request-check request))))
        types)) {} (:clauses request)))

(defn alternatives!
  ([request response] (alternatives! request response nil))
  ([request response accept-metadata!]
   (planning-request! request)
   (fields! response #{:alternatives} #{} :planning)
   (let [check! (request-check request)
         alternatives (:alternatives response)
         clauses (into {} (map (juxt :id identity)) (:clauses request))
         ids (set (keys clauses))
         types (variable-types request)]
     (ensure! (and (vector? alternatives)
                   (<= (count alternatives) (long (:max-alternatives request)))) :planning)
     ;; Admission precedes recursive validation, including discarded candidates
     ;; and backing vectors. The callback accounts for envelopes, never handles.
     (when accept-metadata!
       (accept-metadata! nil (dissoc response :alternatives) 256)
       (loop [items alternatives]
         (accept-metadata! nil (meta items) (+ 320 (* 48 (long (count items)))))
         (doseq [a items]
           (ensure! (v/plain-map? a) :planning)
           (accept-metadata! a (dissoc a :plan) 256))
         (when-let [parent (v/vector-parent items)] (recur parent))))
     (ensure! (= (count alternatives) (count (set (map :id alternatives)))) :planning)
     ;; Only explicit plan handles are opaque. Validate extension fields and
     ;; Clojure metadata as well as the required fields, without visiting handles.
     (ensure! (and (v/portable? (dissoc response :alternatives) check!)
                   (v/portable? (meta alternatives) check!)) :planning)
     (doseq [a alternatives]
       (when check! (check!))
       (fields! a #{:id :plan :covers :prefilters :requires :columns :properties :estimate :explain}
                #{} :planning)
       (ensure! (v/portable? (dissoc a :plan) check!) :planning)
       (ensure! (and (identifier? (:id a) check!) (some? (:plan a))
                     (set? (:covers a)) (set? (:prefilters a))
                     (set/subset? (:covers a) ids) (set/subset? (:prefilters a) ids)
                     (empty? (set/intersection (:covers a) (:prefilters a)))
                     (every? #(= :predicate (:kind (clauses %))) (:prefilters a))
                     (vector? (:columns a))
                     (= (count (:columns a)) (count (set (map :variable (:columns a)))))) :planning)
       (when-not (= [] (:requires a)) (fail! :query-source/unsupported :planning))
       (fields! (:properties a) #{:replay :ordering :blocking-state} #{} :planning)
       (ensure! (#{:reopen :once} (get-in a [:properties :replay])) :planning)
       (when-not (and (= [] (get-in a [:properties :ordering]))
                      (= :none (get-in a [:properties :blocking-state])))
         (fail! :query-source/unsupported :planning))
       (let [covered-types (variable-types (update request :clauses
                                                   #(filterv (fn [c] (contains? (:covers a) (:id c))) %)))]
         (doseq [column (:columns a)]
           (column! column true check!)
           (ensure! (= (:type column) (:type (types (:variable column)))) :planning)
           (when-let [covered (covered-types (:variable column))]
             (ensure! (= (:nullable? column) (:nullable? covered)) :planning))))
       (let [available (into (set (keys (:bindings request))) (map :variable) (:columns a))
             covered-vars (reduce set/union #{} (map #(variables (clauses %)) (:covers a)))
             residual-vars (reduce set/union #{}
                                   (map variables (remove #(contains? (:covers a) (:id %))
                                                          (:clauses request))))
             needed (set/intersection covered-vars
                                      (set/union (:required-vars request) residual-vars))]
         (ensure! (set/subset? needed available) :planning))
       (let [estimate (:estimate a)]
         (fields! estimate #{:rows :row-bytes :startup-ms :per-row-ns :requests :confidence}
                  #{} :planning)
         (ensure! (#{:unknown :low :medium :high} (:confidence estimate)) :planning)
         (doseq [k [:rows :row-bytes :startup-ms :per-row-ns :requests]]
           (let [n (get estimate k)]
             (ensure! (or (nil? n) (and (number? n) (Double/isFinite (double n))
                                      (not (neg? (compare n 0)))))
                      :planning)
             (when (nil? n) (ensure! (= :unknown (:confidence estimate)) :planning)))))
       (fields! (:explain a) #{:label} #{} :planning)
       (ensure! (nonempty-string? (get-in a [:explain :label])) :planning))
     response)))

(defn selection! [request response selected-ids]
  (alternatives! request response)
  (ensure! (and (vector? selected-ids) (= (count selected-ids) (count (set selected-ids)))) :planning)
  (let [by-id (into {} (map (juxt :id identity)) (:alternatives response))
        selected (mapv by-id selected-ids)
        patterns (into #{} (comp (filter #(= :pattern (:kind %))) (map :id)) (:clauses request))
        covers (mapcat :covers selected)]
    (ensure! (every? some? selected) :planning)
    (ensure! (= (count covers) (count (set covers))) :planning)
    (when-not (set/subset? patterns (set covers)) (fail! :query-source/unsupported :planning))
    (when (and (> (count selected) 1)
               (some #(= :once (get-in % [:properties :replay])) selected))
      (fail! :query-source/unsupported :planning))
    selected))

(defn session-info! [request info]
  (let [check! (request-check request)]
    (fields! info #{:spi-version :features :session-id :source-instance-id :compute-context-id
                    :authorization-token :consistency :acquired-at-ms :data-version} #{} :session)
    (ensure! (and (= (:spi-version request) (:spi-version info))
                  (= (:features request) (:features info))
                  (identifier? (:session-id info) check!) (identifier? (:authorization-token info) check!)
                  (nonnegative? (:acquired-at-ms info)) (v/portable? info check!)) :session)
    (when-not (= (:consistency request) (:consistency info))
      (fail! :query-source/unsupported :session))
    (doseq [descriptor (:source-descriptors request)]
      (ensure! (= (select-keys descriptor [:source-instance-id :compute-context-id])
                  (select-keys info [:source-instance-id :compute-context-id])) :session))
    info))

(defn- batch-limit! [category actual maximum]
  (when-not (v/at-most? actual (long maximum))
    (fail! :query-source/limit :batch
           {:category category :maximum maximum :attempted actual})))

(defn batch! [columns request batch]
  (control/check-active! (:control request))
  (fields! batch #{:columns :rows :row-count :byte-count :exhausted?} #{:reservation} :batch)
  (ensure! (and (= columns (:columns batch))
                (pos-int? (:max-rows request)) (pos-int? (:max-bytes request))
                (nonnegative? (:row-count batch)) (nonnegative? (:byte-count batch))
                (boolean? (:exhausted? batch)) (vector? (:rows batch))) :batch)
  (let [rows (:rows batch)]
    (ensure! (and (= (:row-count batch) (count rows))
                  (or (seq rows) (:exhausted? batch))) :batch)
    ;; Reject the envelope before inspecting any provider values. Recompute
    ;; the charge under the cap as well: a provider can understate byte-count.
    (batch-limit! :batch-rows (count rows) (:max-rows request))
    (batch-limit! :batch-bytes (:byte-count batch) (:max-bytes request))
    (let [charged (volatile! 0)
          bytes (v/batch-charge-bytes (:columns batch) rows
                  (fn [increment]
                    (control/check-active! (:control request))
                    ;; Each accepted running total fits the long request cap;
                    ;; the next increment may still promote the sum to BigInt.
                    (batch-limit! :batch-bytes
                                  (vswap! charged #(v/add-charge (long %) increment))
                                  (:max-bytes request))))]
      (ensure! (= (:byte-count batch) bytes) :batch)
      (when (or (not (v/at-most? bytes 0)) (:reservation batch))
        (control/accept! (:control request) (:reservation batch) bytes))))
  batch)
