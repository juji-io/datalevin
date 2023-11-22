(ns datalevin.test.pull-api
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.util :as u]
   [datalevin.core :as d])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(def test-schema
  {:name   { :db/unique :db.unique/identity }
   :aka    { :db/cardinality :db.cardinality/many }
   :child  { :db/cardinality :db.cardinality/many
            :db/valueType    :db.type/ref }
   :friend { :db/cardinality :db.cardinality/many
            :db/valueType    :db.type/ref }
   :enemy  { :db/cardinality :db.cardinality/many
            :db/valueType    :db.type/ref }
   :father { :db/valueType :db.type/ref }

   :part { :db/valueType  :db.type/ref
          :db/isComponent true
          :db/cardinality :db.cardinality/many }
   :spec { :db/valueType  :db.type/ref
          :db/isComponent true
          :db/cardinality :db.cardinality/one }})

(def test-datoms
  (->>
    [[1 :name  "Petr"]
     [1 :aka   "Devil"]
     [1 :aka   "Tupen"]
     [2 :name  "David"]
     [3 :name  "Thomas"]
     [4 :name  "Lucy"]
     [5 :name  "Elizabeth"]
     [6 :name  "Matthew"]
     [7 :name  "Eunan"]
     [8 :name  "Kerri"]
     [9 :name  "Rebecca"]
     [1 :child 2]
     [1 :child 3]
     [2 :father 1]
     [3 :father 1]
     [6 :father 3]
     [10 :name  "Part A"]
     [11 :name  "Part A.A"]
     [10 :part 11]
     [12 :name  "Part A.A.A"]
     [11 :part 12]
     [13 :name  "Part A.A.A.A"]
     [12 :part 13]
     [14 :name  "Part A.A.A.B"]
     [12 :part 14]
     [15 :name  "Part A.B"]
     [10 :part 15]
     [16 :name  "Part A.B.A"]
     [15 :part 16]
     [17 :name  "Part A.B.A.A"]
     [16 :part 17]
     [18 :name  "Part A.B.A.B"]
     [16 :part 18]]
    (map #(apply d/datom %))))

(deftest test-pull-attr-spec
  (let [dir     (u/tmp-dir (str "pull-" (UUID/randomUUID)))
        test-db (d/init-db test-datoms dir test-schema)]
    (is (= {:name "Petr" :aka ["Devil" "Tupen"]}
           (d/pull test-db '[:name :aka] 1)))

    (is (= {:name "Matthew" :father {:db/id 3} :db/id 6}
           (d/pull test-db '[:name :father :db/id] 6)))

    (is (= [{:name "Petr"} {:name "Elizabeth"}
            {:name "Eunan"} {:name "Rebecca"}]
           (d/pull-many test-db '[:name] [1 5 7 9])))
    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-pull-reverse-attr-spec
  (let [dir     (u/tmp-dir (str "pull-or-" (UUID/randomUUID)))
        test-db (d/init-db test-datoms dir test-schema)]
    (is (= {:name "David" :_child [{:db/id 1}]}
           (d/pull test-db '[:name :_child] 2)))

    (is (= {:name "David" :_child [{:name "Petr"}]}
           (d/pull test-db '[:name {:_child [:name]}] 2)))

    (testing "Reverse non-component references yield collections"
      (is (= {:name "Thomas" :_father [{:db/id 6}]}
             (d/pull test-db '[:name :_father] 3)))

      (is (= {:name "Petr" :_father [{:db/id 2} {:db/id 3}]}
             (d/pull test-db '[:name :_father] 1)))

      (is (= {:name "Thomas" :_father [{:name "Matthew"}]}
             (d/pull test-db '[:name {:_father [:name]}] 3)))

      (is (= {:name "Petr" :_father [{:name "David"} {:name "Thomas"}]}
             (d/pull test-db '[:name {:_father [:name]}] 1))))

    (testing "Multiple reverse refs #412"
      (is (= {:name "Petr" :_father [{:db/id 2} {:db/id 3}]}
             (d/pull test-db '[:name :_father :_child] 1))))

    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-pull-component-attr
  (let [dir1    (u/tmp-dir (str "pull-or-" (UUID/randomUUID)))
        test-db (d/init-db test-datoms dir1 test-schema)
        parts   {:name "Part A",
                 :part
                 [{:db/id 11
                   :name  "Part A.A",
                   :part
                   [{:db/id 12
                     :name  "Part A.A.A",
                     :part
                     [{:db/id 13 :name "Part A.A.A.A"}
                      {:db/id 14 :name "Part A.A.A.B"}]}]}
                  {:db/id 15
                   :name  "Part A.B",
                   :part
                   [{:db/id 16
                     :name  "Part A.B.A",
                     :part
                     [{:db/id 17 :name "Part A.B.A.A"}
                      {:db/id 18 :name "Part A.B.A.B"}]}]}]}
        rpart   (update-in
                  parts [:part 0 :part 0 :part]
                  #(into [{:db/id 10
                           :name  "Part A",
                           :part
                           [{:db/id 11}
                            {:name  "Part A.B",
                             :part  [{:name  "Part A.B.A",
                                      :part  [{:name "Part A.B.A.A", :db/id 17}
                                              {:name "Part A.B.A.B", :db/id 18}],
                                      :db/id 16}],
                             :db/id 15}]}] %))
        dir2    (u/tmp-dir (str "pull-or-" (UUID/randomUUID)))
        recdb   (d/init-db
                  (concat test-datoms [(d/datom 12 :part 10)])
                  dir2
                  test-schema)]

    (testing "Component entities are expanded recursively"
      (is (= parts (d/pull test-db '[:name :part] 10))))

    (testing "Reverse component references yield a single result"
      (is (= {:name "Part A.A" :_part {:db/id 10}}
             (d/pull test-db [:name :_part] 11)))

      (is (= {:name "Part A.A" :_part {:name "Part A"}}
             (d/pull test-db [:name {:_part [:name]}] 11))))

    (testing "Like explicit recursion, expansion will not allow loops"
      (is (= rpart (d/pull recdb '[:name :part] 10))))

    (testing "Reverse recursive component #411"
      (is (= {:name  "Part A.A.A.B"
              :_part {:name  "Part A.A.A"
                      :_part {:name "Part A.A" :_part {:name "Part A"}}}}
             (d/pull test-db '[:name {:_part ...}] 14)))
      (is (= {:name  "Part A.A.A.B"
              :_part {:name "Part A.A.A" :_part {:name "Part A.A"}}}
             (d/pull test-db '[:name {:_part 2}] 14))))

    (d/close-db test-db)
    (d/close-db recdb)
    (u/delete-files dir1)
    (u/delete-files dir2)))

(deftest test-pull-wildcard
  (let [dir     (u/tmp-dir (str "pull-wild-" (UUID/randomUUID)))
        test-db (d/init-db test-datoms dir test-schema)]
    (is (= {:db/id 1 :name "Petr" :aka ["Devil" "Tupen"]
            :child [{:db/id 2} {:db/id 3}]}
           (d/pull test-db '[*] 1)))

    (is (= {:db/id 2 :name "David" :_child [{:db/id 1}] :father {:db/id 1}}
           (d/pull test-db '[* :_child] 2)))
    (is (= {:aka  ["Devil" "Tupen"], :child [{:db/id 2} {:db/id 3}],
            :name "Petr",            :db/id 1}
           (d/pull test-db '[:name *] 1)))

    (is (= {:aka  ["Devil" "Tupen"], :child [{:db/id 2} {:db/id 3}],
            :name "Petr",            :db/id 1}
           (d/pull test-db '[:aka :name *] 1)))

    (is (= {:aka  ["Devil" "Tupen"], :child [{:db/id 2} {:db/id 3}],
            :name "Petr",            :db/id 1}
           (d/pull test-db '[:aka :child :name *] 1)))

    (is (= {:alias      ["Devil" "Tupen"], :child [{:db/id 2} {:db/id 3}],
            :first-name "Petr",            :db/id 1}
           (d/pull test-db '[[:aka :as :alias] [:name :as :first-name] *] 1)))

    (is (= {:db/id 1
            :name  "Petr"
            :aka   ["Devil" "Tupen"]
            :child [{:db/id  2
                     :father {:db/id 1}
                     :name   "David"}
                    {:db/id  3
                     :father {:db/id 1}
                     :name   "Thomas"}]}
           (d/pull test-db '[* {:child ...}] 1)))
    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-pull-limit
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (d/init-db
              (concat
                test-datoms
                [(d/datom 4 :friend 5)
                 (d/datom 4 :friend 6)
                 (d/datom 4 :friend 7)
                 (d/datom 4 :friend 8)]
                (for [idx (range 2000)]
                  (d/datom 8 :aka (str "aka-" idx))))
              dir
              test-schema)]

    (testing "Without an explicit limit, the default is 1000"
      (is (= 1000 (->> (d/pull db '[:aka] 8) :aka count))))

    (testing "Explicit limit can reduce the default"
      (is (= 500 (->> (d/pull db '[(limit :aka 500)] 8) :aka count)))
      (is (= 500 (->> (d/pull db '[[:aka :limit 500]] 8) :aka count))))

    (testing "Explicit limit can increase the default"
      (is (= 1500 (->> (d/pull db '[(limit :aka 1500)] 8) :aka count))))

    (testing "A nil limit produces unlimited results"
      (is (= 2000 (->> (d/pull db '[(limit :aka nil)] 8) :aka count))))

    (testing "Limits can be used as map specification keys"
      (is (= {:name   "Lucy"
              :friend [{:name "Elizabeth"} {:name "Matthew"}]}
             (d/pull db '[:name {(limit :friend 2) [:name]}] 4))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-pull-default
  (let [dir     (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        test-db (d/init-db test-datoms dir test-schema)]
    (testing "Empty results return nil"
      (is (nil? (d/pull test-db '[:foo] 1))))

    (testing "A default can be used to replace nil results"
      (is (= {:foo "bar"}
             (d/pull test-db '[(default :foo "bar")] 1)))
      (is (= {:foo "bar"}
             (d/pull test-db '[[:foo :default "bar"]] 1))))

    (testing "default does not override results"
      (is (= {:name  "Petr", :aka ["Devil" "Tupen"]
              :child [{:name "David", :aka "[aka]", :child "[child]"}
                      {:name "Thomas", :aka "[aka]", :child "[child]"}]}
             (d/pull test-db
                     '[[:name :default "[name]"]
                       [:aka :default "[aka]"]
                       {[:child :default "[child]"] ...}]
                     1)))
      (is (= {:name "David", :aka "[aka]", :child "[child]"}
             (d/pull test-db
                     '[[:name :default "[name]"]
                       [:aka :default "[aka]"]
                       {[:child :default "[child]"] ...}]
                     2))))

    (testing "Ref default"
      (is (= {:child 1 :db/id 2}
             (d/pull test-db '[:db/id [:child :default 1]] 2)))
      (is (= {:_child 2 :db/id 1}
             (d/pull test-db '[:db/id [:_child :default 2]] 1))))
    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-pull-as
  (let [dir     (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        test-db (d/init-db test-datoms dir test-schema)]
    (is (= {"Name" "Petr", :alias ["Devil" "Tupen"]}
           (d/pull test-db '[[:name :as "Name"] [:aka :as :alias]] 1)))
    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-pull-attr-with-opts
  (let [dir     (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        test-db (d/init-db test-datoms dir test-schema)]
    (is (= {"Name" "Nothing"}
           (d/pull test-db '[[:x :as "Name" :default "Nothing"]] 1)))
    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-pull-map
  (let [dir     (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        test-db (d/init-db test-datoms dir test-schema)]
    (testing "Single attrs yield a map"
      (is (= {:name "Matthew" :father {:name "Thomas"}}
             (d/pull test-db '[:name {:father [:name]}] 6))))

    (testing "Multi attrs yield a collection of maps"
      (is (= {:name "Petr" :child [{:name "David"}
                                   {:name "Thomas"}]}
             (d/pull test-db '[:name {:child [:name]}] 1))))

    (testing "Missing attrs are dropped"
      (is (= {:name "Petr"}
             (d/pull test-db '[:name {:father [:name]}] 1))))

    (testing "Non matching results are removed from collections"
      (is (= {:name "Petr"}
             (d/pull test-db '[:name {:child [:foo]}] 1))))

    (testing "Map specs can override component expansion"
      (let [parts {:name "Part A"
                   :part [{:name "Part A.A"} {:name "Part A.B"}]}]
        (is (= parts
               (d/pull test-db '[:name {:part [:name]}] 10)))

        (is (= parts
               (d/pull test-db '[:name {:part 1}] 10)))))
    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-pull-recursion-1
  (let [dir     (u/tmp-dir (str "pull-" (UUID/randomUUID)))
        test-db (d/init-db test-datoms dir test-schema)
        db      (-> test-db
                    (d/db-with [[:db/add 4 :friend 5]
                                [:db/add 5 :friend 6]
                                [:db/add 6 :friend 7]
                                [:db/add 7 :friend 8]
                                [:db/add 4 :enemy 6]
                                [:db/add 5 :enemy 7]
                                [:db/add 6 :enemy 8]
                                [:db/add 7 :enemy 4]]))
        friends {:db/id 4
                 :name  "Lucy"
                 :friend
                 [{:db/id 5
                   :name  "Elizabeth"
                   :friend
                   [{:db/id 6
                     :name  "Matthew"
                     :friend
                     [{:db/id 7
                       :name  "Eunan"
                       :friend
                       [{:db/id 8
                         :name  "Kerri"}]}]}]}]}
        enemies {:db/id 4
                 :name  "Lucy"
                 :friend
                 [{:db/id 5
                   :name  "Elizabeth"
                   :friend
                   [{:db/id 6
                     :name  "Matthew"
                     :enemy [{:db/id 8 :name "Kerri"}]}]
                   :enemy
                   [{:db/id 7
                     :name  "Eunan"
                     :friend
                     [{:db/id 8
                       :name  "Kerri"}]
                     :enemy
                     [{:db/id  4
                       :name   "Lucy"
                       :friend [{:db/id 5}]}]}]}]
                 :enemy
                 [{:db/id 6
                   :name  "Matthew"
                   :friend
                   [{:db/id 7
                     :name  "Eunan"
                     :friend
                     [{:db/id 8
                       :name  "Kerri"}]
                     :enemy [{:db/id  4
                              :name   "Lucy"
                              :enemy  [{:db/id 6}]
                              :friend [{:db/id  5
                                        :name   "Elizabeth"
                                        :enemy  [{:db/id 7}],
                                        :friend [{:db/id 6}]}]}]}]
                   :enemy
                   [{:db/id 8
                     :name  "Kerri"}]}]}]

    (testing "Infinite recursion"
      (is (= friends (d/pull db '[:db/id :name {:friend ...}] 4))))

    (testing "Multiple recursion specs in one pattern"
      (is (= enemies (d/pull db '[:db/id :name {:friend 2 :enemy 2}] 4))))

    (testing "Reverse recursion"
      (is (= {:db/id   8,
              :_friend [{:db/id   7,
                         :_friend [{:db/id   6,
                                    :_friend [{:db/id   5,
                                               :_friend [{:db/id 4}]}]}]}]}
             (d/pull db '[:db/id {:_friend ...}] 8)))
      (is (= {:db/id 8, :_friend [{:db/id 7, :_friend [{:db/id 6}]}]}
             (d/pull db '[:db/id {:_friend 2}] 8))))

    (let [db (d/db-with db [[:db/add 8 :friend 4]])]
      (testing "Cycles are handled by returning only the :db/id of entities which have been seen before"
        (is (= (update-in friends (take 8 (cycle [:friend 0]))
                          assoc :friend [{:db/id  4
                                          :name   "Lucy"
                                          :friend [{:db/id 5}]}])
               (d/pull db '[:db/id :name {:friend ...}] 4)))))
    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-dual-recursion-1
  (testing "Seen ids are tracked independently for different branches"
    (let [dir (u/tmp-dir (str "pull-" (UUID/randomUUID)))
          db  (-> (d/empty-db dir {:friend {:db/valueType :db.type/ref}
                                   :enemy  {:db/valueType :db.type/ref}})
                  (d/db-with [{:db/id 1 :name "1" :friend 2 :enemy 2}
                              {:db/id 2 :name "2"}]))]
      (is (= {:name "1" :friend {:name "2"} :enemy {:name "2"}}
             (d/pull db '[:name {:friend [:name], :enemy [:name]}] 1)))
      (d/close-db db)
      (u/delete-files dir))))

(deftest test-dual-recursion-2
  (let [dir (u/tmp-dir (str "pull-" (UUID/randomUUID)))
        recursion-db
        (-> (d/empty-db dir {:friend {:db/valueType :db.type/ref}
                             :enemy  {:db/valueType :db.type/ref}})
            (d/db-with [{:db/id 1 :friend 2}
                        {:db/id 2 :enemy 3}
                        {:db/id 3 :friend 4}
                        {:db/id 4 :enemy 5}
                        {:db/id 5 :friend 6}
                        {:db/id 6 :enemy 7}]))]
    (is (= {:db/id 1 :friend {:db/id 2}}
           (d/pull recursion-db '[:db/id {:friend ...}] 1)))
    (is (= {:db/id 1 :friend {:db/id 2 :enemy {:db/id 3}}}
           (d/pull recursion-db '[:db/id {:friend 1 :enemy 1}] 1)))
    (is (= {:db/id 1 :friend {:db/id 2 :enemy {:db/id 3 :friend {:db/id 4}}}}
           (d/pull recursion-db '[:db/id {:friend 2 :enemy 1}] 1)))
    (is (= {:db/id  1
            :friend {:db/id 2
                     :enemy {:db/id 3 :friend {:db/id 4 :enemy {:db/id 5}}}}}
           (d/pull recursion-db '[:db/id {:friend 2 :enemy 2}] 1)))
    (d/close-db recursion-db)
    (u/delete-files dir)))

(deftest test-dual-recursion-3
  (let [dir   (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        empty (d/empty-db dir {:part { :db/valueType :db.type/ref }
                               :spec { :db/valueType :db.type/ref }})
        db    (d/db-with empty [[:db/add 1 :part 2]
                                [:db/add 2 :part 3]
                                [:db/add 3 :part 1]
                                [:db/add 1 :spec 2]
                                [:db/add 2 :spec 1]])]
    (is (= (d/pull db '[:db/id {:part ...} {:spec ...}] 1)
           {:db/id 1,
            :spec  {:db/id 2
                    :spec  {:db/id 1,
                            :spec  {:db/id 2}, :part {:db/id 2}}
                    :part  {:db/id 3,
                            :part  {:db/id 1,
                                    :spec  {:db/id 2},
                                    :part  {:db/id 2}}}}
            :part  {:db/id 2
                    :spec  {:db/id 1, :spec {:db/id 2}, :part {:db/id 2}}
                    :part  {:db/id 3,
                            :part  {:db/id 1,
                                    :spec  {:db/id 2},
                                    :part  {:db/id 2}}}}}))

    (d/close-db db)
    (u/delete-files dir)))

(deftest test-deep-recursion
  (let [start  100
        depth  1500
        txd    (mapcat
                 (fn [^long idx]
                   [(d/datom idx :name (str "Person-" idx))
                    (d/datom (dec idx) :friend idx)])
                 (range (inc start) depth))
        dir    (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db     (d/init-db (concat
                            test-datoms
                            [(d/datom start :name (str "Person-" start))]
                            txd)
                          dir
                          test-schema)
        pulled (d/pull db '[:name {:friend ...}] start)
        path   (->> [:friend 0]
                    (repeat (dec (- depth start)))
                    (into [] cat))]
    (is (= (str "Person-" (dec depth))
           (:name (get-in pulled path))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-component-reverse
  (let [schema {:ref {:db/valueType   :db.type/ref
                      :db/isComponent true}}
        dir    (u/tmp-dir (str "pull-lookup-" (UUID/randomUUID)))
        db     (d/db-with (d/empty-db dir schema)
                          [{:name "1"
                            :ref  {:name "2"
                                   :ref  {:name "3"}}}])]
    (is (= {:name "1", :ref {:name "2", :ref {:name "3", :_ref {:name "2"}}}}
           (d/pull db
                   [:name {:ref [:name {:ref [:name {:_ref [:name]}]}]}]
                   1)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-lookup-ref-pull
  (let [dir     (u/tmp-dir (str "pull-lookup-" (UUID/randomUUID)))
        test-db (d/init-db test-datoms dir test-schema)]
    (is (= {:name "Petr" :aka ["Devil" "Tupen"]}
           (d/pull test-db '[:name :aka] [:name "Petr"])))
    (is (= nil
           (d/pull test-db '[:name :aka] [:name "NotInDatabase"])))
    (is (= [nil {:aka ["Devil" "Tupen"]} nil nil]
           (d/pull-many test-db
                        '[:aka]
                        [[:name "Elizabeth"]
                         [:name "Petr"]
                         [:name "Eunan"]
                         [:name "Rebecca"]])))
    (is (nil? (d/pull test-db '[*] [:name "No such name"])))
    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-xform
  (let [dir     (u/tmp-dir (str "pull-" (UUID/randomUUID)))
        test-db (d/init-db test-datoms dir test-schema)]
    (is (= {:db/id [1]
            :name  ["Petr"]
            :aka   [["Devil" "Tupen"]]
            :child [[{:db/id [2], :name ["David"], :aka [nil], :child [nil]}
                     {:db/id [3], :name ["Thomas"], :aka [nil], :child [nil]}]]}
           (d/pull test-db
                   '[[:db/id :xform vector]
                     [:name :xform vector]
                     [:aka :xform vector]
                     {[:child :xform vector] ...}]
                   1)))
    (testing "missing attrs are processed by xform"
      (is (= {:normal [nil]
              :aka    [nil]
              :child  [nil]}
             (d/pull test-db
                     '[[:normal :xform vector]
                       [:aka :xform vector]
                       {[:child :xform vector] ...}]
                     2))))
    (testing "default takes precedence"
      (is (= {:unknown "[unknown]"}
             (d/pull test-db
                     '[[:unknown :default "[unknown]" :xform vector]] 1))))

    (testing ":xform on cardinality/one"
      (is (= {:name "David" :father "Petr"}
            (d/pull test-db [:name {[:father :xform #(:name %)] ['*]}] 2))))

    (testing ":xform on reverse ref"
      (is (= {:name "Petr" :_father ["David" "Thomas"]}
            (d/pull test-db [:name {[:_father :xform #(mapv :name %)] [:name]}] 1))))

    (testing ":xform on reverse component ref"
      (is (= {:name "Part A.A" :_part "Part A"}
            (d/pull test-db [:name {[:_part :xform #(:name %)] [:name]}] 11))))

    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-xform-cardinality-one
  (let [dir      (u/tmp-dir (str "pull-xform-cardinality-one" (UUID/randomUUID)))
        test-db  (d/db-with (d/empty-db dir
                              {:statement/effect
                               {:db/valueType :db.type/ref, :db/cardinality :db.cardinality/one}

                               :statement2/effect
                               {:db/valueType :db.type/ref}})
                   [{:statement/effect {:effect :allow}}
                    {:statement2/effect {:effect :deny}}])
        tracker_ (atom 0)]

    (testing "xform is called for cardinality/one ref type"
      (is (= {:statement/effect :allow}
            (d/pull test-db [{[:statement/effect :xform (fn [x] (swap! tracker_ inc) (:effect x))]
                              [:effect]}]
              1)))
      (is (= 1 @tracker_)))

    (testing "xform is called for the leaf before the parent"
      (reset! tracker_ 0)
      (is (= {:statement/effect {:effect :leaf :new :prop}}
            (d/pull test-db
              [{[:statement/effect :xform (fn [x] (swap! tracker_ inc) (assoc x :new :prop))]
                [[:effect :xform (fn [v] (swap! tracker_ inc) :leaf)]]}]
              1)))
      (is (= 2 @tracker_)))

    (testing "xform is called for ref with no cardinality"
      (is (= {:statement2/effect :deny}
            (d/pull test-db [{[:statement2/effect :xform (fn [x] (:effect x))] [:effect]}] 3))))

    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-visitor
  (let [dir     (u/tmp-dir (str "pull-" (UUID/randomUUID)))
        test-db (d/init-db test-datoms dir test-schema)
        *trace  (volatile! nil)
        opts    {:visitor (fn [k e a v] (vswap! *trace conj [k e a v]))}
        test-fn (fn [pattern id]
                  (vreset! *trace [])
                  (d/pull test-db pattern id opts)
                  @*trace)]
    (is (= [[:db.pull/attr 1 :name nil]]
           (test-fn [:name] 1)))

    (testing "multival"
      (is (= [[:db.pull/attr 1 :name nil]
              [:db.pull/attr 1 :aka  nil]]

             (test-fn [:name :aka] 1))))

    (testing ":db/id is ignored"
      (is (= [] (test-fn [:db/id] 1)))
      (is (= [[:db.pull/attr 1 :name nil]]
             (test-fn [:db/id :name] 1))))

    (testing "wildcard"
      (is (= [[:db.pull/wildcard 1 nil    nil]
              [:db.pull/attr     1 :name  nil]
              [:db.pull/attr     1 :aka   nil]
              [:db.pull/attr     1 :child nil]]
             (test-fn ['*] 1))))

    (testing "missing"
      (is (= [[:db.pull/attr 1 :missing nil]]
             (test-fn [:missing] 1)))
      (is (= [[:db.pull/wildcard 1 nil      nil]
              [:db.pull/attr     1 :missing nil]
              [:db.pull/attr     1 :name    nil]
              [:db.pull/attr     1 :aka     nil]
              [:db.pull/attr     1 :child   nil]]
             (test-fn ['* :missing] 1))))

    (testing "default"
      (is (= [[:db.pull/attr 1 :missing nil]]
             (test-fn [[:missing :default 10]] 1)))
      (is (= [[:db.pull/attr 2 :child nil]]
             (test-fn [[:child :default 10]] 2))))

    (testing "recursion"
      (is (= [[:db.pull/attr 1 :child nil]]
             (test-fn [:child] 1)))
      (is (= [[:db.pull/attr 1 :child nil]
              [:db.pull/attr 2 :name  nil]
              [:db.pull/attr 3 :name  nil]]
             (test-fn [{:child [:name]}] 1)))
      (is (= [[:db.pull/attr 1 :name  nil]
              [:db.pull/attr 1 :child nil]
              [:db.pull/attr 2 :name  nil]
              [:db.pull/attr 2 :child nil]
              [:db.pull/attr 3 :name  nil]
              [:db.pull/attr 3 :child nil]]
             (test-fn [:name {:child '...}] 1))))

    (testing "reverse"
      (is (= [[:db.pull/attr    2   :name  nil]
              [:db.pull/reverse nil :child 2]]
             (test-fn [:name :_child] 2))))
    (d/close-db test-db)
    (u/delete-files dir)))
