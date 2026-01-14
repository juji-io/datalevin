(ns datalevin.test.parser-find
  (:require
   [clojure.test :as t :refer        [is are deftest testing]]
   [datalevin.parser :as dp]))

(deftest test-parse-find
  (is (= (dp/parse-find '[?a ?b])
         (dp/->FindRel [(dp/->Variable '?a) (dp/->Variable '?b)])))
  (is (= (dp/parse-find '[[?a ...]])
         (dp/->FindColl (dp/->Variable '?a))))
  (is (= (dp/parse-find '[?a .])
         (dp/->FindScalar (dp/->Variable '?a))))
  (is (= (dp/parse-find '[[?a ?b]])
         (dp/->FindTuple [(dp/->Variable '?a) (dp/->Variable '?b)]))))

(deftest test-parse-aggregate
  (is (= (dp/parse-find '[?a (count ?b)])
         (dp/->FindRel [(dp/->Variable '?a) (dp/->Aggregate (dp/->PlainSymbol 'count) [(dp/->Variable '?b)])])))
  (is (= (dp/parse-find '[[(count ?a) ...]])
         (dp/->FindColl (dp/->Aggregate (dp/->PlainSymbol 'count) [(dp/->Variable '?a)]))))
  (is (= (dp/parse-find '[(count ?a) .])
         (dp/->FindScalar (dp/->Aggregate (dp/->PlainSymbol 'count) [(dp/->Variable '?a)]))))
  (is (= (dp/parse-find '[[(count ?a) ?b]])
         (dp/->FindTuple [(dp/->Aggregate (dp/->PlainSymbol 'count) [(dp/->Variable '?a)]) (dp/->Variable '?b)]))))

(deftest test-parse-custom-aggregates
  (is (= (dp/parse-find '[(aggregate ?f ?a)])
         (dp/->FindRel [(dp/->Aggregate (dp/->Variable '?f) [(dp/->Variable '?a)])])))
  (is (= (dp/parse-find '[?a (aggregate ?f ?b)])
         (dp/->FindRel [(dp/->Variable '?a) (dp/->Aggregate (dp/->Variable '?f) [(dp/->Variable '?b)])])))
  (is (= (dp/parse-find '[[(aggregate ?f ?a) ...]])
         (dp/->FindColl (dp/->Aggregate (dp/->Variable '?f) [(dp/->Variable '?a)]))))
  (is (= (dp/parse-find '[(aggregate ?f ?a) .])
         (dp/->FindScalar (dp/->Aggregate (dp/->Variable '?f) [(dp/->Variable '?a)]))))
  (is (= (dp/parse-find '[[(aggregate ?f ?a) ?b]])
         (dp/->FindTuple [(dp/->Aggregate (dp/->Variable '?f) [(dp/->Variable '?a)]) (dp/->Variable '?b)]))))

(deftest test-parse-find-elements
  (is (= (dp/parse-find '[(count ?b 1 $x) .])
         (dp/->FindScalar (dp/->Aggregate (dp/->PlainSymbol 'count)
                                          [(dp/->Variable '?b)
                                           (dp/->Constant 1)
                                           (dp/->SrcVar '$x)])))))

(deftest test-parse-find-expr
  (testing "Basic arithmetic over aggregates"
    (is (= (dp/parse-find-elem '(+ (sum ?x) (sum ?y)))
           (dp/->FindExpr (dp/->PlainSymbol '+)
                          [(dp/->Aggregate (dp/->PlainSymbol 'sum) [(dp/->Variable '?x)])
                           (dp/->Aggregate (dp/->PlainSymbol 'sum) [(dp/->Variable '?y)])]))))

  (testing "With constant"
    (is (= (dp/parse-find-elem '(* (sum ?x) 10))
           (dp/->FindExpr (dp/->PlainSymbol '*)
                          [(dp/->Aggregate (dp/->PlainSymbol 'sum) [(dp/->Variable '?x)])
                           (dp/->Constant 10)]))))

  (testing "Nested expression"
    (is (= (dp/parse-find-elem '(* 2 (+ (sum ?x) (sum ?y))))
           (dp/->FindExpr (dp/->PlainSymbol '*)
                          [(dp/->Constant 2)
                           (dp/->FindExpr (dp/->PlainSymbol '+)
                                          [(dp/->Aggregate (dp/->PlainSymbol 'sum) [(dp/->Variable '?x)])
                                           (dp/->Aggregate (dp/->PlainSymbol 'sum) [(dp/->Variable '?y)])])]))))

  (testing "All supported operators"
    (is (dp/find-expr? (dp/parse-find-elem '(+ (sum ?x) (sum ?y)))))
    (is (dp/find-expr? (dp/parse-find-elem '(- (sum ?x) (sum ?y)))))
    (is (dp/find-expr? (dp/parse-find-elem '(* (sum ?x) (sum ?y)))))
    (is (dp/find-expr? (dp/parse-find-elem '(/ (sum ?x) (sum ?y)))))
    (is (dp/find-expr? (dp/parse-find-elem '(mod (sum ?x) 7))))
    (is (dp/find-expr? (dp/parse-find-elem '(rem (sum ?x) 7))))
    (is (dp/find-expr? (dp/parse-find-elem '(quot (sum ?x) 7)))))

  (testing "In full find clause"
    (is (= (dp/parse-find '[?name (sum ?x) (sum ?y) (+ (sum ?x) (sum ?y))])
           (dp/->FindRel [(dp/->Variable '?name)
                          (dp/->Aggregate (dp/->PlainSymbol 'sum) [(dp/->Variable '?x)])
                          (dp/->Aggregate (dp/->PlainSymbol 'sum) [(dp/->Variable '?y)])
                          (dp/->FindExpr (dp/->PlainSymbol '+)
                                         [(dp/->Aggregate (dp/->PlainSymbol 'sum) [(dp/->Variable '?x)])
                                          (dp/->Aggregate (dp/->PlainSymbol 'sum) [(dp/->Variable '?y)])])]))))

  (testing "find-vars extracts vars from FindExpr"
    (let [find (dp/parse-find '[?name (* 2 (+ (sum ?x) (sum ?y)))])]
      (is (= (dp/find-vars find)
             '[?name ?x ?y])))))

#_(t/test-ns 'datalevin.test.find-parser)
