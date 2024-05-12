(ns math-bench.core)

(def ^:dynamic *warmup-t* 500)
(def ^:dynamic *bench-t*  1000)
(def ^:dynamic *step*     10)
(def ^:dynamic *repeats*  5)

(defn now ^Long [] (/ (System/nanoTime) 1000000.0))

(defn to-fixed [n places]
  (format (str "%." places "f") (double n)))

(defn round [^long n]
  (cond
    (> n 1)     (to-fixed n 1)
    (> n 0.1)   (to-fixed n 2)
    (> n 0.001) (to-fixed n 2)
    :else       n))

(defn percentile [xs ^long n]
  (->
    (sort xs)
    (nth (min (dec (count xs))
              (int (* n (count xs)))))))

(defmacro dotime [duration & body]
  `(let [start-t# (now)
         end-t#   (+ ~duration start-t#)]
     (loop [iterations# *step*]
       (dotimes [_# *step*] ~@body)
       (let [now# (now)]
         (if (< now# end-t#)
           (recur (+ *step* iterations#))
           (double (/ (- now# start-t#) iterations#)))))))

(defmacro bench [& body]
  `(let [_#       (dotime *warmup-t* ~@body)
         results# (into []
                        (for [_# (range *repeats*)]
                          (dotime *bench-t* ~@body)))
         med#     (percentile results# 0.5)]
     med#))

(defmacro bench-once [& body]
  `(let [start-t# (now)]
     ~@body
     (- (now) start-t#)))

;; rules

(def rule-author '[[(author ?d ?c)
                    [?d :dissertation/cid ?c]]])

(def rule-adv '[[(adv ?x ?y)
                 [?x :person/advised ?d]
                 (author ?d ?y)]])

(def rule-area '[[(area ?c ?a)
                  [?d :dissertation/cid ?c]
                  [?d :dissertation/area ?a]]])

(def rule-univ '[[(univ ?c ?u)
                  [?d :dissertation/cid ?c]
                  [?d :dissertation/univ ?u]]])

(def rule-anc '[[(anc ?x ?y)
                 (adv ?x ?y)]
                [(anc ?x ?y)
                 (adv ?x ?z)
                 (anc ?z ?y)]])

(def rule-q1 (into rule-author rule-adv))
(def rule-q2 (into rule-q1 rule-univ))
(def rule-q3 (into rule-q1 rule-area))
(def rule-q4 (into rule-q1 rule-anc))
