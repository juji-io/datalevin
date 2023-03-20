(ns datalevin-bench.core
  #?(:cljs (:require-macros datalevin-bench.core)))

(def next-eid (volatile! 0))

(defn random-man []
  {:db/id     (str (vswap! next-eid inc))
   :name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
   :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
   :sex       (rand-nth [:male :female])
   :age       (rand-int 100)
   :salary    (rand-int 100000)})


(def people (repeatedly random-man))


(def people20k (shuffle (take 20000 people)))


(def ^:dynamic *warmup-t* 500)
(def ^:dynamic *bench-t*  1000)
(def ^:dynamic *step*     10)
(def ^:dynamic *repeats*  5)


#?(:cljs (defn ^number now [] (js/performance.now))
   :clj  (defn ^Long   now [] (/ (System/nanoTime) 1000000.0)))


(defn to-fixed [n places]
  #?(:cljs (.toFixed n places)
     :clj  (format (str "%." places "f") (double n))))


(defn ^:export round [n]
  (cond
    (> n 1)        (to-fixed n 1)
    (> n 0.1)      (to-fixed n 2)
    (> n 0.001)    (to-fixed n 2)
;;     (> n 0.000001) (to-fixed n 7)
    :else          n))


(defn percentile [xs n]
  (->
    (sort xs)
    (nth (min (dec (count xs))
              (int (* n (count xs)))))))


#?(:clj
  (defmacro dotime [duration & body]
   `(let [start-t# (now)
          end-t#   (+ ~duration start-t#)]
      (loop [iterations# *step*]
        (dotimes [_# *step*] ~@body)
        (let [now# (now)]
          (if (< now# end-t#)
            (recur (+ *step* iterations#))
            (double (/ (- now# start-t#) iterations#)))))))) ;; ms / iteration

#?(:clj
   (defmacro dotime-one-thread [duration & body]
     `(let [start-t# (now)
            end-t#   (+ ~duration start-t#)]
        (loop [iterations# *step*]
          (dotimes [_# *step*] ~@body)
          (let [now# (now)]
            (if (< now# end-t#)
              (recur (+ *step* iterations#))
              iterations#))))))

#?(:clj
  (defmacro bench [& body]
   `(let [_#       (dotime *warmup-t* ~@body)
          results# (into []
                     (for [_# (range *repeats*)]
                       (dotime *bench-t* ~@body)
                       #_(let [
                             f1# (future (dotime-one-thread *bench-t* ~@body))
                             f2# (future (dotime-one-thread *bench-t* ~@body))
                             f3# (future (dotime-one-thread *bench-t* ~@body))
                             f4# (future (dotime-one-thread *bench-t* ~@body))
                             f5# (future (dotime-one-thread *bench-t* ~@body))
                             f6# (future (dotime-one-thread *bench-t* ~@body))
                             f7# (future (dotime-one-thread *bench-t* ~@body))
                             f8# (future (dotime-one-thread *bench-t* ~@body))
                             f9# (future (dotime-one-thread *bench-t* ~@body))
                             f10# (future (dotime-one-thread *bench-t* ~@body))
                             f11# (future (dotime-one-thread *bench-t* ~@body))
                             f12# (future (dotime-one-thread *bench-t* ~@body))
                             f13# (future (dotime-one-thread *bench-t* ~@body))
                             f14# (future (dotime-one-thread *bench-t* ~@body))
                             f15# (future (dotime-one-thread *bench-t* ~@body))
                             f16# (future (dotime-one-thread *bench-t* ~@body))
                             f17# (future (dotime-one-thread *bench-t* ~@body))
                             f18# (future (dotime-one-thread *bench-t* ~@body))
                             f19# (future (dotime-one-thread *bench-t* ~@body))
                             f20# (future (dotime-one-thread *bench-t* ~@body))
                             f21# (future (dotime-one-thread *bench-t* ~@body))
                             f22# (future (dotime-one-thread *bench-t* ~@body))
                             f23# (future (dotime-one-thread *bench-t* ~@body))
                             f24# (future (dotime-one-thread *bench-t* ~@body))
                             ]
                         (+ @f1# @f2# @f3# @f4# @f5# @f6# @f7# @f8# @f9# @f10#
                            @f11# @f12# @f13# @f14# @f15# @f16# @f17# @f18# @f19#
                            @f20# @f21# @f22# @f23# @f24#))))
          ; min#     (reduce min results#)
          med#     (percentile results# 0.5)
          ; max#     (reduce max results#)
          ]
      med#)))

#?(:clj
   (defmacro bench-once [& body]
     `(let [start-t# (now)]
        ~@body
        (- (now) start-t#))))

#?(:clj
   (defmacro bench-10 [& body]
     `(let [_#       (dotime 5 ~@body)
            results# (into []
                           (for [_# (range *repeats*)]
                             (dotime 10 ~@body)))
                                        ; min#     (reduce min results#)
            med#     (percentile results# 0.5)
                                        ; max#     (reduce max results#)
            ]
        med#)))
