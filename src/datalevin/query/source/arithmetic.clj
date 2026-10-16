(ns ^:no-doc datalevin.query.source.arithmetic
  "Reserve arbitrary-precision aggregate arithmetic before invoking native reducers."
  (:require [datalevin.query.source :as source]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value])
  (:import [clojure.lang BigInt Ratio]
           [java.math BigDecimal BigInteger MathContext]))

(defn- wide-number? [x]
  (or (instance? BigDecimal x) (instance? BigInteger x)
      (instance? BigInt x) (instance? Ratio x)))

(defn- workspace-bytes [control values]
  ;; Alignment may expand tiny decimal inputs by billions of digits. Include
  ;; the native sum's initial integer zero (scale 0), without constructing any
  ;; powers of ten or asking BigDecimal to compute its precision/string form.
  ;; Ratio denominators may multiply across a group; count every occurrence.
  (loop [values (seq values), minimum 0, maximum 0, width 128, ratios (identity 0)
         wide? false, decimal? false]
    (source/check-active! control)
    (if values
      (let [x (first values)
            wide (wide-number? x)
            ;; An individual JVM number's storage size fits in a long; the
            ;; accumulated ratio bound remains exact if many sizes overflow it.
            size (if wide (long (value/value-charge x)) 0)
            scale (if (instance? BigDecimal x) (long (.scale ^BigDecimal x)) 0)]
        (recur (next values) (min minimum scale) (max maximum scale)
               (max width size)
               (if (instance? Ratio x) (value/add-charge size ratios) ratios)
               (or wide? wide) (or decimal? (instance? BigDecimal x))))
      (when wide?
        ;; One byte per decimal digit exceeds the binary storage requirement.
        ;; The factor covers aligned operands, results, numeric conversions,
        ;; powers, and arithmetic scratch; fixed space covers carry/count bits.
        ;; A finite MathContext can request a much wider decimal quotient than
        ;; its operands, even when avg eventually converts that result to double.
        (let [precision (if (and decimal? *math-context*)
                          (long (.getPrecision ^MathContext *math-context*)) 0)]
          (value/add-charge 4096
            (value/multiply-charge 32
              (value/add-charge (+ width (- maximum minimum) precision) ratios))))))))

(defn- checked-values [control values]
  ;; Preflight realizes the reducer's input. Recheck on a fresh, unchunked walk
  ;; so cancellation still runs between individual arithmetic operations.
  (lazy-seq
    (when-let [items (seq values)]
      (source/check-active! control)
      (cons (first items) (checked-values control (rest items))))))

(defn wrap-aggregate
  "Keep native sum/avg semantics while reserving their arithmetic workspace.
  A wide result receives its own retained charge before scratch is released;
  the query owns it through grouping, projection, and final result handoff."
  [runtime element f]
  (if (#{'sum 'avg} (:symbol (:fn element)))
    (fn [values]
      (let [control (:control runtime)
            bytes (workspace-bytes control values)
            token (when bytes (source/reserve! control {:retained-bytes bytes}))]
        (try
          (let [result (f (checked-values control values))]
            (source/check-active! control)
            (when (wide-number? result) (rt/charge! runtime result))
            result)
          (finally (when token (source/release! token))))))
    f))
