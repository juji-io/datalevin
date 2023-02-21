(ns ^:no-doc datalevin.hu
  "Encoder and decoder for Hu-Tucker code."
  (:require
   [datalevin.bits :as b])
  (:import
   [java.nio ByteBuffer]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.eclipse.collections.impl.list.mutable.primitive ByteArrayList
    IntArrayList]
   [datalevin.utl OptimalCodeLength]))

(defprotocol INode
  (leaf? [_]))

(deftype Node [^int left-idx
               ^int right-idx
               left-child
               right-child]
  INode
  (leaf? [_] (nil? left-child)))

(defn- new-node
  ([i] (Node. i i nil nil))
  ([li ri lc rc] (Node. li ri lc rc)))

(defn- init-leaves
  [^bytes levels]
  (let [nodes (FastList.)]
    (dotimes [i (alength levels)] (.add nodes (new-node i)))
    nodes))

(defn- merge-nodes
  [^FastList nodes idx1 idx2]
  (let [^Node ln (.get nodes idx1)
        ^Node rn (.get nodes idx2)]
    (.set nodes idx2 nil)
    (.set nodes idx1 (new-node (.-left-idx ln) (.-right-idx rn) ln rn))))

(defn- build-tree
  [^bytes levels ^FastList nodes]
  (let [n            (alength levels)
        max-level    (apply max (seq levels))
        tmp-levels   (ByteArrayList. levels)
        node-indices (IntArrayList.)]
    (doseq [^long level (range max-level 0 -1)]
      (.clear node-indices)
      (dotimes [i n]
        (when (= (.get tmp-levels i) (byte level) )
          (.add node-indices i)))
      (doseq [^long i (range 0 (.size node-indices) 2)
              :let    [idx1 (.get node-indices i)
                       idx2 (.get node-indices (inc i))]]
        (merge-nodes nodes idx1 idx2)
        (.set tmp-levels idx1 (int (dec level)))
        (.set tmp-levels idx2 (int 0))))))

(defn- build*
  [^bytes lens ^ints codes ^FastList nodes idx]
  (loop [^Node node (.get nodes 0) len 0 code 0]
    (if (leaf? node)
      (do (aset lens idx (byte len)) (aset codes idx code))
      (let [c (bit-shift-left code 1)
            l (inc len)]
        (if (< ^int (.-right-idx ^Node (.-left-child node)) ^int idx)
          (recur (.-right-child node) l (inc c))
          (recur (.-left-child node) l c))))))

(defn build
  [^bytes lens ^ints codes ^longs freqs]
  (let [n      (alength freqs)
        levels (OptimalCodeLength/generate freqs)
        nodes  (init-leaves levels)]
    (build-tree levels nodes)
    (dotimes [i n] (build* lens codes nodes i))))

(defprotocol IHuTucker
  (encode [this src-bf dst-bf])
  (decode [this src-bf dst-bf]))

(deftype HuTucker [^bytes lens
                   ^ints codes]
  IHuTucker
  (encode [_ src dst]
    (let [^ByteBuffer src src
          ^ByteBuffer dst dst
          total           (.remaining src)]
      (if (< total 2)
        (b/buffer-transfer src dst total)
        (loop [i 0 bf (int 0) r 32]
          (if (< i total)
            (let [cur  (bit-and (.getShort src) 0x0000FFFF)
                  len  (aget lens cur)
                  code (aget codes cur)]
              (if (<= len r)
                (let [r'  (- r len)
                      bf' (bit-or bf (int (bit-shift-left code r')))]
                  (if (zero? r')
                    (do (.putInt dst bf')
                        (recur (+ i 2) (int 0) 32))
                    (recur (+ i 2) bf' r')))
                (let [o   (- len r)
                      ob  (bit-and code (dec (bit-shift-left 1 r)))
                      bf' (bit-or bf (int (unsigned-bit-shift-right code o)))
                      r'  (- 32 o)]
                  (.putInt dst bf')
                  (recur (+ i 2) (bit-shift-left ob r') r'))))
            (if (= i (inc total))
              (let [b (.get src)]
                (cond
                  (< 8 r) (.putInt dst (bit-or bf (bit-shift-left b (- r 8))))
                  (= 8 r) (.putInt dst (bit-or bf b))
                  :else   (let [o   (- 8 r)
                                ob  (bit-and b (dec (bit-shift-left 1 r)))
                                bf' (bit-or bf (unsigned-bit-shift-right b o))
                                r'  (- 32 o)]
                            (.putInt dst bf')
                            (.putInt dst (bit-shift-left ob r')))))
              (when-not (zero? bf) (.putInt dst bf)))))))))

(defn new-hu-tucker
  [^longs freqs]
  (let [n     (alength freqs)
        lens  (byte-array n)
        codes (int-array n)]
    (build lens codes freqs)
    (HuTucker. lens codes)))
