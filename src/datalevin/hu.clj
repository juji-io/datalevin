(ns ^:no-doc datalevin.hu
  "Encoder and decoder for Hu-Tucker code. We consider 2^16 symbols,
  as we treat every 2 bytes as a symbol."
  (:import
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.eclipse.collections.impl.list.mutable.primitive IntArrayList]
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
  ([] (Node. 0 0 nil nil))
  ([i] (Node. i i nil nil))
  ([li ri lc rc] (Node. li ri lc rc)))

(defn- init-leaves
  [^ints levels]
  (let [nodes (FastList.)]
    (dotimes [i (alength levels)] (.add nodes (new-node i)))
    nodes))

(defn- merge-nodes
  [^FastList nodes idx1 idx2]
  (let [^Node ln (.get nodes idx1)
        ^Node rn (.get nodes idx2)]
    (.set nodes idx2 nil)
    (.set nodes idx1
          (new-node (.-left-idx ln) (.-right-idx rn) ln rn))))

(defn- build-tree
  [^ints levels ^FastList nodes]
  (let [n            (alength levels)
        max-level    (apply max (seq levels))
        tmp-levels   (IntArrayList. levels)
        node-indices (IntArrayList.)]
    (doseq [^long level (range max-level 0 -1)]
      (.clear node-indices)
      (dotimes [i n]
        (when (= (.get tmp-levels i) (int level) )
          (.add node-indices i)))
      (doseq [^long i (range 0 (.size node-indices) 2)
              :let    [idx1 (.get node-indices i)
                       idx2 (.get node-indices (inc i))]]
        (merge-nodes nodes idx1 idx2)
        (.set tmp-levels idx1 (int (dec level)))
        (.set tmp-levels idx2 (int 0))))))

(deftype Code [^byte len ^int code])

(defn- encode*
  [^FastList nodes idx]
  (loop [^Node node (.get nodes 0)
         len        0
         code       0]
    (if (leaf? node)
      (Code. (unchecked-byte len) (int code))
      (let [c (bit-shift-left code 1)]
        (if (< ^int (.-right-idx ^Node (.-left-child node)) ^int idx)
          (recur (.-right-child node) (inc len) (inc c))
          (recur (.-left-child node) (inc len) c))))))

(defn encode
  [^longs freqs]
  (let [n      (alength freqs)
        levels (OptimalCodeLength/generate freqs)
        nodes  (init-leaves levels)
        codes  (make-array Code n)]
    (build-tree levels nodes)
    (dotimes [i n]
      (aset codes i (encode* nodes i)))
    codes))
