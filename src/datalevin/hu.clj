(ns ^:no-doc datalevin.hu
  "Encoder and decoder for Hu-Tucker code."
  (:require
   [datalevin.constants :as c]
   [datalevin.util :as u])
  (:import
   [java.io Writer]
   [java.nio ByteBuffer]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.eclipse.collections.impl.list.mutable.primitive ByteArrayList
    IntArrayList]
   [org.eclipse.collections.impl.map.mutable UnifiedMap]
   [datalevin.utl OptimalCodeLength]))

(defprotocol INode
  (leaf? [_])
  (left-child [_])
  (right-child [_])
  (set-left-child [_ node])
  (set-right-child [_ node]))

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

(defn- create-code
  [^bytes lens ^ints codes ^FastList nodes idx]
  (loop [^Node node (.get nodes 0) len 0 code 0]
    (if (leaf? node)
      (do (aset lens idx (byte len)) (aset codes idx code))
      (let [c (bit-shift-left code 1)
            l (inc len)]
        (if (< ^int (.-right-idx ^Node (.-left-child node)) ^int idx)
          (recur (.-right-child node) l (inc c))
          (recur (.-left-child node) l c))))))

(defn create-codes
  [^bytes lens ^ints codes ^longs freqs]
  (let [levels (OptimalCodeLength/generate freqs)
        nodes  (init-leaves levels)]
    (build-tree levels nodes)
    (dotimes [i (alength freqs)] (create-code lens codes nodes i))))

(deftype DecodeNode [^int prefix
                     ^byte len
                     sym
                     ^:unsynchronized-mutable left-child
                     ^:unsynchronized-mutable right-child]
  INode
  (leaf? [_] (some? sym))
  (left-child [_] left-child)
  (right-child [_] right-child)
  (set-left-child [_ n] (set! left-child n) n)
  (set-right-child [_ n] (set! right-child n) n))

(defmethod print-method DecodeNode
  [^DecodeNode n ^Writer w]
  (.write w "[")
  (.write w (str (.-prefix n)))
  (.write w " ")
  (.write w (str (.-len n)))
  (.write w " ")
  (.write w (str (.-sym n)))
  (.write w " ")
  (.write w (str (if-let [^DecodeNode ln (left-child n)] (.-sym ln) nil)))
  (.write w " ")
  (.write w (str (if-let [^DecodeNode rn (right-child n)] (.-sym rn) nil)))
  (.write w "]"))

(defn- new-decode-node
  ([prefix len] (DecodeNode. prefix len nil nil nil))
  ([sym] (DecodeNode. 0 0 (short sym) nil nil)))

(defn- child-prefix
  [^DecodeNode n left?]
  (let [t (bit-shift-left ^int (.-prefix n) 1)]
    (if left? t (bit-or t 1))))

(defn- build-decode-tree
  [^bytes lens ^ints codes]
  (let [root (new-decode-node 0 0)]
    (dotimes [i (alength codes)]
      (let [len   (aget lens i)
            len-1 (dec len)
            code  (aget codes i)]
        (loop [j    0
               mask (bit-shift-left (int 1) len-1)
               node root]
          (let [left? (zero? (bit-and code mask))]
            (if (< j len-1)
              (let [j+1 (inc j)]
                (recur j+1
                       (unsigned-bit-shift-right mask 1)
                       (if left?
                         (or (left-child node)
                             (set-left-child
                               node (new-decode-node
                                      (child-prefix node true)
                                      j+1)))
                         (or (right-child node)
                             (set-right-child
                               node (new-decode-node
                                      (child-prefix node false)
                                      j+1))))))
              (if left?
                (set-left-child node (new-decode-node i))
                (set-right-child node (new-decode-node i))))))))
    root))

(deftype TableKey [^byte len ^int prefix]
  Object
  (hashCode [_] (u/szudzik len prefix))
  (equals [_ that]
    (and (= len (.-len ^TableKey that))
         (= prefix (.-prefix ^TableKey that)))))

(defmethod print-method TableKey
  [^TableKey k ^Writer w]
  (.write w "[")
  (.write w (str (.-prefix k)))
  (.write w " ")
  (.write w (str (.-len k)))
  (.write w "]"))

(deftype TableEntry [decoded ^TableKey link]
  Object
  (equals [_ that]
    (and (= link (.-link ^TableEntry that))
         (= decoded (.-decoded ^TableEntry that)))))

(defmethod print-method TableEntry
  [^TableEntry e ^Writer w]
  (.write w "[")
  (.write w (str (.-decoded e)))
  (.write w " ")
  (.write w (pr-str (.-link e)))
  (.write w "]"))

(defn- create-entry
  [tree ^DecodeNode node decoding-bits
   ^"[Ldatalevin.hu.TableEntry;" entries i]
  (let [decoded   (volatile! nil)
        prefix    (.-prefix node)
        len       (.-len node)
        n         (+ ^long decoding-bits len)
        to-decode (bit-or (bit-shift-left prefix ^long decoding-bits)
                          ^long i)]
    (loop [j 0 mask (bit-shift-left 1 (dec n)) ^DecodeNode cur tree]
      (if (< j n)
        (let [nn (if (zero? (bit-and to-decode mask))
                   (left-child cur) (right-child cur))]
          ;; (println "nn =>" nn)
          (if (leaf? nn)
            (do (vreset! decoded (.-sym ^DecodeNode nn))
                (recur (inc j) (unsigned-bit-shift-right mask 1) tree))
            (recur (inc j) (unsigned-bit-shift-right mask 1) nn)))
        (let [entry (TableEntry. @decoded
                                 (TableKey. (.-len cur)
                                            (.-prefix cur)))]
          (aset entries i entry))))))

(defn create-decode-tables
  [^bytes lens ^ints codes ^long decoding-bits]
  (let [tree  (build-decode-tree lens codes)
        n     (bit-shift-left 1 decoding-bits)
        table (UnifiedMap.)]
    (letfn [(traverse [^DecodeNode node]
              (when-not (leaf? node)
                (let [entries (make-array TableEntry n)]
                  (dotimes [i n]
                    (create-entry tree node decoding-bits entries i))
                  (.put table (TableKey. (.-len node) (.-prefix node))
                        entries))
                (when-let [ln (left-child node)] (traverse ln))
                (when-let [rn (right-child node)] (traverse rn))))]
      (traverse tree))
    table))

(defprotocol IHuTucker
  (encode [this src-bf dst-bf])
  (decode [this src-bf dst-bf]))

(deftype HuTucker [^bytes lens        ;; array of code lengths
                   ^ints codes        ;; array of codes
                   ^long decode-bits  ;; number of bits decoded at a time
                   ^UnifiedMap tables ;; decoding tables
                   ]
  IHuTucker
  (encode [_ src dst]
    (let [^ByteBuffer src src
          ^ByteBuffer dst dst
          total           (.remaining src)
          pad?            (odd? total)
          ^long total     (if pad? (inc total) total)
          t-2             (- total 2)]
      (loop [i 0 bf 0 r 64]
        (if (< i total)
          (let [cur  (if (and (= i t-2) pad?)
                       (-> (.get src)
                           (bit-and 0x000000FF)
                           (bit-shift-left 8))
                       (bit-and (.getShort src) 0x0000FFFF))
                len  (aget lens cur)
                code (aget codes cur)]
            (if (<= len r)
              (let [r1  (- r len)
                    bf1 (bit-or bf (bit-shift-left code r1))]
                (if (zero? r1)
                  (do (.putLong dst bf1)
                      (recur (+ i 2) 0 64))
                  (recur (+ i 2) bf1 r1)))
              (let [o   (- len r)
                    ob  (bit-and code (dec (bit-shift-left 1 r)))
                    bf1 (bit-or bf (unsigned-bit-shift-right code o))
                    r1  (- 64 o)]
                (.putLong dst bf1)
                (recur (+ i 2) (bit-shift-left ob r1) r1))))
          (when-not (zero? bf) (.putLong dst bf))))))

  (decode [_ src dst]
    (let [^ByteBuffer src src
          ^ByteBuffer dst dst
          total           (.remaining src)
          decode-mask     (byte (u/n-bits-mask decode-bits))
          ^long n         (/ 8 decode-bits)]
      (dotimes [_ total]
        (let [s (bit-and (.get src) 0x000000FF)]
          (loop [i (dec n) k (TableKey. 0 0)]
            (when (<= 0 i)
              (let [c  (bit-and decode-mask
                                (unsigned-bit-shift-right s (* 2 i)))
                    es ^"[Ldatalevin.hu.TableEntry;" (.get tables k)
                    e  ^TableEntry (aget es c)
                    w  (.-decoded e)]
                (when w (.putShort dst w))
                (recur (dec i) (.-link e))))))))))

(defn new-hu-tucker
  ([freqs]
   (new-hu-tucker freqs c/decoding-bits))
  ([^longs freqs ^long decode-bits]
   (let [n     (alength freqs)
         lens  (byte-array n)
         codes (int-array n)]
     (create-codes lens codes freqs)
     (HuTucker. lens codes decode-bits
                (create-decode-tables lens codes decode-bits)))))
