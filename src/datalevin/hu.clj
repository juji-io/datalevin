(ns ^:no-doc datalevin.hu
  "Encoder and decoder for Hu-Tucker code."
  (:require
   [datalevin.constants :as c]
   [datalevin.util :as u])
  (:import
   [java.util ArrayDeque Arrays LinkedList]
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

;; Create codes

(deftype Node [level sym left-child right-child]
  INode
  (leaf? [_] (nil? left-child)))

(defn- build-tree
  [^bytes levels]
  (let [n     (alength levels)
        cur   (volatile! 0)
        stack (LinkedList.)]
    (while (not (and (= n @cur) (= 1 (.size stack))))
      (if (and (<= 2 (.size stack)) (= (.-level ^Node (.get stack 0))
                                       (.-level ^Node (.get stack 1))))
        (let [top   ^Node (.pop stack)
              top-1 ^Node (.pop stack)
              level (dec ^byte (.-level top))]
          (.push stack (Node. level nil top-1 top)))
        (when (< ^long @cur n)
          (let [sym   @cur
                level (aget levels sym)]
            (.push stack (Node. level sym nil nil))
            (vswap! cur u/long-inc)))))
    (.pop stack)))

(defn create-codes
  [^bytes lens ^ints codes ^longs freqs]
  (let [levels (OptimalCodeLength/generate freqs)
        root   (build-tree levels)]
    (letfn [(traverse [^Node node ^long code]
              (if (leaf? node)
                (let [sym (.-sym node)]
                  (aset lens sym (byte (.-level node)))
                  (aset codes sym (int code)))
                (let [code1 (bit-shift-left code 1)]
                  (traverse (.-left-child node) code1)
                  (traverse (.-right-child node) (inc code1)))))]
      (traverse root 0))))

;; Create decoding tables

(deftype DecodeNode [^int prefix
                     ^byte len
                     sym
                     ^:unsynchronized-mutable left-child
                     ^:unsynchronized-mutable right-child]
  INode
  (leaf? [_] (some? sym))
  (left-child [_] left-child)
  (right-child [_] right-child)
  (set-left-child [_ n]
    (when left-child (println "SHOULD NOT HAPPEN: left child" left-child "exists"))
    (set! left-child n) n)
  (set-right-child [_ n]
    (when right-child (println "SHOULD NOT HAPPEN: right child" right-child "exists"))
    (set! right-child n) n))

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
  (println "min-lens =>" (apply min (seq lens)))
  (println "max-lens =>" (apply max (seq lens)))
  (let [root    (new-decode-node 0 0)
        ncounts (atom 1)
        tcounts (atom 0)]
    (dotimes [i (alength codes)]
      (let [len   (aget lens i)
            len-1 (dec len)
            code  (aget codes i)]
        ;; (println "len: " len  "code:" (Integer/toBinaryString code))
        (loop [j 0 mask (bit-shift-left 1 len-1) node root]
          (let [left? (zero? (bit-and code mask))
                j+1   (inc j)]
            (if (< j len-1)
              (recur j+1
                     (unsigned-bit-shift-right mask 1)
                     (if left?
                       (or (left-child node)
                           (set-left-child node
                                           (do (swap! ncounts inc)
                                               (new-decode-node
                                                 (child-prefix node true)
                                                 j+1))))
                       (or (right-child node)
                           (set-right-child node
                                            (do (swap! ncounts inc)
                                                (new-decode-node
                                                  (child-prefix node false)
                                                  j+1))))))
              (do #_(println " create " (if left? "left" "right")
                             "leaf node for " node)
                  (swap! tcounts inc)
                  (if left?
                    (set-left-child node (new-decode-node i))
                    (set-right-child node (new-decode-node i)))))))))
    (println "internal node count => " @ncounts)
    (println "terminal node count => " @tcounts)
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
        n         (+ ^long decoding-bits (.-len node))
        to-decode (bit-or (bit-shift-left (.-prefix node) ^long decoding-bits)
                          ^long i)]
    (loop [j 0 mask (bit-shift-left 1 (dec n)) ^DecodeNode cur tree]
      (if (< j n)
        (let [nn (if (zero? (bit-and to-decode mask))
                   (left-child cur) (right-child cur))]
          (if (leaf? nn)
            (do (vreset! decoded (.-sym ^DecodeNode nn))
                (recur (inc j) (unsigned-bit-shift-right mask 1) tree))
            (recur (inc j) (unsigned-bit-shift-right mask 1) nn)))
        (aset entries i
              (TableEntry. @decoded
                           (TableKey. (.-len cur) (.-prefix cur))))))))

(defn create-decode-tables
  [^bytes lens ^ints codes ^long decoding-bits]
  (let [tree   (build-decode-tree lens codes)
        n      (bit-shift-left 1 decoding-bits)
        tables (UnifiedMap.)]
    (letfn [(traverse [^DecodeNode node]
              (when-not (leaf? node)
                (if-let [ln (left-child node)]
                  (traverse ln)
                  (println "non-leaf node no left child =>" node))
                (if-let [rn (right-child node)]
                  (traverse rn)
                  (println "non-leaf node no right child =>" node))
                (let [entries (make-array TableEntry n)]
                  (dotimes [i n]
                    (create-entry tree node decoding-bits entries i))
                  (.put tables (TableKey. (.-len node) (.-prefix node))
                        entries))))]
      (traverse tree))
    tables))

(defprotocol IHuTucker
  (encode [this src-bf dst-bf]
    "Encode data, it is possible to produce some extra trailing zeros")
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
          t-1             (dec total)]
      (loop [i 0 bf (unchecked-byte 0) r (byte 8)]
        (if (< i total)
          (let [cur  (if (= i t-1)
                       (-> (.get src)
                           (bit-and 0x000000FF)
                           (bit-shift-left 8))
                       (bit-and (.getShort src) 0x0000FFFF))
                len  (aget lens cur)
                code (aget codes cur)
                [bf2 r2]
                (loop [len1 len code1 code bf1 bf r1 r]
                  (let [o (- len1 r1)]
                    (cond
                      (< 0 o)
                      (let [b (unchecked-byte
                                (bit-or
                                  bf1
                                  (unsigned-bit-shift-right code1 o)))]
                        (.put dst b)
                        (recur (byte o) (bit-and code1 (u/n-bits-mask o))
                               (unchecked-byte 0) (byte 8)))
                      (< o 0)
                      (let [rr (- o)]
                        [(bit-or bf1 (bit-shift-left code1 rr)) rr])
                      :else
                      (do (.put dst (unchecked-byte (bit-or bf1 code1)))
                          [0 8]))))]
            (recur (+ i 2) (byte bf2) (byte r2)))
          (when-not (zero? bf) (.put dst bf))))))

  (decode [_ src dst]
    (let [^ByteBuffer src src
          ^ByteBuffer dst dst
          total           (.remaining src)
          decode-mask     (byte (u/n-bits-mask decode-bits))
          ^long n         (/ 8 decode-bits)]
      (loop [i 0 k (TableKey. 0 0)]
        (if (< i total)
          (let [s  (bit-and (.get src) 0x000000FF)
                k2 (loop [j (dec n) k1 k]
                     (if (<= 0 j)
                       (let [c  (bit-and
                                  decode-mask
                                  (unsigned-bit-shift-right
                                    s (* decode-bits j)))
                             es ^"[Ldatalevin.hu.TableEntry;" (.get tables k1)
                             e  ^TableEntry (aget es c)
                             w  (.-decoded e)]
                         (when w (.putShort dst w))
                         (recur (dec j) (.-link e)))
                       k1))]
            (recur (inc i) k2))
          (println "left over k =>" k))))))

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
