;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.hu
  "Fast encoder and decoder for Hu-Tucker codes. Used for key compression."
  (:require
   [datalevin.util :as u])
  (:import
   [java.util LinkedList]
   [java.nio ByteBuffer]
   [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]
   [org.eclipse.collections.impl.map.mutable.primitive ObjectLongHashMap]
   [datalevin.utl LeftistHeap BitOps]))

(defprotocol INode
  (leaf? [_])
  (left-child [_])
  (right-child [_])
  (set-left-child [_ node])
  (set-right-child [_ node]))

;; Find optimal code lengths

(deftype SeqNode [^long sum          ;; sum of min freq and 2nd min freq
                  ^int i             ;; idx of min freq TreeNode
                  ^int j             ;; idx of 2nd min freq TreeNode
                  ^int l             ;; idx of the left terminal TreeNode
                  ^int r             ;; idx of the right terminal TreeNode
                  ^LeftistHeap heap] ;; heap of TreeNodes in this seq
  Object
  (hashCode [_] l)
  (equals [_ other] (= l (.-l ^SeqNode other))))

(defn- master-pq
  []
  (proxy [LeftistHeap] []
    (lessThan [^SeqNode a ^SeqNode b]
      (< (long (u/combine-cmp
                 (compare ^long (.-sum a) ^long (.-sum b))
                 (compare ^int (.-l a) ^int (.-l b)))) ;; tie breaker
         0))))

(defprotocol ITreeNode
  (left-seq [_])
  (right-seq [_])
  (set-left-seq [_ s])
  (set-right-seq [_ s]))

(deftype TreeNode [^int idx
                   ^long freq
                   ^:unsynchronized-mutable ^SeqNode left-seq
                   ^:unsynchronized-mutable ^SeqNode right-seq
                   left-child
                   right-child]
  INode
  (leaf? [_] (nil? left-child))

  ITreeNode
  (left-seq [_] left-seq)
  (right-seq [_] right-seq)
  (set-left-seq [_ s] (set! left-seq s))
  (set-right-seq [_ s] (set! right-seq s))

  Object
  (hashCode [_] idx)
  (equals [_ other] (= idx (.-idx ^TreeNode other))))

(defn- huffman-pq
  []
  (proxy [LeftistHeap] []
    (lessThan [^TreeNode a ^TreeNode b]
      (< (long (u/combine-cmp
                 (compare ^long (.-freq a) ^long (.-freq b))
                 (compare ^int (.-idx a) ^int (.-idx b))))
         0))))

(defn- init-queues
  [n ^longs freqs ^"[Ldatalevin.hu.TreeNode;" work
   ^"[Ldatalevin.hu.TreeNode;" terminals ^LeftistHeap mpq]
  (dotimes [k n]
    (let [node (TreeNode. k (aget freqs k) nil nil nil nil)]
      (aset terminals k node)
      (aset work k node)))
  (dotimes [k (dec ^long n)]
    (let [k+1  (inc k)
          t    ^TreeNode (aget work k)
          t+1  ^TreeNode (aget work k+1)
          wk   (.-freq t)
          wk+1 (.-freq t+1)
          i    (if (<= wk wk+1) k k+1)
          j    (if (= i k) k+1 k)
          hpq  ^LeftistHeap (huffman-pq)
          sn   (SeqNode. (+ wk wk+1) i j k k+1 hpq)]
      (set-right-seq t sn)
      (set-left-seq t+1 sn)
      (.insert hpq t)
      (.insert hpq t+1)
      (.insert mpq sn))))

(defn- build-level-tree
  [^long n ^longs freqs]
  (let [^"[Ldatalevin.hu.TreeNode;" work      (make-array TreeNode n)
        ^"[Ldatalevin.hu.TreeNode;" terminals (make-array TreeNode n)
        ^LeftistHeap mpq                      (master-pq)]
    (init-queues n freqs work terminals mpq)
    (dotimes [_ (dec n)]
      (let [m  ^SeqNode (.findMin mpq)
            i  (.-i m)
            j  (.-j m)
            l  (if (<= i j) i j)
            r  (if (= l i) j i)
            nl (aget work l)
            nr (aget work r)
            nn (TreeNode. l (.-sum m) nil nil nl nr)]
        (aset work l nn)
        (aset work r nil)
        (cond
          ;; combine 2 terminal nodes, need to merge 3 or 2 seqs
          (and (leaf? nl) (leaf? nr))
          (let [tl     (aget terminals l)
                tr     (aget terminals r)
                ll-seq ^SeqNode (left-seq tl)
                l      (if ll-seq (.-l ll-seq) -1)
                ll-hpq (when ll-seq
                         (doto ^LeftistHeap (.-heap ll-seq)
                           (.deleteElement tl)))
                lr-hpq (doto ^LeftistHeap (.-heap ^SeqNode (right-seq tl))
                         (.deleteElement tl))
                _      (doto ^LeftistHeap (.-heap ^SeqNode (left-seq tr))
                         (.deleteElement tr))
                rr-seq ^SeqNode (right-seq tr)
                r      (if rr-seq (.r rr-seq) n)
                rr-hpq (when rr-seq
                         (doto ^LeftistHeap (.-heap rr-seq)
                           (.deleteElement tr)))
                n-hpq  ^LeftistHeap (doto lr-hpq
                                      (.merge ll-hpq) (.merge rr-hpq)
                                      (.insert nn))
                minn   ^TreeNode (.findMin n-hpq)
                minn1  ^TreeNode (.findNextMin n-hpq)
                sn     (SeqNode. (+ (.-freq minn) (.-freq minn1))
                                 (.-idx minn) (.-idx minn1) l r n-hpq)]
            (when-not (= l -1) (set-right-seq (aget terminals l) sn))
            (when-not (= r n) (set-left-seq (aget terminals r) sn))
            (doto mpq
              (.deleteElement ll-seq) (.deleteElement rr-seq)
              (.deleteMin) (.insert sn)))
          ;; combine 2 internal nodes, no seq merge needed
          (and (not (leaf? nl)) (not (leaf? nr)))
          (let [hpq   ^LeftistHeap (.-heap m)
                n-hpq (doto hpq
                        (.deleteMin) (.deleteMin) (.insert nn))
                minn  ^TreeNode (.findMin n-hpq)
                minn1 ^TreeNode (.findNextMin n-hpq)
                l     (.-l m)
                r     (.-r m)
                sn    (when minn1
                        (SeqNode. (+ (.-freq minn) (.-freq minn1))
                                  (.-idx minn) (.-idx minn1) l r n-hpq))]
            (when-not (= l -1) (set-right-seq (aget terminals l) sn))
            (when-not (= r n) (set-left-seq (aget terminals r) sn))
            (doto mpq (.deleteMin) (.insert sn)))
          ;; combine a terminal and an internal node, need to merge two seqs
          :else
          (let [hpq   ^LeftistHeap (.-heap m)
                t     ^TreeNode (if (leaf? nl) nl nr)
                l-seq ^SeqNode (left-seq t)
                l     (if l-seq (.l l-seq) -1)
                l-hpq (when l-seq ^LeftistHeap (.-heap l-seq))
                r-seq ^SeqNode (right-seq t)
                r     (if r-seq (.r r-seq) n)
                r-hpq (when r-seq ^LeftistHeap (.-heap r-seq))
                o-hpq ^SeqNode (if (= hpq l-hpq) r-hpq l-hpq)
                o-seq (if (= o-hpq r-hpq) r-seq l-seq)
                _     (when o-hpq (.deleteElement o-hpq t))
                n-hpq (doto hpq
                        (.deleteMin) (.deleteMin)
                        (.merge o-hpq) (.insert nn))
                minn  ^TreeNode (.findMin n-hpq)
                minn1 ^TreeNode (.findNextMin n-hpq)
                sn    (SeqNode. (+ (.-freq minn) (.-freq minn1))
                                (.-idx minn) (.-idx minn1) l r n-hpq)]
            (when-not (= l -1) (set-right-seq (aget terminals l) sn))
            (when-not (= r n) (set-left-seq (aget terminals r) sn))
            (doto mpq
              (.deleteMin) (.deleteElement o-seq) (.insert sn))))))
    (aget work 0)))

(defn create-levels
  [^long n ^longs freqs]
  (let [tree   (build-level-tree n freqs)
        levels (byte-array n)]
    (letfn [(traverse [^TreeNode node ^long level]
              (if (leaf? node)
                (aset levels (.-idx node) (byte level))
                (let [l+1 (inc level)]
                  (traverse (.-left-child node) l+1)
                  (traverse (.-right-child node) l+1))))]
      (traverse tree 0)
      levels)))

;; Create codes

(deftype Node [level sym left-child right-child]
  INode
  (leaf? [_] (nil? left-child)))

(defn- build-code-tree
  [^long n ^bytes levels]
  (let [cur   (volatile! 0)
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
  [^long n ^bytes lens ^ints codes ^longs freqs]
  (let [levels (create-levels n freqs)
        root   (build-code-tree n levels)]
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
  (set-left-child [_ n] (set! left-child n) n)
  (set-right-child [_ n] (set! right-child n) n))

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
        (loop [j 0 mask (bit-shift-left 1 len-1) node root]
          (let [left? (zero? (bit-and code mask))
                j+1   (inc j)]
            (if (< j len-1)
              (recur j+1
                     (unsigned-bit-shift-right mask 1)
                     (if left?
                       (or (left-child node)
                           (set-left-child node
                                           (new-decode-node
                                             (child-prefix node true)
                                             j+1)))
                       (or (right-child node)
                           (set-right-child node
                                            (new-decode-node
                                              (child-prefix node false)
                                              j+1)))))
              (if left?
                (set-left-child node (new-decode-node i))
                (set-right-child node (new-decode-node i))))))))
    root))

(defn get-prefix [^long value] (bit-and value 0x00000000FFFFFFFF))

(defn get-len
  [^long value]
  (-> value (unsigned-bit-shift-right 32) (bit-and 0x00000000000000FF)))

(defn get-link [^long value] (bit-and value 0x000000FFFFFFFFFF))

(defn get-decoded
  [^long value]
  (when-not (neg? value) (unsigned-bit-shift-right value 40)))

(defn- set-prefix [^long value prefix] (bit-or value ^int prefix))

(defn- set-len [^long value len] (bit-or value (bit-shift-left ^byte len 32)))

(defn- set-decoded
  [^long value decoded]
  (bit-or value (if decoded
                  (bit-shift-left (BitOps/intAnd ^short decoded 0x0000FFFF) 40)
                  -9223372036854775808)))

(defn- create-entry
  [tree ^ObjectLongHashMap ks ^DecodeNode node entries i]
  (let [decoded   (volatile! nil)
        n         (+ 4 (.-len node))
        to-decode (bit-or (bit-shift-left (.-prefix node) 4) ^long i)]
    (loop [j 0 mask (bit-shift-left 1 (dec n)) ^DecodeNode cur tree]
      (if (< j n)
        (let [nn (if (zero? (bit-and to-decode mask))
                   (left-child cur) (right-child cur))]
          (if (leaf? nn)
            (do (vreset! decoded (.-sym ^DecodeNode nn))
                (recur (inc j) (unsigned-bit-shift-right mask 1) tree))
            (recur (inc j) (unsigned-bit-shift-right mask 1) nn)))
        (aset ^longs entries i ^long (set-decoded (.get ks cur) @decoded))))))

(defn create-decode-tables
  [^bytes lens ^ints codes]
  (let [tree   (build-decode-tree lens codes)
        ks     (ObjectLongHashMap.)
        tables (LongObjectHashMap.)]
    (letfn [(create-key [^DecodeNode node]
              (.put ks node (-> 0
                                (set-prefix (.-prefix node))
                                (set-len (.-len node)))))
            (create-table [node]
              (let [entries (long-array 16)]
                (dotimes [i 16] (create-entry tree ks node entries i))
                (.put tables (.get ks node) entries)))
            (traverse [^DecodeNode node f]
              (when-not (leaf? node)
                (f node)
                (traverse (left-child node) f)
                (traverse (right-child node) f)))]
      (traverse tree create-key)
      (traverse tree create-table))
    tables))

(defprotocol IEncodeBuf
  (set-br [this b r])
  (get-b [this])
  (get-r [this]))

(deftype EncodeBuf [^:unsynchronized-mutable ^byte b   ;; bits buffer
                    ^:unsynchronized-mutable ^byte r]  ;; remaining bits
  IEncodeBuf
  (set-br [_ bf remain]
    (set! b (unchecked-byte bf))
    (set! r (unchecked-byte remain)))
  (get-b [_] b)
  (get-r [_] r))

(defprotocol IHuTucker
  (encode [this src-bf dst-bf])
  (decode [this src-bf dst-bf]))

(deftype HuTucker [^bytes lens         ;; array of code lengths
                   ^ints codes         ;; array of codes
                   ^LongObjectHashMap tables] ;; decoding tables
  IHuTucker
  (encode [_ src dst]
    (let [total (.remaining ^ByteBuffer src)
          t-1   (dec total)
          bf    (EncodeBuf. (unchecked-byte 0) (unchecked-byte 8))]
      (loop [i 0]
        (if (< i total)
          (let [cur  (if (= i t-1)
                       (-> (.get ^ByteBuffer src)
                           (BitOps/intAnd 0x000000FF)
                           (bit-shift-left 8))
                       (BitOps/intAnd (.getShort ^ByteBuffer src)
                                      0x0000FFFF))
                len  ^byte (aget lens cur)
                code ^int (aget codes cur)]
            (loop [len1 len code1 code]
              (let [o  (- len1 ^byte (get-r bf))
                    b1 (get-b bf)]
                (cond
                  (< 0 o)
                  (do
                    (.put ^ByteBuffer dst
                          (unchecked-byte
                            (bit-or
                              ^byte b1
                              (unsigned-bit-shift-right code1 o))))
                    (set-br bf 0 8)
                    (recur (byte o)
                           (BitOps/intAnd code1 (u/n-bits-mask o))))
                  (< o 0)
                  (let [rr (- o)]
                    (set-br bf
                            (bit-or ^byte b1 (bit-shift-left code1 rr))
                            rr))
                  :else
                  (do
                    (.put ^ByteBuffer dst
                          (unchecked-byte (bit-or ^byte b1 code1)))
                    (set-br bf 0 8)))))
            (recur (+ i 2)))
          (.put ^ByteBuffer dst ^byte (get-b bf))))
      (.putShort ^ByteBuffer dst (short total))))

  (decode [_ src dst]
    (let [^ByteBuffer src src
          ^ByteBuffer dst dst
          total           (- (.remaining src) 2)]
      (loop [i 0 k 0]
        (when (< i total)
          (let [b (BitOps/intAnd (.get src) 0x000000FF)]
            (recur (inc i)
                   (long (loop [j 1 k1 k]
                           (if (<= 0 j)
                             (let [e (aget ^longs (.get tables k1)
                                           (bit-and (byte 15)
                                                    (unsigned-bit-shift-right
                                                      b (* 4 j))))
                                   w (get-decoded e)]
                               (when w (.putShort dst w))
                               (recur (dec j) (long (get-link e))))
                             k1)))))))
      (.limit dst (.getShort src)))))

(defn new-hu-tucker
  [^longs freqs]
  (let [n     (alength freqs)
        lens  (byte-array n)
        codes (int-array n)]
    (create-codes n lens codes freqs)
    (HuTucker. lens codes (create-decode-tables lens codes))))

(defn codes->hu-tucker
  [^bytes lens ^ints codes]
  (HuTucker. lens codes (create-decode-tables lens codes)))
