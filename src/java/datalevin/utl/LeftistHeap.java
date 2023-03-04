package datalevin.utl;

import java.util.Collection;
import java.util.LinkedList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

public abstract class LeftistHeap<T> {

    Node root;
    UnifiedMap<T, Node> nodes;

    public LeftistHeap() {
        root = null;
        nodes = new UnifiedMap<T, Node>();
    }

    // public LeftistHeap(T e) {
    //     root = new Node(e);
    //     nodes = new UnifiedMap<T, Node>();
    //     nodes.put(e, root);
    // }

    class Node {

        T element;
        int s;
        Node parent;
        Node leftChild;
        Node rightChild;

        public Node(T e) {
            this.element = e;
            this.s = 1;
            this.parent = null;
            this.leftChild = null;
            this.rightChild = null;
        }

        public String toString() {
            return "[" + element + " " + s
                + " p: " + (parent != null ? parent.element : null)
                + " l: " + (leftChild != null ? leftChild.element : null)
                + " r: " + (rightChild != null ? rightChild.element : null)
                + "]";
        }
    }

    protected abstract boolean lessThan(T a, T b);

    // public static LeftistHeap init(Collection<LeftistHeap> coll) {
    //     LinkedList<LeftistHeap> queue = new LinkedList<LeftistHeap>();
    //     queue.addAll(coll);

    //     while (queue.size() > 1) {
    //         LeftistHeap top = queue.poll();
    //         LeftistHeap top1 = queue.poll();
    //         queue.add(top.merge(top1));
    //     }

    //     return queue.poll();
    // }

    public LeftistHeap merge(LeftistHeap rhs) {
        if (this == rhs) return this;

        root = merge(root, rhs.root);
        rhs.root = null;

        nodes.putAll(rhs.nodes);

        return this;
    }

    private Node merge(Node x, Node y) {
        if (x == null) return y;
        if (y == null) return x;

        if (lessThan(y.element, x.element)) return merge(y, x);

        Node m = merge(x.rightChild, y);
        x.rightChild = m;
        m.parent = x;

        adjust(x);
        return x;
    }

    private void adjust(Node x) {
        if (x.leftChild == null && x.rightChild == null) {
            x.s = 1;
            return;
        }

        if (x.leftChild == null) {
            x.leftChild = x.rightChild;
            x.rightChild = null;
        }

        if (x.rightChild == null) {
            x.s = 1;
        } else {
            if (x.leftChild.s < x.rightChild.s) {
                Node temp = x.leftChild;
                x.leftChild = x.rightChild;
                x.rightChild = temp;
            }
            x.s = x.rightChild.s + 1;
        }
    }

    public void insert(T e) {
        Node n = new Node(e);
        root = merge(root, n);
        nodes.put(e, n);
    }

    public void deleteMin() {
        T e = findMin();
        root = merge(root.leftChild, root.rightChild);
        nodes.remove(e);
    }

    public T findMin() {
        return root.element;
    }

    public T findNextMin() {
        T le = root.leftChild.element;
        T re = root.rightChild.element;
        if (lessThan(le, re)) {
            return le;
        } else {
            return re;
        }
    }

    public void deleteElement(T e) {
        if (root == null) return;

        if (e.equals(root.element)) {
            deleteMin();
            return;
        }

        Node h = findNode(e);
        if (h == null) return;

        Node h1 = merge(h.leftChild, h.rightChild);
        Node p = h.parent;

        if (h1 != null) h1.parent = p;

        if (h == p.leftChild) {
            p.leftChild = h1;
        } else {
            p.rightChild = h1;
        }

        int before = p.s;
        adjust(p);
        int after = p.s;
        while (before != after && p != root) {
            p = p.parent;
            before = p.s;
            adjust(p);
            after = p.s;
        }
    }

    public Node findNode(T e) {
        return nodes.get(e);
    }

    public void print() {
        print(root);
        System.out.println();
    }

    private void print(Node r) {
        if (r != null) {
            System.out.println("[" + r.element + " " + r.s + "]");
            print(r.leftChild);
            print(r.rightChild);
        }
    }
}
