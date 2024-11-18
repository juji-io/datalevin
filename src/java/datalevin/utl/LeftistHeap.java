package datalevin.utl;

import java.util.HashMap;

public abstract class LeftistHeap<T> {

    Node root;
    HashMap<T, Node> nodes;

    public LeftistHeap() {
        root = null;
        nodes = new HashMap<T, Node>();
    }

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
    }

    protected abstract boolean lessThan(T a, T b);

    public LeftistHeap<T> merge(LeftistHeap<T> rhs) {
        if (this == rhs) return this;
        if (rhs == null) return this;

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

    // ensure the leftist properties
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
        if (e == null) return;

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
        Node l = root.leftChild;
        Node r = root.rightChild;
        if (l == null) return null;
        if (r == null) return l.element;

        T le = l.element;
        T re = r.element;
        if (lessThan(le, re)) {
            return le;
        } else {
            return re;
        }
    }

    public void deleteElement(T e) {
        if (root == null) return;
        if (e == null) return;

        if (e.equals(root.element)) {
            deleteMin();
            return;
        }

        Node h = findNode(e);
        if (h == null) return;

        nodes.remove(e);

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
}
