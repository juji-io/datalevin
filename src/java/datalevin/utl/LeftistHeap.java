package datalevin.utl;

public abstract class LeftistHeap<T> {

    Node root;

    public LeftistHeap() { root = null; }

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

    public void merge(LeftistHeap rhs) {
        if (this == rhs) return;

        root = merge(root, rhs.root);
        rhs.root = null;
    }

    public Node merge(Node x, Node y) {
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
        root = merge(root, new Node(e));
    }

    public void deleteMin() {
        root = merge(root.leftChild, root.rightChild);
    }

    public T findMin() {
        return root.element;
    }

    public void deleteElement(T e) {
        if (root == null) return;

        if (e.equals(root.element)) {
            root = merge(root.leftChild, root.rightChild);
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
        return findNode(root, e);
    }

    private Node findNode(Node n, T e) {
        if (n == null) return null;
        if (e.equals(n.element)) return n;
        Node l = findNode(n.leftChild, e);
        if (l != null) return l;
        Node r = findNode(n.rightChild, e);
        if (r != null) return r;
        return null;
    }

    public void order() {
        order(root);
        System.out.println();
    }

    private void order(Node r) {
        if (r != null) {
            System.out.println("[" + r.element + " " + r.s + "]");
            order(r.leftChild);
            order(r.rightChild);
        }
    }
}
