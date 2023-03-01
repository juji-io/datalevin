package datalevin.utl;

public class LeftistHeap {

    int element;
    int s;
    LeftistHeap leftChild;
    LeftistHeap rightChild;

    public LeftistHeap(int e) {
        this.element = e;
        this.s = 1;
        this.leftChild = null;
        this.rightChild = null;
    }

    public LeftistHeap merge(LeftistHeap x, LeftistHeap y) {
        if (x == null) return y;
        if (y == null) return x;

        // if this were a max-heap, then the
        // next line would be: if (x.element < y.element)
        if (x.element > y.element) return merge(y, x);

        x.rightChild = merge(x.rightChild, y);

        if (x.leftChild == null) {
            // left child doesn't exist, so move right child to the left side
            x.leftChild = x.rightChild;
            x.rightChild = null;
            // x.s was, and remains, 1
        } else {
            // left child does exist, so compare s-values
            if (x.leftChild.s < x.rightChild.s) {
                LeftistHeap temp = x.leftChild;
                x.leftChild = x.rightChild;
                x.rightChild = temp;
            }
            // since we know the right child has the lower s-value, we can just
            // add one to its s-value
            x.s = x.rightChild.s + 1;
        }
        return x;
    }

    public LeftistHeap insert(LeftistHeap x, int e) {
        LeftistHeap y = new LeftistHeap(e);
        return merge(x, y);
    }

    public LeftistHeap deleteMin(LeftistHeap x) {
        return merge(x.leftChild, x.rightChild);
    }


}
