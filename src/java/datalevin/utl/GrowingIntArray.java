package datalevin.utl;

import java.util.Arrays;

public final class GrowingIntArray {

    private int[] buf;
    private int size;

    public GrowingIntArray() {
        this(64);
    }

    public GrowingIntArray(int capacity) {
        buf = new int[capacity];
    }

    public int size() {
        return size;
    }

    private void ensureCapacity(int len) {
        if (size + len > buf.length) {
            int n = buf.length * 3 / 2 + 1;
            if (size + len > n) {
                n = size + len;
            }
            int[] a = new int[n];
            System.arraycopy(buf, 0, a, 0, size);
            buf = a;
        }
    }

    /**
     * Insert a value at a specified index of the array, grow the array
     */
    public void insert(int index, int value) {
        ensureCapacity(size + 1);
        if (index == size) {
            buf[size++] = value;
        } else {
            System.arraycopy(buf, index, buf, index + 1, size - index);
            buf[index] = value;
            size++;
        }
    }

    /**
     * Set a value at a specified index inside the array, does not grow
     */
    public void set(int index, int value) {
        buf[index] = value;
    }

    /**
     * Add all values to the array
     */
    public void addAll(int[] values) {
        buf = values;
        size = values.length;
    }

    /**
     * Remove a value at a specified index
     */
    public void remove(int index) {
        int offset = size - index - 1;
        if (offset > 0) {
            System.arraycopy(buf, index + 1, buf, index, offset);
        }
        size--;
    }

    /**
     * Constructs and returns a simple array containing the same data as held
     * in this growable array.
     */
    public int[] toArray() {
        int[] a = new int[size];
        System.arraycopy(buf, 0, a, 0, size);
        return a;
    }

    public void clear() {
        size = 0;
    }

    /**
     * Retrieve the value present at an index position in the array.
     */
    public int get(int index) {
        return buf[index];
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof GrowingIntArray)) {
            return false;
        }

        GrowingIntArray g = (GrowingIntArray) o;

        return size == g.size() && Arrays.equals(toArray(), g.toArray());
    }

}
