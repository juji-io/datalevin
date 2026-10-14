package datalevin.cpp;

import org.bytedeco.javacpp.*;

import java.nio.ByteBuffer;
import java.util.List;

import clojure.lang.RT;
import datalevin.dtlvnative.DTLV;

public class VecIdx {

    public static double[] toDoubleArray(List<?> vector) {
        int size = vector.size();
        double[] result = new double[size];
        for (int i = 0; i < size; i++)
            result[i] = RT.doubleCast(vector.get(i));
        return result;
    }

    public static float[] toFloatArray(List<?> vector) {
        int size = vector.size();
        float[] result = new float[size];
        for (int i = 0; i < size; i++)
            result[i] = RT.floatCast(vector.get(i));
        return result;
    }

    public static short[] toShortArray(List<?> vector) {
        int size = vector.size();
        short[] result = new short[size];
        for (int i = 0; i < size; i++)
            result[i] = RT.shortCast(vector.get(i));
        return result;
    }

    public static byte[] toByteArray(List<?> vector) {
        int size = vector.size();
        byte[] result = new byte[size];
        for (int i = 0; i < size; i++)
            result[i] = RT.byteCast(vector.get(i));
        return result;
    }

    static void expect(boolean mustTrue, String message) {
        if (mustTrue)
            return;

        message = (message != null ? message : "unexpected result");
        throw new RuntimeException("usearch error: " + message);
    }

    static void expectNoError(PointerPointer<BytePointer> error, String message) {

        // USearch owns the error string; only the pointer slot is ours.
        BytePointer errPtr = error.get(BytePointer.class);
        if (errPtr != null) {
            String msg = errPtr.getString();
            throw new RuntimeException(message + ": " + msg);
        }
    }

    public static DTLV.usearch_index_t create(final long dimensions,
            final int metricType, final int quantization, final long connectivity,
            final long expansionAdd, final long expansionSearch) {
        try (DTLV.usearch_init_options_t opts = new DTLV.usearch_init_options_t();
             PointerPointer<BytePointer> error = new PointerPointer<>(1)) {
            opts.metric_kind(metricType)
                    .metric((DTLV.usearch_metric_t) null)
                    .quantization(quantization)
                    .dimensions(dimensions)
                    .connectivity(connectivity)
                    .expansion_add(expansionAdd)
                    .expansion_search(expansionSearch)
                    .multi(false);

            error.put(0, (BytePointer) null);
            DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
            expectNoError(error, "Failed to init index");

            return index;
        }
    }

    public static void free(DTLV.usearch_index_t index) {
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1)) {
            error.put(0, (BytePointer) null);
            DTLV.usearch_free(index, error);
            expectNoError(error, "Fail to free index");
        }
    }

    public static void save(DTLV.usearch_index_t index, String fname) {
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1)) {
            error.put(0, (BytePointer) null);
            DTLV.usearch_save(index, fname, error);
            expectNoError(error, "Fail to add vector");
        }
    }

    public static void load(DTLV.usearch_index_t index, String fname) {
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1)) {
            error.put(0, (BytePointer) null);
            DTLV.usearch_load(index, fname, error);
            expectNoError(error, "Fail to load index");
        }
    }

    public static long serializedLength(DTLV.usearch_index_t index) {
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1)) {
            error.put(0, (BytePointer) null);
            long length = DTLV.usearch_serialized_length(index, error);
            expectNoError(error, "Fail to get serialized length");
            return length;
        }
    }

    public static void saveBuffer(DTLV.usearch_index_t index,
                                  Pointer buffer, long length) {
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1)) {
            error.put(0, (BytePointer) null);
            DTLV.usearch_save_buffer(index, buffer, length, error);
            expectNoError(error, "Fail to save index to buffer");
        }
    }

    public static void loadBuffer(DTLV.usearch_index_t index,
                                  Pointer buffer, long length) {
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1)) {
            error.put(0, (BytePointer) null);
            DTLV.usearch_load_buffer(index, buffer, length, error);
            expectNoError(error, "Fail to load index from buffer");
        }
    }

    private static void reserveOne(DTLV.usearch_index_t index,
            PointerPointer<BytePointer> error) {
        error.put(0, (BytePointer) null);
        long size = DTLV.usearch_size(index, error);
        expectNoError(error, "Fail to get index size");

        error.put(0, (BytePointer) null);
        long capacity = DTLV.usearch_capacity(index, error);
        expectNoError(error, "Fail to get index capacity");

        if (size >= capacity) {
            long requested = capacity <= 0 ? 1 : Math.max(size + 1, capacity * 2);

            error.put(0, (BytePointer) null);
            DTLV.usearch_reserve(index, requested, error);
            expectNoError(error, "Fail to reserve capacity");
        }
    }

    public static void addDouble(DTLV.usearch_index_t index, long key, double[] vector) {
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1);
             DoublePointer vecPtr = new DoublePointer(vector)) {
            reserveOne(index, error);

            error.put(0, (BytePointer) null);
            DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_f64_k, error);
            expectNoError(error, "Fail to add vector");
        }
    }

    public static void addFloat(DTLV.usearch_index_t index, long key, float[] vector) {
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1);
             FloatPointer vecPtr = new FloatPointer(vector)) {
            reserveOne(index, error);

            error.put(0, (BytePointer) null);
            DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_f32_k, error);
            expectNoError(error, "Fail to add vector");
        }
    }

    public static void addShort(DTLV.usearch_index_t index, long key, short[] vector) {
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1);
             ShortPointer vecPtr = new ShortPointer(vector)) {
            reserveOne(index, error);

            error.put(0, (BytePointer) null);
            DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_f16_k, error);
            expectNoError(error, "Fail to add vector");
        }
    }

    public static void addInt8(DTLV.usearch_index_t index, long key, byte[] vector) {
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1);
             BytePointer vecPtr = new BytePointer(vector)) {
            reserveOne(index, error);

            error.put(0, (BytePointer) null);
            DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_i8_k, error);
            expectNoError(error, "Fail to add vector");
        }
    }

    public static void addByte(DTLV.usearch_index_t index, long key, byte[] vector) {
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1);
             BytePointer vecPtr = new BytePointer(vector)) {
            reserveOne(index, error);

            error.put(0, (BytePointer) null);
            DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_b1_k, error);
            expectNoError(error, "Fail to add vector");
        }
    }

    public static void remove(DTLV.usearch_index_t index, long key) {
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1)) {
            error.put(0, (BytePointer) null);
            DTLV.usearch_remove(index, key, error);
            expectNoError(error, "Fail to add vector");
        }
    }

    public static double[] getDouble(DTLV.usearch_index_t index,
        long key, int dimensions) {
        double[] result = new double[dimensions];
        try (DoublePointer resPtr = new DoublePointer(result);
             PointerPointer<BytePointer> error = new PointerPointer<>(1)) {
            error.put(0, (BytePointer) null);
            long found = DTLV.usearch_get(index, key, 1, (Pointer)resPtr,
                                          DTLV.usearch_scalar_f64_k, error);
            expectNoError(error, "Fail to get vector");
            if (found == 1) {
                resPtr.get(result);
                return result;
            } else {
                return null;
            }
        }
    }

    public static float[] getFloat(DTLV.usearch_index_t index,
        long key, int dimensions) {
        float[] result = new float[dimensions];
        try (FloatPointer resPtr = new FloatPointer(result);
             PointerPointer<BytePointer> error = new PointerPointer<>(1)) {
            error.put(0, (BytePointer) null);
            long found = DTLV.usearch_get(index, key, 1, (Pointer)resPtr,
                                          DTLV.usearch_scalar_f32_k, error);
            expectNoError(error, "Fail to get vector");
            if (found == 1) {
                resPtr.get(result);
                return result;
            } else {
                return null;
            }
        }
    }

    public static short[] getShort(DTLV.usearch_index_t index,
        long key, int dimensions) {
        short[] result = new short[dimensions];
        try (ShortPointer resPtr = new ShortPointer(result);
             PointerPointer<BytePointer> error = new PointerPointer<>(1)) {
            error.put(0, (BytePointer) null);
            long found = DTLV.usearch_get(index, key, 1, (Pointer)resPtr,
                                          DTLV.usearch_scalar_f16_k, error);
            expectNoError(error, "Fail to get vector");
            if (found == 1) {
                resPtr.get(result);
                return result;
            } else {
                return null;
            }
        }
    }

    public static byte[] getInt8(DTLV.usearch_index_t index,
        long key, int dimensions) {
        byte[] result = new byte[dimensions];
        try (BytePointer resPtr = new BytePointer(result);
             PointerPointer<BytePointer> error = new PointerPointer<>(1)) {
            error.put(0, (BytePointer) null);
            long found = DTLV.usearch_get(index, key, 1, (Pointer)resPtr,
                                          DTLV.usearch_scalar_i8_k, error);
            expectNoError(error, "Fail to get vector");
            if (found == 1) {
                resPtr.get(result);
                return result;
            } else {
                return null;
            }
        }
    }

    public static byte[] getByte(DTLV.usearch_index_t index,
        long key, int dimensions) {
        byte[] result = new byte[dimensions];
        try (BytePointer resPtr = new BytePointer(result);
             PointerPointer<BytePointer> error = new PointerPointer<>(1)) {
            error.put(0, (BytePointer) null);
            long found = DTLV.usearch_get(index, key, 1, (Pointer)resPtr,
                                          DTLV.usearch_scalar_b1_k, error);
            expectNoError(error, "Fail to get vector");
            if (found == 1) {
                resPtr.get(result);
                return result;
            } else {
                return null;
            }
        }
    }

    public static SearchResult searchDouble(DTLV.usearch_index_t index,
            double[] query, int count) {
        long[] keys = new long[count];
        float[] distances = new float[count];
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1);
             DoublePointer vecPtr = new DoublePointer(query);
             LongPointer keyPtr = new LongPointer(keys);
             FloatPointer distPtr = new FloatPointer(distances)) {
            error.put(0, (BytePointer) null);
            DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_f64_k,
                    count, keyPtr, distPtr, error);
            expectNoError(error, "Fail to search vector");
            keyPtr.get(keys);
            distPtr.get(distances);
            return new SearchResult(keys, distances);
        }
    }

    public static SearchResult searchFloat(DTLV.usearch_index_t index,
            float[] query, int count) {
        long[] keys = new long[count];
        float[] distances = new float[count];
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1);
             FloatPointer vecPtr = new FloatPointer(query);
             LongPointer keyPtr = new LongPointer(keys);
             FloatPointer distPtr = new FloatPointer(distances)) {
            error.put(0, (BytePointer) null);
            DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_f32_k,
                    count, keyPtr, distPtr, error);
            expectNoError(error, "Fail to search vector");
            keyPtr.get(keys);
            distPtr.get(distances);
            return new SearchResult(keys, distances);
        }
    }

    public static SearchResult searchShort(DTLV.usearch_index_t index,
            short[] query, int count) {
        long[] keys = new long[count];
        float[] distances = new float[count];
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1);
             ShortPointer vecPtr = new ShortPointer(query);
             LongPointer keyPtr = new LongPointer(keys);
             FloatPointer distPtr = new FloatPointer(distances)) {
            error.put(0, (BytePointer) null);
            DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_f16_k,
                    count, keyPtr, distPtr, error);
            expectNoError(error, "Fail to search vector");
            keyPtr.get(keys);
            distPtr.get(distances);
            return new SearchResult(keys, distances);
        }
    }

    public static SearchResult searchInt8(DTLV.usearch_index_t index,
            byte[] query, int count) {
        long[] keys = new long[count];
        float[] distances = new float[count];
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1);
             BytePointer vecPtr = new BytePointer(query);
             LongPointer keyPtr = new LongPointer(keys);
             FloatPointer distPtr = new FloatPointer(distances)) {
            error.put(0, (BytePointer) null);
            DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_i8_k,
                    count, keyPtr, distPtr, error);
            expectNoError(error, "Fail to search vector");
            keyPtr.get(keys);
            distPtr.get(distances);
            return new SearchResult(keys, distances);
        }
    }

    public static SearchResult searchByte(DTLV.usearch_index_t index,
            byte[] query, int count) {
        long[] keys = new long[count];
        float[] distances = new float[count];
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1);
             BytePointer vecPtr = new BytePointer(query);
             LongPointer keyPtr = new LongPointer(keys);
             FloatPointer distPtr = new FloatPointer(distances)) {
            error.put(0, (BytePointer) null);
            DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_b1_k,
                    count, keyPtr, distPtr, error);
            expectNoError(error, "Fail to search vector");
            keyPtr.get(keys);
            distPtr.get(distances);
            return new SearchResult(keys, distances);
        }
    }

    public static IndexInfo info(DTLV.usearch_index_t index) {
        try (PointerPointer<BytePointer> error = new PointerPointer<>(1)) {
            error.put(0, (BytePointer) null);
            long size = DTLV.usearch_size(index, error);
            expectNoError(error, "Fail to get index size");

            error.put(0, (BytePointer) null);
            long capacity = DTLV.usearch_capacity(index, error);
            expectNoError(error, "Fail to get index capacity");

            error.put(0, (BytePointer) null);
            long memory = DTLV.usearch_memory_usage(index, error);
            expect(memory > 0, "Failed to get index memory usage");

            error.put(0, (BytePointer) null);
            // The ISA name belongs to USearch, not this temporary error slot.
            BytePointer hardware = DTLV.usearch_hardware_acceleration(index, error);
            expectNoError(error, "Fail to get hardware ISA name");

            return new IndexInfo(size, capacity, memory, hardware.getString());
        }
    }

    public static class SearchResult {

        private long[] keys;
        private float[] dists;

        public SearchResult(long[] keys, float[] dists) {
            this.keys = keys;
            this.dists = dists;
        }

        public long[] getKeys() {
            return keys;
        }

        public float[] getDists() {
            return dists;
        }
    }

    public static class IndexInfo {

        private long size;
        private long capacity;
        private long memory;
        private String hardware;

        public IndexInfo(long size, long capacity, long memory, String hardware) {
            this.size = size;
            this.capacity = capacity;
            this.memory = memory;
            this.hardware = hardware;
        }

        public long getSize() {
            return size;
        }

        public long getCapacity() {
            return capacity;
        }

        public long getMemory() {
            return memory;
        }

        public String getHardware() {
            return hardware;
        }
    }

}
