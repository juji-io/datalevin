package datalevin.cpp;

import java.util.*;
import org.bytedeco.javacpp.*;
import datalevin.dtlvnative.DTLV;

public class VecIdx {

    public static float[] randomVector(final int dimensions) {
        Random rand = new Random();
        float[] data = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            data[i] = rand.nextFloat();
        }
        return data;
    }

    static void expect(boolean must_be_true, String message) {
        if (must_be_true) return;

        message = (message != null ? message : "usearch call failed");
        throw new RuntimeException("usearch error: " + message);
    }

    static void expectNoError(PointerPointer<BytePointer> error, String message) {

        BytePointer errPtr = error.get(BytePointer.class);
        if (errPtr != null) {
            String msg = errPtr.getString();
            throw new RuntimeException(message + ": " + msg);
        }
    }

    private DTLV.usearch_index_t index;

    public VecIdx(long dimensions, int metricType, int quantization,
            long connectivity, long expansionAdd, long expansionSearch) {

        DTLV.usearch_init_options_t opts = createOpts(dimensions, metricType,
                                                      quantization, connectivity,
                                                      expansionAdd, expansionSearch);

        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);

        index = DTLV.usearch_init(opts, error);
        // System.out.println("called init");
        expect(index != null, "Failed to init index");

    }

    public void addFloat(long key, float[] vector) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);

        FloatPointer vecPtr = new FloatPointer(vector);
        DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_f32_k, error);
        expectNoError(error, "Fail to add vector");
    }

    private DTLV.usearch_init_options_t createOpts(final long dimensions, final int metricType,
            final int quantization, final long connectivity,
            final long expansionAdd, final long expansionSearch) {

        DTLV.usearch_init_options_t opts = new DTLV.usearch_init_options_t();

        opts.metric_kind(metricType)
                .metric((DTLV.usearch_metric_t) null)
                .quantization(quantization)
                .dimensions(dimensions)
                .connectivity(connectivity)
                .expansion_add(expansionAdd)
                .expansion_search(expansionSearch)
                .multi(false);

        return opts;
    }

}
