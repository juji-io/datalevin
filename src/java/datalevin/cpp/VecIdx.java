package datalevin.cpp;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.function.LongPredicate;

import javax.naming.directory.SearchResult;

import java.nio.file.*;
import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;

import datalevin.dtlvnative.DTLV;

public class VecIdx {

    static void deleteDirectoryFiles(final String path) {
        File directory = new File(path);
        if (!directory.isDirectory()) {
            directory.delete();
            return;
        }

        for (File f : directory.listFiles())
            f.delete();
        directory.delete();
    }

    static float[][] randomVectors(final int n, final int dimensions) {
        Random rand = new Random();
        float[][] data = new float[n][dimensions];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < dimensions; j++) {
                data[i][j] = rand.nextFloat();
            }
        }
        return data;
    }

    static void expect(boolean must_be_true, String message) {
        if (must_be_true)
            return;

        message = (message != null ? message : "unexpected result");
        throw new RuntimeException("usearch error: " + message);
    }

    static void expectNoError(PointerPointer<BytePointer> error, String message) {

        BytePointer errPtr = error.get(BytePointer.class);
        if (errPtr != null) {
            String msg = errPtr.getString();
            throw new RuntimeException(message + ": " + msg);
        }
    }

    // static void testUsearchInit(int collSize, int dimensions) {

    // DTLV.usearch_init_options_t opts = createOpts(dimensions);

    // expect(opts.metric_kind() == DTLV.usearch_metric_ip_k, "fail to get
    // metric_kind");
    // expect(opts.quantization() == DTLV.usearch_scalar_f32_k, "fail to get
    // quantization");
    // expect(opts.connectivity() == connectivity, "fail to get connectivity");
    // expect(opts.dimensions() == dimensions, "fail to get dimensions");
    // expect(opts.expansion_add() == expansion_add, "fail to get expansion_add");
    // expect(opts.expansion_search() == expansion_search, "fail to get
    // expansion_search");
    // expect(opts.multi() == false, "fail to get multi");

    // PointerPointer<BytePointer> error = new PointerPointer<>(1);

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
    // System.out.println("called init");
    // expect(index != null, "Failed to init index");

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_free(index, error);
    // expectNoError(error, "Fail to free index");

    // error.put(0, (BytePointer) null);
    // index = DTLV.usearch_init(opts, error);
    // expect(index != null, "Failed to init index");

    // error.put(0, (BytePointer) null);
    // long size = DTLV.usearch_size(index, error);
    // expect(size == 0, "Failed to get index size");

    // error.put(0, (BytePointer) null);
    // long capacity = DTLV.usearch_capacity(index, error);
    // expect(capacity == 0, "Failed to get index capacity");

    // error.put(0, (BytePointer) null);
    // long dims = DTLV.usearch_dimensions(index, error);
    // expect(dimensions == dims, "Failed to get index dimensions");

    // error.put(0, (BytePointer) null);
    // long connectivity = DTLV.usearch_connectivity(index, error);
    // expect(connectivity == opts.connectivity(),
    // "Failed to get index connectivity");

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_reserve(index, collSize, error);
    // expectNoError(error, "Fail to reserve");

    // error.put(0, (BytePointer) null);
    // size = DTLV.usearch_size(index, error);
    // expect(size == 0, "Failed to get index size");

    // error.put(0, (BytePointer) null);
    // capacity = DTLV.usearch_capacity(index, error);
    // expect(capacity >= collSize, "Failed to get index capacity");

    // error.put(0, (BytePointer) null);
    // dims = DTLV.usearch_dimensions(index, error);
    // expect(dimensions == dims, "Failed to get index dimensions");

    // error.put(0, (BytePointer) null);
    // connectivity = DTLV.usearch_connectivity(index, error);
    // expect(connectivity == opts.connectivity(),
    // "Failed to get index connectivity");

    // error.put(0, (BytePointer) null);
    // BytePointer hardware = DTLV.usearch_hardware_acceleration(index, error);
    // expectNoError(error, "Fail to get hardware");
    // System.out.println("SIMD Hardware ISA Name is: " + hardware.getString());

    // error.put(0, (BytePointer) null);
    // long memory = DTLV.usearch_memory_usage(index, error);
    // expect(memory > 0, "Failed to get memory usage");
    // System.out.println("Memory Usage is: " + memory);

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_free(index, error);
    // expectNoError(error, "Fail to free index");

    // System.out.println("Passed init.");
    // }

    // public static void testAdd(int collSize, int dimensions) {

    // PointerPointer<BytePointer> error = new PointerPointer<>(1);

    // DTLV.usearch_init_options_t opts = createOpts(dimensions);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_reserve(index, 1, error);

    // float[][] data = randomVectors(collSize, dimensions);

    // for (int i = 0; i < collSize; i++) {
    // FloatPointer vecPtr = new FloatPointer(data[i]);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_add(index, (long)i, vecPtr, DTLV.usearch_scalar_f32_k, error);
    // expectNoError(error, "Fail to add vector");
    // }

    // error.put(0, (BytePointer) null);
    // long size = DTLV.usearch_size(index, error);
    // expect(size == collSize, "Failed to get index size");

    // error.put(0, (BytePointer) null);
    // long capacity = DTLV.usearch_capacity(index, error);
    // expect(capacity >= collSize, "Failed to get index capacity");

    // for (int i = 0; i < collSize; i++) {
    // error.put(0, (BytePointer) null);
    // expect(DTLV.usearch_contains(index, (long) i, error),
    // "Failed to find key in index");
    // }
    // error.put(0, (BytePointer) null);
    // expect(!DTLV.usearch_contains(index, (long) -1, error),
    // "Found non existing key in index");

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_free(index, error);
    // System.out.println("Passed add.");
    // }

    // static void testUsearchFind(int collSize, int dimensions) {

    // PointerPointer<BytePointer> error = new PointerPointer<>(1);

    // DTLV.usearch_init_options_t opts = createOpts(dimensions);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_reserve(index, collSize, error);

    // float[][] data = randomVectors(collSize, dimensions);

    // for (int i = 0; i < collSize; i++) {
    // FloatPointer vecPtr = new FloatPointer(data[i]);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_add(index, (long) i, vecPtr, DTLV.usearch_scalar_f32_k, error);
    // expectNoError(error, "Fail to add vector");
    // }

    // LongPointer keys = new LongPointer(new long[collSize]);
    // FloatPointer distances = new FloatPointer(new float[collSize]);

    // for (int i = 0; i < collSize; i++) {
    // FloatPointer vecPtr = new FloatPointer(data[i]);
    // error.put(0, (BytePointer) null);
    // long found = DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_f32_k,
    // (long)collSize, keys, distances, error);
    // expectNoError(error, "Fail to search");
    // expect(found >= 1 && found <= collSize, "Vector cannot be found");
    // }

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_free(index, error);
    // System.out.println("Passed find.");
    // }

    // static void testUsearchGet(int collSize, int dimensions) {

    // PointerPointer<BytePointer> error = new PointerPointer<>(1);

    // DTLV.usearch_init_options_t opts = createOpts(dimensions);
    // opts.multi(true);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_reserve(index, collSize, error);

    // float[][] data = randomVectors(collSize, dimensions);

    // long key = 1;
    // for (int i = 0; i < collSize; i++) {
    // FloatPointer vecPtr = new FloatPointer(data[i]);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_f32_k, error);
    // expectNoError(error, "Fail to add vector");
    // }

    // float[] vectors = new float[collSize * dimensions];
    // FloatPointer vPtr= new FloatPointer(vectors);
    // error.put(0, (BytePointer) null);
    // long found = DTLV.usearch_get(index, key, (long) collSize, vPtr,
    // DTLV.usearch_scalar_f32_k, error);
    // expectNoError(error, "Fail to get");
    // expect(found == collSize, "Key is missing");

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_free(index, error);

    // System.out.println("Passed get.");
    // }

    // static void testUsearchRemove(int collSize, int dimensions) {

    // PointerPointer<BytePointer> error = new PointerPointer<>(1);

    // DTLV.usearch_init_options_t opts = createOpts(dimensions);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_reserve(index, collSize, error);

    // float[][] data = randomVectors(collSize, dimensions);

    // for (int i = 0; i < collSize; i++) {
    // FloatPointer vecPtr = new FloatPointer(data[i]);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_add(index, (long)i, vecPtr, DTLV.usearch_scalar_f32_k, error);
    // expectNoError(error, "Fail to add");
    // }

    // for (int i = 0; i < collSize; i++) {
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_remove(index, (long) i, error);
    // expectNoError(error, "Fail to remove");
    // }

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_free(index, error);
    // System.out.println("Passed remove.");
    // }

    // static void testUsearchLoad(int collSize, int dimensions) {

    // PointerPointer<BytePointer> error = new PointerPointer<>(1);

    // DTLV.usearch_init_options_t weird_opts = createOpts(dimensions);
    // weird_opts.connectivity(11)
    // .expansion_add(15)
    // .expansion_search(19)
    // .metric_kind(DTLV.usearch_metric_pearson_k)
    // .quantization(DTLV.usearch_scalar_f64_k);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_index_t index = DTLV.usearch_init(weird_opts, error);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_reserve(index, collSize, error);

    // float[][] data = randomVectors(collSize, dimensions);

    // for (int i = 0; i < collSize; i++) {
    // FloatPointer vecPtr = new FloatPointer(data[i]);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_add(index, (long) i, vecPtr, DTLV.usearch_scalar_f32_k, error);
    // expectNoError(error, "Fail to add");
    // }

    // String dir = "us";

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_save(index, dir, error);
    // expectNoError(error, "Fail to save");
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_free(index, error);

    // error.put(0, (BytePointer) null);
    // index = DTLV.usearch_init(null, error);
    // expectNoError(error, "Fail to init");

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_load(index, "us", error);
    // expectNoError(error, "Fail to load");

    // error.put(0, (BytePointer) null);
    // long size = DTLV.usearch_size(index, error);
    // expect(size == collSize, "Failed to get index size");
    // error.put(0, (BytePointer) null);
    // long capacity = DTLV.usearch_capacity(index, error);
    // expect(capacity == collSize, "Failed to get index capacity");
    // error.put(0, (BytePointer) null);
    // long dims = DTLV.usearch_dimensions(index, error);
    // expect(dimensions == dims, "Failed to get index dimensions");
    // error.put(0, (BytePointer) null);
    // long connectivity = DTLV.usearch_connectivity(index, error);
    // expect(connectivity == weird_opts.connectivity(),
    // "Failed to get index connectivity" + weird_opts.connectivity());

    // for (int i = 0; i < collSize; i++) {
    // error.put(0, (BytePointer) null);
    // expect(DTLV.usearch_contains(index, (long)i, error),
    // "Fail to find key in index");
    // }

    // LongPointer keys = new LongPointer(new long[collSize]);
    // FloatPointer distances = new FloatPointer(new float[collSize]);

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_change_threads_search(index, 1, error);
    // for (int i = 0; i < collSize; i++) {
    // FloatPointer vecPtr = new FloatPointer(data[i]);
    // error.put(0, (BytePointer) null);
    // long found = DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_f32_k,
    // (long)collSize, keys, distances, error);
    // expectNoError(error, "Fail to search");
    // expect(found >= 1 && found <= collSize, "Vector cannot be found");
    // }

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_free(index, error);

    // deleteDirectoryFiles(dir);

    // System.out.println("Passed load.");
    // }

    // static void testUsearchView(int collSize, int dimensions) {

    // PointerPointer<BytePointer> error = new PointerPointer<>(1);

    // DTLV.usearch_init_options_t opts = createOpts(dimensions);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_reserve(index, collSize, error);

    // float[][] data = randomVectors(collSize, dimensions);

    // for (int i = 0; i < collSize; i++) {
    // FloatPointer vecPtr = new FloatPointer(data[i]);
    // error.put(0, (BytePointer) null);
    // DTLV.usearch_add(index, (long)i, vecPtr, DTLV.usearch_scalar_f32_k, error);
    // expectNoError(error, "Fail to add");
    // }

    // String dir = "us";

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_save(index, dir, error);
    // expectNoError(error, "Fail to save");

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_free(index, error);

    // error.put(0, (BytePointer) null);
    // index = DTLV.usearch_init(opts, error);

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_view(index, dir, error);
    // expectNoError(error, "Fail to view");

    // error.put(0, (BytePointer) null);
    // DTLV.usearch_free(index, error);

    // deleteDirectoryFiles(dir);

    // System.out.println("Passed view.");
    // }

    // public static void testUsearch() {
    // System.err.println("Testing usearch ...");

    // int[] collSizes = { 11, 512 };
    // int[] dims = { 83, 200 };

    // for (int i = 0; i < collSizes.length; i++) {
    // for (int j = 0; j < dims.length; j++) {
    // System.err.println("Testing " + collSizes[i] + " " + dims[j]);
    // // testUsearchInit(collSizes[i], dims[j]);
    // // testUsearchAdd(collSizes[i], dims[j]);
    // // testUsearchFind(collSizes[i], dims[j]);
    // // testUsearchGet(collSizes[i], dims[j]);
    // // testUsearchRemove(collSizes[i], dims[j]);
    // // testUsearchLoad(collSizes[i], dims[j]);
    // // testUsearchView(collSizes[i], dims[j]);
    // }
    // }

    // System.out.println("Passed all usearch tests.");
    // }

    static DTLV.usearch_init_options_t createOpts(final long dimensions,
            final int metricType, final int quantization, final long connectivity,
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

    public static DTLV.usearch_index_t create(final long dimensions,
            final int metricType, final int quantization, final long connectivity,
            final long expansionAdd, final long expansionSearch) {

        DTLV.usearch_init_options_t opts = createOpts(dimensions,
                metricType, quantization, connectivity, expansionAdd, expansionSearch);

        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
        expectNoError(error, "Failed to init index");

        return index;
    }

    public static void free(DTLV.usearch_index_t index) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        DTLV.usearch_free(index, error);
        expectNoError(error, "Fail to free index");
    }

    public static void save(DTLV.usearch_index_t index, String fname) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        DTLV.usearch_save(index, fname, error);
        expectNoError(error, "Fail to add vector");
    }

    public static void load(DTLV.usearch_index_t index, String fname) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        DTLV.usearch_load(index, fname, error);
        expectNoError(error, "Fail to load index");
    }

    public static void addDouble(DTLV.usearch_index_t index, long key, double[] vector) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);

        error.put(0, (BytePointer) null);
        DTLV.usearch_reserve(index, 1, error);
        expectNoError(error, "Fail to reserve capacity");

        DoublePointer vecPtr = new DoublePointer(vector);
        error.put(0, (BytePointer) null);
        DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_f64_k, error);
        expectNoError(error, "Fail to add vector");
    }

    public static void addFloat(DTLV.usearch_index_t index, long key, float[] vector) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);

        error.put(0, (BytePointer) null);
        DTLV.usearch_reserve(index, 1, error);
        expectNoError(error, "Fail to reserve capacity");

        FloatPointer vecPtr = new FloatPointer(vector);
        error.put(0, (BytePointer) null);
        DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_f32_k, error);
        expectNoError(error, "Fail to add vector");
    }

    public static void addShort(DTLV.usearch_index_t index, long key, short[] vector) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);

        error.put(0, (BytePointer) null);
        DTLV.usearch_reserve(index, 1, error);
        expectNoError(error, "Fail to reserve capacity");

        ShortPointer vecPtr = new ShortPointer(vector);
        error.put(0, (BytePointer) null);
        DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_f16_k, error);
        expectNoError(error, "Fail to add vector");
    }

    public static void addInt8(DTLV.usearch_index_t index, long key, byte[] vector) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);

        error.put(0, (BytePointer) null);
        DTLV.usearch_reserve(index, 1, error);
        expectNoError(error, "Fail to reserve capacity");

        BytePointer vecPtr = new BytePointer(vector);
        error.put(0, (BytePointer) null);
        DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_i8_k, error);
        expectNoError(error, "Fail to add vector");
    }

    public static void addByte(DTLV.usearch_index_t index, long key,
            byte[] vector) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        DTLV.usearch_reserve(index, 1, error);
        expectNoError(error, "Fail to reserve capacity");

        BytePointer vecPtr = new BytePointer(vector);
        error.put(0, (BytePointer) null);
        DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_b1_k, error);
        expectNoError(error, "Fail to add vector");
    }

    public static void remove(DTLV.usearch_index_t index, long key) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        DTLV.usearch_remove(index, key, error);
        expectNoError(error, "Fail to add vector");
    }

    public static SearchResult searchDouble(DTLV.usearch_index_t index, double[] query,
            long count, long[] keys, float[] distances) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        DoublePointer vecPtr = new DoublePointer(query);
        LongPointer keyPtr = new LongPointer(keys);
        FloatPointer distPtr = new FloatPointer(distances);
        DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_f64_k,
                count, keyPtr, distPtr, error);
        expectNoError(error, "Fail to search vector");
        return new SearchResult(count, keyPtr, distPtr);
    }

    public static SearchResult searchFloat(DTLV.usearch_index_t index, float[] query,
            long count, long[] keys, float[] distances) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        FloatPointer vecPtr = new FloatPointer(query);
        LongPointer keyPtr = new LongPointer(keys);
        FloatPointer distPtr = new FloatPointer(distances);
        DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_f32_k,
                count, keyPtr, distPtr, error);
        expectNoError(error, "Fail to search vector");
        return new SearchResult(count, keyPtr, distPtr);
    }

    public static SearchResult searchShort(DTLV.usearch_index_t index, short[] query,
            long count, long[] keys, float[] distances) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        ShortPointer vecPtr = new ShortPointer(query);
        LongPointer keyPtr = new LongPointer(keys);
        FloatPointer distPtr = new FloatPointer(distances);
        DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_f16_k,
                count, keyPtr, distPtr, error);
        expectNoError(error, "Fail to search vector");
        return new SearchResult(count, keyPtr, distPtr);
    }

    public static SearchResult searchInt8(DTLV.usearch_index_t index, byte[] query,
            long count, long[] keys, float[] distances) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        BytePointer vecPtr = new BytePointer(query);
        LongPointer keyPtr = new LongPointer(keys);
        FloatPointer distPtr = new FloatPointer(distances);
        DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_i8_k,
                count, keyPtr, distPtr, error);
        expectNoError(error, "Fail to search vector");
        return new SearchResult(count, keyPtr, distPtr);
    }

    public static SearchResult searchByte(DTLV.usearch_index_t index, byte[] query,
            long count, long[] keys, float[] distances) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        BytePointer vecPtr = new BytePointer(query);
        LongPointer keyPtr = new LongPointer(keys);
        FloatPointer distPtr = new FloatPointer(distances);
        DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_b1_k,
                count, keyPtr, distPtr, error);
        expectNoError(error, "Fail to search vector");
        return new SearchResult(count, keyPtr, distPtr);
    }

    public static class SearchResult {

        private int count;
        private LongPointer keyPtr;
        private FloatPointer distPtr;

        public SearchResult(long count,
                            LongPointer keyPtr, FloatPointer distPtr) {
            this.count = (int)count;
            this.keyPtr = keyPtr;
            this.distPtr = distPtr;
        }

        public long[] getKeys() {
            long[] out = new long[count];
            keyPtr.get(out);
            return out;
        }

        public float[] getDists() {
            float[] out = new float[count];
            distPtr.get(out);
            return out;
        }

    }
}
