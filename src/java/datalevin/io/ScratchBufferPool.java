/* Copyright (c) Huahai Yang. All rights reserved.
 * Distributed under the Eclipse Public License 2.0. */
package datalevin.io;

import datalevin.cpp.UnsafeAccess;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Shared idle storage, never a registry of active buffers. Removal transfers
 * exclusive ownership to the borrower; return transfers it back to the pool.
 * The byte budget covers buffers, including those owned by cached wire codecs. */
public final class ScratchBufferPool implements AutoCloseable {
    public static final int LOCAL_LIMIT = 1024 * 1024;

    private static final class Shared {
        private static final ScratchBufferPool INSTANCE = new ScratchBufferPool(
            Long.getLong("datalevin.buffer-cache-bytes", 64L * 1024 * 1024),
            Long.getLong("datalevin.buffer-cache-idle-ms", 30000L),
            System::nanoTime);
    }

    public static ScratchBufferPool shared() { return Shared.INSTANCE; }

    private static final class Entry {
        final Object value;
        final long bytes, returnedAt;
        Entry(Object value, long bytes, long returnedAt) {
            this.value = value;
            this.bytes = bytes;
            this.returnedAt = returnedAt;
        }
    }

    private final long budget, idleNanos;
    private final LongSupplier clock;
    private final ArrayList<Entry> idle = new ArrayList<>();
    private long retainedBytes;
    private boolean closed;
    private ScheduledExecutorService reaper;

    /** An explicit clock permits deterministic expiry tests without sleeping. */
    public ScratchBufferPool(long budget, long idleMillis, LongSupplier clock) {
        if (budget < 0 || idleMillis <= 0)
            throw new IllegalArgumentException("Invalid scratch buffer cache limits");
        this.budget = budget;
        this.idleNanos = TimeUnit.MILLISECONDS.toNanos(idleMillis);
        this.clock = clock;
    }

    private static void dispose(Object value) {
        if (value instanceof ByteBuffer) {
            try { UnsafeAccess.clean((ByteBuffer) value); }
            catch (RuntimeException ignored) { /* Fall back to the JVM cleaner. */ }
        } else {
            ((WireCompression) value).close();
        }
    }

    private void expire(long now, ArrayList<Object> discarded) {
        Iterator<Entry> entries = idle.iterator();
        while (entries.hasNext()) {
            Entry entry = entries.next();
            if (now - entry.returnedAt >= idleNanos) {
                entries.remove();
                retainedBytes -= entry.bytes;
                discarded.add(entry.value);
            }
        }
    }

    private Object take(boolean buffer, long minimum) {
        ArrayList<Object> discarded = new ArrayList<>();
        Object result = null;
        synchronized (this) {
            expire(clock.getAsLong(), discarded);
            int best = -1;
            long size = Long.MAX_VALUE;
            for (int i = 0; i < idle.size(); i++) {
                Entry entry = idle.get(i);
                if ((entry.value instanceof ByteBuffer) == buffer
                    && entry.bytes >= minimum && entry.bytes < size) {
                    best = i;
                    size = entry.bytes;
                }
            }
            if (best >= 0) {
                Entry entry = idle.remove(best);
                retainedBytes -= entry.bytes;
                result = entry.value;
            }
        }
        discarded.forEach(ScratchBufferPool::dispose);
        return result;
    }

    private void give(Object value, long bytes) {
        ArrayList<Object> discarded = new ArrayList<>();
        synchronized (this) {
            long now = clock.getAsLong();
            expire(now, discarded);
            if (closed || bytes <= LOCAL_LIMIT || bytes > budget) {
                discarded.add(value);
            } else {
                // Prefer recently used buffers when the shared budget is full.
                while (retainedBytes > budget - bytes) {
                    Entry entry = idle.remove(0);
                    retainedBytes -= entry.bytes;
                    discarded.add(entry.value);
                }
                idle.add(new Entry(value, bytes, now));
                retainedBytes += bytes;
                // Only the process-wide cache starts maintenance. Independent
                // pools expose sweep() for callers controlling their lifecycle.
                if (this == Shared.INSTANCE && reaper == null) {
                    reaper = Executors.newSingleThreadScheduledExecutor(task -> {
                        Thread thread = new Thread(task, "datalevin-buffer-cache");
                        thread.setDaemon(true);
                        return thread;
                    });
                    long interval = Math.max(1L, Math.min(1000L,
                        TimeUnit.NANOSECONDS.toMillis(idleNanos)));
                    reaper.scheduleWithFixedDelay(this::sweep, interval, interval,
                                                   TimeUnit.MILLISECONDS);
                }
            }
        }
        discarded.forEach(ScratchBufferPool::dispose);
    }

    /** Try reuse without allocating; useful when an owner remembers a size. */
    public ByteBuffer cachedBuffer(int minimum) {
        if (minimum < 0) throw new IllegalArgumentException("Negative buffer size");
        ByteBuffer buffer = (ByteBuffer) take(true, minimum);
        if (buffer != null) buffer.clear().order(ByteOrder.BIG_ENDIAN);
        return buffer;
    }

    public ByteBuffer acquireBuffer(int minimum) {
        ByteBuffer buffer = minimum > LOCAL_LIMIT ? cachedBuffer(minimum) : null;
        if (buffer != null) return buffer;
        try {
            return ByteBuffer.allocateDirect(minimum);
        } catch (OutOfMemoryError pressure) {
            // A smaller JVM direct-memory limit or another subsystem may need
            // the idle budget. Relinquish cached allocations and retry once.
            clear();
            return ByteBuffer.allocateDirect(minimum);
        }
    }

    /** Only pass an owned root allocation; never a borrowed LMDB view or slice. */
    public void releaseBuffer(ByteBuffer buffer) {
        if (buffer == null) return;
        if (!buffer.isDirect() || buffer.capacity() <= LOCAL_LIMIT) dispose(buffer);
        else give(buffer, buffer.capacity());
    }

    public WireCompression acquireCompression() {
        WireCompression codec = (WireCompression) take(false, 0);
        return codec != null ? codec : new WireCompression(this);
    }

    public void releaseCompression(WireCompression codec) {
        give(codec, codec.bufferBytes());
    }

    public synchronized long retainedBytes() { return retainedBytes; }

    /** Relinquish idle memory under allocation pressure; active leases survive. */
    public void clear() {
        ArrayList<Object> discarded = new ArrayList<>();
        synchronized (this) {
            for (Entry entry : idle) discarded.add(entry.value);
            idle.clear();
            retainedBytes = 0;
        }
        discarded.forEach(ScratchBufferPool::dispose);
    }

    /** Expire only resources in the idle cache, never a checked-out resource. */
    public void sweep() {
        ArrayList<Object> discarded = new ArrayList<>();
        synchronized (this) { expire(clock.getAsLong(), discarded); }
        discarded.forEach(ScratchBufferPool::dispose);
    }

    @Override public void close() {
        ArrayList<Object> discarded = new ArrayList<>();
        synchronized (this) {
            closed = true;
            for (Entry entry : idle) discarded.add(entry.value);
            idle.clear();
            retainedBytes = 0;
            if (reaper != null) reaper.shutdown();
        }
        discarded.forEach(ScratchBufferPool::dispose);
    }
}
