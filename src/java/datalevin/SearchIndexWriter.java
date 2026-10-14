package datalevin;

import java.util.Objects;

/**
 * Batched writer for a KV-backed full-text search index.
 *
 * <p>Documents are buffered in memory and flushed to the KV store in batches.
 * Call {@link #commit()} when all documents have been written. A committed
 * writer is closed and cannot be reused.
 */
public final class SearchIndexWriter extends HandleResource {

    SearchIndexWriter(Object writer) {
        super(writer,
              resource -> {
              },
              "search-writer",
              "search-writer");
    }

    /**
     * Returns whether this writer has been committed or closed.
     */
    public boolean closed() {
        return isReleased();
    }

    /**
     * Adds one document to the pending search index batch.
     */
    public SearchIndexWriter write(Object docRef, String docText) {
        Objects.requireNonNull(docText, "docText");
        DatalevinInterop.searchWrite(this, docRef, docText);
        return this;
    }

    /**
     * Flushes all pending documents and closes this writer.
     * The writer is closed even if flushing fails.
     */
    public Object commit() {
        try {
            return DatalevinInterop.searchCommit(resource());
        } finally {
            close();
        }
    }
}
