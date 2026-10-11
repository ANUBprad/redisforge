package Components.Repository;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

/**
 * The size a key is charged against {@code maxmemory}.
 *
 * <p>This is a logical estimate of what one entry costs the keyspace, not the heap
 * footprint of its Java objects. It counts the bytes of the key and the value as they
 * would be written out, plus a fixed overhead for the entry itself and, for a key that
 * carries a deadline, for the metadata that deadline needs. The estimate is stable
 * across runs and platforms, which is what a limit has to be to mean the same thing
 * everywhere; it deliberately does not try to guess what a particular JVM allocates for
 * a string or a map node.</p>
 *
 * <p>Because it is an estimate, the number a client reads back from {@code INFO memory}
 * describes the dataset the way the server reasons about it, not the process's resident
 * memory. The guide is documented in the README under memory limits.</p>
 */
public final class MemoryEstimator {

    /** What one entry costs before the key and value bytes are counted. */
    public static final long ENTRY_OVERHEAD_BYTES = 64;
    /** What a deadline adds to an entry, for the metadata that records it. */
    public static final long EXPIRY_METADATA_BYTES = 16;

    private MemoryEstimator() {
    }

    /**
     * Whether a deadline is one the server tracks. A {@link LocalDateTime#MAX} is the
     * sentinel for "no deadline" everywhere else in the store, so it is not counted here
     * either.
     */
    public static boolean hasDeadline(LocalDateTime expiry) {
        return expiry != null && !LocalDateTime.MAX.equals(expiry);
    }

    /**
     * The bytes one entry contributes: a fixed overhead, the UTF-8 length of the key and
     * the value, and the deadline metadata when the key carries one.
     */
    public static long entryBytes(String key, String value, LocalDateTime expiry) {
        long bytes = ENTRY_OVERHEAD_BYTES + utf8Length(key) + utf8Length(value);
        if (hasDeadline(expiry)) {
            bytes += EXPIRY_METADATA_BYTES;
        }
        return bytes;
    }

    private static long utf8Length(String text) {
        return text == null ? 0 : text.getBytes(StandardCharsets.UTF_8).length;
    }
}
