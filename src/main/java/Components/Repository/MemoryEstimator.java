package Components.Repository;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Map;

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
    /**
     * What one field, element or member of a collection adds beyond its own bytes, for
     * the slot that holds it. The same for every collection kind: the estimate is about
     * the dataset the server reasons about, not about a particular map node.
     */
    public static final long ELEMENT_OVERHEAD_BYTES = 24;

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

    /**
     * The bytes one entry of any kind contributes: the fixed overhead, the key, the
     * deadline metadata when there is one, and then what the kind itself holds. A string
     * charges its value; a hash charges each field and its value; a list and a set charge
     * each element or member. Every element carries {@link #ELEMENT_OVERHEAD_BYTES} on
     * top of its own bytes.
     */
    public static long entryBytes(String key, Value value) {
        long bytes = ENTRY_OVERHEAD_BYTES + utf8Length(key);
        if (hasDeadline(value.expiry)) {
            bytes += EXPIRY_METADATA_BYTES;
        }
        switch (value.kind) {
            case STRING -> bytes += utf8Length(value.val);
            case HASH -> {
                for (Map.Entry<String, String> field : value.hash.entrySet()) {
                    bytes += ELEMENT_OVERHEAD_BYTES
                            + utf8Length(field.getKey()) + utf8Length(field.getValue());
                }
            }
            case LIST -> {
                for (String element : value.list) {
                    bytes += ELEMENT_OVERHEAD_BYTES + utf8Length(element);
                }
            }
            case SET -> {
                for (String member : value.set) {
                    bytes += ELEMENT_OVERHEAD_BYTES + utf8Length(member);
                }
            }
        }
        return bytes;
    }

    private static long utf8Length(String text) {
        return text == null ? 0 : text.getBytes(StandardCharsets.UTF_8).length;
    }
}
