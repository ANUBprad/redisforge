package Components.Infra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The dataset a full resync hands over: the live keys of the store, serialized into
 * versioned binary form, and decoded back into the entries a replica installs.
 *
 * <p>This is an internal format owned by this project, not a Redis RDB. It names no
 * compatibility with any other server, so its bytes can change between versions as long
 * as the version byte rejects what it does not understand.</p>
 *
 * <p>Layout, big endian throughout: an 8 byte header (the magic {@code RFSS}, a version
 * byte and three reserved bytes), a four byte entry count, and one entry per key. Each
 * entry is a deadline flag byte, the epoch millis of the deadline when the flag is 1, a
 * four byte key length and the key's UTF-8 bytes, then a four byte value length and the
 * value's UTF-8 bytes. A key with no deadline carries the flag 0 and nothing after it.</p>
 *
 * <p>Every length is checked against what remains before the bytes are read, and the count
 * against what is too few bytes to possibly hold it, so a snapshot that was cut short or
 * spliced is refused whole rather than half applied. The magic and version are checked
 * first, so bytes that are not a snapshot at all fail before any field is trusted.</p>
 */
public final class StoreSnapshot {

    public static final String MAGIC = "RFSS";
    public static final int VERSION = 1;
    /** the magic, the version and the reserved bytes */
    private static final int HEADER_LENGTH = 8;
    /** how large one key or value may be, so a mangled length cannot ask for an out of memory read */
    private static final long MAX_FIELD_BYTES = 512L * 1024 * 1024;

    /** One live key: its name, its value and the epoch millis of its deadline, or null for none. */
    public record Entry(String key, String value, Long deadlineEpochMillis) {
    }

    private StoreSnapshot() {
    }

    /**
     * Encodes entries into the snapshot bytes, in a canonical order, so the bytes a
     * dataset is handed over as do not depend on the hash order of the map that held it.
     */
    public static byte[] serialize(List<Entry> entries) {
        List<Entry> ordered = new ArrayList<>(entries);
        ordered.sort(Comparator.comparing(Entry::key));

        int total = HEADER_LENGTH + 4;
        for (Entry entry : ordered) {
            total += (entry.deadlineEpochMillis() == null ? 1 : 9)
                    + 8 + utf8Length(entry.key()) + utf8Length(entry.value());
        }

        ByteBuffer out = ByteBuffer.allocate(total);
        out.put(MAGIC.getBytes(StandardCharsets.US_ASCII));
        out.put((byte) VERSION);
        out.put((byte) 0);
        out.put((byte) 0);
        out.put((byte) 0);
        out.putInt(ordered.size());
        for (Entry entry : ordered) {
            if (entry.deadlineEpochMillis() == null) {
                out.put((byte) 0);
            } else {
                out.put((byte) 1);
                out.putLong(entry.deadlineEpochMillis());
            }
            writeField(out, entry.key());
            writeField(out, entry.value());
        }
        return out.array();
    }

    /**
     * Decodes the entries a full resync carried.
     *
     * @param data the bytes behind the {@code $<length>} header
     * @return the keys, with their deadlines intact
     * @throws IOException when the bytes are not a snapshot this version understands, or a
     *         declared length runs past the end of the payload
     */
    public static List<Entry> parse(byte[] data) throws IOException {
        if (data == null || data.length < HEADER_LENGTH) {
            throw new IOException("not a store snapshot: only "
                    + (data == null ? 0 : data.length) + " bytes");
        }
        ByteBuffer in = ByteBuffer.wrap(data);
        byte[] magic = new byte[4];
        in.get(magic);
        if (!MAGIC.equals(new String(magic, StandardCharsets.US_ASCII))) {
            throw new IOException("not a store snapshot: bad magic");
        }
        int version = in.get() & 0xFF;
        if (version != VERSION) {
            throw new IOException("store snapshot of an unknown version: " + version);
        }
        in.get();
        in.get();
        in.get();

        long countLong = in.getInt() & 0xFFFFFFFFL;
        // every entry carries at least a flag and two lengths, so a count that needs more
        // bytes than remain could never be satisfied no matter what it declares afterwards
        if (countLong * 9 > in.remaining()) {
            throw new IOException("store snapshot declared " + countLong
                    + " entries in only " + in.remaining() + " bytes");
        }

        List<Entry> entries = new ArrayList<>((int) countLong);
        for (int i = 0; i < countLong; i++) {
            int flag = in.get() & 0xFF;
            if (flag != 0 && flag != 1) {
                throw new IOException("store snapshot entry " + i + " carries an unknown deadline flag: " + flag);
            }
            Long deadline = flag == 1 ? in.getLong() : null;
            String key = readField(in, "key", i);
            String value = readField(in, "value", i);
            entries.add(new Entry(key, value, deadline));
        }
        if (in.hasRemaining()) {
            throw new IOException("store snapshot had " + in.remaining() + " trailing bytes");
        }
        return entries;
    }

    private static void writeField(ByteBuffer out, String field) {
        byte[] bytes = field.getBytes(StandardCharsets.UTF_8);
        out.putInt(bytes.length);
        out.put(bytes);
    }

    private static String readField(ByteBuffer in, String kind, int index) throws IOException {
        if (in.remaining() < 4) {
            throw new IOException("store snapshot entry " + index + " has no " + kind + " length");
        }
        long length = in.getInt() & 0xFFFFFFFFL;
        if (length > MAX_FIELD_BYTES) {
            throw new IOException("store snapshot entry " + index + " declares a " + kind
                    + " of " + length + " bytes");
        }
        if (length > in.remaining()) {
            throw new IOException("store snapshot entry " + index + " declares a " + kind
                    + " of " + length + " bytes but only " + in.remaining() + " remain");
        }
        byte[] bytes = new byte[(int) length];
        in.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static int utf8Length(String text) {
        return text.getBytes(StandardCharsets.UTF_8).length;
    }
}