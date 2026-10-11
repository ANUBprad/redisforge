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

    /**
     * The version that carries every value kind. Version {@link #VERSION} keeps its
     * string-only form so an older snapshot is still read; a dataset written by this
     * build is this version.
     */
    public static final int DATASET_VERSION = 2;

    /** One live key: its name, its value and the epoch millis of its deadline, or null for none. */
    public record Entry(String key, String value, Long deadlineEpochMillis) {
    }

    /** The kind a host holds at a key, in the typed dataset format. */
    public enum Kind { STRING, HASH, LIST, SET }

    /**
     * One live key of any kind: its name, its kind, its content and the epoch millis of
     * its deadline, or null for none. The content is the value's own bytes for a string,
     * {@code field, value, field, value, ...} for a hash, the head-to-tail elements for a
     * list, and the members for a set.
     */
    public record TypedEntry(String key, Kind kind, List<String> content, Long deadlineEpochMillis) {
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

    /**
     * Encodes typed entries into the dataset bytes, in canonical key order. A hash's
     * fields, a list's elements and a set's members travel in the order the store keeps
     * them, so a replica installs the same ordering the master had.
     */
    public static byte[] serializeDataset(List<TypedEntry> entries) {
        List<TypedEntry> ordered = new ArrayList<>(entries);
        ordered.sort(Comparator.comparing(TypedEntry::key));

        int total = HEADER_LENGTH + 4;
        for (TypedEntry entry : ordered) {
            total += (entry.deadlineEpochMillis() == null ? 1 : 9) + 1 + 4 + utf8Length(entry.key());
            total += contentLength(entry);
        }

        ByteBuffer out = ByteBuffer.allocate(total);
        out.put(MAGIC.getBytes(StandardCharsets.US_ASCII));
        out.put((byte) DATASET_VERSION);
        out.put((byte) 0);
        out.put((byte) 0);
        out.put((byte) 0);
        out.putInt(ordered.size());
        for (TypedEntry entry : ordered) {
            if (entry.deadlineEpochMillis() == null) {
                out.put((byte) 0);
            } else {
                out.put((byte) 1);
                out.putLong(entry.deadlineEpochMillis());
            }
            out.put((byte) entry.kind().ordinal());
            writeField(out, entry.key());
            List<String> content = entry.content();
            switch (entry.kind()) {
                case STRING -> writeField(out, content.isEmpty() ? "" : content.get(0));
                case HASH -> {
                    out.putInt(content.size() / 2);
                    for (int i = 0; i + 1 < content.size(); i += 2) {
                        writeField(out, content.get(i));
                        writeField(out, content.get(i + 1));
                    }
                }
                case LIST, SET -> {
                    out.putInt(content.size());
                    for (String element : content) {
                        writeField(out, element);
                    }
                }
            }
        }
        return out.array();
    }

    /**
     * Decodes the dataset a full resync carried. A version 1 snapshot is a string-only
     * dataset and is read as such, so a replica can still take a handover from an older
     * host; every value in the current format carries its own kind.
     *
     * @throws IOException when the bytes are not a snapshot this build understands, or a
     *         declared length runs past the end of the payload
     */
    public static List<TypedEntry> parseDataset(byte[] data) throws IOException {
        if (data == null || data.length < HEADER_LENGTH) {
            throw new IOException("not a store snapshot: only "
                    + (data == null ? 0 : data.length) + " bytes");
        }
        byte[] magic = new byte[4];
        System.arraycopy(data, 0, magic, 0, 4);
        if (!MAGIC.equals(new String(magic, StandardCharsets.US_ASCII))) {
            throw new IOException("not a store snapshot: bad magic");
        }
        int version = data[4] & 0xFF;
        if (version == VERSION) {
            List<TypedEntry> entries = new ArrayList<>();
            for (Entry entry : parse(data)) {
                entries.add(new TypedEntry(entry.key(), Kind.STRING,
                        List.of(entry.value()), entry.deadlineEpochMillis()));
            }
            return entries;
        }
        if (version != DATASET_VERSION) {
            throw new IOException("store snapshot of an unknown version: " + version);
        }

        ByteBuffer in = ByteBuffer.wrap(data);
        in.position(HEADER_LENGTH);
        long countLong = in.getInt() & 0xFFFFFFFFL;
        if (countLong * 2 > in.remaining()) {
            throw new IOException("store snapshot declared " + countLong
                    + " entries in only " + in.remaining() + " bytes");
        }

        List<TypedEntry> entries = new ArrayList<>((int) countLong);
        for (int i = 0; i < countLong; i++) {
            int flag = in.get() & 0xFF;
            if (flag != 0 && flag != 1) {
                throw new IOException("store snapshot entry " + i + " carries an unknown deadline flag: " + flag);
            }
            Long deadline = flag == 1 ? in.getLong() : null;
            int kindOrdinal = in.get() & 0xFF;
            if (kindOrdinal >= Kind.values().length) {
                throw new IOException("store snapshot entry " + i + " carries an unknown kind: " + kindOrdinal);
            }
            Kind kind = Kind.values()[kindOrdinal];
            String key = readField(in, "key", i);
            List<String> content = new ArrayList<>();
            switch (kind) {
                case STRING -> content.add(readField(in, "value", i));
                case HASH -> {
                    int fields = readCount(in, "hash fields", i);
                    for (int f = 0; f < fields; f++) {
                        content.add(readField(in, "field", i));
                        content.add(readField(in, "field value", i));
                    }
                }
                case LIST -> {
                    int elements = readCount(in, "list elements", i);
                    for (int e = 0; e < elements; e++) {
                        content.add(readField(in, "element", i));
                    }
                }
                case SET -> {
                    int members = readCount(in, "set members", i);
                    for (int m = 0; m < members; m++) {
                        content.add(readField(in, "member", i));
                    }
                }
            }
            entries.add(new TypedEntry(key, kind, content, deadline));
        }
        if (in.hasRemaining()) {
            throw new IOException("store snapshot had " + in.remaining() + " trailing bytes");
        }
        return entries;
    }

    private static int contentLength(TypedEntry entry) {
        List<String> content = entry.content();
        return switch (entry.kind()) {
            case STRING -> 4 + utf8Length(content.isEmpty() ? "" : content.get(0));
            case HASH -> {
                int bytes = 4;
                for (int i = 0; i + 1 < content.size(); i += 2) {
                    bytes += 4 + utf8Length(content.get(i)) + 4 + utf8Length(content.get(i + 1));
                }
                yield bytes;
            }
            case LIST, SET -> {
                int bytes = 4;
                for (String element : content) {
                    bytes += 4 + utf8Length(element);
                }
                yield bytes;
            }
        };
    }

    private static int readCount(ByteBuffer in, String kind, int index) throws IOException {
        if (in.remaining() < 4) {
            throw new IOException("store snapshot entry " + index + " has no " + kind + " count");
        }
        long count = in.getInt() & 0xFFFFFFFFL;
        if (count > in.remaining() / 4) {
            throw new IOException("store snapshot entry " + index + " declares " + count
                    + " " + kind + " in only " + in.remaining() + " bytes");
        }
        return (int) count;
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