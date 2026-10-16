package com.redisforge.store;

import com.redisforge.store.MemoryEstimator;
import com.redisforge.store.Store;
import com.redisforge.store.Value;
import com.redisforge.protocol.RespSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The dataset format a full resync hands over: every value kind survives the round trip,
 * a version 1 snapshot is still read as the strings it holds, and a damaged payload is
 * refused whole rather than half applied.
 */
class TypedSnapshotTest {

    @Test
    void everyKindRoundTripsWithItsContentAndDeadline() throws IOException {
        long deadline = System.currentTimeMillis() + 60_000;
        List<StoreSnapshot.TypedEntry> entries = List.of(
                new StoreSnapshot.TypedEntry("str", StoreSnapshot.Kind.STRING,
                        List.of("hello"), null),
                new StoreSnapshot.TypedEntry("hash", StoreSnapshot.Kind.HASH,
                        List.of("a", "1", "b", "2"), deadline),
                new StoreSnapshot.TypedEntry("list", StoreSnapshot.Kind.LIST,
                        List.of("x", "y", "z"), null),
                new StoreSnapshot.TypedEntry("set", StoreSnapshot.Kind.SET,
                        List.of("m", "n"), null));

        List<StoreSnapshot.TypedEntry> decoded = StoreSnapshot.parseDataset(StoreSnapshot.serializeDataset(entries));

        // the codec hands the entries back in canonical key order
        assertEquals(List.of("hash", "list", "set", "str"),
                decoded.stream().map(StoreSnapshot.TypedEntry::key).toList());
        StoreSnapshot.TypedEntry str = byKey(decoded, "str");
        assertEquals(StoreSnapshot.Kind.STRING, str.kind());
        assertEquals(List.of("hello"), str.content());
        assertNull(str.deadlineEpochMillis());
        assertEquals(deadline, byKey(decoded, "hash").deadlineEpochMillis());
        assertEquals(List.of("a", "1", "b", "2"), byKey(decoded, "hash").content());
        assertEquals(List.of("x", "y", "z"), byKey(decoded, "list").content());
        assertEquals(List.of("m", "n"), byKey(decoded, "set").content());
    }

    @Test
    void anEmptyStringValueSurvivesTheRoundTrip() throws IOException {
        byte[] bytes = StoreSnapshot.serializeDataset(List.of(
                new StoreSnapshot.TypedEntry("k", StoreSnapshot.Kind.STRING, List.of(""), null)));
        assertEquals("", byKey(StoreSnapshot.parseDataset(bytes), "k").content().get(0));
    }

    @Test
    void aVersionOneSnapshotIsReadAsTheStringsItHolds() throws IOException {
        byte[] v1 = StoreSnapshot.serialize(List.of(
                new StoreSnapshot.Entry("legacy", "value", null)));

        List<StoreSnapshot.TypedEntry> decoded = StoreSnapshot.parseDataset(v1);

        assertEquals(1, decoded.size());
        assertEquals(StoreSnapshot.Kind.STRING, decoded.get(0).kind());
        assertEquals(List.of("value"), decoded.get(0).content());
    }

    @Test
    void aPayloadThatIsNotADatasetIsRefused() {
        assertThrows(IOException.class, () -> StoreSnapshot.parseDataset(null));
        assertThrows(IOException.class, () -> StoreSnapshot.parseDataset(new byte[]{1, 2, 3}));
        assertThrows(IOException.class, () -> StoreSnapshot.parseDataset("nope".getBytes()));
    }

    @Test
    void aTrailingByteAfterTheLastEntryIsRefused() throws IOException {
        byte[] clean = StoreSnapshot.serializeDataset(List.of(
                new StoreSnapshot.TypedEntry("k", StoreSnapshot.Kind.STRING, List.of("v"), null)));
        byte[] padded = new byte[clean.length + 1];
        System.arraycopy(clean, 0, padded, 0, clean.length);
        assertThrows(IOException.class, () -> StoreSnapshot.parseDataset(padded));
    }

    @Test
    void aStoreHandsItsTypedKeysOverAndInstallsThemBack() throws IOException {
        Store master = new Store();
        master.respSerializer = new RespSerializer();
        long deadline = System.currentTimeMillis() + 60_000;
        Value hash = Value.hash(LocalDateTime.now(), LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(deadline), java.time.ZoneId.systemDefault()));
        hash.hash.put("field", "value");
        master.commit("h", hash);
        Value list = Value.list(LocalDateTime.now(), LocalDateTime.MAX);
        list.list.add("only");
        master.commit("l", list);

        byte[] bytes = StoreSnapshot.serializeDataset(master.liveTypedSnapshot());

        Store replica = new Store();
        replica.respSerializer = new RespSerializer();
        replica.installTypedSnapshot(StoreSnapshot.parseDataset(bytes));

        Value recoveredHash = replica.getValue("h");
        assertEquals(Value.Kind.HASH, recoveredHash.kind);
        assertEquals("value", recoveredHash.hash.get("field"));
        assertEquals(deadline, replica.absoluteExpiryMillis(recoveredHash));
        Value recoveredList = replica.getValue("l");
        assertEquals(Value.Kind.LIST, recoveredList.kind);
        assertEquals(List.of("only"), new ArrayList<>(recoveredList.list));
        assertEquals(MemoryEstimator.entryBytes("h", recoveredHash) + MemoryEstimator.entryBytes("l", recoveredList),
                replica.currentBytes(), "the installed store's byte count did not match its entries");
    }

    @Test
    void installingADatasetReplacesEverythingAndSkipsPastDeadlines() {
        Store replica = new Store();
        replica.respSerializer = new RespSerializer();
        replica.commit("stale", new Value("old", LocalDateTime.now(), LocalDateTime.MAX));
        long past = System.currentTimeMillis() - 60_000;
        long future = System.currentTimeMillis() + 60_000;

        replica.installTypedSnapshot(List.of(
                new StoreSnapshot.TypedEntry("fresh", StoreSnapshot.Kind.STRING, List.of("v"), null),
                new StoreSnapshot.TypedEntry("expired", StoreSnapshot.Kind.STRING, List.of("v"), past),
                new StoreSnapshot.TypedEntry("soon", StoreSnapshot.Kind.STRING, List.of("v"), future)));

        assertNull(replica.peekValue("stale"), "a key the snapshot did not carry survived the install");
        assertNull(replica.peekValue("expired"), "a key whose deadline had passed was resurrected");
        assertEquals("v", replica.getValue("fresh").val);
        assertTrue(replica.peekValue("soon") != null);
    }

    private static StoreSnapshot.TypedEntry byKey(List<StoreSnapshot.TypedEntry> entries, String key) {
        for (StoreSnapshot.TypedEntry entry : entries) {
            if (entry.key().equals(key)) {
                return entry;
            }
        }
        throw new AssertionError("no entry named " + key);
    }
}
