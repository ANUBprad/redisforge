package Components.Infra;

import Components.Repository.Store;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The snapshot a full resync hands over, and the way a store takes one and installs one.
 *
 * <p>No server is started here: the codec is exercised byte for byte, and a store built
 * directly - without one host behind it - is what is snapshotted and installed into, so
 * the format can be made and unmade at word level, and a mangled byte is refused rather
 * than half applied.</p>
 */
class StoreSnapshotTest {

    @Test
    void aSnapshotCarriesItsKeysValuesAndDeadlinesWhole() throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        List<StoreSnapshot.Entry> entries = List.of(
                new StoreSnapshot.Entry("plain", "value", null),
                new StoreSnapshot.Entry("timed", "value", deadline),
                new StoreSnapshot.Entry("कुंजी", "मूल्य \uD83C\uDF0D", 123_456_789_012L));

        List<StoreSnapshot.Entry> back = StoreSnapshot.parse(StoreSnapshot.serialize(entries));

        assertEquals(entries.size(), back.size());
        for (int i = 0; i < back.size(); i++) {
            assertEquals(entries.get(i).key(), back.get(i).key());
            assertEquals(entries.get(i).value(), back.get(i).value());
            assertEquals(entries.get(i).deadlineEpochMillis(), back.get(i).deadlineEpochMillis());
        }
    }

    @Test
    void anEmptySnapshotRoundTripsToo() throws Exception {
        assertTrue(StoreSnapshot.parse(StoreSnapshot.serialize(List.of())).isEmpty());
    }

    @Test
    void serializationIsCanonicalWhateverOrderTheKeysAreGivenIn() {
        long deadline = System.currentTimeMillis() + 60_000;
        List<StoreSnapshot.Entry> firstOrder = List.of(
                new StoreSnapshot.Entry("b", "2", null),
                new StoreSnapshot.Entry("a", "1", null),
                new StoreSnapshot.Entry("c", "3", deadline));
        List<StoreSnapshot.Entry> shuffled = List.of(
                new StoreSnapshot.Entry("c", "3", deadline),
                new StoreSnapshot.Entry("a", "1", null),
                new StoreSnapshot.Entry("b", "2", null));

        assertArrayEquals(StoreSnapshot.serialize(firstOrder), StoreSnapshot.serialize(shuffled),
                "the same keys came out different bytes in a different input order");
    }

    @Test
    void bytesThatAreNotASnapshotAreRefusedWhole() {
        assertThrows(java.io.IOException.class, () -> StoreSnapshot.parse(new byte[0]));
        assertThrows(java.io.IOException.class, () -> StoreSnapshot.parse(new byte[]{1, 2, 3}));

        byte[] wrongMagic = header();
        for (int i = 0; i < 4; i++) {
            wrongMagic[i] = 'X';
        }
        assertThrows(java.io.IOException.class, () -> StoreSnapshot.parse(wrongMagic),
                "bytes that do not start with the magic were taken for a snapshot");
    }

    @Test
    void aVersionThisBuildDoesNotUnderstandIsRefused() throws Exception {
        byte[] unknownVersion = validSnapshot();
        unknownVersion[4] = 2;
        assertThrows(java.io.IOException.class, () -> StoreSnapshot.parse(unknownVersion),
                "a snapshot of a newer version was taken for this one");
    }

    @Test
    void aCountThePayloadCouldNeverHoldIsRefused() {
        ByteBuffer tooMany = ByteBuffer.allocate(12);
        tooMany.put(header());
        tooMany.putInt(1_000_000);
        assertThrows(java.io.IOException.class, () -> StoreSnapshot.parse(tooMany.array()),
                "a count that needs more bytes than remain was trusted");
    }

    @Test
    void aDeclaredLengthThatRunsPastTheEndOfThePayloadIsRefused() throws Exception {
        byte[] valid = StoreSnapshot.serialize(List.of(new StoreSnapshot.Entry("k", "v", null)));
        byte[] missingTheValue = Arrays.copyOf(valid, valid.length - 1);

        assertThrows(java.io.IOException.class, () -> StoreSnapshot.parse(missingTheValue),
                "a value declared larger than the payload was trusted");
    }

    @Test
    void aDeadlineFlagThatIsNeitherAgingNorPermanentIsRefused() throws Exception {
        byte[] valid = validSnapshot();
        valid[12] = 7;
        assertThrows(java.io.IOException.class, () -> StoreSnapshot.parse(valid),
                "an unknown deadline flag was accepted");
    }

    @Test
    void bytesBehindTheDeclaredSnapshotAreRefused() throws Exception {
        byte[] valid = validSnapshot();
        byte[] padded = new byte[valid.length + 1];
        System.arraycopy(valid, 0, padded, 0, valid.length);
        padded[valid.length] = 'x';

        assertThrows(java.io.IOException.class, () -> StoreSnapshot.parse(padded),
                "bytes behind the declared snapshot were ignored");
    }

    @Test
    void liveSnapshotTakesTheLiveKeysAtOneInstantWithTheirDeadlines() throws Exception {
        Store store = new Store();
        store.set("plain", "value");
        store.set("timed", "value", 5_000);
        store.set("gone", "value", 1);
        Thread.sleep(20);

        List<StoreSnapshot.Entry> snapshot = store.liveSnapshot();

        assertEquals(List.of("plain", "timed"),
                snapshot.stream().map(StoreSnapshot.Entry::key).toList(),
                "the snapshot carried a key that was not live, or lost order");
        assertNull(snapshot.get(0).deadlineEpochMillis(),
                "a key with no deadline picked one up in the snapshot");
        long remaining = snapshot.get(1).deadlineEpochMillis() - System.currentTimeMillis();
        assertTrue(remaining > 3_000 && remaining <= 5_000,
                "the snapshot did not carry the absolute deadline: " + remaining + "ms left");
    }

    @Test
    void installingASnapshotReplacesTheWholeStore() {
        Store store = new Store();
        store.set("a", "stale");
        store.set("gone", "stale");

        store.installSnapshot(List.of(
                new StoreSnapshot.Entry("a", "fresh", null),
                new StoreSnapshot.Entry("b", "added", null)));

        assertEquals("fresh", store.getValue("a").val);
        assertEquals("added", store.getValue("b").val);
        assertNull(store.getValue("gone"),
                "a key the snapshot did not carry survived the install");
    }

    @Test
    void installingDropsAKeyWhoseDeadlineHasAlreadyPassed() {
        Store store = new Store();
        store.installSnapshot(List.of(
                new StoreSnapshot.Entry("old", "value", System.currentTimeMillis() - 100),
                new StoreSnapshot.Entry("current", "value", null)));

        assertNull(store.getValue("old"),
                "a key whose deadline had passed by the time it was installed was resurrected");
        assertEquals("value", store.getValue("current").val);
    }

    @Test
    void aSnapshotMovesFromOneStoreIntoAnotherWithValuesAndDeadlinesWhole() throws Exception {
        Store source = new Store();
        source.set("alpha", "one");
        source.set("beta", "two", 10_000);
        Store target = new Store();
        target.set("stale", "x");

        List<StoreSnapshot.Entry> entries =
                StoreSnapshot.parse(StoreSnapshot.serialize(source.liveSnapshot()));
        target.installSnapshot(entries);

        assertEquals("one", target.getValue("alpha").val);
        assertEquals("two", target.getValue("beta").val);
        assertNull(target.getValue("stale"));
        assertEquals(source.absoluteExpiryMillis(source.peekValue("beta")),
                target.absoluteExpiryMillis(target.peekValue("beta")),
                "the deadline did not survive the move from one store to the other");
        assertNull(target.absoluteExpiryMillis(target.peekValue("alpha")),
                "a no-deadline key came out of the move with a deadline");
    }

    /** An empty snapshot with magic, version and a count of one, to corrupt per test. */
    private static byte[] validSnapshot() throws Exception {
        return StoreSnapshot.serialize(List.of(new StoreSnapshot.Entry("k", "v", null)));
    }

    private static byte[] header() {
        ByteBuffer header = ByteBuffer.allocate(8);
        header.put(StoreSnapshot.MAGIC.getBytes(StandardCharsets.US_ASCII));
        header.put((byte) StoreSnapshot.VERSION);
        header.put((byte) 0);
        header.put((byte) 0);
        header.put((byte) 0);
        return header.array();
    }
}