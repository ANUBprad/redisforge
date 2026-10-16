package com.redisforge.store;

import com.redisforge.store.StoreSnapshot;
import com.redisforge.protocol.RespSerializer;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The byte account the store keeps of its own keyspace. Every write that puts, replaces
 * or removes an entry has to move it by exactly the difference, because that account is
 * what a memory limit is judged against.
 */
class StoreMemoryAccountingTest {

    private static long bytes(String key, String value) {
        return MemoryEstimator.entryBytes(key, value, LocalDateTime.MAX);
    }

    private static Store newStore() {
        Store store = new Store();
        store.respSerializer = new RespSerializer();
        return store;
    }

    @Test
    void puttingReplacingAndDeletingMoveTheAccountByTheDifference() {
        Store store = newStore();
        assertEquals(0, store.currentBytes());

        store.set("k", "v");
        assertEquals(bytes("k", "v"), store.currentBytes());

        store.set("k", "22");
        assertEquals(bytes("k", "22"), store.currentBytes());

        assertTrue(store.delete("k"));
        assertEquals(0, store.currentBytes());

        // a second delete of the same key removes nothing, so it must not move the account
        assertFalse(store.delete("k"));
        assertEquals(0, store.currentBytes());
    }

    @Test
    void aDeadlineIsChargedAndPersistingGivesItBack() {
        Store store = newStore();
        store.set("k", "v", 60_000);
        assertEquals(bytes("k", "v") + MemoryEstimator.EXPIRY_METADATA_BYTES, store.currentBytes());

        store.persist("k");
        assertEquals(bytes("k", "v"), store.currentBytes());
    }

    @Test
    void expireAndPersistThroughTheCommandsMoveTheAccountTheSameWay() {
        Store store = newStore();
        store.set("k", "v");
        assertEquals(bytes("k", "v"), store.currentBytes());

        assertEquals(1, store.expire("k", 100));
        assertEquals(bytes("k", "v") + MemoryEstimator.EXPIRY_METADATA_BYTES, store.currentBytes());

        assertEquals(1, store.persist("k"));
        assertEquals(bytes("k", "v"), store.currentBytes());
    }

    @Test
    void anExpiredKeyIsTakenOffTheAccountWhenItIsRead() throws Exception {
        Store store = newStore();
        store.set("e", "v", 1);
        assertEquals(bytes("e", "v") + MemoryEstimator.EXPIRY_METADATA_BYTES, store.currentBytes());

        Thread.sleep(20);
        store.get("e");
        assertEquals(0, store.currentBytes());
    }

    @Test
    void theActiveCleanupTakesExpiredKeysOffTheAccount() throws Exception {
        Store store = newStore();
        store.set("gone", "v", 1);
        store.set("live", "v");

        Thread.sleep(20);
        store.removeExpired();
        assertEquals(bytes("live", "v"), store.currentBytes());
        assertNotNull(store.peekValue("live"));
    }

    @Test
    void aCounterIsChargedForTheValueItGrowsInto() {
        Store store = newStore();
        store.increment("c");
        assertEquals(bytes("c", "1"), store.currentBytes());

        for (int i = 0; i < 9; i++) {
            store.increment("c");
        }
        assertEquals(bytes("c", "10"), store.currentBytes());
    }

    @Test
    void installingASnapshotRecomputesTheAccountFromTheKeysItLands() {
        Store store = newStore();
        store.set("old", "v");
        assertTrue(store.currentBytes() > 0);

        store.installSnapshot(List.of(new StoreSnapshot.Entry("new", "value", null)));
        assertEquals(bytes("new", "value"), store.currentBytes());
        assertNull(store.peekValue("old"));
        assertNotNull(store.peekValue("new"));
    }

    @Test
    void installingASnapshotChargesADeadlineOnlyWhileItIsStillAhead() {
        Store store = newStore();
        store.installSnapshot(List.of(
                new StoreSnapshot.Entry("live", "v", System.currentTimeMillis() + 100_000),
                new StoreSnapshot.Entry("passed", "v", System.currentTimeMillis() - 1_000)));

        // the entry whose deadline has passed is not carried into the new map at all, and
        // the live one is charged for the metadata its deadline needs
        assertNull(store.peekValue("passed"));
        assertEquals(bytes("live", "v") + MemoryEstimator.EXPIRY_METADATA_BYTES, store.currentBytes());
    }
}
