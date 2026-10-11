package Components.Repository;

import Components.Server.RedisConfig;
import Components.Service.RespSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The decision of whether a write fits the memory ceiling, and which keys are removed when
 * it does not. These tests wire a store and a config straight into the manager, so each
 * decision is made against a keyspace built for that one case.
 */
class MemoryManagerTest {

    private Store store;
    private RedisConfig config;
    private MemoryManager manager;

    @BeforeEach
    void wireUp() {
        store = new Store();
        store.respSerializer = new RespSerializer();
        config = new RedisConfig();
        manager = new MemoryManager();
        manager.store = store;
        manager.redisConfig = config;
    }

    private static long entry(String key) {
        return MemoryEstimator.entryBytes(key, "v", LocalDateTime.MAX);
    }

    @Test
    void withNoLimitEveryWriteIsAdmittedUntouched() {
        config.setMaxmemory(0);
        MemoryManager.Admission admission = manager.admit("anything", 1_000_000_000L);

        assertFalse(admission.rejected());
        assertEquals(List.of(), admission.evictedKeys());
        assertEquals(0, store.currentBytes());
    }

    @Test
    void noevictionRefusesRatherThanRemovingAKey() {
        store.set("a", "v");
        config.setMaxmemory(store.currentBytes());
        config.setMaxmemoryPolicy("noeviction");

        MemoryManager.Admission admission = manager.admit("b", entry("b"));

        assertTrue(admission.rejected());
        assertEquals(MemoryManager.OOM_ERROR, admission.error());
        // refusing a write leaves the keyspace exactly as it was
        assertNotNull(store.peekValue("a"));
        assertNull(store.peekValue("b"));
        assertEquals(0, store.evictedKeys());
    }

    @Test
    void aWriteThatFitsIsAdmittedWithoutEvicting() {
        store.set("a", "v");
        config.setMaxmemory(store.currentBytes() + entry("b"));
        config.setMaxmemoryPolicy("noeviction");

        MemoryManager.Admission admission = manager.admit("b", entry("b"));

        assertFalse(admission.rejected());
        assertEquals(List.of(), admission.evictedKeys());
        assertNotNull(store.peekValue("a"));
    }

    @Test
    void allkeysLruRemovesTheLeastRecentlyUsedKey() {
        store.set("aaa", "v");
        store.set("bbb", "v");
        store.set("ccc", "v");
        // reading aaa makes bbb, untouched since it was written, the least recently used
        store.get("aaa");

        config.setMaxmemory(entry("aaa") * 3);
        config.setMaxmemoryPolicy("allkeys-lru");

        MemoryManager.Admission admission = manager.admit("ddd", entry("ddd"));

        assertFalse(admission.rejected());
        assertEquals(List.of("bbb"), admission.evictedKeys());
        assertNull(store.peekValue("bbb"));
        assertNotNull(store.peekValue("aaa"));
        assertNotNull(store.peekValue("ccc"));
        assertEquals(1, store.evictedKeys());
    }

    @Test
    void aWriteOverTheLimitEvictsUntilItFits() {
        store.set("aaa", "v");
        store.set("bbb", "v");
        store.set("ccc", "v");
        store.get("aaa");
        store.get("ccc");
        // room for the new key and one of the three already there, so two must go
        config.setMaxmemory(entry("ddd") * 2);
        config.setMaxmemoryPolicy("allkeys-lru");

        MemoryManager.Admission admission = manager.admit("ddd", entry("ddd"));

        assertFalse(admission.rejected());
        // bbb goes first as the least recently used, then aaa, until only ccc and the new
        // key remain
        assertEquals(List.of("bbb", "aaa"), admission.evictedKeys());
        assertNull(store.peekValue("bbb"));
        assertNull(store.peekValue("aaa"));
        assertNotNull(store.peekValue("ccc"));
        assertEquals(2, store.evictedKeys());
    }

    @Test
    void aVolatilePolicyWillNotRemoveAKeyWithoutADeadline() {
        store.set("keep", "v");
        config.setMaxmemory(store.currentBytes());
        config.setMaxmemoryPolicy("volatile-lru");

        // nothing in the keyspace carries a deadline, so the policy has no candidate and
        // the write is refused the way noeviction would refuse it
        MemoryManager.Admission admission = manager.admit("new", entry("new"));

        assertTrue(admission.rejected());
        assertNotNull(store.peekValue("keep"));
    }

    @Test
    void aVolatilePolicyRemovesAKeyWithADeadlineAndLeavesTheRest() {
        store.set("keep", "v");
        store.set("vol1", "v", 600_000);
        config.setMaxmemory(store.currentBytes());
        config.setMaxmemoryPolicy("volatile-lru");

        MemoryManager.Admission admission = manager.admit("vol2", entry("vol2"));

        assertFalse(admission.rejected());
        assertEquals(List.of("vol1"), admission.evictedKeys());
        assertNull(store.peekValue("vol1"));
        assertNotNull(store.peekValue("keep"));
    }

    @Test
    void volatileTtlRemovesTheKeyWithTheNearestDeadline() {
        store.set("soon", "v", 60_000);
        store.set("late", "v", 6_000_000);
        config.setMaxmemory(store.currentBytes());
        config.setMaxmemoryPolicy("volatile-ttl");

        MemoryManager.Admission admission = manager.admit("new", entry("new"));

        assertFalse(admission.rejected());
        assertEquals(List.of("soon"), admission.evictedKeys());
        assertNotNull(store.peekValue("late"));
    }

    @Test
    void anAllkeysRandomPolicyRemovesOneOfTheKeysOnOffer() {
        store.set("aaa", "v");
        store.set("bbb", "v");
        store.set("ccc", "v");
        config.setMaxmemory(entry("aaa") * 3);
        config.setMaxmemoryPolicy("allkeys-random");

        MemoryManager.Admission admission = manager.admit("ddd", entry("ddd"));

        assertFalse(admission.rejected());
        assertEquals(1, admission.evictedKeys().size());
        assertTrue(Set.of("aaa", "bbb", "ccc").contains(admission.evictedKeys().get(0)),
                "evicted a key that was never a candidate: " + admission.evictedKeys());
    }

    @Test
    void theKeyBeingWrittenIsNeverTheOneEvicted() {
        store.set("target", "v");
        store.set("other", "v");
        store.get("other");
        // replacing target with a larger value overshoots the limit, but the write cannot
        // make room by removing the very key it is writing
        config.setMaxmemory(store.currentBytes());
        config.setMaxmemoryPolicy("allkeys-lru");

        long biggerValue = MemoryEstimator.entryBytes("target", "vvvvv", LocalDateTime.MAX);
        MemoryManager.Admission admission = manager.admit("target", biggerValue);

        assertFalse(admission.rejected());
        assertEquals(List.of("other"), admission.evictedKeys());
        assertNotNull(store.peekValue("target"));
        assertNull(store.peekValue("other"));
    }

    @Test
    void aTransactionIsRefusedWholeWhenItWouldNotFit() {
        store.set("a", "v");
        config.setMaxmemory(store.currentBytes() + entry("b"));

        Map<String, Value> view = new HashMap<>();
        view.put("b", new Value("v", LocalDateTime.now(), LocalDateTime.MAX));
        view.put("c", new Value("v", LocalDateTime.now(), LocalDateTime.MAX));

        assertEquals(MemoryManager.OOM_ERROR, manager.admitTransaction(view));
    }

    @Test
    void aTransactionThatFitsExactlyIsAdmitted() {
        store.set("a", "v");
        config.setMaxmemory(store.currentBytes() + entry("b"));

        Map<String, Value> view = new HashMap<>();
        view.put("b", new Value("v", LocalDateTime.now(), LocalDateTime.MAX));

        assertNull(manager.admitTransaction(view));
    }

    @Test
    void aTransactionThatDeletesEnoughIsAdmitted() {
        store.set("big", "value that will go away");
        config.setMaxmemory(store.currentBytes());

        Map<String, Value> view = new HashMap<>();
        Value deleted = new Value("gone", LocalDateTime.now(), LocalDateTime.MAX);
        deleted.isDeletedInTransaction = true;
        view.put("big", deleted);

        assertNull(manager.admitTransaction(view));
    }

    @Test
    void aTransactionIsNotJudgedWhenThereIsNoLimit() {
        config.setMaxmemory(0);
        Map<String, Value> view = new HashMap<>();
        view.put("b", new Value("v", LocalDateTime.now(), LocalDateTime.MAX));

        assertNull(manager.admitTransaction(view));
    }
}
