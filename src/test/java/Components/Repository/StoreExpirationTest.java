package Components.Repository;

import Components.Service.RespSerializer;
import Config.AppConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The store-level expiration surface: the deadline commands, the reads that report the
 * remaining time, PERSIST taking a deadline back off, and the bounded active sweep.
 */
@SpringBootTest(classes = AppConfig.class)
class StoreExpirationTest {

    @Autowired
    private Store store;
    @Autowired
    private RespSerializer respSerializer;

    @BeforeEach
    void reset() {
        store.map.clear();
    }

    @Test
    void ttlAndPttlReportMissingAndPersistentKeys() {
        assertEquals(respSerializer.respInteger(-2), store.ttl("nope"));
        assertEquals(respSerializer.respInteger(-2), store.pttl("nope"));

        store.set("persistent", "value");
        assertEquals(respSerializer.respInteger(-1), store.ttl("persistent"));
        assertEquals(respSerializer.respInteger(-1), store.pttl("persistent"));
    }

    @Test
    void expireSetsADeadlineAndTtlReportsItInSeconds() {
        store.set("k", "v");

        assertEquals(1, store.expire("k", 100));
        String ttl = store.ttl("k");
        int seconds = Integer.parseInt(ttl.substring(1).trim());
        assertEquals(100, seconds);
        assertNotNull(store.peekValue("k").expiry);
    }

    @Test
    void pexpireUsesMilliseconds() {
        store.set("k", "v");

        assertEquals(1, store.pexpire("k", 2500));
        int millis = Integer.parseInt(store.pttl("k").substring(1).trim());
        assertEquals(2500, millis, 200);
    }

    @Test
    void expireAtAndPexpireAtUseAbsoluteInstants() {
        store.set("seconds", "v");
        store.set("millis", "v");
        long nowSeconds = System.currentTimeMillis() / 1000;
        long nowMillis = System.currentTimeMillis();

        assertEquals(1, store.expireAt("seconds", nowSeconds + 100));
        assertEquals(1, store.pexpireAt("millis", nowMillis + 100_000));

        assertEquals(100, Integer.parseInt(store.ttl("seconds").substring(1).trim()), 1);
        assertEquals(100_000, Integer.parseInt(store.pttl("millis").substring(1).trim()), 500);
    }

    @Test
    void aDeadlineInThePastDeletesTheKey() {
        store.set("k", "v");

        assertEquals(1, store.expire("k", 0));
        assertNull(store.getValue("k"));
        assertEquals(respSerializer.respInteger(-2), store.ttl("k"));
    }

    @Test
    void expirationOnAMissingKeyChangesNothing() {
        assertEquals(0, store.expire("missing", 100));
        assertEquals(0, store.pexpire("missing", 100));
        assertEquals(0, store.pexpireAt("missing", System.currentTimeMillis() + 1000));
        assertEquals(0, store.persist("missing"));
    }

    @Test
    void persistRemovesTheDeadline() {
        store.set("k", "v");
        store.expire("k", 100);

        assertEquals(1, store.persist("k"));
        assertEquals(respSerializer.respInteger(-1), store.ttl("k"));
        // a second PERSIST has nothing left to remove
        assertEquals(0, store.persist("k"));
    }

    @Test
    void anExpiredKeyIsTreatedAsMissingOnRead() throws InterruptedException {
        store.set("k", "v");
        store.pexpire("k", 30);

        Thread.sleep(80);

        assertEquals("$-1\r\n", store.get("k"));
        assertEquals(respSerializer.respInteger(-2), store.ttl("k"));
        assertNull(store.getValue("k"));
    }

    @Test
    void theExpirySurvivesARewriteOfTheValueAndIncrementKeepsIt() {
        store.set("k", "1");
        store.expire("k", 100);

        assertEquals(":2\r\n", store.increment("k"));
        LocalDateTime deadline = store.peekValue("k").expiry;
        assertNotNull(deadline);
        assertEquals(100, Integer.parseInt(store.ttl("k").substring(1).trim()), 1);

        // setting the key again drops the old deadline unless the command carries one
        store.set("k", "9");
        assertEquals(respSerializer.respInteger(-1), store.ttl("k"));
    }

    @Test
    void theActiveSweepRemovesExpiredKeysAndLeavesLiveOnes() {
        Set<String> expiredPrefix = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            String key = "sweep:dead:" + i;
            store.set(key, "v");
            store.pexpire(key, 1);
            expiredPrefix.add(key);
        }
        for (int i = 0; i < 50; i++) {
            store.set("sweep:live:" + i, "v");
        }

        try {
            Thread.sleep(30);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // a few bounded passes are enough for the sample to reach every expired key here
        for (int pass = 0; pass < 5; pass++) {
            store.removeExpired();
        }

        for (String key : expiredPrefix) {
            assertNull(store.peekValue(key), "an expired key survived the active sweep: " + key);
        }
        for (int i = 0; i < 50; i++) {
            assertNotNull(store.peekValue("sweep:live:" + i), "the sweep dropped a live key");
        }
    }

    @Test
    void aDeadlineIsStoredAsAnAbsoluteInstantNotARelativeDelay() {
        store.set("k", "v");
        store.expire("k", 100);

        long millis = store.peekValue("k").expiry
                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long expected = System.currentTimeMillis() + 100_000;
        assertEquals(expected, millis, 1_000);
    }
}
