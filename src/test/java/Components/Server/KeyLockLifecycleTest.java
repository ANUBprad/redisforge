package Components.Server;

import Components.Infra.Client;
import Components.Repository.Store;
import Components.Service.CommandHandler;
import Config.AppConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The key lock striping itself: the number of locks must stay fixed whatever the
 * keyspace grows to, a snapshot of the keys must be a snapshot, and transactions that
 * touch many keys in shuffled or even opposite orders must always finish.
 *
 * <p>No server is involved here. The store is driven directly, so a failure points at
 * the locking rather than at a socket.</p>
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class KeyLockLifecycleTest {

    /** what the striping is pinned to, and what a per-key map could never promise */
    private static final int KEY_LOCK_COUNT = 256;
    private static final int KEYS_TOUCHED = 50_000;
    private static final int SHUFFLE_THREADS = 8;
    private static final int TRANSACTIONS_PER_THREAD = 30;
    private static final int SHARED_KEYS = 40;
    private static final int OPPOSITE_ORDER_ROUNDS = 150;
    private static final long WORKLOAD_BUDGET_SECONDS = 30;

    @Autowired
    private Store store;
    @Autowired
    private CommandHandler commandHandler;

    @Test
    void theLockStripesNeverGrowWithTheKeyspace() {
        for (int i = 0; i < KEYS_TOUCHED; i++) {
            store.set("stripe:" + i, "v" + i);
        }
        assertEquals(KEY_LOCK_COUNT, store.keyLockCount(),
                "the store's lock count moved with the keyspace, so a long running server "
                        + "would keep allocating locks forever");
        // the same number again after a second wave, which is the part a lazily grown
        // structure would get wrong
        for (int i = 0; i < 1_000; i++) {
            store.increment("stripe:" + i);
        }
        assertEquals(KEY_LOCK_COUNT, store.keyLockCount(),
                "touching the keys a second way changed the lock count");
    }

    @Test
    void aKeySnapshotIsTakenOnceAndDoesNotFollowLaterWrites() {
        List<String> first = new ArrayList<>();
        for (int i = 0; i < 2_000; i++) {
            String key = "snap:first:" + i;
            first.add(key);
            store.set(key, "v");
        }

        Set<String> snapshot = store.getKeys();

        List<String> second = new ArrayList<>();
        for (int i = 0; i < 2_000; i++) {
            String key = "snap:second:" + i;
            second.add(key);
            store.set(key, "v");
        }

        // iterating the snapshot while the store keeps moving is the whole point of it
        int seen = 0;
        for (String key : snapshot) {
            if (key.startsWith("snap:first:")) {
                seen++;
            }
        }
        assertEquals(first.size(), seen, "the snapshot lost keys it was taken when they existed");
        for (String key : second) {
            assertFalse(snapshot.contains(key),
                    "the snapshot followed a write that happened after it was taken");
        }
        assertTrue(store.getKeys().containsAll(second),
                "a fresh snapshot did not see the later writes");
        assertThrows(UnsupportedOperationException.class, () -> snapshot.add("snap:mutated"),
                "the snapshot is live enough to change the store through it");
    }

    @Test
    void transactionsOfShuffledKeysAllFinishAndAllLand() throws InterruptedException {
        String[] keys = new String[SHARED_KEYS];
        for (int i = 0; i < SHARED_KEYS; i++) {
            keys[i] = "shuffle:key:" + i;
        }

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(SHUFFLE_THREADS);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        for (int thread = 0; thread < SHUFFLE_THREADS; thread++) {
            final int threadNumber = thread;
            new Thread(() -> {
                try {
                    start.await();
                    for (int round = 0; round < TRANSACTIONS_PER_THREAD; round++) {
                        List<String[]> commands = new ArrayList<>(SHARED_KEYS);
                        for (String key : keys) {
                            commands.add(new String[]{"SET", key, "t" + threadNumber + ":" + round});
                        }
                        // every thread walks the same keys in its own order, so their
                        // stripes are taken in orders that disagree all over the place
                        Collections.shuffle(commands, new Random(threadNumber * 1_000L + round));
                        executeTransaction(commands);
                    }
                } catch (Throwable thrown) {
                    failure.set(thrown);
                } finally {
                    done.countDown();
                }
            }, "shuffled-transaction-" + thread).start();
        }

        start.countDown();
        assertTrue(done.await(WORKLOAD_BUDGET_SECONDS, TimeUnit.SECONDS),
                "the shuffled transactions did not finish in " + WORKLOAD_BUDGET_SECONDS
                        + " seconds, which is what a lock order that can deadlock looks like");
        if (failure.get() != null) {
            fail("a shuffled transaction failed", failure.get());
        }
        for (String key : keys) {
            assertNotNull(store.peekValue(key),
                    "no transaction ever wrote " + key + ", so the workload proved nothing");
        }
    }

    @Test
    void transactionsThatQueueTheSameKeysInOppositeOrdersDoNotDeadlock() throws InterruptedException {
        // two keys whose stripes run the other way from their names: whoever locks them
        // in queue order walks into them from opposite sides, which is the classic way
        // two transactions wait on each other forever
        String first = null;
        String second = null;
        List<String> candidates = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            candidates.add("deadlock:key:" + i);
        }
        search:
        for (int i = 0; i < candidates.size(); i++) {
            for (int j = i + 1; j < candidates.size(); j++) {
                String a = candidates.get(i);
                String b = candidates.get(j);
                if (a.compareTo(b) < 0 && stripeOf(a) > stripeOf(b)) {
                    first = a;
                    second = b;
                    break search;
                }
            }
        }
        assertNotNull(first, "no pair of keys disagreed with the stripe order, so this "
                + "test cannot probe the lock order it is here for");

        final String keyA = first;
        final String keyB = second;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Runnable forwards = () -> transactionWays(start, done, failure, keyA, keyB);
        Runnable backwards = () -> transactionWays(start, done, failure, keyB, keyA);
        new Thread(forwards, "opposite-order-forwards").start();
        new Thread(backwards, "opposite-order-backwards").start();

        start.countDown();
        assertTrue(done.await(WORKLOAD_BUDGET_SECONDS, TimeUnit.SECONDS),
                "opposite queue orders of the same two keys did not finish in "
                        + WORKLOAD_BUDGET_SECONDS + " seconds: the transactions deadlocked");
        if (failure.get() != null) {
            fail("an opposite order transaction failed", failure.get());
        }
    }

    private void transactionWays(CountDownLatch start, CountDownLatch done,
                                 AtomicReference<Throwable> failure, String firstKey, String secondKey) {
        try {
            start.await();
            for (int round = 0; round < OPPOSITE_ORDER_ROUNDS; round++) {
                executeTransaction(List.of(
                        new String[]{"SET", firstKey, "a" + round},
                        new String[]{"SET", secondKey, "b" + round}));
            }
        } catch (Throwable thrown) {
            failure.set(thrown);
        } finally {
            done.countDown();
        }
    }

    /** Runs one transaction the way EXEC does, on a client of its own. */
    private void executeTransaction(List<String[]> commands) {
        Client client = new Client(null, null, null, -1);
        client.beginTransaction();
        for (String[] command : commands) {
            client.commandQueue.offer(command);
        }
        store.executeTransaction(client, commandHandler.getTransactionCommandCacheApplier());
        List<String> replies = new ArrayList<>(client.transactionResponse);
        client.endTransaction();
        assertEquals(replies.size(), commands.size(),
                "the transaction did not answer one reply per queued command");
        for (String reply : replies) {
            assertEquals("+OK\r\n", reply, "a queued SET was refused: " + reply);
        }
    }

    /** The stripe a key lands on, the same arithmetic the store does. */
    private static int stripeOf(String key) {
        return (key.hashCode() & 0x7fffffff) % KEY_LOCK_COUNT;
    }
}
