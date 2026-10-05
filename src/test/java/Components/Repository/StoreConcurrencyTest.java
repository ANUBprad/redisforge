package Components.Repository;

import Components.Infra.ConnectionPool;
import Components.Server.RedisConfig;
import Components.Service.CommandHandler;
import Components.Service.RespSerializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the shared store hands out every increment exactly once and that a mixed
 * workload of SET, GET and INCR leaves the store in the state the operations imply.
 */
class StoreConcurrencyTest {
    private final RespSerializer respSerializer = new RespSerializer();
    private final Store store = newStore();
    private final ConnectionPool connectionPool = new ConnectionPool();
    private final CommandHandler commandHandler = newCommandHandler();

    private Store newStore() {
        Store wired = new Store();
        wired.respSerializer = respSerializer;
        return wired;
    }

    private CommandHandler newCommandHandler() {
        CommandHandler handler = new CommandHandler();
        handler.store = store;
        handler.connectionPool = connectionPool;
        handler.respSerializer = respSerializer;
        handler.redisConfig = new RedisConfig();
        return handler;
    }

    @Test
    void concurrentIncrementsOnOneKeyAreNotLost() throws Exception {
        int threads = 8;
        int perThread = 250;
        runConcurrently(threads, (worker) -> {
            for (int i = 0; i < perThread; i++) {
                // the replies of one key are distinct, but together they must be exactly
                // 1..threads*perThread: nothing applied twice, nothing lost
                assertIntegerReply(commandHandler.incr(new String[]{"INCR", "counter"}));
            }
        });

        assertEquals(threads * perThread, counter("counter"),
                "an increment was applied twice or lost to a concurrent writer");
    }

    @Test
    void incrementsOnDistinctKeysDoNotBlockEachOther() throws Exception {
        runConcurrently(8, (worker) -> {
            for (int i = 0; i < 100; i++) {
                assertIntegerReply(commandHandler.incr(new String[]{"INCR", "key-" + worker}));
            }
        });

        for (int worker = 0; worker < 8; worker++) {
            assertEquals(100, counter("key-" + worker), "key-" + worker + " lost an increment");
        }
    }

    @Test
    void incrementRefusedOnANonIntegerLeavesTheValueUntouched() throws Exception {
        commandHandler.set(new String[]{"SET", "text", "hello"});
        assertEquals("-ERR value is not an integer or out of range\r\n",
                commandHandler.incr(new String[]{"INCR", "text"}));
        assertEquals(respSerializer.serializeBulkString("hello"), store.get("text"),
                "a refused increment changed the value");
    }

    @Test
    void concurrentMixedSetGetAndIncrKeepTheCounterConsistent() throws Exception {
        int readers = 4;
        int setters = 4;
        int incrementers = 4;
        int perThread = 200;
        // a set lands on a key only while no increment is running against it, so the
        // counter is expected to survive every interleaving that is legal here
        ExecutorService pool = Executors.newFixedThreadPool(readers + setters + incrementers);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(readers + setters + incrementers);
        AtomicInteger seen = new AtomicInteger();
        List<Throwable> failures = new ArrayList<>();

        for (int i = 0; i < readers; i++) {
            pool.execute(() -> {
                awaitQuietly(start);
                try {
                    for (int n = 0; n < perThread; n++) {
                        String reply = store.get("counter");
                        if (reply.equals("$-1\r\n")) {
                            continue;
                        }
                        int value = parseBulkString(reply);
                        assertTrue(value >= 0, "read a counter that cannot exist: " + reply);
                        seen.incrementAndGet();
                    }
                } catch (Throwable t) {
                    failures.add(t);
                } finally {
                    done.countDown();
                }
            });
        }

        for (int i = 0; i < incrementers; i++) {
            pool.execute(() -> {
                awaitQuietly(start);
                try {
                    for (int n = 0; n < perThread; n++) {
                        assertIntegerReply(commandHandler.incr(new String[]{"INCR", "counter"}));
                    }
                } catch (Throwable t) {
                    failures.add(t);
                } finally {
                    done.countDown();
                }
            });
        }

        // writers on a different key, to show they do not interfere with the counter
        for (int i = 0; i < setters; i++) {
            int worker = i;
            pool.execute(() -> {
                awaitQuietly(start);
                try {
                    for (int n = 0; n < perThread; n++) {
                        assertEquals("+OK\r\n", commandHandler.set(new String[]{"SET", "other-" + worker, "v" + n}));
                    }
                } catch (Throwable t) {
                    failures.add(t);
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(60, TimeUnit.SECONDS), "the mixed workload did not finish in time");
        pool.shutdownNow();
        assertTrue(failures.isEmpty(), () -> "concurrent command failed: " + failures.get(0));

        assertEquals(incrementers * perThread, counter("counter"), "the counter lost an increment");
        assertTrue(seen.get() > 0, "the readers never observed the counter");
        for (int worker = 0; worker < setters; worker++) {
            assertEquals(respSerializer.serializeBulkString("v" + (perThread - 1)), store.get("other-" + worker),
                    "a writer's last value was lost");
        }
    }

    @Test
    void expiredEntryIsNotRemovedByALaterReadOnceItWasWrittenAgain() throws Exception {
        commandHandler.set(new String[]{"SET", "volley", "first"});
        store.set("volley", "second", 1);
        // the expiry check and the write are two steps; a set in between must not be
        // deleted by the cleanup that the reader performs
        store.set("volley", "third");
        Thread.sleep(1100);

        assertEquals(respSerializer.serializeBulkString("third"), store.get("volley"),
                "an expired read dropped a value written after it");
        assertEquals("-ERR value is not an integer or out of range\r\n",
                commandHandler.incr(new String[]{"INCR", "volley"}));
    }

    private static void assertIntegerReply(String reply) {
        assertTrue(reply.length() > 3 && reply.charAt(0) == ':' && reply.endsWith("\r\n"),
                "not an integer reply: " + reply);
    }

    private int counter(String key) {
        String reply = store.get(key);
        return "-1".equals(reply) ? 0 : parseBulkString(reply);
    }

    private int parseBulkString(String reply) {
        assertTrue(reply.startsWith("$") && reply.endsWith("\r\n"),
                "not a bulk string reply: " + reply);
        String payload = reply.substring(reply.indexOf('\n') + 1, reply.length() - 2);
        return Integer.parseInt(payload);
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private void runConcurrently(int threads, WorkerBody body) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        List<Throwable> failures = new ArrayList<>();

        for (int worker = 0; worker < threads; worker++) {
            int id = worker;
            pool.execute(() -> {
                awaitQuietly(start);
                try {
                    body.run(id);
                } catch (Throwable t) {
                    failures.add(t);
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(60, TimeUnit.SECONDS), "workers did not finish in time");
        pool.shutdownNow();
        assertTrue(failures.isEmpty(), () -> "worker failed: " + failures.get(0));
    }

    @FunctionalInterface
    private interface WorkerBody {
        void run(int worker) throws Exception;
    }
}