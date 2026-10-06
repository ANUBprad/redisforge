package Components.Persistence;

import Components.Repository.Store;
import Components.Server.RedisConfig;
import Components.Service.RespSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What lands in the append only file: the framing, the UTF-8 byte counts, the shape of a
 * transaction entry, and that writes from many clients cannot be spliced into each other.
 */
class AppendOnlyPersistenceTest {

    private final RespSerializer respSerializer = new RespSerializer();

    @Test
    void appliedSetIsWrittenAsARespCommand(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        Store store = newStore();
        AppendOnlyPersistence aof = started(masterConfig(file, "always"), store);

        aof.appendApplied(new String[]{"SET", "greeting", "hello"});
        aof.close();

        assertEquals("*3\r\n$3\r\nSET\r\n$8\r\ngreeting\r\n$5\r\nhello\r\n",
                Files.readString(file));
    }

    @Test
    void byteCountsAreUtf8BytesRatherThanCharCounts(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        Store store = newStore();
        AppendOnlyPersistence aof = started(masterConfig(file, "always"), store);

        // five chars, six bytes
        aof.appendApplied(new String[]{"SET", "k", "héllo"});
        // one char, four bytes: a surrogate pair is two chars in the JVM
        aof.appendApplied(new String[]{"SET", "k", "😀"});
        // a command right after a multi byte value has to start where it claims to
        store.increment("k");
        aof.appendApplied(new String[]{"INCR", "k"});
        aof.close();

        assertEquals("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$6\r\nhéllo\r\n"
                        + "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\n😀\r\n"
                        + "*2\r\n$4\r\nINCR\r\n$1\r\nk\r\n",
                Files.readString(file));
    }

    @Test
    void appliedIncrementAndDeleteAreWritten(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        Store store = newStore();
        AppendOnlyPersistence aof = started(masterConfig(file, "no"), store);

        aof.appendApplied(new String[]{"INCR", "counter"});
        aof.appendApplied(new String[]{"DEL", "gone", "also-gone"});
        aof.close();

        assertArrayEquals(new String[]{"INCR", "counter"}, frames(file).get(0));
        assertArrayEquals(new String[]{"DEL", "gone", "also-gone"}, frames(file).get(1));
    }

    @Test
    void setWithARelativeExpiryIsWrittenWithAnAbsoluteDeadline(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        Store store = newStore();
        AppendOnlyPersistence aof = started(masterConfig(file, "everysec"), store);

        // the live command asks for a relative delay, the store turns it into a deadline
        store.set("temporary", "value", 60_000);
        aof.appendApplied(new String[]{"SET", "temporary", "value", "px", "60000"});
        aof.close();

        String[] written = frames(file).get(0);
        assertEquals(5, written.length, "the entry was not rewritten with a deadline: " + String.join(" ", written));
        assertArrayEquals(new String[]{"SET", "temporary", "value"}, java.util.Arrays.copyOf(written, 3));
        assertEquals("PXAT", written[3]);

        long deadline = Long.parseLong(written[4]);
        long expected = System.currentTimeMillis() + 60_000;
        assertTrue(Math.abs(deadline - expected) < 5_000,
                "the deadline is not about a minute out, it is " + (deadline - expected) + "ms away");
    }

    @Test
    void setWithoutAnExpiryIsWrittenWithoutADeadline(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        Store store = newStore();
        AppendOnlyPersistence aof = started(masterConfig(file, "everysec"), store);

        store.set("forever", "value");
        aof.appendApplied(new String[]{"SET", "forever", "value"});
        aof.close();

        assertArrayEquals(new String[]{"SET", "forever", "value"}, frames(file).get(0));
    }

    @Test
    void aTransactionIsWrittenAsOneMultiBlock(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        Store store = newStore();
        AppendOnlyPersistence aof = started(masterConfig(file, "always"), store);

        store.set("a", "1");
        store.set("b", "2");
        store.delete("c");
        aof.appendAppliedTransaction(List.of(
                new String[]{"SET", "a", "1"},
                new String[]{"INCR", "b"},
                new String[]{"DEL", "c"}));
        aof.close();

        List<String[]> written = frames(file);
        assertEquals(5, written.size());
        assertArrayEquals(new String[]{"MULTI"}, written.get(0));
        assertArrayEquals(new String[]{"SET", "a", "1"}, written.get(1));
        assertArrayEquals(new String[]{"INCR", "b"}, written.get(2));
        assertArrayEquals(new String[]{"DEL", "c"}, written.get(3));
        assertArrayEquals(new String[]{"EXEC"}, written.get(4));
    }

    @Test
    void writesFromManyClientsNeverInterleave(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        Store store = newStore();
        AppendOnlyPersistence aof = started(masterConfig(file, "no"), store);

        int writers = 6;
        int writesPerWriter = 150;
        int transactionsPerWriter = 40;
        int commandsPerTransaction = 3;
        int blockWriters = 2;
        // every transaction costs its commands plus the MULTI and EXEC around them
        int expectedFrames = writers * writesPerWriter
                + writers * transactionsPerWriter * (commandsPerTransaction + 2)
                + blockWriters * transactionsPerWriter * (2 + 2);

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(writers + 2);
        for (int writer = 0; writer < writers; writer++) {
            int id = writer;
            pool.submit(() -> {
                awaitQuietly(start);
                for (int i = 0; i < writesPerWriter; i++) {
                    aof.appendApplied(new String[]{"SET", "single-" + id + "-" + i, "v" + i});
                }
                for (int t = 0; t < transactionsPerWriter; t++) {
                    List<String[]> transaction = new ArrayList<>();
                    for (int c = 0; c < commandsPerTransaction; c++) {
                        transaction.add(new String[]{"SET", "tx-" + id + "-" + t + "-" + c, "v"});
                    }
                    aof.appendAppliedTransaction(transaction);
                }
                return null;
            });
        }
        // two writers that only ever write whole transactions, so the file is full of blocks
        for (int writer = 0; writer < blockWriters; writer++) {
            int id = writer;
            pool.submit(() -> {
                awaitQuietly(start);
                for (int t = 0; t < transactionsPerWriter; t++) {
                    aof.appendAppliedTransaction(List.of(
                            new String[]{"SET", "block-" + id + "-" + t, "v"},
                            new String[]{"INCR", "counter-" + id}));
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "the writers did not finish");
        aof.close();

        List<String[]> written = frames(file);
        assertEquals(expectedFrames, written.size(),
                "entries were lost or spliced together, so the file is not whole frames");

        // every value a single writer claimed is readable back on its own
        Set<String> singles = new HashSet<>();
        for (int writer = 0; writer < writers; writer++) {
            for (int i = 0; i < writesPerWriter; i++) {
                singles.add("single-" + writer + "-" + i);
            }
        }
        Set<String> seen = new HashSet<>();
        int blocks = 0;
        boolean insideBlock = false;
        int insideBlockCount = 0;
        for (String[] frame : written) {
            if (frame[0].equals("MULTI")) {
                assertFalse(insideBlock, "a MULTI was written while another block was still open");
                insideBlock = true;
                insideBlockCount = 0;
                continue;
            }
            if (frame[0].equals("EXEC")) {
                assertTrue(insideBlock, "an EXEC was written with no open block");
                assertTrue(insideBlockCount > 0, "a transaction was written with no commands in it");
                insideBlock = false;
                blocks++;
                continue;
            }
            if (insideBlock) {
                insideBlockCount++;
            }
            if (frame.length == 3) {
                seen.add(frame[1]);
            }
        }
        assertFalse(insideBlock, "the file ends inside a transaction block");
        assertTrue(blocks >= writers * transactionsPerWriter + 2, "transactions went missing: " + blocks);
        assertTrue(seen.containsAll(singles), "a write from a client was not readable back whole");
    }

    @Test
    void appendonlyOffCreatesNoFileAndWritesNothing(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        RedisConfig config = new RedisConfig();
        config.setRole("master");
        config.setAppendfilename(file.toString());
        Store store = newStore();
        AppendOnlyPersistence aof = new AppendOnlyPersistence(config, store);

        assertEquals(0, aof.start(), "a server with appendonly off replayed something");
        assertFalse(aof.isEnabled());
        aof.appendApplied(new String[]{"SET", "k", "v"});
        aof.appendAppliedTransaction(List.<String[]>of(new String[]{"SET", "k", "v"}));
        aof.close();

        assertFalse(Files.exists(file), "appendonly off created a file");
        assertFalse(store.map.containsKey("k"));
    }

    @Test
    void aReplicaOpensNoFileAndReplaysNothing(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        // a file a previous master wrote, which a replica must not touch
        Files.writeString(file, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");

        RedisConfig config = new RedisConfig();
        config.setRole("slave");
        config.setAppendonly(true);
        config.setAppendfilename(file.toString());
        Store store = newStore();
        AppendOnlyPersistence aof = new AppendOnlyPersistence(config, store);

        assertEquals(0, aof.start(), "a replica replayed an append only file");
        assertFalse(aof.isEnabled(), "a replica opened a local append only file");
        aof.appendApplied(new String[]{"SET", "k", "from-master"});
        aof.close();

        assertFalse(store.map.containsKey("k"), "a replica applied commands from the file");
        assertEquals("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n", Files.readString(file),
                "a replica wrote to its own append only file");
    }

    @Test
    void closingFlushesTheFileAndLeavesItReadable(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        Store store = newStore();
        AppendOnlyPersistence aof = started(masterConfig(file, "everysec"), store);

        aof.appendApplied(new String[]{"SET", "k", "v"});
        assertTrue(aof.isEnabled());
        aof.close();
        assertFalse(aof.isEnabled(), "closing left the file open");

        // a second server picks the file up as it was left, which is what a restart does
        Store restarted = newStore();
        AppendOnlyPersistence second = started(masterConfig(file, "always"), restarted);
        second.close();

        assertEquals("v", restarted.getValue("k").val);
    }

    @Test
    void theFsyncPoliciesAreParsedFromTheirNames() {
        assertEquals(FsyncPolicy.ALWAYS, FsyncPolicy.parse("always"));
        assertEquals(FsyncPolicy.EVERYSEC, FsyncPolicy.parse("everysec"));
        assertEquals(FsyncPolicy.NO, FsyncPolicy.parse("no"));
        assertEquals(FsyncPolicy.ALWAYS, FsyncPolicy.parse("ALWAYS"));
    }

    @Test
    void aRewriteKeepsTheStateAndDropsTheHistory(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        Store store = newStore();
        AppendOnlyPersistence aof = started(masterConfig(file, "always"), store);

        // the same key written over and over, which is what makes a file grow
        for (int i = 1; i <= 5; i++) {
            store.set("churn", "v" + i);
            aof.appendApplied(new String[]{"SET", "churn", "v" + i});
        }
        store.set("keep", "steady");
        aof.appendApplied(new String[]{"SET", "keep", "steady"});
        assertEquals(6, frames(file).size(), "the history was not written down to begin with");

        assertEquals(2, aof.rewrite(), "the rewrite did not describe every live key");
        aof.close();

        List<String[]> rewritten = frames(file);
        assertEquals(2, rewritten.size(), "the history survived the rewrite");
        Store replayed = replay(file);
        assertEquals("v5", replayed.getValue("churn").val,
                "the rewrite kept an older value than the store holds");
        assertEquals("steady", replayed.getValue("keep").val);
    }

    @Test
    void aRewriteKeepsTheAbsoluteDeadlineOfAnExpiringKey(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        Store store = newStore();
        AppendOnlyPersistence aof = started(masterConfig(file, "everysec"), store);

        store.set("temporary", "value", 600_000);
        aof.appendApplied(new String[]{"SET", "temporary", "value", "px", "600000"});
        long deadline = Long.parseLong(frames(file).get(0)[4]);

        assertEquals(1, aof.rewrite());
        aof.close();

        String[] rewritten = frames(file).get(0);
        assertEquals(5, rewritten.length, "the expiry was dropped: " + String.join(" ", rewritten));
        assertEquals("PXAT", rewritten[3], "the deadline was not carried over: "
                + String.join(" ", rewritten));
        assertEquals(String.valueOf(deadline), rewritten[4],
                "the deadline moved, so the key would expire at a different time");

        Store replayed = replay(file);
        long replayedDeadline = replayed.getValue("temporary").expiry
                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        assertEquals(deadline, replayedDeadline, "the key came back with a different deadline");
    }

    @Test
    void aRewriteLeavesOutKeysWhoseDeadlineHadAlreadyPassed(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        Store store = newStore();
        AppendOnlyPersistence aof = started(masterConfig(file, "no"), store);

        store.setAt("already-gone", "value", System.currentTimeMillis() - 60_000);
        store.set("still-here", "value");

        assertEquals(1, aof.rewrite(), "a key whose deadline had passed was written down");
        aof.close();

        assertArrayEquals(new String[]{"SET", "still-here", "value"}, frames(file).get(0));
        Store replayed = replay(file);
        assertNull(replayed.getValue("already-gone"), "an expired key came back from the rewrite");
        assertEquals("value", replayed.getValue("still-here").val);
    }

    @Test
    void aRewriteLeavesOnlyTheAppendOnlyFileBehind(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        Store store = newStore();
        AppendOnlyPersistence aof = started(masterConfig(file, "always"), store);

        store.set("k", "v");
        aof.appendApplied(new String[]{"SET", "k", "v"});
        aof.rewrite();

        try (var listed = Files.list(dir)) {
            assertEquals(List.of("appendonly.aof"),
                    listed.map(path -> path.getFileName().toString()).sorted().toList(),
                    "the rewrite left files beside the append only file");
        }
        aof.close();
    }

    @Test
    void writesThatArriveWhileARewriteRunsAreNeverLost(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        Store store = newStore();
        AppendOnlyPersistence aof = started(masterConfig(file, "no"), store);

        int writers = 4;
        int writesPerWriter = 120;
        int rewrites = 25;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(writers + 1);
        for (int writer = 0; writer < writers; writer++) {
            int id = writer;
            pool.submit(() -> {
                awaitQuietly(start);
                for (int i = 0; i < writesPerWriter; i++) {
                    final int write = i;
                    // the pattern the server itself uses: store and file, one hold of the lock
                    aof.locked(() -> {
                        store.set("rw:" + id, "v" + write);
                        aof.appendApplied(new String[]{"SET", "rw:" + id, "v" + write});
                        return null;
                    });
                }
                return null;
            });
        }
        pool.submit(() -> {
            awaitQuietly(start);
            for (int i = 0; i < rewrites; i++) {
                aof.rewrite();
            }
            return null;
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS),
                "the writers and the rewrites did not finish");
        // one last rewrite, so the file is exactly the state the writers left behind
        assertEquals(writers, aof.rewrite(), "the final rewrite did not describe every key");
        aof.close();

        Store replayed = replay(file);
        for (int writer = 0; writer < writers; writer++) {
            assertEquals("v" + (writesPerWriter - 1), replayed.getValue("rw:" + writer).val,
                    "a write from writer " + writer + " was lost across a rewrite");
        }
    }

    @Test
    void appendsKeepFollowingTheFsyncPolicyAfterARewrite(@TempDir Path dir) throws Exception {
        for (String policy : List.of("always", "everysec", "no")) {
            Path file = dir.resolve("appendonly-" + policy + ".aof");
            Store store = newStore();
            AppendOnlyPersistence aof = started(masterConfig(file, policy), store);

            store.set("before", policy);
            aof.appendApplied(new String[]{"SET", "before", policy});
            assertEquals(1, aof.rewrite(), "appendfsync " + policy + " rewrote nothing");
            store.set("after", policy);
            aof.appendApplied(new String[]{"SET", "after", policy});
            aof.close();

            Store replayed = replay(file);
            assertEquals(policy, replayed.getValue("before").val,
                    "the entry before the rewrite was lost under appendfsync " + policy);
            assertEquals(policy, replayed.getValue("after").val,
                    "an entry after the rewrite was lost under appendfsync " + policy);
        }
    }

    @Test
    void aRewriteIsRefusedWhenThereIsNoFile(@TempDir Path dir) {
        RedisConfig config = new RedisConfig();
        config.setRole("master");
        config.setAppendfilename(dir.resolve("appendonly.aof").toString());
        Store store = newStore();
        AppendOnlyPersistence aof = new AppendOnlyPersistence(config, store);

        IllegalStateException e = assertThrows(IllegalStateException.class, aof::rewrite);
        assertTrue(e.getMessage().contains("no append only file"),
                "the refusal does not say what was wrong: " + e.getMessage());
    }

    @Test
    void anUnknownFsyncPolicyIsRefused() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> FsyncPolicy.parse("whenever"));
        assertTrue(e.getMessage().contains("whenever"), "the refusal does not say what was wrong: " + e.getMessage());
        assertThrows(IllegalArgumentException.class, () -> FsyncPolicy.parse(null));
    }

    private Store newStore() {
        Store store = new Store();
        store.respSerializer = respSerializer;
        return store;
    }

    private RedisConfig masterConfig(Path file, String fsync) {
        RedisConfig config = new RedisConfig();
        config.setRole("master");
        config.setAppendonly(true);
        config.setAppendfilename(file.toString());
        config.setAppendfsync(fsync);
        return config;
    }

    private AppendOnlyPersistence started(RedisConfig config, Store store) throws IOException {
        AppendOnlyPersistence aof = new AppendOnlyPersistence(config, store);
        aof.start();
        return aof;
    }

    /** A fresh store replayed from the file, which is what a restarted server would hold. */
    private Store replay(Path file) throws IOException {
        Store fresh = newStore();
        AppendOnlyPersistence second = started(masterConfig(file, "always"), fresh);
        second.close();
        return fresh;
    }

    /** Every whole frame in the file, in the order it was written. */
    private List<String[]> frames(Path file) throws IOException {
        byte[] contents = Files.readAllBytes(file);
        List<String[]> frames = new ArrayList<>();
        int offset = 0;
        while (offset < contents.length) {
            int length = respSerializer.frameLength(contents, offset, contents.length);
            assertTrue(length > 0, "byte " + offset + " of the file is not the start of a whole frame");
            List<String[]> decoded = respSerializer.deseralize(contents, offset, offset + length);
            assertEquals(1, decoded.size(), "a frame at byte " + offset + " decoded into " + decoded.size());
            frames.add(decoded.get(0));
            offset += length;
        }
        assertEquals(contents.length, offset, "the file has bytes left over after its last frame");
        return frames;
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}