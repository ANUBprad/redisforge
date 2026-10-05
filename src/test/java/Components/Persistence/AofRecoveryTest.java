package Components.Persistence;

import Components.Repository.Store;
import Components.Server.RedisConfig;
import Components.Service.RespSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a restart makes of the file: the dataset comes back, deadlines are not restarted,
 * a transaction comes back as the single transaction it was, a write that was cut in half
 * is dropped, and a file that is not RESP stops the server instead of loading some of it.
 */
class AofRecoveryTest {

    private final RespSerializer respSerializer = new RespSerializer();

    @Test
    void aSetComesBack(@TempDir Path dir) throws Exception {
        Store recovered = restart(afterWriting(dir, aof -> {
            aof.appendApplied(new String[]{"SET", "greeting", "hello"});
        }));

        assertEquals("hello", value(recovered, "greeting"));
    }

    @Test
    void aValueWithNonAsciiCharactersComesBackWhole(@TempDir Path dir) throws Exception {
        Store recovered = restart(afterWriting(dir, aof -> {
            aof.appendApplied(new String[]{"SET", "unicode", "héllo 😀 world"});
        }));

        assertEquals("héllo 😀 world", value(recovered, "unicode"));
    }

    @Test
    void incrementsComeBackCounted(@TempDir Path dir) throws Exception {
        Store recovered = restart(afterWriting(dir, aof -> {
            aof.appendApplied(new String[]{"SET", "counter", "41"});
            aof.appendApplied(new String[]{"INCR", "counter"});
            aof.appendApplied(new String[]{"INCR", "counter"});
        }));

        assertEquals("43", value(recovered, "counter"));
    }

    @Test
    void aDeleteComesBackDeleted(@TempDir Path dir) throws Exception {
        Store recovered = restart(afterWriting(dir, aof -> {
            aof.appendApplied(new String[]{"SET", "gone", "here"});
            aof.appendApplied(new String[]{"SET", "stays", "here"});
            aof.appendApplied(new String[]{"DEL", "gone"});
        }));

        assertNull(recovered.getValue("gone"), "a deleted key came back");
        assertEquals("here", value(recovered, "stays"));
    }

    @Test
    void aTransactionComesBackAsOneTransaction(@TempDir Path dir) throws Exception {
        Store recovered = restart(afterWriting(dir, aof -> {
            aof.appendAppliedTransaction(List.of(
                    new String[]{"SET", "a", "1"},
                    new String[]{"INCR", "b"},
                    new String[]{"DEL", "c"}));
        }));

        assertEquals("1", value(recovered, "a"));
        assertEquals("1", value(recovered, "b"));
    }

    @Test
    void aDeadlineThatHasNotPassedIsKeptRatherThanRestarted(@TempDir Path dir) throws Exception {
        Store before = newStore();
        Path file = dir.resolve("appendonly.aof");
        AppendOnlyPersistence aof = started(masterConfig(file), before);

        before.set("temporary", "value", 600_000);
        aof.appendApplied(new String[]{"SET", "temporary", "value", "px", "600000"});
        long deadline = before.getValue("temporary").expiry
                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        aof.close();

        // a restart must not hand the key a fresh 600 seconds
        Store recovered = restart(file, newStore());
        assertEquals("value", value(recovered, "temporary"));
        long recoveredDeadline = recovered.getValue("temporary").expiry
                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        assertTrue(Math.abs(recoveredDeadline - deadline) < 2_000,
                "the deadline moved by " + (recoveredDeadline - deadline) + "ms across the restart");
    }

    @Test
    void aKeyThatExpiredWhileTheServerWasDownComesBackAbsent(@TempDir Path dir) throws Exception {
        Store before = newStore();
        Path file = dir.resolve("appendonly.aof");
        AppendOnlyPersistence aof = started(masterConfig(file), before);

        before.set("brief", "value");
        aof.appendApplied(new String[]{"SET", "brief", "value"});
        // the same key is written again with a deadline that has since passed, which is what
        // restarting after the key expired looks like in the file
        before.setAt("brief", "value", System.currentTimeMillis() - 60_000);
        aof.appendApplied(new String[]{"SET", "brief", "value"});
        aof.close();

        Store recovered = restart(file, newStore());
        assertNull(recovered.getValue("brief"), "a key that had already expired came back");
        assertFalse(recovered.map.containsKey("brief"), "the expired key was left in the keyspace");
    }

    @Test
    void aWriteThatWasCutInHalfAtTheEndIsDropped(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        // a whole frame, then one that stops halfway through its value
        Files.writeString(file, frame("SET", "whole", "value")
                + "*3\r\n$3\r\nSET\r\n$4\r\ntorn\r\n$5\r\nval");

        Store recovered = restart(file, newStore());
        assertEquals("value", value(recovered, "whole"), "the writes that were whole did not come back");

        // the file is cut back to its last whole frame, so what is written next still parses
        AppendOnlyPersistence aof = started(masterConfig(file), newStore());
        aof.appendApplied(new String[]{"SET", "after", "restart"});
        aof.close();

        Store again = restart(file, newStore());
        assertEquals("value", value(again, "whole"));
        assertEquals("restart", value(again, "after"));
        assertNull(again.getValue("torn"));
    }

    @Test
    void bytesThatAreNotRespInTheMiddleOfTheFileStopTheServer(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        Files.writeString(file, frame("SET", "first", "v")
                + "this is not a frame at all\r\n"
                + frame("SET", "second", "v"));

        AppendOnlyCorruptedException e = assertThrows(AppendOnlyCorruptedException.class,
                () -> started(masterConfig(file), newStore()));
        assertTrue(e.getMessage().contains("corrupt"), "the failure does not say the file is corrupt: " + e.getMessage());
    }

    @Test
    void aFrameThatIsNotACommandStopsTheServer(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        Files.writeString(file, frame("SET", "first", "v")
                + frame("SHUTDOW")
                + frame("SET", "second", "v"));

        AppendOnlyCorruptedException e = assertThrows(AppendOnlyCorruptedException.class,
                () -> started(masterConfig(file), newStore()));
        assertTrue(e.getMessage().contains("SHUTDOW"), "the failure does not name the command: " + e.getMessage());
    }

    @Test
    void aTransactionWithNoExecStopsTheServer(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        Files.writeString(file, frame("MULTI") + frame("SET", "k", "v"));

        AppendOnlyCorruptedException e = assertThrows(AppendOnlyCorruptedException.class,
                () -> started(masterConfig(file), newStore()));
        assertTrue(e.getMessage().contains("MULTI"), "the failure does not say what is missing: " + e.getMessage());
    }

    @Test
    void anEmptyOrMissingFileStartsClean(@TempDir Path dir) throws Exception {
        Path missing = dir.resolve("never-written.aof");
        Store store = restart(missing, newStore());
        assertTrue(store.map.isEmpty());

        Path empty = dir.resolve("empty.aof");
        Files.writeString(empty, "");
        assertTrue(restart(empty, newStore()).map.isEmpty());
    }

    @Test
    void aFileThatEndsOnAFrameBoundaryIsNotTruncated(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        String whole = frame("SET", "k", "v");
        Files.writeString(file, whole);

        AppendOnlyPersistence aof = started(masterConfig(file), newStore());
        aof.close();

        assertEquals(whole, Files.readString(file), "a file that was already whole was cut back");
    }

    @Test
    void laterWritesLandAfterTheRecoveredOnes(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("appendonly.aof");
        AppendOnlyPersistence aof = started(masterConfig(file), newStore());
        aof.appendApplied(new String[]{"SET", "before", "restart"});
        aof.close();

        Store recovered = newStore();
        AppendOnlyPersistence second = started(masterConfig(file), recovered);
        second.appendApplied(new String[]{"INCR", "counter"});
        second.close();

        Store again = restart(file, newStore());
        assertEquals("restart", value(again, "before"));
        assertEquals("1", value(again, "counter"));
    }

    @Test
    void theWholeLogIsReplayedInOrder(@TempDir Path dir) throws Exception {
        Store recovered = restart(afterWriting(dir, aof -> {
            aof.appendApplied(new String[]{"SET", "k", "0"});
            for (int i = 1; i <= 20; i++) {
                aof.appendApplied(new String[]{"INCR", "k"});
            }
        }));

        assertEquals("20", value(recovered, "k"), "the log was not replayed in the order it was written");
    }

    @Test
    void recoveryIsFinishedWhenStartReturns(@TempDir Path dir) throws Exception {
        // start() is what runs before the listening socket exists, so once it returns the
        // keyspace is the whole file and there is nothing left to wait for
        Path file = dir.resolve("appendonly.aof");
        Files.writeString(file, frame("SET", "first", "v") + frame("INCR", "second"));

        Store store = newStore();
        AppendOnlyPersistence aof = new AppendOnlyPersistence(masterConfig(file), store);

        assertEquals(2, aof.start(), "the number of replayed commands is not what was in the file");
        assertEquals("v", value(store, "first"));
        assertEquals("1", value(store, "second"));
        aof.close();
    }

    /** Runs the given writes against a server, then takes its file as the restart input. */
    private Path afterWriting(Path dir, Writes writes) throws IOException {
        Path file = dir.resolve("appendonly.aof");
        Store store = newStore();
        AppendOnlyPersistence aof = started(masterConfig(file), store);
        try {
            writes.run(aof);
        } finally {
            aof.close();
        }
        return file;
    }

    /** Starts a server on the given file and closes it again, as a restart would. */
    private Store restart(Path file) throws IOException {
        return restart(file, newStore());
    }

    private Store restart(Path file, Store store) throws IOException {
        AppendOnlyPersistence aof = started(masterConfig(file), store);
        aof.close();
        return store;
    }

    private interface Writes {
        void run(AppendOnlyPersistence aof);
    }

    /** A RESP command as it goes into the file, byte counts and all. */
    private static String frame(String... parts) {
        StringBuilder sb = new StringBuilder("*" + parts.length + "\r\n");
        for (String part : parts) {
            sb.append("$").append(part.getBytes(StandardCharsets.UTF_8).length)
                    .append("\r\n").append(part).append("\r\n");
        }
        return sb.toString();
    }

    private Store newStore() {
        Store store = new Store();
        store.respSerializer = respSerializer;
        return store;
    }

    private RedisConfig masterConfig(Path file) {
        RedisConfig config = new RedisConfig();
        config.setRole("master");
        config.setAppendonly(true);
        config.setAppendfilename(file.toString());
        config.setAppendfsync("always");
        return config;
    }

    private AppendOnlyPersistence started(RedisConfig config, Store store) throws IOException {
        AppendOnlyPersistence aof = new AppendOnlyPersistence(config, store);
        aof.start();
        return aof;
    }

    private String value(Store store, String key) {
        Components.Repository.Value value = store.getValue(key);
        assertNotNull(value, "the key " + key + " did not come back");
        return value.val;
    }
}