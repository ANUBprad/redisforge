package Components.Server;

import Components.Persistence.AppendOnlyPersistence;
import Components.Repository.MemoryEstimator;
import Components.Repository.Store;
import Components.Service.RespSerializer;
import Config.AppConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The memory ceiling as a client meets it over a real socket: a write past the limit
 * evicts by the configured policy, a transaction is admitted whole or refused whole, a
 * refusal answers {@code -OOM} and leaves the keyspace alone, and every key the policy
 * removes is written down as the {@code DEL} it is.
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class MemoryEvictionServerTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;

    @Autowired
    private MasterTcpServer masterTcpServer;
    @Autowired
    private RedisConfig redisConfig;
    @Autowired
    private Store store;
    @Autowired
    private AppendOnlyPersistence appendOnly;
    @Autowired
    private RespSerializer respSerializer;

    private int masterPort;
    private Path file;

    @BeforeAll
    void startMaster(@TempDir Path dir) throws Exception {
        masterPort = freePort();
        file = dir.resolve("appendonly.aof");
        redisConfig.setRole("master");
        redisConfig.setPort(masterPort);
        redisConfig.setAppendonly(true);
        redisConfig.setAppendfilename(file.toString());
        redisConfig.setAppendfsync("no");
        appendOnly.start();
        Thread server = new Thread(masterTcpServer::startServer, "memory-master");
        server.setDaemon(true);
        server.start();
        awaitPortOpen(masterPort);
    }

    @BeforeEach
    void resetKeyspaceAndCeiling() {
        redisConfig.setMaxmemory(0);
        redisConfig.setMaxmemoryPolicy("noeviction");
        store.installSnapshot(List.of());
    }

    private static long entry(String key) {
        return MemoryEstimator.entryBytes(key, "v", LocalDateTime.MAX);
    }

    @Test
    void aWritePastTheLimitEvictsTheLeastRecentlyUsedKey() throws Exception {
        // three equal-sized entries fit exactly; a fourth has to remove one
        redisConfig.setMaxmemory(entry("ev:1") * 3);
        redisConfig.setMaxmemoryPolicy("allkeys-lru");
        long evictedBefore = store.evictedKeys();

        try (RespSocket client = new RespSocket(masterPort)) {
            for (String key : List.of("ev:1", "ev:2", "ev:3")) {
                client.send(frame("SET", key, "v"));
                assertEquals("+OK", client.readReply());
            }
            // ev:1 is the most recently used now, which leaves ev:2 the oldest
            client.send(frame("GET", "ev:1"));
            assertTrue(client.readReply().startsWith("$"));

            client.send(frame("SET", "ev:4", "v"));
            assertEquals("+OK", client.readReply());

            client.send(frame("GET", "ev:2"));
            assertEquals("$-1", client.readReply());
            for (String key : List.of("ev:1", "ev:3", "ev:4")) {
                client.send(frame("GET", key));
                assertTrue(client.readReply().startsWith("$"), key + " was evicted but should not have been");
            }
        }
        assertEquals(evictedBefore + 1, store.evictedKeys(), "the eviction was not counted");
    }

    @Test
    void noevictionRefusesAWriteRatherThanRemovingAKey() throws Exception {
        redisConfig.setMaxmemory(entry("oom:1"));
        redisConfig.setMaxmemoryPolicy("noeviction");
        long evictedBefore = store.evictedKeys();

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "oom:1", "v"));
            assertEquals("+OK", client.readReply());

            client.send(frame("SET", "oom:2", "v"));
            assertTrue(client.readReply().startsWith("-OOM"),
                    "a write past maxmemory was not refused with OOM");

            client.send(frame("GET", "oom:1"));
            assertTrue(client.readReply().startsWith("$"), "the refused write dropped the key that was there");
        }
        assertEquals(evictedBefore, store.evictedKeys());
    }

    @Test
    void aRefusedWriteLeavesTheKeyspaceAndTheFileAlone() throws Exception {
        redisConfig.setMaxmemory(entry("keep:o"));
        redisConfig.setMaxmemoryPolicy("noeviction");

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "keep:o", "v"));
            assertEquals("+OK", client.readReply());
        }

        int before = frames().size();
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "rejected:o", "v"));
            assertTrue(client.readReply().startsWith("-OOM"));
        }
        assertEquals(before, frames().size(), "a refused write reached the file");
    }

    @Test
    void aVolatilePolicyRemovesOnlyAKeyWithADeadline() throws Exception {
        redisConfig.setMaxmemoryPolicy("volatile-lru");

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "keep:vol", "v"));
            assertEquals("+OK", client.readReply());
            client.send(frame("SET", "vol:a", "v", "px", "600000"));
            assertEquals("+OK", client.readReply());

            redisConfig.setMaxmemory(store.currentBytes());

            client.send(frame("SET", "vol:b", "v", "px", "600000"));
            assertEquals("+OK", client.readReply());

            client.send(frame("GET", "vol:a"));
            assertEquals("$-1", client.readReply(), "the volatile key was not the one evicted");
            client.send(frame("GET", "keep:vol"));
            assertTrue(client.readReply().startsWith("$"), "a key without a deadline was evicted by a volatile policy");
            client.send(frame("GET", "vol:b"));
            assertTrue(client.readReply().startsWith("$"));
        }
    }

    @Test
    void aVolatilePolicyRefusesWhenThereIsNoKeyWithADeadline() throws Exception {
        redisConfig.setMaxmemoryPolicy("volatile-lru");
        // a ceiling above zero but smaller than any entry: no key fits unless one is evicted
        redisConfig.setMaxmemory(1);

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "only:persistent", "v"));
            assertTrue(client.readReply().startsWith("-OOM"),
                    "a volatile policy evicted a key that had no deadline");
            client.send(frame("GET", "only:persistent"));
            assertEquals("$-1", client.readReply());
        }
    }

    @Test
    void aTransactionThatWouldNotFitIsRefusedWhole() throws Exception {
        // a ceiling above zero but smaller than any entry: the queued write cannot fit
        redisConfig.setMaxmemory(1);
        redisConfig.setMaxmemoryPolicy("noeviction");

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("MULTI"));
            assertEquals("+OK", client.readReply());
            client.send(frame("SET", "tx:refused", "v"));
            assertEquals("+QUEUED", client.readReply());
            client.send(frame("EXEC"));
            assertTrue(client.readReply().startsWith("-OOM"),
                    "an over-limit transaction was not refused with OOM");

            client.send(frame("GET", "tx:refused"));
            assertEquals("$-1", client.readReply(), "a refused transaction committed a key");
        }
    }

    @Test
    void aTransactionThatFitsIsCommitted() throws Exception {
        long size = entry("tx:a");
        redisConfig.setMaxmemory(size * 2);
        redisConfig.setMaxmemoryPolicy("allkeys-lru");

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("MULTI"));
            assertEquals("+OK", client.readReply());
            client.send(frame("SET", "tx:a", "v"));
            assertEquals("+QUEUED", client.readReply());
            client.send(frame("SET", "tx:b", "v"));
            assertEquals("+QUEUED", client.readReply());
            client.send(frame("EXEC"));
            assertEquals("*2", client.readReply());
        }

        // a fresh connection, so the array's element replies before the header are not
        // mistaken for the answer to this read
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("GET", "tx:a"));
            assertTrue(client.readReply().startsWith("$"), "a transaction that fit was not committed");
            client.send(frame("GET", "tx:b"));
            assertTrue(client.readReply().startsWith("$"));
        }
    }

    @Test
    void anEvictionIsWrittenToTheFileAsTheDelItIs() throws Exception {
        redisConfig.setMaxmemory(entry("aof:1"));
        redisConfig.setMaxmemoryPolicy("allkeys-lru");

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "aof:1", "v"));
            assertEquals("+OK", client.readReply());
        }

        int before = frames().size();
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "aof:2", "v"));
            assertEquals("+OK", client.readReply());
        }

        List<String[]> written = framesSince(before);
        assertEquals(2, written.size(), "the eviction and the write were not both recorded: " + describe(written));
        assertEquals("DEL", written.get(0)[0], "the eviction was not written as a DEL: " + String.join(" ", written.get(0)));
        assertEquals("aof:1", written.get(0)[1]);
        assertEquals("SET", written.get(1)[0]);
        assertEquals("aof:2", written.get(1)[1]);
    }

    @Test
    void incrementingANonIntegerIsRefusedWithoutTouchingTheLimit() throws Exception {
        redisConfig.setMaxmemoryPolicy("noeviction");
        long evictedBefore = store.evictedKeys();

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "not:int", "text"));
            assertEquals("+OK", client.readReply());

            redisConfig.setMaxmemory(store.currentBytes());

            client.send(frame("INCR", "not:int"));
            assertTrue(client.readReply().startsWith("-ERR value is not an integer"),
                    "INCR on a non-integer was not refused as such");

            client.send(frame("GET", "not:int"));
            assertTrue(client.readReply().contains("text"), "the value was changed by a refused INCR");
        }
        assertEquals(evictedBefore, store.evictedKeys());
    }

    @Test
    void infoMemoryReportsTheCeilingThePolicyAndTheKeyspaceSize() throws Exception {
        redisConfig.setMaxmemory(12_345);
        redisConfig.setMaxmemoryPolicy("allkeys-random");

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "info:1", "v"));
            assertEquals("+OK", client.readReply());

            client.send(frame("INFO", "memory"));
            String report = client.readReply();
            assertTrue(report.contains("# Memory"), "the memory section is missing: " + report);
            assertTrue(report.contains("used_memory:" + entry("info:1")),
                    "used_memory does not describe the keyspace: " + report);
            assertTrue(report.contains("maxmemory:12345"), "maxmemory is missing: " + report);
            assertTrue(report.contains("maxmemory_policy:allkeys-random"), "the policy is missing: " + report);
            assertTrue(report.contains("evicted_keys:"), "evicted_keys is missing: " + report);
        }
    }

    @Test
    void infoAllStillCarriesEverySection() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("INFO"));
            String report = client.readReply();
            assertTrue(report.contains("# Server"));
            assertTrue(report.contains("# Replication"));
            assertTrue(report.contains("# Memory"));
        }
    }

    private List<String[]> framesSince(int mark) {
        List<String[]> all = frames();
        return all.subList(mark, all.size());
    }

    private List<String[]> frames() {
        byte[] contents;
        try {
            contents = Files.readAllBytes(file);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        List<String[]> frames = new ArrayList<>();
        int offset = 0;
        while (offset < contents.length) {
            int length = respSerializer.frameLength(contents, offset, contents.length);
            assertTrue(length > 0, "byte " + offset + " of the append only file is not a whole frame");
            frames.addAll(respSerializer.deseralize(contents, offset, offset + length));
            offset += length;
        }
        return frames;
    }

    private static String describe(List<String[]> frames) {
        List<String> out = new ArrayList<>();
        for (String[] frame : frames) {
            out.add(String.join(" ", frame));
        }
        return out.toString();
    }

    private static byte[] frame(String... parts) {
        StringBuilder sb = new StringBuilder("*" + parts.length + "\r\n");
        for (String part : parts) {
            sb.append("$").append(part.getBytes(StandardCharsets.UTF_8).length)
                    .append("\r\n").append(part).append("\r\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void awaitPortOpen(int port) throws Exception {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        Exception lastFailure = null;
        while (System.currentTimeMillis() < deadline) {
            try (Socket ignored = new Socket("127.0.0.1", port)) {
                return;
            } catch (IOException e) {
                lastFailure = e;
                Thread.sleep(25);
            }
        }
        throw new IllegalStateException("nothing started listening on port " + port, lastFailure);
    }

    /** A raw RESP client, so a test reads exactly what the server wrote back. */
    private final class RespSocket implements AutoCloseable {
        private final Socket socket;
        private final InputStream in;
        private final OutputStream out;

        RespSocket(int port) throws IOException {
            socket = new Socket("127.0.0.1", port);
            socket.setSoTimeout((int) AWAIT_TIMEOUT_MS);
            socket.setTcpNoDelay(true);
            in = socket.getInputStream();
            out = socket.getOutputStream();
        }

        void send(byte[] bytes) throws IOException {
            out.write(bytes);
            out.flush();
        }

        String readLine() throws IOException {
            StringBuilder sb = new StringBuilder();
            int b;
            while ((b = in.read()) != -1) {
                if (b == '\r') {
                    if (in.read() != '\n') {
                        throw new IOException("expected LF after CR");
                    }
                    return sb.toString();
                }
                sb.append((char) b);
            }
            throw new EOFException("connection closed mid line");
        }

        String readReply() throws IOException {
            String header = readLine();
            if (!header.startsWith("$")) {
                return header;
            }
            int length = Integer.parseInt(header.substring(1));
            if (length < 0) {
                return header;
            }
            byte[] payload = in.readNBytes(length);
            if (payload.length != length) {
                throw new EOFException("held " + payload.length + " of " + length + " bytes");
            }
            in.readNBytes(2);
            return header + "\r\n" + new String(payload, StandardCharsets.UTF_8) + "\r\n";
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}
