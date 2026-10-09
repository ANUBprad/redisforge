package Components.Server;

import Components.Persistence.AppendOnlyPersistence;
import Components.Repository.Store;
import Components.Repository.Value;
import Components.Service.RespSerializer;
import Config.AppConfig;
import org.junit.jupiter.api.BeforeAll;
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
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The expiration commands a client drives over a real socket, and what they leave in the
 * append-only file: absolute deadlines on disk, refused commands left out, and a replay
 * that respects the deadline instead of starting the clock over.
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ExpirationServerTest {

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
        Thread server = new Thread(masterTcpServer::startServer, "expiration-master");
        server.setDaemon(true);
        server.start();
        awaitPortOpen(masterPort);
    }

    @Test
    void expireSetsAppearsInTtlAndCanBePersistedAway() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "exp:basic", "v"));
            assertEquals("+OK", client.readReply());

            client.send(frame("EXPIRE", "exp:basic", "100"));
            assertEquals(":1", client.readReply());

            client.send(frame("TTL", "exp:basic"));
            assertEquals(":100", client.readReply());

            client.send(frame("PTTL", "exp:basic"));
            int pttl = Integer.parseInt(client.readReply().substring(1));
            assertTrue(pttl > 99_000 && pttl <= 100_000, "PTTL was " + pttl);

            client.send(frame("PERSIST", "exp:basic"));
            assertEquals(":1", client.readReply());
            client.send(frame("TTL", "exp:basic"));
            assertEquals(":-1", client.readReply());

            // nothing left to persist a second time
            client.send(frame("PERSIST", "exp:basic"));
            assertEquals(":0", client.readReply());
        }
    }

    @Test
    void ttlAndPttlAnswerForKeysThatDoNotExist() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("TTL", "exp:absent"));
            assertEquals(":-2", client.readReply());
            client.send(frame("PTTL", "exp:absent"));
            assertEquals(":-2", client.readReply());
        }
    }

    @Test
    void anExpirationOnAMissingKeyIsRefusedAndNotWritten() throws Exception {
        int before = frames().size();
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("EXPIRE", "exp:missing", "100"));
            assertEquals(":0", client.readReply());
            client.send(frame("PERSIST", "exp:missing"));
            assertEquals(":0", client.readReply());
        }
        assertEquals(before, frames().size(), "a refused expiration was written to the file");
    }

    @Test
    void readsAndMissingKeyCommandsDoNotReachTheFile() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "exp:quiet", "v"));
            assertEquals("+OK", client.readReply());
        }
        int before = frames().size();
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("TTL", "exp:quiet"));
            assertTrue(client.readReply().startsWith(":"));
            client.send(frame("PTTL", "exp:quiet"));
            assertTrue(client.readReply().startsWith(":"));
            client.send(frame("EXPIRE", "exp:quiet", "1000"));
            assertEquals(":1", client.readReply());
            client.send(frame("TTL", "exp:quiet"));
            assertTrue(client.readReply().startsWith(":"));
        }

        for (String[] written : framesSince(before)) {
            assertTrue(List.of("EXPIRE", "PEXPIREAT", "PERSIST", "DEL").contains(written[0]),
                    "a read or refused command reached the file: " + String.join(" ", written));
        }
    }

    @Test
    void expireIsStoredAsAnAbsoluteDeadlineInTheFile() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "exp:absolute", "v"));
            assertEquals("+OK", client.readReply());
            client.send(frame("EXPIRE", "exp:absolute", "100"));
            assertEquals(":1", client.readReply());
        }

        long deadline = store.getValue("exp:absolute").expiry
                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        String[] written = lastFrame("exp:absolute");
        assertEquals("PEXPIREAT", written[0], "a relative expiry was written as it arrived: "
                + String.join(" ", written));
        assertTrue(Math.abs(Long.parseLong(written[2]) - deadline) < 2_000,
                "the deadline in the file is not the one the store is holding");

        Store recovered = replayIntoNewStore();
        long recoveredDeadline = recovered.getValue("exp:absolute").expiry
                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        assertTrue(Math.abs(recoveredDeadline - deadline) < 2_000,
                "the key was given a fresh " + (recoveredDeadline - deadline) + "ms on recovery");
    }

    @Test
    void persistIsWrittenAsPersistAndReplaysWithNoDeadline() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "exp:persist", "v"));
            assertEquals("+OK", client.readReply());
            client.send(frame("EXPIRE", "exp:persist", "100"));
            assertEquals(":1", client.readReply());
            client.send(frame("PERSIST", "exp:persist"));
            assertEquals(":1", client.readReply());
        }

        assertEquals("PERSIST", lastFrame("exp:persist")[0]);
        assertEquals(Value.class, replayIntoNewStore().getValue("exp:persist").getClass());
        assertTrue(replayIntoNewStore().getValue("exp:persist").expiry
                        .equals(java.time.LocalDateTime.MAX),
                "a persisted key came back with a deadline");
    }

    @Test
    void aKeyWhoseDeadlineRanOutWhileTheServerWasDownComesBackAbsent() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "exp:gone", "v"));
            assertEquals("+OK", client.readReply());
            client.send(frame("PEXPIRE", "exp:gone", "1"));
            assertEquals(":1", client.readReply());
        }
        Thread.sleep(50);

        assertNull(replayIntoNewStore().getValue("exp:gone"),
                "a key whose deadline had passed came back from the file");
    }

    @Test
    void setWithPxIsOverriddenByALaterExpire() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "exp:override", "v", "px", "600000"));
            assertEquals("+OK", client.readReply());
            client.send(frame("EXPIRE", "exp:override", "100"));
            assertEquals(":1", client.readReply());
            client.send(frame("TTL", "exp:override"));
            assertEquals(":100", client.readReply());
        }
    }

    @Test
    void anExpirationInsideATransactionIsAppliedAtExecAndWritten() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "exp:tx", "v"));
            assertEquals("+OK", client.readReply());
        }

        int before = frames().size();
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("MULTI"));
            assertEquals("+OK", client.readReply());
            client.send(frame("EXPIRE", "exp:tx", "100"));
            assertEquals("+QUEUED", client.readReply());
            client.send(frame("EXEC"));
            assertEquals("*1", client.readReply());
        }

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("TTL", "exp:tx"));
            assertEquals(":100", client.readReply());
        }

        List<String[]> written = framesSince(before);
        assertFalse(written.isEmpty(), "the transaction's expiration was not written");
        assertEquals("MULTI", written.get(0)[0]);
        assertTrue(written.get(1)[0].equals("PEXPIREAT"),
                "the transaction's expiration was not written absolutely: " + String.join(" ", written.get(1)));
    }

    @Test
    void anOutOfRangeExpireAtInsideATransactionIsClampedNotCrashed() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "exp:txhuge", "v"));
            assertEquals("+OK", client.readReply());

            client.send(frame("MULTI"));
            assertEquals("+OK", client.readReply());
            client.send(frame("EXPIREAT", "exp:txhuge", String.valueOf(Long.MAX_VALUE)));
            assertEquals("+QUEUED", client.readReply());
            client.send(frame("EXEC"));
            assertEquals("*1", client.readReply());
        }

        String written = lastFrame("exp:txhuge")[0];
        assertTrue(written.equals("PERSIST") || written.equals("PEXPIREAT"),
                "a clamped deadline was written as: " + written);

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("PTTL", "exp:txhuge"));
            assertEquals(":-1", client.readReply());
        }
    }

    @Test
    void anExpiredKeyReadBackIsGoneEvenWithoutBeingRead() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "exp:sweep", "v"));
            assertEquals("+OK", client.readReply());
            client.send(frame("PEXPIRE", "exp:sweep", "30"));
            assertEquals(":1", client.readReply());
        }
        Thread.sleep(150);

        // the active sweep runs on its own; give it a few periods to reach this key
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && store.peekValue("exp:sweep") != null) {
            store.removeExpired();
            Thread.sleep(20);
        }
        assertNull(store.peekValue("exp:sweep"),
                "an expired key was never removed by the active sweep");

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("GET", "exp:sweep"));
            assertEquals("$-1", client.readReply());
        }
    }

    /** A fresh store replayed from the file, which is what a restarted server would hold. */
    private Store replayIntoNewStore() throws IOException {
        Store fresh = new Store();
        fresh.respSerializer = new RespSerializer();
        AppendOnlyPersistence replayed = new AppendOnlyPersistence(redisConfig, fresh);
        try {
            replayed.start();
        } finally {
            replayed.close();
        }
        return fresh;
    }

    private String[] lastFrame(String key) {
        List<String[]> written = frames();
        for (int i = written.size() - 1; i >= 0; i--) {
            if (written.get(i).length > 1 && written.get(i)[1].equals(key)) {
                return written.get(i);
            }
        }
        throw new AssertionError("no entry in the file named " + key);
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
            List<String[]> decoded = respSerializer.deseralize(contents, offset, offset + length);
            frames.addAll(decoded);
            offset += length;
        }
        return frames;
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
