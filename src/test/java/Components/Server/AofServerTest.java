package Components.Server;

import Components.Persistence.AppendOnlyPersistence;
import Components.Repository.Store;
import Components.Repository.Value;
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
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The master with persistence on: what a client writes is in the file, a write the store
 * refused is not, and the file replays into the state the clients left behind.
 *
 * <p>The server is started once for the class the way the process does it, with the file
 * already open, and each test plays a client against it over a real socket.</p>
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class AofServerTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;
    private static final long NOTHING_EXPECTED_MS = 300;

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
        // recovery runs before the listening socket exists, and that is the order the process
        // starts a persisted master in
        appendOnly.start();
        Thread server = new Thread(masterTcpServer::startServer, "aof-master");
        server.setDaemon(true);
        server.start();
        awaitPortOpen(masterPort);
    }

    @BeforeEach
    void reset() {
        assertTrue(appendOnly.isEnabled(), "the master has no append only file open");
    }

    @Test
    void aWriteFromAClientIsInTheFileAndComesBackAfterARestart() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "aof:set", "hello"));
            assertEquals("+OK", client.readReply());
            client.send(frame("GET", "aof:set"));
            assertEquals("$5\r\nhello\r\n", client.readReply());
        }

        assertArrayEquals(new String[]{"SET", "aof:set", "hello"}, lastFrame("aof:set"));
        assertEquals("hello", recovered("aof:set").val);
    }

    @Test
    void incrementsAreInTheFileAndKeepCountingAfterARestart() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "aof:counter", "41"));
            assertEquals("+OK", client.readReply());
            client.send(frame("INCR", "aof:counter"));
            assertEquals(":42", client.readReply());
        }

        assertArrayEquals(new String[]{"INCR", "aof:counter"}, lastFrame("aof:counter"));
        // the replayed log leaves the counter where the client left it, not at its first value
        assertEquals("42", recovered("aof:counter").val);
    }

    @Test
    void anIncrementTheStoreRefusedIsNotInTheFile() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "aof:word", "not-a-number"));
            assertEquals("+OK", client.readReply());
            client.send(frame("INCR", "aof:word"));
            String reply = client.readReply();
            assertTrue(reply.startsWith("-ERR"), "the increment should have been refused, got: " + reply);
        }

        for (String[] written : frames()) {
            assertFalse(written.length > 1 && written[1].equals("aof:word") && written[0].equals("INCR"),
                    "a refused increment was written to the file: " + String.join(" ", written));
        }
        // and the refused increment is not applied a second time by the replay either
        assertEquals("not-a-number", recovered("aof:word").val);
    }

    @Test
    void readsAndControlCommandsAreNotInTheFile() throws Exception {
        int before = frames().size();
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "aof:quiet", "value"));
            assertEquals("+OK", client.readReply());
            client.send(frame("GET", "aof:quiet"));
            assertEquals("$5\r\nvalue\r\n", client.readReply());
            client.send(frame("PING"));
            assertEquals("+PONG", client.readReply());
            client.send(frame("MULTI"));
            assertEquals("+OK", client.readReply());
            client.send(frame("EXEC"));
            assertEquals("*0", client.readReply(), "an empty transaction should answer with nothing queued");
        }

        for (String[] written : framesSince(before)) {
            assertFalse(List.of("GET", "PING", "MULTI", "EXEC", "DISCARD", "REPLCONF", "PSYNC", "INFO")
                            .contains(written[0]),
                    "a command that changes nothing was written to the file: " + written[0]);
        }
    }

    @Test
    void aDiscardedTransactionIsNotInTheFile() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("MULTI"));
            assertEquals("+OK", client.readReply());
            client.send(frame("SET", "aof:discarded", "value"));
            assertEquals("+QUEUED", client.readReply());
            client.send(frame("DISCARD"));
            assertEquals("+OK", client.readReply());
        }

        assertNull(recovered("aof:discarded"), "a discarded transaction came back after a restart");
    }

    @Test
    void aTransactionIsInTheFileAsOneBlockAndComesBackWhole() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "aof:tx:a", "keep"));
            assertEquals("+OK", client.readReply());
            client.send(frame("SET", "aof:tx:keep", "present"));
            assertEquals("+OK", client.readReply());
        }

        // only what the transaction itself writes is this test's to make claims about
        int before = frames().size();
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("MULTI"));
            assertEquals("+OK", client.readReply());
            client.send(frame("SET", "aof:tx:a", "changed"));
            assertEquals("+QUEUED", client.readReply());
            client.send(frame("INCR", "aof:tx:counter"));
            assertEquals("+QUEUED", client.readReply());
            client.send(frame("DEL", "aof:tx:keep"));
            assertEquals("+QUEUED", client.readReply());
            client.send(frame("EXEC"));
            assertEquals("*3", client.readReply(), "the transaction did not answer with three replies");
        }

        List<String[]> written = framesSince(before);
        int multi = indexOf(written, "MULTI", -1);
        assertTrue(multi >= 0, "no MULTI was written for the transaction");
        assertArrayEquals(new String[]{"SET", "aof:tx:a", "changed"}, written.get(multi + 1));
        assertArrayEquals(new String[]{"INCR", "aof:tx:counter"}, written.get(multi + 2));
        assertArrayEquals(new String[]{"DEL", "aof:tx:keep"}, written.get(multi + 3));
        assertEquals("EXEC", written.get(multi + 4)[0], "the transaction was not closed by its EXEC");
        assertEquals(5, written.size(), "something was written between the transaction's commands");

        Store recovered = replayIntoNewStore();
        assertEquals("changed", recovered.getValue("aof:tx:a").val);
        assertEquals("1", recovered.getValue("aof:tx:counter").val);
        assertNull(recovered.getValue("aof:tx:keep"), "a key deleted by the transaction came back");
    }

    @Test
    void aKeyWithATtlIsRecoveredWithoutRestartingItsDeadline() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "aof:ttl", "temporary"));
            assertEquals("+OK", client.readReply());
            client.send(frame("SET", "aof:ttl", "temporary", "px", "600000"));
            assertEquals("+OK", client.readReply());
        }

        long deadline = store.getValue("aof:ttl").expiry
                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        String[] written = lastFrame("aof:ttl");
        assertEquals("PXAT", written[3], "a relative expiry was written as it arrived: " + String.join(" ", written));
        assertTrue(Math.abs(Long.parseLong(written[4]) - deadline) < 2_000,
                "the deadline in the file is not the one the store is holding");

        Store recovered = replayIntoNewStore();
        assertEquals("temporary", recovered.getValue("aof:ttl").val);
        long recoveredDeadline = recovered.getValue("aof:ttl").expiry
                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        assertTrue(Math.abs(recoveredDeadline - deadline) < 2_000,
                "the key was given a fresh " + (recoveredDeadline - deadline) + "ms on recovery");
    }

    @Test
    void aKeyWhoseTtlRanOutWhileTheServerWasDownComesBackAbsent() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "aof:gone", "brief", "px", "1"));
            assertEquals("+OK", client.readReply());
        }
        // the deadline is already in the past by the time anything reads the file
        Thread.sleep(50);

        assertNull(replayIntoNewStore().getValue("aof:gone"),
                "a key whose deadline had passed came back from the file");
    }

    @Test
    void writesFromManyClientsAllReachTheFileWhole() throws Exception {
        int clients = 6;
        int perClient = 25;
        List<List<String[]>> sent = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();

        for (int c = 0; c < clients; c++) {
            int id = c;
            List<String[]> commands = new ArrayList<>();
            for (int i = 0; i < perClient; i++) {
                commands.add(new String[]{"SET", "aof:many:" + id + ":" + i, "value-" + i});
            }
            sent.add(commands);
            Thread thread = new Thread(() -> {
                try (RespSocket client = new RespSocket(masterPort)) {
                    for (String[] command : commands) {
                        client.send(frame(command));
                        assertEquals("+OK", client.readReply());
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }, "aof-client-" + c);
            threads.add(thread);
        }
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join(30_000);
            assertFalse(thread.isAlive(), "a client did not finish writing");
        }

        // every frame in the file is whole, and every write a client made is in it
        List<String[]> written = frames();
        List<String[]> writtenSets = new ArrayList<>();
        for (String[] command : written) {
            if (command.length == 3 && command[0].equals("SET")) {
                writtenSets.add(command);
            }
        }
        for (int c = 0; c < clients; c++) {
            for (String[] command : sent.get(c)) {
                boolean found = false;
                for (String[] candidate : writtenSets) {
                    if (Arrays.equals(candidate, command)) {
                        found = true;
                        break;
                    }
                }
                assertTrue(found, "a client's write is not in the file: " + String.join(" ", command));
            }
        }
    }

    @Test
    void aTransactionWhoseCommandWasRefusedIsWrittenWithoutThatCommand() throws Exception {
        // the transaction commits the SET and skips the INCR, because the value is not a
        // number. Writing the INCR down would make the file describe a change that never
        // happened, and replaying it would fail the same way all over again
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "aof:partial", "words"));
            assertEquals("+OK", client.readReply());
        }

        int before = frames().size();
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("MULTI"));
            assertEquals("+OK", client.readReply());
            client.send(frame("SET", "aof:partial", "other"));
            assertEquals("+QUEUED", client.readReply());
            client.send(frame("INCR", "aof:partial"));
            assertEquals("+QUEUED", client.readReply());
            client.send(frame("EXEC"));
            assertEquals("*2", client.readReply());
        }

        List<String[]> written = framesSince(before);
        assertEquals(3, written.size(), "the refused command was written down: " + describe(written));
        assertArrayEquals(new String[]{"MULTI"}, written.get(0));
        assertArrayEquals(new String[]{"SET", "aof:partial", "other"}, written.get(1));
        assertArrayEquals(new String[]{"EXEC"}, written.get(2));

        // and the file replays without tripping over the command that was left out
        Store recovered = replayIntoNewStore();
        assertEquals("other", recovered.getValue("aof:partial").val);
    }

    @Test
    void aReadInsideATransactionIsNotWrittenToTheFile() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "aof:read", "value"));
            assertEquals("+OK", client.readReply());
        }

        int before = frames().size();
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("MULTI"));
            assertEquals("+OK", client.readReply());
            client.send(frame("GET", "aof:read"));
            assertEquals("+QUEUED", client.readReply());
            client.send(frame("SET", "aof:read", "written"));
            assertEquals("+QUEUED", client.readReply());
            client.send(frame("EXEC"));
            assertEquals("*2", client.readReply());
        }

        List<String[]> written = framesSince(before);
        assertEquals(3, written.size(), "the read was written down: " + describe(written));
        assertArrayEquals(new String[]{"MULTI"}, written.get(0));
        assertArrayEquals(new String[]{"SET", "aof:read", "written"}, written.get(1));
        assertArrayEquals(new String[]{"EXEC"}, written.get(2));
    }

    @Test
    void aTransactionOfOnlyReadsIsNotWrittenToTheFile() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "aof:onlyread", "value"));
            assertEquals("+OK", client.readReply());
        }

        int before = frames().size();
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("MULTI"));
            assertEquals("+OK", client.readReply());
            client.send(frame("GET", "aof:onlyread"));
            assertEquals("+QUEUED", client.readReply());
            client.send(frame("EXEC"));
            assertEquals("*1", client.readReply());
        }

        assertEquals(0, framesSince(before).size(), "a transaction that changed nothing was written: "
                + describe(framesSince(before)));
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

    private Value recovered(String key) {
        try {
            return replayIntoNewStore().getValue(key);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The last entry in the file that named this key. */
    private String[] lastFrame(String key) {
        List<String[]> written = frames();
        for (int i = written.size() - 1; i >= 0; i--) {
            if (written.get(i).length > 1 && written.get(i)[1].equals(key)) {
                return written.get(i);
            }
        }
        throw new AssertionError("no entry in the file named " + key);
    }

    private static int indexOf(List<String[]> written, String name, int from) {
        for (int i = from + 1; i < written.size(); i++) {
            if (written.get(i)[0].equals(name)) {
                return i;
            }
        }
        return -1;
    }

    /** The frames a test added to the file, which is what it is allowed to make claims about. */
    private List<String[]> framesSince(int mark) {
        List<String[]> all = frames();
        return all.subList(mark, all.size());
    }

    private static String describe(List<String[]> frames) {
        StringBuilder sb = new StringBuilder();
        for (String[] frame : frames) {
            sb.append('[').append(String.join(" ", frame)).append(']');
        }
        return sb.toString();
    }

    /** Every whole frame in the file, which is also the check that none of them were split. */
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
            assertEquals(1, decoded.size(), "a frame at byte " + offset + " decoded into " + decoded.size());
            frames.add(decoded.get(0));
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
        private byte[] buffered = new byte[0];
        private int filled;

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

        /** Reads one reply, CRLF included, so a bulk string keeps its "$3\r\n" header. */
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

        String[] readFrame() throws IOException {
            while (true) {
                List<String[]> frames = takeWholeFrames();
                if (!frames.isEmpty()) {
                    return frames.get(0);
                }
                fill();
            }
        }

        String expectNothing() throws IOException {
            socket.setSoTimeout((int) NOTHING_EXPECTED_MS);
            try {
                return readReply();
            } catch (SocketTimeoutException e) {
                return "";
            } finally {
                socket.setSoTimeout((int) AWAIT_TIMEOUT_MS);
            }
        }

        private List<String[]> takeWholeFrames() {
            List<String[]> frames = new ArrayList<>();
            int consumed = 0;
            while (true) {
                int frameLength = respSerializer.frameLength(buffered, consumed, filled);
                if (frameLength < 0) {
                    break;
                }
                frames.addAll(respSerializer.deseralize(buffered, consumed, consumed + frameLength));
                consumed += frameLength;
            }
            if (consumed > 0) {
                buffered = Arrays.copyOfRange(buffered, consumed, filled);
                filled -= consumed;
            }
            return frames;
        }

        private void fill() throws IOException {
            byte[] chunk = in.readNBytes(4096);
            if (chunk.length == 0) {
                throw new EOFException("the server hung up");
            }
            buffered = Arrays.copyOf(buffered, filled + chunk.length);
            System.arraycopy(chunk, 0, buffered, filled, chunk.length);
            filled += chunk.length;
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}