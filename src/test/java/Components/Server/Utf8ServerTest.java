package Components.Server;

import Components.Infra.Client;
import Components.Infra.ConnectionPool;
import Components.Repository.Store;
import Components.Service.RespSerializer;
import Config.AppConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A value that is not ASCII has to survive the whole way a command travels: the bytes a
 * client writes, the bytes a replica reads, the bytes that go on downstream, and the bytes
 * that come back out of a GET.
 *
 * <p>The first half of these tests talks to a real master over a real socket. The second
 * half hands a replica the write stream a master would send it, one awkward chunk at a
 * time, and plays the replica's own downstream over another real socket, so a value is
 * checked on both hops without a second server having to be started.</p>
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class Utf8ServerTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;

    private static final String VALUE = "नमस्ते 🌍 café";
    private static final String OTHER = "こんにちは";
    private static final String WIDE = "🌍🌍🌍";
    private static final String KEY = "unicode:key";
    /** 18 bytes of Devanagari, 1 space, a 4 byte emoji, 1 space, 5 bytes of "café" */
    private static final int VALUE_BYTES = 29;

    @Autowired
    private MasterTcpServer masterTcpServer;
    @Autowired
    private SlaveTcpServer slaveTcpServer;
    @Autowired
    private ConnectionPool connectionPool;
    @Autowired
    private RedisConfig redisConfig;
    @Autowired
    private Store store;
    @Autowired
    private RespSerializer respSerializer;

    private int masterPort;
    private int replicaPort;

    @BeforeAll
    void startServers() throws Exception {
        masterPort = freePort();
        redisConfig.setRole("master");
        redisConfig.setPort(masterPort);
        CompletableFuture.runAsync(masterTcpServer::startServer);
        awaitPortOpen(masterPort);

        replicaPort = freePort();
        redisConfig.setRole("slave");
        redisConfig.setPort(replicaPort);
        // pointed at a closed port, so the replica gives up on its own upstream at once and
        // these tests can hand it that stream themselves
        redisConfig.setMasterHost("127.0.0.1");
        redisConfig.setMasterPort(freePort());
        CompletableFuture.runAsync(slaveTcpServer::startServer);
        awaitPortOpen(replicaPort);
    }

    @BeforeEach
    void clearKeys() {
        for (String key : List.of(KEY, OTHER, "कुंजी", "काउंटर", "empty:key", "chain:utf8", "chain:wide")) {
            store.map.remove(key);
        }
    }

    // ---------- a master over a real socket ----------

    @Test
    void aUtf8ValueComesBackFromAGetExactly() throws Exception {
        // the header has to say 29 for a value the JVM counts as 13 characters
        assertEquals(VALUE_BYTES, VALUE.getBytes(StandardCharsets.UTF_8).length);
        assertTrue(VALUE.length() < VALUE_BYTES);

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", KEY, VALUE));
            assertEquals("+OK\r\n", client.readReply());

            client.send(frame("GET", KEY));
            assertEquals("$" + VALUE_BYTES + "\r\n" + VALUE + "\r\n", client.readReply(),
                    "the reply did not carry the value with a header that counts its bytes");
        }
    }

    @Test
    void aUtf8KeyIsStoredUnderTheNameItWasSentWith() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "कुंजी", OTHER));
            assertEquals("+OK\r\n", client.readReply());

            client.send(frame("GET", "कुंजी"));
            assertEquals("$15\r\n" + OTHER + "\r\n", client.readReply(),
                    "the value was not returned for the key it was stored under");
        }
    }

    @Test
    void aMissingKeyStillAnswersWithTheNullBulkString() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("GET", "unicode:absent"));
            assertEquals("$-1\r\n", client.readReply());
        }
    }

    @Test
    void anEmptyValueIsAZeroLengthBulkString() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "empty:key", ""));
            assertEquals("+OK\r\n", client.readReply());

            client.send(frame("GET", "empty:key"));
            assertEquals("$0\r\n\r\n", client.readReply());
        }
    }

    @Test
    void pipelinedUtf8CommandsAllRun() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(concat(frame("SET", KEY, VALUE), frame("GET", KEY), frame("PING")));

            assertEquals("+OK\r\n", client.readReply());
            assertEquals("$" + VALUE_BYTES + "\r\n" + VALUE + "\r\n", client.readReply());
            assertEquals("+PONG\r\n", client.readReply());
        }
    }

    @Test
    void aCommandCutInsideAMultibyteCharacterIsHeldUntilItIsWhole() throws Exception {
        byte[] command = frame("SET", KEY, VALUE);
        // the offsets that matter: the head of the frame, and a cut in the middle of a
        // three byte character, where a reader counting characters would decode garbage
        int insideCharacter = indexOf(command, "न".getBytes(StandardCharsets.UTF_8)) + 1;
        int[] cuts = {4, insideCharacter, command.length - 2, command.length - 1};

        for (int cut : cuts) {
            try (RespSocket client = new RespSocket(masterPort)) {
                client.send(Arrays.copyOfRange(command, 0, cut));
                assertEquals("", client.expectNothing(),
                        "a command cut at " + cut + " was acted on before the rest arrived");

                client.send(Arrays.copyOfRange(command, cut, command.length));
                assertEquals("+OK\r\n", client.readReply(), "the command did not run once it was whole");

                client.send(frame("GET", KEY));
                assertEquals("$" + VALUE_BYTES + "\r\n" + VALUE + "\r\n", client.readReply(),
                        "the value did not survive being cut at " + cut);
            }
        }
    }

    @Test
    void aTransactionOfUtf8WritesIsApplied() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("MULTI"));
            assertEquals("+OK\r\n", client.readReply());
            client.send(frame("SET", KEY, VALUE));
            assertEquals("+QUEUED\r\n", client.readReply());
            client.send(frame("SET", "chain:wide", WIDE));
            assertEquals("+QUEUED\r\n", client.readReply());
            client.send(frame("DEL", KEY));
            assertEquals("+QUEUED\r\n", client.readReply());
            client.send(frame("EXEC"));
            assertEquals("*3\r\n+OK\r\n+OK\r\n+OK\r\n", client.readArrayReply());

            client.send(frame("GET", KEY));
            assertEquals("$-1\r\n", client.readReply(), "the transaction's delete did not take effect");
            client.send(frame("GET", "chain:wide"));
            assertEquals("$12\r\n" + WIDE + "\r\n", client.readReply(),
                    "the transaction kept the value it did not delete");
        }
    }

    @Test
    void incrCountsFromAUtf8KeyNameWithoutChangingItsSemantics() throws Exception {
        // INCR still refuses anything that is not a number: only the key's name is not
        // ASCII here, which is all that is needed to put a multibyte key through the
        // command path and its reply
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "काउंटर", "41"));
            assertEquals("+OK\r\n", client.readReply());

            client.send(frame("INCR", "काउंटर"));
            assertEquals(":42\r\n", client.readReply());

            client.send(frame("GET", "काउंटर"));
            assertEquals("$2\r\n42\r\n", client.readReply());

            client.send(frame("SET", "काउंटर", VALUE));
            assertEquals("+OK\r\n", client.readReply());
            client.send(frame("INCR", "काउंटर"));
            assertEquals("-ERR value is not an integer or out of range\r\n", client.readReply(),
                    "INCR started to accept something it used to refuse");
        }
    }

    // ---------- a replica, and the replica after it ----------

    @Test
    void aUtf8WriteIsAppliedExactlyAndPassedOnExactly() throws Exception {
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            register(downstream);

            feed(frame("SET", KEY, VALUE));

            assertEquals("$" + VALUE_BYTES + "\r\n" + VALUE + "\r\n", store.get(KEY), "the replica did not store the value it was sent");
            List<String[]> passedOn = downstream.readFramesUntilAck();
            assertEquals(1, passedOn.size());
            assertArrayEquals(new String[]{"SET", KEY, VALUE}, passedOn.get(0),
                    "the next hop was not sent the same value");
        }
    }

    @Test
    void theBytesPassedDownstreamDeclareTheirOwnLength() throws Exception {
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            register(downstream);

            feed(frame("SET", KEY, VALUE), frame("SET", "chain:wide", WIDE));

            // the frames the replica sends have to measure their payloads in bytes as well,
            // or the replica reading them lands inside the payload
            List<String[]> passedOn = downstream.readFramesUntilAck();
            assertEquals(2, passedOn.size());
            assertArrayEquals(new String[]{"SET", KEY, VALUE}, passedOn.get(0));
            assertArrayEquals(new String[]{"SET", "chain:wide", WIDE}, passedOn.get(1));
        }
    }

    @Test
    void aUtf8WriteIsAppliedWhenTheStreamIsCutInsideAMultibyteCharacter() throws Exception {
        byte[] command = frame("SET", KEY, VALUE);
        int cut = indexOf(command, "न".getBytes(StandardCharsets.UTF_8)) + 2;

        try (RespSocket downstream = new RespSocket(replicaPort)) {
            register(downstream);

            feed(Arrays.copyOfRange(command, 0, cut), Arrays.copyOfRange(command, cut, command.length));

            assertEquals("$" + VALUE_BYTES + "\r\n" + VALUE + "\r\n", store.get(KEY),
                    "a payload split inside a multibyte character was not stored whole");
            List<String[]> passedOn = downstream.readFramesUntilAck();
            assertArrayEquals(new String[]{"SET", KEY, VALUE}, passedOn.get(0));
        }
    }

    @Test
    void aUtf8DeleteIsAppliedAndPassedOn() throws Exception {
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            register(downstream);

            feed(frame("SET", KEY, VALUE), frame("DEL", KEY));

            assertNull(store.getValue(KEY), "the replica kept a key the master deleted");
            List<String[]> passedOn = downstream.readFramesUntilAck();
            assertEquals(2, passedOn.size());
            assertArrayEquals(new String[]{"DEL", KEY}, passedOn.get(1),
                    "the delete was not passed on with the key it names");
        }
    }

    @Test
    void theWritesOfATransactionOfUtf8ValuesArePassedOnInOrder() throws Exception {
        // a master sends the writes of a transaction one frame at a time, in the order it
        // applied them, so a replica sees them the same way a client would
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            register(downstream);

            feed(frame("SET", KEY, VALUE), frame("SET", "chain:wide", WIDE), frame("DEL", KEY));

            assertNull(store.getValue(KEY));
            assertEquals("$12\r\n" + WIDE + "\r\n", store.get("chain:wide"));
            List<String[]> passedOn = downstream.readFramesUntilAck();
            assertEquals(3, passedOn.size());
            assertArrayEquals(new String[]{"SET", KEY, VALUE}, passedOn.get(0));
            assertArrayEquals(new String[]{"SET", "chain:wide", WIDE}, passedOn.get(1));
            assertArrayEquals(new String[]{"DEL", KEY}, passedOn.get(2));
        }
    }

    @Test
    void aUtf8WriteIsCountedInTheBytesTheNextHopSees() throws Exception {
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            register(downstream);
            long before = redisConfig.getMasterReplOffset().longValue();
            long bytesBefore = connectionPool.bytesSentToSlaves.get();

            feed(frame("SET", KEY, VALUE), frame("DEL", KEY));

            long frames = frame("SET", KEY, VALUE).length + frame("DEL", KEY).length;
            assertEquals(before + frames, redisConfig.getMasterReplOffset().longValue(),
                    "the offset did not move by the bytes the frame is worth");
            assertEquals(bytesBefore + frames, connectionPool.bytesSentToSlaves.get(),
                    "the bytes sent onwards do not match the frames that were propagated");
        }
    }

    // ---------- helpers ----------

    private void register(RespSocket downstream) throws Exception {
        downstream.send(frame("REPLCONF", "listening-port", "" + replicaPort));
        assertEquals("+OK\r\n", downstream.readReply());
        await(() -> !connectionPool.getSlaves().isEmpty(), "the downstream replica never registered");
    }

    /** Runs the upstream stream the way the master's socket delivers it. */
    private void feed(byte[]... reads) throws IOException {
        Client master = new Client(new Socket(), new ChunkedInputStream(List.of(reads)),
                new ByteArrayOutputStream(), -1);
        slaveTcpServer.streamFromMaster(master);
    }

    private static byte[] frame(String... parts) {
        StringBuilder sb = new StringBuilder("*").append(parts.length).append("\r\n");
        for (String part : parts) {
            byte[] payload = part.getBytes(StandardCharsets.UTF_8);
            sb.append("$").append(payload.length).append("\r\n").append(part).append("\r\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] concat(byte[]... frames) {
        int length = 0;
        for (byte[] part : frames) {
            length += part.length;
        }
        byte[] all = new byte[length];
        int at = 0;
        for (byte[] part : frames) {
            System.arraycopy(part, 0, all, at, part.length);
            at += part.length;
        }
        return all;
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        fail("the bytes being looked for are not in the frame");
        return -1;
    }

    private static void assertArrayEquals(String[] expected, String[] actual, String... because) {
        String why = because.length == 0 ? "" : String.join(" ", because) + ": ";
        org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual,
                why + "expected " + String.join(" ", expected) + " but got " + String.join(" ", actual));
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

    private static void await(BooleanSupplier condition, String failureMessage) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        fail(failureMessage);
    }

    /** Hands out one scripted chunk per read, which is what a socket does. */
    private static final class ChunkedInputStream extends InputStream {
        private final List<byte[]> chunks;
        private int next;
        private int position;

        private ChunkedInputStream(List<byte[]> chunks) {
            this.chunks = chunks;
        }

        private byte[] current() {
            while (next < chunks.size()) {
                byte[] chunk = chunks.get(next);
                if (position < chunk.length) {
                    return chunk;
                }
                next++;
                position = 0;
            }
            return null;
        }

        @Override
        public int read() {
            byte[] chunk = current();
            return chunk == null ? -1 : chunk[position++] & 0xFF;
        }

        @Override
        public int read(byte[] destination, int offset, int length) {
            byte[] chunk = current();
            if (chunk == null) {
                return -1;
            }
            // a read stops at the end of what is available, so a chunk bigger than the
            // buffer is handed over in pieces instead of losing its tail
            int count = Math.min(length, chunk.length - position);
            System.arraycopy(chunk, position, destination, offset, count);
            position += count;
            return count;
        }
    }

    /** A raw RESP client, so tests can split a command at an exact byte. */
    private final class RespSocket implements AutoCloseable {
        private final Socket socket;
        private final InputStream in;
        private final OutputStream out;
        private byte[] buffered = new byte[0];
        private int filled = 0;

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

        void send(String text) throws IOException {
            send(text.getBytes(StandardCharsets.UTF_8));
        }

        /** Reads one reply, CRLF included, so a bulk string keeps its "$29\r\n" header. */
        String readReply() throws IOException {
            String header = readLine();
            if (!header.startsWith("$")) {
                return header + "\r\n";
            }
            int length = Integer.parseInt(header.substring(1));
            if (length < 0) {
                return header + "\r\n";
            }
            byte[] payload = in.readNBytes(length);
            if (payload.length != length) {
                throw new EOFException("reply held " + payload.length + " of " + length + " bytes");
            }
            byte[] terminator = in.readNBytes(2);
            if (terminator[0] != '\r' || terminator[1] != '\n') {
                throw new IOException("bulk string was not closed by CRLF");
            }
            return header + "\r\n" + new String(payload, StandardCharsets.UTF_8) + "\r\n";
        }

        /** Reads an array reply whole: the "*n" header and one reply per element. */
        String readArrayReply() throws IOException {
            String header = readLine();
            int elements = Integer.parseInt(header.substring(1));
            StringBuilder reply = new StringBuilder(header).append("\r\n");
            for (int element = 0; element < elements; element++) {
                reply.append(readReply());
            }
            return reply.toString();
        }

        /** Reads the frames the replica propagated, using a GETACK as a barrier. */
        List<String[]> readFramesUntilAck() throws IOException {
            List<String[]> frames = new ArrayList<>();
            send(frame("REPLCONF", "GETACK", "*"));
            while (true) {
                for (String[] command : takeWholeFrames()) {
                    if (command[0].equalsIgnoreCase("REPLCONF") && command[1].equalsIgnoreCase("ACK")) {
                        return frames;
                    }
                    frames.add(command);
                }
                fill();
            }
        }

        String expectNothing() throws IOException {
            socket.setSoTimeout(300);
            try {
                return readReply();
            } catch (java.net.SocketTimeoutException e) {
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
            byte[] chunk = new byte[4096];
            int bytesRead = in.read(chunk);
            if (bytesRead == -1) {
                throw new EOFException("the replica hung up");
            }
            buffered = Arrays.copyOf(buffered, filled + bytesRead);
            System.arraycopy(chunk, 0, buffered, filled, bytesRead);
            filled += bytesRead;
        }

        private String readLine() throws IOException {
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

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}