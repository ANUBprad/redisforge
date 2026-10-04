package Components.Server;

import Components.Infra.Client;
import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
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
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A replica that can be the upstream master of another replica. The downstream replica
 * runs the same handshake against it (PING, REPLCONF listening-port, REPLCONF capa,
 * PSYNC), registers itself as a slave, and then receives the writes this replica applies.
 * These tests play that downstream replica over a real socket and drive this replica's
 * own upstream with a scripted byte stream, so both hops are exercised deterministically.
 *
 * <p>The project's PSYNC ships an empty RDB, so a downstream replica starts empty and
 * picks up the writes that happen after it attaches. Handing it the writes that were
 * already applied would mean generating an RDB of the current dataset, which is out of
 * scope here.</p>
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ChainedReplicationTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;
    private static final long NOTHING_EXPECTED_MS = 300;

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

    private int replicaPort;

    @BeforeAll
    void startReplica() throws Exception {
        replicaPort = freePort();
        // the upstream handshake is pointed at a closed port so it gives up at once; these
        // tests feed the upstream stream in directly instead
        redisConfig.setRole("slave");
        redisConfig.setPort(replicaPort);
        redisConfig.setMasterHost("127.0.0.1");
        redisConfig.setMasterPort(freePort());
        // one server for the whole class, on a thread of its own: startServer never
        // returns, so a server per test would pile up accept loops on the common pool and
        // starve the other test classes that start their servers the same way
        Thread server = new Thread(slaveTcpServer::startServer, "chained-replication-replica");
        server.setDaemon(true);
        server.start();
        awaitPortOpen(replicaPort);
    }

    @BeforeEach
    void resetReplicaState() throws Exception {
        await(() -> connectionPool.getSlaves().isEmpty(), "a replica from an earlier test is still registered");
        connectionPool.slavesThatAreCaughtUp = 0;
        connectionPool.bytesSentToSlaves = 0;
        redisConfig.setMasterReplOffset(0L);
    }

    @Test
    void replconfListeningPortRegistersTheReplicaAndAnswersOk() throws Exception {
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            downstream.send(frame("REPLCONF", "listening-port", "" + replicaPort));

            assertEquals("+OK", downstream.readReply());
            await(() -> connectionPool.getSlaves().size() == 1, "the connection was not registered as a replica");
            assertTrue(connectionPool.getClients().isEmpty(),
                    "a connection that registered itself as a replica is still held as a plain client");
        }
    }

    @Test
    void replconfCapaPsync2IsAcceptedAndRecorded() throws Exception {
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            register(downstream);

            downstream.send(frame("REPLCONF", "capa", "psync2"));

            assertEquals("+OK", downstream.readReply());
            Slave registered = awaitSlave();
            assertTrue(registered.capabilities.contains("psync2"),
                    "the capability was not recorded: " + registered.capabilities);
        }
    }

    @Test
    void replconfAckMatchingTheStreamCountsTheReplicaAsCaughtUp() throws Exception {
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            register(downstream);
            connectionPool.slavesThatAreCaughtUp = 0;
            connectionPool.bytesSentToSlaves = 42;

            downstream.send(frame("REPLCONF", "ACK", "42"));

            await(() -> connectionPool.slavesThatAreCaughtUp == 1,
                    "an ACK that matches the bytes sent did not count the replica as caught up");
            assertEquals("", downstream.expectNothing(),
                    "an ACK is consumed, it is not answered");
        }
    }

    @Test
    void replconfAckThatDoesNotMatchTheStreamCountsNothing() throws Exception {
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            register(downstream);
            connectionPool.slavesThatAreCaughtUp = 0;
            connectionPool.bytesSentToSlaves = 42;

            downstream.send(frame("REPLCONF", "ACK", "7"));

            assertEquals("", downstream.expectNothing());
            assertEquals(0, connectionPool.slavesThatAreCaughtUp,
                    "an ACK below what was sent counted the replica as caught up");
        }
    }

    @Test
    void replconfGetackIsAnsweredWithTheCurrentOffset() throws Exception {
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            register(downstream);
            redisConfig.setMasterReplOffset(1234L);

            downstream.send(frame("REPLCONF", "GETACK", "*"));

            assertArrayEquals(new String[]{"REPLCONF", "ACK", "1234"}, downstream.readFrame());
        }
    }

    @Test
    void replicaCompletesTheHandshakeOfADownstreamReplica() throws Exception {
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            downstream.send(frame("PING"));
            assertEquals("+PONG", downstream.readReply());

            downstream.send(frame("REPLCONF", "listening-port", "" + replicaPort));
            assertEquals("+OK", downstream.readReply());
            downstream.send(frame("REPLCONF", "capa", "psync2"));
            assertEquals("+OK", downstream.readReply());

            downstream.send(frame("PSYNC", "?", "-1"));

            String status = downstream.readLine();
            assertTrue(status.startsWith("+FULLRESYNC "), "no FULLRESYNC for the downstream replica: " + status);
            int declaredLength = Integer.parseInt(downstream.readLine().substring(1));
            assertTrue(declaredLength > 0, "FULLRESYNC declared an empty payload");
            byte[] payload = downstream.readExactly(declaredLength);
            assertEquals("REDIS", new String(payload, 0, 5, StandardCharsets.UTF_8),
                    "the RDB payload did not arrive whole");
            // the payload is counted off rather than scanned for a delimiter, so the bytes
            // after it are the start of a streamed command and nothing else
            assertEquals("", downstream.expectNothing(),
                    "something followed the RDB payload that was not a streamed command");
            await(() -> connectionPool.getSlaves().size() == 1,
                    "the downstream replica was not registered as a slave");
        }
    }

    @Test
    void replicaStreamsUpstreamWritesDownstreamInOrder() throws Exception {
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            register(downstream);
            store.map.remove("chain:order");

            // enough writes that propagation on its own task per write would not keep them
            // in step, and the whole batch arrives in one read, as a master floods a replica
            List<String> values = new ArrayList<>();
            ByteArrayOutputStream upstream = new ByteArrayOutputStream();
            for (int i = 0; i < 60; i++) {
                String value = "value-" + i;
                values.add(value);
                upstream.writeBytes(frame("SET", "chain:order", value));
            }
            feed(upstream.toByteArray());

            List<String[]> received = downstream.readFramesUntilAck();

            assertEquals(values.size(), received.size(), "the downstream replica did not get every write");
            for (int i = 0; i < values.size(); i++) {
                assertArrayEquals(new String[]{"SET", "chain:order", values.get(i)}, received.get(i),
                        "write " + i + " did not arrive in the order it was applied");
            }
            assertEquals(values.get(values.size() - 1), store.getValue("chain:order").val);
        }
    }

    @Test
    void downstreamReplicaGetsSpecialValuesLargeValuesAndPipelinedWritesIntact() throws Exception {
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            register(downstream);
            store.map.remove("chain:special");

            String large = "v".repeat(1024 * 1024);
            // CRLF and RESP punctuation inside a payload, spaces, and an empty value are
            // what a hand written parser mangles
            List<String[]> commands = List.of(
                    new String[]{"SET", "chain:special", "line1\r\nline2"},
                    new String[]{"SET", "chain:special", "*2\r\n$5\r\nhello"},
                    new String[]{"SET", "chain:special", "  padded  "},
                    new String[]{"SET", "chain:special", ""},
                    new String[]{"SET", "chain:special", large});

            ByteArrayOutputStream upstream = new ByteArrayOutputStream();
            for (String[] command : commands) {
                upstream.writeBytes(frame(command));
            }
            // all of them pipelined, cut across two reads so a frame has to be carried over
            byte[] all = upstream.toByteArray();
            feed(Arrays.copyOfRange(all, 0, 37), Arrays.copyOfRange(all, 37, all.length));

            List<String[]> received = downstream.readFramesUntilAck();

            assertEquals(commands.size(), received.size());
            for (int i = 0; i < commands.size(); i++) {
                assertArrayEquals(commands.get(i), received.get(i),
                        "the downstream replica got something else for write " + i);
            }
            assertEquals(large, store.getValue("chain:special").val);
        }
    }

    @Test
    void waitCountsTheDownstreamReplicaThatAcknowledged() throws Exception {
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            register(downstream);
            byte[] write = frame("SET", "chain:wait", "acknowledged");
            feed(write);

            // WAIT asks the downstream replica for its offset the way a master does. The
            // existing semantics are simplified: every replica that reports back exactly
            // the number of write bytes it was sent counts as one, and the count is reset
            // after each WAIT.
            try (RespSocket client = new RespSocket(replicaPort)) {
                client.send(frame("WAIT", "1", "5000"));

                String[] getack = downstream.readControlFrame();
                assertArrayEquals(new String[]{"REPLCONF", "GETACK", "*"}, getack);
                // the downstream replica has received exactly the one write so far
                downstream.send(frame("REPLCONF", "ACK", "" + write.length));

                assertEquals(":1", client.readReply(), "the downstream replica was not counted by WAIT");
            }
            assertEquals(0, connectionPool.slavesThatAreCaughtUp, "the caught up count was not reset");

            // a downstream replica that does not answer counts for nothing
            feed(frame("SET", "chain:wait", "unanswered"));
            try (RespSocket client = new RespSocket(replicaPort)) {
                client.send(frame("WAIT", "1", "150"));
                assertEquals(":0", client.readReply());
            }
        }
    }

    @Test
    void replicaKeepsServingItsOwnClientsWhileItReplicates() throws Exception {
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            register(downstream);
            feed(frame("SET", "chain:client", "served"));

            try (RespSocket client = new RespSocket(replicaPort)) {
                client.send(frame("PING"));
                assertEquals("+PONG", client.readReply());
                client.send(frame("GET", "chain:client"));
                assertEquals("$6\r\nserved\r\n", client.readReply());
                client.send(frame("SET", "chain:client", "nope"));
                String write = client.readReply();
                assertTrue(write.startsWith("-READONLY"), "the replica accepted a write, answering: " + write);
                client.send(frame("GET", "chain:never-written"));
                assertEquals("$-1", client.readReply());
            }
        }
    }

    private void register(RespSocket downstream) throws Exception {
        downstream.send(frame("REPLCONF", "listening-port", "" + replicaPort));
        assertEquals("+OK", downstream.readReply());
        await(() -> !connectionPool.getSlaves().isEmpty(), "the downstream replica never registered");
    }

    /** Runs the upstream stream the way the master's socket delivers it. */
    private void feed(byte[]... reads) throws IOException {
        Client master = new Client(new Socket(), new ChunkedInputStream(List.of(reads)),
                new ByteArrayOutputStream(), -1);
        slaveTcpServer.streamFromMaster(master);
    }

    private Slave awaitSlave() throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (!connectionPool.getSlaves().isEmpty()) {
                return connectionPool.getSlaves().iterator().next();
            }
            Thread.sleep(25);
        }
        fail("no replica registered");
        return null;
    }

    private static byte[] frame(String... parts) {
        StringBuilder sb = new StringBuilder("*").append(parts.length).append("\r\n");
        for (String part : parts) {
            sb.append("$").append(part.length()).append("\r\n").append(part).append("\r\n");
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

        ChunkedInputStream(List<byte[]> chunks) {
            this.chunks = chunks;
        }

        private byte[] current() throws IOException {
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
        public int read() throws IOException {
            byte[] chunk = current();
            return chunk == null ? -1 : chunk[position++] & 0xFF;
        }

        @Override
        public int read(byte[] destination, int offset, int length) throws IOException {
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

    /** A raw RESP client, so a test can read exactly the bytes a replica was sent. */
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
            byte[] payload = readExactly(length);
            readExactly(2);
            return header + "\r\n" + new String(payload, StandardCharsets.UTF_8) + "\r\n";
        }

        byte[] readExactly(int length) throws IOException {
            byte[] payload = in.readNBytes(length);
            if (payload.length != length) {
                throw new EOFException("held " + payload.length + " of " + length + " bytes");
            }
            return payload;
        }

        /** Reads the next whole frame the replica sent. */
        String[] readFrame() throws IOException {
            while (true) {
                List<String[]> frames = takeWholeFrames();
                if (!frames.isEmpty()) {
                    return frames.get(0);
                }
                fill();
            }
        }

        /** Reads the next replication control frame, stepping over propagated writes. */
        String[] readControlFrame() throws IOException {
            while (true) {
                String[] command = readFrame();
                if (command[0].equalsIgnoreCase("REPLCONF")) {
                    return command;
                }
            }
        }

        /**
         * Reads the frames the replica propagated, using a GETACK as a barrier: propagation
         * happens on the upstream thread, so once the upstream stream has finished, the
         * answer to the barrier sits behind every byte that was propagated.
         */
        List<String[]> readFramesUntilAck() throws IOException {
            List<String[]> frames = new ArrayList<>();
            send(frame("REPLCONF", "GETACK", "*"));
            while (true) {
                for (String[] command : takeWholeFrames()) {
                    frames.add(command);
                    if (command[0].equalsIgnoreCase("REPLCONF") && command[1].equalsIgnoreCase("ACK")) {
                        frames.remove(frames.size() - 1);
                        return frames;
                    }
                }
                fill();
            }
        }

        /** Returns "" when the replica sends nothing at all, which some replies are. */
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
            byte[] chunk = new byte[4096];
            int bytesRead = in.read(chunk);
            if (bytesRead == -1) {
                throw new EOFException("the replica hung up");
            }
            buffered = Arrays.copyOf(buffered, filled + bytesRead);
            System.arraycopy(chunk, 0, buffered, filled, bytesRead);
            filled += bytesRead;
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}