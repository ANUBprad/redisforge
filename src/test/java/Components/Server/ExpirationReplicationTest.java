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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The expiration commands a replica receives from its master. The master sends them in
 * their absolute form, so the replica hangs the same deadline on the key and passes the
 * same frame down the chain; a client cannot expire a key on a replica itself.
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ExpirationReplicationTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;

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
        redisConfig.setRole("slave");
        redisConfig.setPort(replicaPort);
        redisConfig.setMasterHost("127.0.0.1");
        redisConfig.setMasterPort(freePort());
        Thread server = new Thread(slaveTcpServer::startServer, "expiration-replica");
        server.setDaemon(true);
        server.start();
        awaitPortOpen(replicaPort);
    }

    @BeforeEach
    void resetReplicaState() throws Exception {
        await(() -> connectionPool.getSlaves().isEmpty(), "a replica from an earlier test is still registered");
        connectionPool.resetCaughtUpAccounting();
        redisConfig.setMasterReplOffset(0L);
    }

    @Test
    void anAbsoluteDeadlineFromTheMasterIsAppliedAndPassedOn() throws Exception {
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            register(downstream);
            store.map.remove("rexpire:key");

            long deadline = System.currentTimeMillis() + 100_000;
            feed(frame("SET", "rexpire:key", "v"),
                    frame("PEXPIREAT", "rexpire:key", String.valueOf(deadline)));

            assertTrue(store.peekValue("rexpire:key") != null, "the set was not applied");
            long applied = store.peekValue("rexpire:key").expiry
                    .atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
            assertEquals(deadline, applied, 5, "the replica did not keep the master's absolute deadline");

            List<String[]> received = downstream.readFramesUntilAck();
            assertEquals(2, received.size(), "the expiration was not passed on once: " + received);
            assertArrayEquals(new String[]{"SET", "rexpire:key", "v"}, received.get(0));
            assertArrayEquals(new String[]{"PEXPIREAT", "rexpire:key", String.valueOf(deadline)},
                    received.get(1));
        }
    }

    @Test
    void persistingFromTheMasterIsAppliedAndPassedOn() throws Exception {
        try (RespSocket downstream = new RespSocket(replicaPort)) {
            register(downstream);
            store.map.remove("rexpire:persist");

            feed(frame("SET", "rexpire:persist", "v"),
                    frame("PEXPIREAT", "rexpire:persist", String.valueOf(System.currentTimeMillis() + 100_000)),
                    frame("PERSIST", "rexpire:persist"));

            assertTrue(store.peekValue("rexpire:persist").expiry
                            .equals(java.time.LocalDateTime.MAX),
                    "the replica kept a deadline its master had taken off");
            List<String[]> received = downstream.readFramesUntilAck();
            assertEquals(3, received.size());
            assertArrayEquals(new String[]{"PERSIST", "rexpire:persist"}, received.get(2));
        }
    }

    @Test
    void aClientCanReadTtlButCannotWriteAnExpiration() throws Exception {
        store.map.remove("rexpire:readonly");
        store.set("rexpire:readonly", "v");
        store.expire("rexpire:readonly", 100);

        try (RespSocket client = new RespSocket(replicaPort)) {
            client.send(frame("TTL", "rexpire:readonly"));
            assertEquals(":100", client.readReply());

            client.send(frame("PTTL", "rexpire:readonly"));
            assertTrue(client.readReply().startsWith(":"));

            for (String command : List.of("EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST")) {
                client.send(frame(command, "rexpire:readonly", "100"));
                String reply = client.readReply();
                assertTrue(reply.startsWith("-READONLY"),
                        command + " against a replica answered: " + reply);
            }

            client.send(frame("TTL", "rexpire:nowhere"));
            assertEquals(":-2", client.readReply());
        }
    }

    private void register(RespSocket downstream) throws Exception {
        downstream.send(frame("REPLCONF", "listening-port", "" + replicaPort));
        assertEquals("+OK", downstream.readReply());
        await(() -> !connectionPool.getSlaves().isEmpty(), "the downstream replica never registered");
        downstream.send(frame("REPLCONF", "capa", "psync2"));
        assertEquals("+OK", downstream.readReply());
        downstream.send(frame("PSYNC", "?", "-1"));
        String status = downstream.readLine();
        assertTrue(status.startsWith("+FULLRESYNC "), "no FULLRESYNC: " + status);
        int declaredLength = Integer.parseInt(downstream.readLine().substring(1));
        downstream.readExactly(declaredLength);
    }

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

    private static void await(java.util.function.BooleanSupplier condition, String failureMessage)
            throws InterruptedException {
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
            int count = Math.min(length, chunk.length - position);
            System.arraycopy(chunk, position, destination, offset, count);
            position += count;
            return count;
        }
    }

    /** A raw RESP client, so a test reads exactly the bytes the replica was sent. */
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
