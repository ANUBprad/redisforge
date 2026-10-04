package Components.Server;

import Components.Infra.ConnectionPool;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * TCP is a byte stream, so a socket read is not a command. These tests drive the server
 * over a real socket and hand it commands split at every awkward place: several commands
 * in one write, one command across several writes, a frame boundary inside a length
 * header, and a command bigger than the socket receive buffer.
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RespFramingTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;
    private static final String KEY = "framing:key";

    @Autowired
    private MasterTcpServer masterTcpServer;
    @Autowired
    private SlaveTcpServer slaveTcpServer;
    @Autowired
    private ConnectionPool connectionPool;
    @Autowired
    private RedisConfig redisConfig;

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
        // the upstream handshake is given a closed port so it gives up at once; these
        // tests only care about how each server frames what a client sends it
        redisConfig.setMasterHost("127.0.0.1");
        redisConfig.setMasterPort(freePort());
        CompletableFuture.runAsync(slaveTcpServer::startServer);
        awaitPortOpen(replicaPort);
    }

    @BeforeEach
    void waitForCleanRegistry() throws InterruptedException {
        await(() -> connectionPool.getClients().isEmpty() && connectionPool.getSlaves().isEmpty(),
                "registry still holds a connection from an earlier test");
    }

    @Test
    void singleCompleteCommandIsExecuted() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("PING"));
            assertEquals("+PONG\r\n", client.readReply());
        }
    }

    @Test
    void twoCommandsInOneWriteBothRun() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(concat(frame("PING"), frame("PING")));
            assertEquals("+PONG\r\n", client.readReply());
            assertEquals("+PONG\r\n", client.readReply());
        }
    }

    @Test
    void pipelinedSetThenGetSeesTheStoredValue() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(concat(frame("SET", KEY, "bar"), frame("GET", KEY)));
            assertEquals("+OK\r\n", client.readReply());
            assertEquals("$3\r\nbar\r\n", client.readReply());
        }
    }

    @Test
    void splitPingFrameIsHeldUntilTheRestArrives() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send("*1\r\n$4\r\nPI");
            client.send("NG\r\n");
            assertEquals("+PONG\r\n", client.readReply());
        }
    }

    @Test
    void splitSetFrameIsHeldUntilTheRestArrives() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send("*3\r\n$3\r\nSET\r\n$" + KEY.length() + "\r\n" + KEY + "\r\n$3\r\nba");
            client.send("r\r\n");
            assertEquals("+OK\r\n", client.readReply());
        }

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("GET", KEY));
            assertEquals("$3\r\nbar\r\n", client.readReply());
        }
    }

    @Test
    void aFrameBoundaryInsideAWriteDoesNotStrandTheNextCommand() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            // first write holds one whole command plus the start of the next, and the
            // split falls inside the length header of the second command's payload
            client.send(concat(frame("PING"), bytes("*3\r\n$3\r\nSET\r\n$" + KEY.length() + "\r\n" + KEY + "\r\n$3\r\nba")));
            client.send(concat(bytes("r\r\n"), frame("PING")));
            assertEquals("+PONG\r\n", client.readReply());
            assertEquals("+OK\r\n", client.readReply());
            assertEquals("+PONG\r\n", client.readReply());
        }
    }

    @Test
    void commandDeliveredOneByteAtATimeStillRuns() throws Exception {
        byte[] command = concat(frame("SET", KEY, "drip"), frame("GET", KEY));
        try (RespSocket client = new RespSocket(masterPort)) {
            for (byte b : command) {
                client.send(new byte[]{b});
            }
            assertEquals("+OK\r\n", client.readReply());
            assertEquals("$4\r\ndrip\r\n", client.readReply());
        }
    }

    @Test
    void commandLargerThanTheSocketReceiveBufferIsNotTruncated() throws Exception {
        String value = "v".repeat(1024 * 1024);
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", KEY, value));
            assertEquals("+OK\r\n", client.readReply());
        }

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("GET", KEY));
            assertEquals("$" + value.length() + "\r\n" + value + "\r\n", client.readReply());
        }
    }

    @Test
    void pipelinedCommandsAnswerInTheOrderTheyArrived() throws Exception {
        int rounds = 20;
        List<byte[]> writes = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < rounds; i++) {
            writes.add(frame("SET", "framing:" + i, "v" + i));
            expected.add("+OK\r\n");
            writes.add(frame("GET", "framing:" + i));
            expected.add("$" + ("v" + i).length() + "\r\nv" + i + "\r\n");
        }

        try (RespSocket client = new RespSocket(masterPort)) {
            for (byte[] write : writes) {
                client.send(write);
            }
            for (String reply : expected) {
                assertEquals(reply, client.readReply());
            }
        }
    }

    @Test
    void replicaFramesClientCommandsTheSameWay() throws Exception {
        try (RespSocket client = new RespSocket(replicaPort)) {
            client.send(concat(frame("PING"), frame("GET", "framing:never-written")));
            assertEquals("+PONG\r\n", client.readReply());
            assertEquals("$-1\r\n", client.readReply());
        }
    }

    @Test
    void disconnectMidFrameCleansUpAndLeavesTheServerUsable() throws Exception {
        RespSocket client = new RespSocket(masterPort);
        client.send("*3\r\n$3\r\nSET\r\n$" + KEY.length() + "\r\n" + KEY + "\r\n$3\r\nba");
        client.close();

        await(() -> connectionPool.getClients().isEmpty(),
                "connection was not cleaned up after a disconnect mid frame");

        try (RespSocket next = new RespSocket(masterPort)) {
            next.send(frame("PING"));
            assertEquals("+PONG\r\n", next.readReply());
        }
    }

    @Test
    void malformedInputClosesTheConnectionInsteadOfSpinning() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send("this is not RESP at all\r\n");
            assertEquals("", client.readUntilClosed());
        }
        await(() -> connectionPool.getClients().isEmpty(),
                "connection was not cleaned up after malformed input");
    }

    private static byte[] frame(String... parts) {
        StringBuilder sb = new StringBuilder("*").append(parts.length).append("\r\n");
        for (String part : parts) {
            sb.append("$").append(part.length()).append("\r\n").append(part).append("\r\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
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

    /** A raw RESP client, so tests can split a command at an exact byte. */
    private static final class RespSocket implements AutoCloseable {
        private final Socket socket;
        private final InputStream in;
        private final OutputStream out;

        RespSocket(int port) throws IOException {
            socket = new Socket("127.0.0.1", port);
            socket.setSoTimeout((int) AWAIT_TIMEOUT_MS);
            // keeps the byte at a time test from waiting on Nagle
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

        /** Reads one reply, CRLF included, so a bulk string keeps its "$3\r\n" header. */
        String readReply() throws IOException {
            String header = readLine();
            if (!header.startsWith("$")) {
                return header + "\r\n";
            }
            int length = Integer.parseInt(header.substring(1));
            if (length < 0) {
                // a null bulk string, such as the reply to GET on a key that is not there
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

        /** Reads to end of stream and returns what came back, empty if the server hung up. */
        String readUntilClosed() throws IOException {
            ByteArrayOutputStream received = new ByteArrayOutputStream();
            int b;
            while ((b = in.read()) != -1) {
                received.write(b);
            }
            return received.toString(StandardCharsets.UTF_8);
        }

        private String readLine() throws IOException {
            StringBuilder sb = new StringBuilder();
            int b;
            while ((b = in.read()) != -1) {
                if (b == '\r') {
                    int lf = in.read();
                    if (lf != '\n') {
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