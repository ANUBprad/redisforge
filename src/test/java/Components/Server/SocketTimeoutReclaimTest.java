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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The idle timeout: a connection that sends nothing must be reclaimed by the server
 * itself, a connection that keeps talking must not be, and a registered replica - which
 * is silent by design between the writes its master pushes at it - must be exempt.
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SocketTimeoutReclaimTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;
    private static final int IDLE_TIMEOUT_MS = 1_500;
    /** four rounds spaced out like this span well past the limit above */
    private static final int TALK_ROUNDS = 4;
    private static final long TALK_SPACING_MS = 600;

    @Autowired
    private MasterTcpServer masterTcpServer;
    @Autowired
    private ConnectionPool connectionPool;
    @Autowired
    private RedisConfig redisConfig;

    private int masterPort;

    @BeforeAll
    void startServerWithAShortIdleTimeout() throws Exception {
        masterPort = freePort();
        redisConfig.setRole("master");
        redisConfig.setPort(masterPort);
        // the read limit is applied when a connection is admitted, so it has to stand
        // before the listener starts
        redisConfig.setClientTimeoutMs(IDLE_TIMEOUT_MS);
        CompletableFuture.runAsync(masterTcpServer::startServer);
        awaitPortOpen(masterPort, AWAIT_TIMEOUT_MS);
    }

    @BeforeEach
    void waitForCleanRegistry() throws InterruptedException {
        await(() -> connectionPool.getClients().isEmpty() && masterTcpServer.activeClientHandlers() == 0,
                "the server still holds a connection from an earlier test");
    }

    @Test
    void aSilentConnectionIsReclaimedAfterTheTimeout() throws Exception {
        try (Socket silent = new Socket("127.0.0.1", masterPort)) {
            silent.setSoTimeout((int) AWAIT_TIMEOUT_MS);
            await(() -> connectionPool.getClients().size() == 1,
                    "the silent connection was never admitted");
            // if the server never lets go, this await is what fails the test
            await(() -> connectionPool.getClients().isEmpty() && masterTcpServer.activeClientHandlers() == 0,
                    "the server never reclaimed a connection that sent nothing for "
                            + IDLE_TIMEOUT_MS + "ms");
            assertEquals(-1, silent.getInputStream().read(),
                    "the server dropped the connection from its registry but left it open on the wire");
        }
    }

    @Test
    void aConnectionThatKeepsTalkingIsNeverReclaimed() throws Exception {
        try (RespSocket talker = new RespSocket(masterPort)) {
            talker.send(frame("PING"));
            assertEquals("+PONG\r\n", talker.readReply());
            await(() -> connectionPool.getClients().size() == 1,
                    "the talkative connection was never admitted");

            for (int round = 0; round < TALK_ROUNDS; round++) {
                Thread.sleep(TALK_SPACING_MS);
                talker.send(frame("PING"));
                // a reclaim at any point in here ends in EOF or a read timeout instead
                assertEquals("+PONG\r\n", talker.readReply(),
                        "the connection was reclaimed after round " + round
                                + ", although it had spoken only " + TALK_SPACING_MS + "ms earlier");
            }
        }
    }

    @Test
    void aRegisteredReplicaIsExemptFromTheIdleTimeout() throws Exception {
        RespSocket replica = new RespSocket(masterPort);
        try {
            replica.send(frame("REPLCONF", "listening-port", "6399"));
            assertEquals("+OK\r\n", replica.readReply(),
                    "the master refused the replica's registration");
            await(() -> connectionPool.getSlaves().size() == 1,
                    "the replica was never registered");

            // longer than the limit with not a byte on the wire: a replica between writes
            // is silent by design, and reclaiming it for that would make it reconnect and
            // resync forever
            Thread.sleep(IDLE_TIMEOUT_MS + 1_000);

            replica.send(frame("PING"));
            assertEquals("+PONG\r\n", replica.readReply(),
                    "the registered replica was reclaimed for being silent between writes");
        } finally {
            replica.close();
        }
        await(() -> connectionPool.getSlaves().isEmpty() && connectionPool.getClients().isEmpty(),
                "the replica connection was not cleaned up when its client left");
    }

    @Test
    void aConnectionStalledMidFrameIsReclaimedToo() throws Exception {
        try (RespSocket staller = new RespSocket(masterPort)) {
            // the value stops three bytes short of its declared length, so the handler
            // blocks waiting for bytes that are never coming
            staller.send("*3\r\n$3\r\nSET\r\n$5\r\nstall\r\n$3\r\nva"
                    .getBytes(StandardCharsets.UTF_8));
            await(() -> connectionPool.getClients().size() == 1,
                    "the stalling connection was never admitted");
            await(() -> connectionPool.getClients().isEmpty() && masterTcpServer.activeClientHandlers() == 0,
                    "a connection stalled mid frame was never reclaimed");
            assertEquals("", staller.readUntilClosed(),
                    "the reclaimed connection was left open on the client side");
        }
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

    private static void awaitPortOpen(int port, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
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

    /** A raw RESP client that can also watch the server hang up on it. */
    private static final class RespSocket implements AutoCloseable {
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

        String readReply() throws IOException {
            String header = readLine();
            if (header.startsWith("$")) {
                int length = Integer.parseInt(header.substring(1));
                if (length < 0) {
                    return header + "\r\n";
                }
                byte[] payload = in.readNBytes(length);
                if (payload.length != length) {
                    throw new EOFException("the reply held " + payload.length + " of " + length + " bytes");
                }
                in.readNBytes(2);
                return header + "\r\n" + new String(payload, StandardCharsets.UTF_8) + "\r\n";
            }
            return header + "\r\n";
        }

        /** Reads to end of stream and returns what came back, empty if the server hung up. */
        String readUntilClosed() throws IOException {
            StringBuilder received = new StringBuilder();
            int b;
            while ((b = in.read()) != -1) {
                received.append((char) b);
            }
            return received.toString();
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
