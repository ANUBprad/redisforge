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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
 * The admission gate: a server with a full house must refuse what comes next with an
 * error instead of queueing it for a thread that may never come, and the connections it
 * already holds must never stop it answering the ones it admitted.
 *
 * <p>The gate here is 100, deliberately below the default of 256, so a run that quietly
 * fell back to the unbounded default would hold 156 more connections than these tests
 * allow and fail rather than pass unnoticed.</p>
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ClientStarvationTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;
    /** how long an active client may wait for its answer before the test calls it starved */
    private static final long ACTIVE_READ_TIMEOUT_MS = 3_000;
    private static final int MAX_CLIENTS = 100;
    private static final int[] ROUND_SIZES = {5, 15, 25, 50, 100};

    @Autowired
    private MasterTcpServer masterTcpServer;
    @Autowired
    private ConnectionPool connectionPool;
    @Autowired
    private RedisConfig redisConfig;

    private int masterPort;

    @BeforeAll
    void startServerWithABoundedGate() throws Exception {
        masterPort = freePort();
        redisConfig.setRole("master");
        redisConfig.setPort(masterPort);
        // the gate is built when the listener starts, so the limit has to stand first
        redisConfig.setMaxClients(MAX_CLIENTS);
        CompletableFuture.runAsync(masterTcpServer::startServer);
        awaitPortOpen(masterPort, AWAIT_TIMEOUT_MS);
    }

    @BeforeEach
    void waitForCleanRegistry() throws InterruptedException {
        await(() -> connectionPool.getClients().isEmpty() && masterTcpServer.activeClientHandlers() == 0,
                "the server still holds a connection from an earlier test");
    }

    @Test
    void theConnectionBeyondTheGateIsRefusedWithAnErrorAndHungUp() throws Exception {
        List<Socket> idle = openIdleConnections(MAX_CLIENTS);
        await(() -> connectionPool.getClients().size() == MAX_CLIENTS,
                "only " + connectionPool.getClients().size() + " of " + MAX_CLIENTS
                        + " connections were admitted, so the gate never filled");

        try (Socket refused = new Socket("127.0.0.1", masterPort)) {
            refused.setSoTimeout((int) AWAIT_TIMEOUT_MS);
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(refused.getInputStream(), StandardCharsets.UTF_8));
            assertEquals("-ERR max number of clients reached", reader.readLine(),
                    "the connection beyond the gate was not turned away with an error");
            assertEquals(-1, refused.getInputStream().read(),
                    "the refused connection was left open, so its client would wait forever");
        }

        closeAll(idle);
    }

    @Test
    void anActiveClientIsAnsweredWhileEveryOtherSlotHoldsAnIdleConnection() throws Exception {
        try (RespSocket active = new RespSocket(masterPort)) {
            active.send(frame("PING"));
            assertEquals("+PONG\r\n", active.readReply());

            List<Socket> idle = openIdleConnections(MAX_CLIENTS - 1);
            await(() -> connectionPool.getClients().size() == MAX_CLIENTS,
                    "the active client plus " + (MAX_CLIENTS - 1)
                            + " idle connections did not fill the gate");

            // a read limit of this test's own, so a starved answer fails here and now
            // instead of waiting out the general timeout
            active.socket.setSoTimeout((int) ACTIVE_READ_TIMEOUT_MS);
            active.send(frame("PING"));
            assertEquals("+PONG\r\n", active.readReply(),
                    "the active client got no answer while idle connections held every slot");

            closeAll(idle);
        }
    }

    @Test
    void twentyRoundsOfGrowingCrowdsLeaveTheServerAnswering() throws Exception {
        for (int round = 0; round < 20; round++) {
            int size = ROUND_SIZES[round % ROUND_SIZES.length];
            try (RespSocket active = new RespSocket(masterPort)) {
                active.send(frame("PING"));
                assertEquals("+PONG\r\n", active.readReply(),
                        "round " + round + ": the first client got no answer");

                List<Socket> idle = openIdleConnections(size - 1);
                await(() -> connectionPool.getClients().size() == size,
                        "round " + round + ": " + size + " connections did not fill up, only "
                                + connectionPool.getClients().size() + " were admitted");

                active.socket.setSoTimeout((int) ACTIVE_READ_TIMEOUT_MS);
                active.send(frame("PING"));
                assertEquals("+PONG\r\n", active.readReply(),
                        "round " + round + " with " + size
                                + " connections: the active client was starved by the idle ones");

                if (size == MAX_CLIENTS) {
                    try (Socket refused = new Socket("127.0.0.1", masterPort)) {
                        refused.setSoTimeout((int) AWAIT_TIMEOUT_MS);
                        BufferedReader reader = new BufferedReader(
                                new InputStreamReader(refused.getInputStream(), StandardCharsets.UTF_8));
                        assertEquals("-ERR max number of clients reached", reader.readLine(),
                                "round " + round + ": the full gate let one more connection in");
                    }
                }

                closeAll(idle);
            }
            await(() -> connectionPool.getClients().isEmpty() && masterTcpServer.activeClientHandlers() == 0,
                    "round " + round + ": the server did not let the crowd go");
        }
    }

    /** Opens connections that never send a byte, so each one holds its slot indefinitely. */
    private List<Socket> openIdleConnections(int count) throws IOException {
        List<Socket> idle = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            idle.add(new Socket("127.0.0.1", masterPort));
        }
        return idle;
    }

    private static void closeAll(List<Socket> sockets) {
        for (Socket socket : sockets) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // the point is that they are gone, not that closing them was elegant
            }
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

    /** A raw RESP client with its own read limit, so starvation fails fast. */
    private static final class RespSocket implements AutoCloseable {
        private final Socket socket;
        private final java.io.InputStream in;
        private final java.io.OutputStream out;

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
                    throw new IOException("the reply held " + payload.length + " of " + length + " bytes");
                }
                in.readNBytes(2);
                return header + "\r\n" + new String(payload, StandardCharsets.UTF_8) + "\r\n";
            }
            return header + "\r\n";
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
            throw new java.io.EOFException("connection closed mid line");
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}
