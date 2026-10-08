package Components.Server;

import Components.Infra.ConnectionPool;
import Config.AppConfig;
import jakarta.annotation.PreDestroy;
import org.junit.jupiter.api.AfterEach;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Shutdown as a server owes it to its operator: stop returns promptly, every open
 * client sees the connection end, no handler is left running, the port can be bound
 * again, stopping twice is harmless, and a server that is started after a stop works
 * exactly like a fresh one.
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ServerShutdownTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;
    private static final long STOP_BUDGET_MS = 5_000;
    private static final int RESTART_CYCLES = 5;

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
    void choosePorts() {
        try {
            masterPort = freePort();
            replicaPort = freePort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @BeforeEach
    void startBothServers() throws Exception {
        // whatever the last test left behind, each test starts from a clean pair
        masterTcpServer.stop();
        slaveTcpServer.stop();

        redisConfig.setRole("master");
        redisConfig.setPort(masterPort);
        CompletableFuture.runAsync(masterTcpServer::startServer);
        awaitPortOpen(masterPort);

        redisConfig.setRole("slave");
        redisConfig.setPort(replicaPort);
        redisConfig.setMasterHost("127.0.0.1");
        redisConfig.setMasterPort(freePort());
        CompletableFuture.runAsync(slaveTcpServer::startServer);
        awaitPortOpen(replicaPort);

        await(cleanRegistry(), "the fresh servers did not start with an empty registry");
    }

    @AfterEach
    void leaveNothingRunning() {
        masterTcpServer.stop();
        slaveTcpServer.stop();
    }

    @Test
    void aStopReturnsPromptlyAndHangsUpEveryOpenClient() throws Exception {
        List<RespSocket> open = new ArrayList<>();
        open.add(new RespSocket(masterPort));
        open.add(new RespSocket(masterPort));
        open.add(new RespSocket(replicaPort));
        for (RespSocket client : open) {
            client.send(frame("PING"));
            assertEquals("+PONG\r\n", client.readReply(), "the server did not answer before the stop");
        }

        long started = System.currentTimeMillis();
        masterTcpServer.stop();
        long masterStop = System.currentTimeMillis() - started;
        started = System.currentTimeMillis();
        slaveTcpServer.stop();
        long replicaStop = System.currentTimeMillis() - started;

        assertTrue(masterStop < STOP_BUDGET_MS,
                "the master took " + masterStop + "ms to stop, over the " + STOP_BUDGET_MS + "ms budget");
        assertTrue(replicaStop < STOP_BUDGET_MS,
                "the replica took " + replicaStop + "ms to stop, over the " + STOP_BUDGET_MS + "ms budget");

        for (RespSocket client : open) {
            assertEquals("", client.readUntilClosed(),
                    "a client was left with a connection the server had already stopped serving");
            client.close();
        }

        assertEquals(0, masterTcpServer.activeClientHandlers(), "a master handler outlived the stop");
        assertEquals(0, slaveTcpServer.activeClientHandlers(), "a replica handler outlived the stop");
        await(cleanRegistry(), "a stopped server still holds a connection");
    }

    @Test
    void thePortCanBeBoundAgainAndTheServerRestarted() throws Exception {
        masterTcpServer.stop();
        await(() -> !portAcceptsConnections(masterPort),
                "the port still accepted connections after the stop");

        // an outright bind, which fails outright if the listener did not really let go;
        // reuse is set the way a server sets it, so connections still in TIME_WAIT from
        // the run that just ended are not mistaken for a listener that is still there
        try (ServerSocket rebound = new ServerSocket()) {
            rebound.setReuseAddress(true);
            rebound.bind(new java.net.InetSocketAddress(masterPort));
            assertEquals(masterPort, rebound.getLocalPort(), "the port could not be bound again");
        }

        // both servers read the port from the shared config when they start, and the
        // setup left it on the replica's port, so the master's port is put back first
        redisConfig.setPort(masterPort);
        CompletableFuture.runAsync(masterTcpServer::startServer);
        awaitPortOpen(masterPort);

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("PING"));
            assertEquals("+PONG\r\n", client.readReply(),
                    "the server did not answer after being stopped and started again");
        }
    }

    @Test
    void stoppingTwiceIsHarmlessAndAStartStillWorksAfterIt() throws Exception {
        masterTcpServer.stop();
        masterTcpServer.stop();
        slaveTcpServer.stop();
        slaveTcpServer.stop();

        redisConfig.setPort(masterPort);
        CompletableFuture.runAsync(masterTcpServer::startServer);
        awaitPortOpen(masterPort);

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("PING"));
            assertEquals("+PONG\r\n", client.readReply(),
                    "a double stop left the server unable to start again");
        }
    }

    @Test
    void repeatedStopStartCyclesLeaveNoHandlersAndNoConnections() throws Exception {
        for (int cycle = 0; cycle < RESTART_CYCLES; cycle++) {
            List<RespSocket> open = new ArrayList<>();
            open.add(new RespSocket(masterPort));
            open.add(new RespSocket(replicaPort));
            for (RespSocket client : open) {
                client.send(frame("PING"));
                assertEquals("+PONG\r\n", client.readReply(),
                        "cycle " + cycle + ": the server did not answer before its stop");
            }

            masterTcpServer.stop();
            slaveTcpServer.stop();

            assertEquals(0, masterTcpServer.activeClientHandlers(),
                    "cycle " + cycle + ": a master handler outlived the stop");
            assertEquals(0, slaveTcpServer.activeClientHandlers(),
                    "cycle " + cycle + ": a replica handler outlived the stop");
            await(cleanRegistry(), "cycle " + cycle + ": a stopped server still holds a connection");
            for (RespSocket client : open) {
                assertEquals("", client.readUntilClosed(),
                        "cycle " + cycle + ": a client was left hanging on a stopped server");
                client.close();
            }

            // the shared config holds one port, so each server is pointed at its own
            // before it starts, and the master is awaited before the port is switched
            redisConfig.setPort(masterPort);
            CompletableFuture.runAsync(masterTcpServer::startServer);
            awaitPortOpen(masterPort);
            redisConfig.setPort(replicaPort);
            CompletableFuture.runAsync(slaveTcpServer::startServer);
            awaitPortOpen(replicaPort);
        }

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("PING"));
            assertEquals("+PONG\r\n", client.readReply(),
                    "the server did not answer after the last restart cycle");
        }
    }

    @Test
    void bothServersCarryTheShutdownAnnotationSpringCalls() throws NoSuchMethodException {
        // @PreDestroy is how a context close reaches stop(); without it the server would
        // outlive its application
        assertNotNull(MasterTcpServer.class.getMethod("shutdown").getAnnotation(PreDestroy.class),
                "the master's shutdown is not wired to the context closing");
        assertNotNull(SlaveTcpServer.class.getMethod("shutdown").getAnnotation(PreDestroy.class),
                "the replica's shutdown is not wired to the context closing");
    }

    private BooleanSupplier cleanRegistry() {
        return () -> connectionPool.getClients().isEmpty()
                && connectionPool.getSlaves().isEmpty()
                && masterTcpServer.activeClientHandlers() == 0
                && slaveTcpServer.activeClientHandlers() == 0;
    }

    private static boolean portAcceptsConnections(int port) {
        try (Socket ignored = new Socket("127.0.0.1", port)) {
            return true;
        } catch (IOException refused) {
            return false;
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
