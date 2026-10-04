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
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regression coverage for connection teardown. A client that disconnects must end its
 * handling loop and be dropped from the connection pool; otherwise the loop keeps
 * spinning on a closed peer and the registry keeps a dead connection around.
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ConnectionLifecycleTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;

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
        awaitPortOpen(masterPort, AWAIT_TIMEOUT_MS);

        replicaPort = freePort();
        redisConfig.setRole("slave");
        redisConfig.setPort(replicaPort);
        // point the upstream handshake at a closed port so it gives up immediately;
        // this test only exercises the replica's own client handling loop
        redisConfig.setMasterHost("127.0.0.1");
        redisConfig.setMasterPort(freePort());
        CompletableFuture.runAsync(slaveTcpServer::startServer);
        awaitPortOpen(replicaPort, AWAIT_TIMEOUT_MS);
    }

    @BeforeEach
    void waitForCleanRegistry() throws InterruptedException {
        // a leftover connection from an earlier test would make the pool assertions below lie
        await(() -> connectionPool.getClients().isEmpty() && connectionPool.getSlaves().isEmpty(),
                "registry still holds a connection from an earlier test");
    }

    @Test
    void masterDropsClientFromRegistryWhenItDisconnects() throws Exception {
        try (RespClient client = new RespClient(masterPort)) {
            client.send("PING");
            assertEquals("+PONG", client.read());
            await(() -> connectionPool.getClients().size() == 1, "client was never registered");
        }
        await(() -> connectionPool.getClients().isEmpty(),
                "disconnected client was left in the pool, so its handling loop never exited");
    }

    @Test
    void masterStaysUsableAfterAClientDisconnects() throws Exception {
        try (RespClient first = new RespClient(masterPort)) {
            first.send("PING");
            assertEquals("+PONG", first.read());
        }
        await(() -> connectionPool.getClients().isEmpty(), "first client was not cleaned up");

        try (RespClient second = new RespClient(masterPort)) {
            second.send("PING");
            assertEquals("+PONG", second.read());
        }
    }

    @Test
    void masterDropsReplicaFromRegistryWhenItDisconnects() throws Exception {
        RespClient replica = new RespClient(masterPort);
        try {
            replica.send("REPLCONF", "listening-port", "6380");
            assertEquals("+OK", replica.read());
            await(() -> connectionPool.getSlaves().size() == 1, "replica was never registered");
        } finally {
            replica.close();
        }
        await(() -> connectionPool.getSlaves().isEmpty() && connectionPool.getClients().isEmpty(),
                "disconnected replica was left in the pool");
    }

    @Test
    void replicaDropsClientFromRegistryWhenItDisconnects() throws Exception {
        try (RespClient client = new RespClient(replicaPort)) {
            client.send("PING");
            assertEquals("+PONG", client.read());
            await(() -> connectionPool.getClients().size() == 1, "client was never registered");
        }
        await(() -> connectionPool.getClients().isEmpty(),
                "disconnected client was left in the pool, so its handling loop never exited");
    }

    @Test
    void replicaStaysUsableAfterAClientDisconnects() throws Exception {
        try (RespClient first = new RespClient(replicaPort)) {
            first.send("PING");
            assertEquals("+PONG", first.read());
        }
        await(() -> connectionPool.getClients().isEmpty(), "first client was not cleaned up");

        try (RespClient second = new RespClient(replicaPort)) {
            second.send("PING");
            assertEquals("+PONG", second.read());
        }
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

    private static final class RespClient implements AutoCloseable {
        private final Socket socket;
        private final BufferedReader reader;
        private final OutputStream out;

        RespClient(int port) throws IOException {
            socket = new Socket("127.0.0.1", port);
            socket.setSoTimeout((int) AWAIT_TIMEOUT_MS);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            out = socket.getOutputStream();
        }

        void send(String... parts) throws IOException {
            StringBuilder request = new StringBuilder("*").append(parts.length).append("\r\n");
            for (String part : parts) {
                request.append("$").append(part.length()).append("\r\n").append(part).append("\r\n");
            }
            out.write(request.toString().getBytes(StandardCharsets.UTF_8));
            out.flush();
        }

        String read() throws IOException {
            return reader.readLine();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}