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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Connections coming and going under load: a storm of connect, command, close cycles
 * must never lose an answer or leave a corpse in the registry, and writes must keep
 * succeeding while replicas register and drop off in the middle of them.
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ConnectionChurnTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;
    private static final int STORM_THREADS = 8;
    private static final int STORM_CYCLES = 50;
    private static final int WRITE_THREADS = 3;
    private static final int WRITES_PER_THREAD = 40;
    private static final int REPLICA_ROUNDS = 10;

    @Autowired
    private MasterTcpServer masterTcpServer;
    @Autowired
    private ConnectionPool connectionPool;
    @Autowired
    private RedisConfig redisConfig;

    private int masterPort;

    @BeforeAll
    void startMaster() throws Exception {
        masterPort = freePort();
        redisConfig.setRole("master");
        redisConfig.setPort(masterPort);
        CompletableFuture.runAsync(masterTcpServer::startServer);
        awaitPortOpen(masterPort, AWAIT_TIMEOUT_MS);
    }

    @BeforeEach
    void waitForCleanRegistry() throws InterruptedException {
        await(() -> connectionPool.getClients().isEmpty() && connectionPool.getSlaves().isEmpty(),
                "the server still holds a connection from an earlier test");
    }

    @Test
    void aStormOfConnectCommandCloseCyclesNeverLosesAnAnswer() throws InterruptedException {
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(STORM_THREADS);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        for (int thread = 0; thread < STORM_THREADS; thread++) {
            final int threadNumber = thread;
            new Thread(() -> {
                try {
                    start.await();
                    for (int cycle = 0; cycle < STORM_CYCLES; cycle++) {
                        try (RespSocket client = new RespSocket(masterPort)) {
                            client.send(frame("PING"));
                            assertEquals("+PONG\r\n", client.readReply(),
                                    "thread " + threadNumber + " cycle " + cycle
                                            + ": the storm lost an answer");
                        }
                    }
                } catch (Throwable thrown) {
                    failure.set(thrown);
                } finally {
                    done.countDown();
                }
            }, "churn-storm-" + thread).start();
        }

        start.countDown();
        assertTrue(done.await(40, TimeUnit.SECONDS),
                "the storm of " + (STORM_THREADS * STORM_CYCLES)
                        + " connections did not finish in 40 seconds");
        if (failure.get() != null) {
            fail("the storm had a failure", failure.get());
        }
        await(() -> connectionPool.getClients().isEmpty() && masterTcpServer.activeClientHandlers() == 0,
                "the storm left a connection or a handler behind");
    }

    @Test
    void writesKeepSucceedingWhileReplicasRegisterAndDropOff() throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(WRITE_THREADS);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        for (int thread = 0; thread < WRITE_THREADS; thread++) {
            final int threadNumber = thread;
            new Thread(() -> {
                try {
                    start.await();
                    try (RespSocket writer = new RespSocket(masterPort)) {
                        for (int write = 0; write < WRITES_PER_THREAD; write++) {
                            String key = "churn:writer:" + threadNumber + ":" + write;
                            writer.send(frame("SET", key, "v" + write));
                            assertEquals("+OK\r\n", writer.readReply(),
                                    "thread " + threadNumber + " lost its write " + write
                                            + " while a replica was coming or going");
                        }
                    }
                } catch (Throwable thrown) {
                    failure.set(thrown);
                } finally {
                    done.countDown();
                }
            }, "churn-writer-" + thread).start();
        }

        // the replica churn runs beside the writers, so propagation has to walk a set
        // that is changing while it walks it
        start.countDown();
        for (int round = 0; round < REPLICA_ROUNDS; round++) {
            try (RespSocket replica = new RespSocket(masterPort)) {
                replica.send(frame("REPLCONF", "listening-port", "" + (6500 + round)));
                assertEquals("+OK\r\n", replica.readReply(),
                        "the master refused a replica registration in round " + round);
                await(() -> connectionPool.getSlaves().size() == 1,
                        "round " + round + ": the replica never showed up in the registry");
            }
            await(() -> connectionPool.getSlaves().isEmpty(),
                    "round " + round + ": the departed replica was left in the registry");
        }

        assertTrue(done.await(40, TimeUnit.SECONDS),
                "the writers did not finish while replicas churned");
        if (failure.get() != null) {
            fail("a write failed during the replica churn", failure.get());
        }

        // the writes survived the churn: a fresh read sees them
        try (RespSocket reader = new RespSocket(masterPort)) {
            for (int thread = 0; thread < WRITE_THREADS; thread++) {
                String key = "churn:writer:" + thread + ":" + (WRITES_PER_THREAD - 1);
                reader.send(frame("GET", key));
                String lastValue = "v" + (WRITES_PER_THREAD - 1);
                assertEquals("$" + lastValue.length() + "\r\n" + lastValue + "\r\n", reader.readReply(),
                        "the last write of thread " + thread + " did not survive the churn");
            }
        }
        await(() -> connectionPool.getClients().isEmpty() && connectionPool.getSlaves().isEmpty()
                        && masterTcpServer.activeClientHandlers() == 0,
                "the churn left a connection or a handler behind");
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

    /** A raw RESP client with its own read limit, so a lost answer fails fast. */
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
