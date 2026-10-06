package Components.Server;

import Components.Persistence.AppendOnlyPersistence;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * An increment and a transaction that touch the same key both need the key's lock and the
 * file's lock, and the server gives them one order: the file's lock first, the key's lock
 * inside it. Taking them the other way round lets each side hold one lock and wait for the
 * other forever, which does not just stop those two clients but every write that arrives
 * behind them.
 *
 * <p>The workload runs plain increments, transactions of increments and plain sets against
 * one master with its file open, forcing every append to disk so the file lock is held long
 * enough for the two paths to really meet. Every increment the master answered has to be
 * accounted for exactly once at the end: a deadlock shows up as a workload that never
 * finishes, a lost or double increment as a value that is missing or repeated.</p>
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class LockOrderTest {

    // the appends are forced one by one, so the workload is disk bound: the generous
    // deadline keeps a slow filesystem from reading as a deadlock, while a real one
    // still fails here because neither side ever lets go
    private static final long AWAIT_TIMEOUT_MS = 45_000;
    private static final int PLAIN_WRITERS = 3;
    private static final int INCREMENTS_PER_WRITER = 60;
    private static final int TRANSACTION_WRITERS = 4;
    private static final int TRANSACTIONS_PER_WRITER = 20;
    private static final int INCREMENTS_PER_TRANSACTION = 2;
    private static final int SETTERS = 4;
    private static final int SETS_PER_WRITER = 20;
    private static final String COUNTER = "order:counter";

    @Autowired
    private MasterTcpServer masterTcpServer;
    @Autowired
    private RedisConfig redisConfig;
    @Autowired
    private AppendOnlyPersistence appendOnly;
    @Autowired
    private RespSerializer respSerializer;

    private int masterPort;

    @BeforeAll
    void startMaster(@TempDir Path dir) throws Exception {
        masterPort = freePort();
        redisConfig.setRole("master");
        redisConfig.setPort(masterPort);
        redisConfig.setAppendonly(true);
        redisConfig.setAppendfilename(dir.resolve("appendonly.aof").toString());
        // every append is forced to disk while it holds the file lock, which is what keeps
        // that lock held long enough for an increment and a transaction to meet on it
        redisConfig.setAppendfsync("always");
        appendOnly.start();
        Thread server = new Thread(masterTcpServer::startServer, "lock-order-master");
        server.setDaemon(true);
        server.start();
        awaitPortOpen(masterPort);
    }

    @BeforeEach
    void fileIsOpen() {
        assertTrue(appendOnly.isEnabled(), "the master has no append only file open");
    }

    @Test
    void anIncrementAndATransactionOnOneKeyBothFinish() throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(
                PLAIN_WRITERS + TRANSACTION_WRITERS + SETTERS);
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        List<Integer> answered = new CopyOnWriteArrayList<>();

        for (int writer = 0; writer < PLAIN_WRITERS; writer++) {
            Thread thread = new Thread(() -> {
                try (RespSocket client = new RespSocket(masterPort)) {
                    awaitLatch(start);
                    for (int i = 0; i < INCREMENTS_PER_WRITER; i++) {
                        client.send(frame("INCR", COUNTER));
                        answered.add(parseInteger(client.readReply()));
                    }
                } catch (Throwable t) {
                    failures.add(t);
                } finally {
                    done.countDown();
                }
            }, "plain-increment-" + writer);
            thread.setDaemon(true);
            thread.start();
        }

        for (int writer = 0; writer < TRANSACTION_WRITERS; writer++) {
            Thread thread = new Thread(() -> {
                try (RespSocket client = new RespSocket(masterPort)) {
                    awaitLatch(start);
                    for (int i = 0; i < TRANSACTIONS_PER_WRITER; i++) {
                        client.send(frame("MULTI"));
                        assertEquals("+OK\r\n", client.readReply());
                        for (int n = 0; n < INCREMENTS_PER_TRANSACTION; n++) {
                            client.send(frame("INCR", COUNTER));
                            assertEquals("+QUEUED\r\n", client.readReply());
                        }
                        client.send(frame("EXEC"));
                        String reply = client.readReply();
                        assertEquals(INCREMENTS_PER_TRANSACTION, parseIntegers(reply).size(),
                                "the transaction did not answer every queued increment: " + reply);
                        answered.addAll(parseIntegers(reply));
                    }
                } catch (Throwable t) {
                    failures.add(t);
                } finally {
                    done.countDown();
                }
            }, "transaction-" + writer);
            thread.setDaemon(true);
            thread.start();
        }

        // sets of their own: they hold only the file lock, which keeps the queue on it
        // moving while an increment on the counter waits its turn behind the key's lock
        for (int writer = 0; writer < SETTERS; writer++) {
            final int id = writer;
            Thread thread = new Thread(() -> {
                try (RespSocket client = new RespSocket(masterPort)) {
                    awaitLatch(start);
                    for (int i = 0; i < SETS_PER_WRITER; i++) {
                        client.send(frame("SET", "order:flood:" + id + ":" + i, "v" + i));
                        assertEquals("+OK\r\n", client.readReply());
                    }
                } catch (Throwable t) {
                    failures.add(t);
                } finally {
                    done.countDown();
                }
            }, "flood-" + writer);
            thread.setDaemon(true);
            thread.start();
        }

        start.countDown();
        assertTrue(done.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                "the workload did not finish in time: an increment and a transaction on the "
                        + "same key are waiting for each other's lock");
        assertTrue(failures.isEmpty(), () -> "a worker failed: " + failures.get(0));

        int expected = PLAIN_WRITERS * INCREMENTS_PER_WRITER
                + TRANSACTION_WRITERS * TRANSACTIONS_PER_WRITER * INCREMENTS_PER_TRANSACTION;
        // the values the master handed out are 1..expected: a lost increment would repeat
        // a value and a double application would skip one
        List<Integer> sorted = new ArrayList<>(answered);
        Collections.sort(sorted);
        assertEquals(sequence(expected), sorted,
                "the increments the master answered do not account for every one it applied");

        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("GET", COUNTER));
            assertEquals(respSerializer.serializeBulkString(String.valueOf(expected)),
                    client.readReply(),
                    "the counter is not the sum of the increments that were answered");
        }
    }

    private static List<Integer> sequence(int size) {
        List<Integer> values = new ArrayList<>(size);
        for (int i = 1; i <= size; i++) {
            values.add(i);
        }
        return values;
    }

    private static int parseInteger(String reply) {
        assertTrue(reply.startsWith(":") && reply.endsWith("\r\n"),
                "not an integer reply: " + reply);
        return Integer.parseInt(reply.substring(1, reply.length() - 2));
    }

    /** An EXEC reply holds one integer per queued command. */
    private static List<Integer> parseIntegers(String respArray) {
        List<Integer> values = new ArrayList<>();
        int i = 0;
        while (true) {
            int at = respArray.indexOf(':', i);
            if (at < 0) {
                return values;
            }
            int end = respArray.indexOf("\r\n", at);
            values.add(Integer.parseInt(respArray.substring(at + 1, end)));
            i = end + 2;
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void await(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        fail(message);
    }

    private static void awaitPortOpen(int port) throws Exception {
        await(() -> {
            try (Socket probe = new Socket("127.0.0.1", port)) {
                return true;
            } catch (IOException e) {
                return false;
            }
        }, "nothing accepted a connection on port " + port);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static byte[] frame(String... parts) {
        StringBuilder sb = new StringBuilder("*").append(parts.length).append("\r\n");
        for (String part : parts) {
            sb.append("$").append(part.length()).append("\r\n").append(part).append("\r\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** A raw RESP client, so a test can read exactly what the master sent. */
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
                String payload = new String(readExactly(length), StandardCharsets.UTF_8);
                readExactly(2);
                return header + "\r\n" + payload + "\r\n";
            }
            if (header.startsWith("*")) {
                // EXEC answers with an array of the queued replies, so the array has to be
                // read element by element or only its header is left in the socket
                int elements = Integer.parseInt(header.substring(1));
                StringBuilder whole = new StringBuilder(header).append("\r\n");
                for (int i = 0; i < elements; i++) {
                    whole.append(readReply());
                }
                return whole.toString();
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

        private byte[] readExactly(int length) throws IOException {
            byte[] payload = in.readNBytes(length);
            if (payload.length != length) {
                throw new EOFException("held " + payload.length + " of " + length + " bytes");
            }
            return payload;
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}
