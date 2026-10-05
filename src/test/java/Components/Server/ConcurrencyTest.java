package Components.Server;

import Components.Infra.ConnectionPool;
import Components.Service.RespSerializer;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
     * A master serves many client threads at once, and every write it accepts is pushed to
     * every replica attached at that moment. These tests put real sockets on both ends:
     * clients that issue INCR from several threads in the same instant, and replicas that
     * register themselves and then collect the frames the master streams to them.
     *
     * <p>Each test works on keys of its own, and the store is the one bean this Spring
     * context shares, so the counters a test leaves behind never reach another test.</p>
     */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ConcurrencyTest {

    private static final long AWAIT_TIMEOUT_MS = 15_000;
    private static final int WRITERS = 6;
    private static final int INCREMENTS_PER_WRITER = 40;

    @Autowired
    private MasterTcpServer masterTcpServer;
    @Autowired
    private ConnectionPool connectionPool;
    @Autowired
    private RedisConfig redisConfig;
    @Autowired
    private RespSerializer respSerializer;

    private int masterPort;

    @BeforeAll
    void startMaster() throws Exception {
        masterPort = freePort();
        redisConfig.setRole("master");
        redisConfig.setPort(masterPort);
        // one master for the whole class: startServer never returns, so a server per test
        // would pile up accept loops on the common pool
        Thread server = new Thread(masterTcpServer::startServer, "concurrency-master");
        server.setDaemon(true);
        server.start();
        awaitPortOpen(masterPort);
    }

    @BeforeEach
    void resetSharedState() throws Exception {
        // a replica that hung up mid test is only dropped once its write fails, so give
        // the master a moment to notice rather than failing the next test on its debris
        await(() -> connectionPool.getClients().isEmpty() && connectionPool.getSlaves().isEmpty(),
                "the registry still holds a connection from an earlier test");
        connectionPool.bytesSentToSlaves.set(0);
        connectionPool.slavesThatAreCaughtUp.set(0);
    }

    @Test
    void concurrentIncrementsReachEveryReplicaExactlyOnce() throws Exception {
        List<Replica> replicas = List.of(
                Replica.attach(masterPort, 40_001, respSerializer),
                Replica.attach(masterPort, 40_002, respSerializer));
        try {
            List<Integer> returned = new CopyOnWriteArrayList<>();
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(WRITERS);
            List<Throwable> failures = new CopyOnWriteArrayList<>();

            for (int writer = 0; writer < WRITERS; writer++) {
                Thread thread = new Thread(() -> {
                    try (RespSocket client = new RespSocket(masterPort, respSerializer)) {
                        awaitLatch(start);
                        for (int i = 0; i < INCREMENTS_PER_WRITER; i++) {
                            client.send(frame("INCR", "shared"));
                            returned.add(parseInteger(client.readReply()));
                        }
                    } catch (Throwable t) {
                        failures.add(t);
                    } finally {
                        done.countDown();
                    }
                }, "incr-writer-" + writer);
                thread.setDaemon(true);
                thread.start();
            }

            start.countDown();
            assertTrue(done.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                    "the writers did not finish in time");
            assertTrue(failures.isEmpty(), () -> "a writer failed: " + failures.get(0));

            int expected = WRITERS * INCREMENTS_PER_WRITER;
            // the values the master handed out are 1..expected: a lost increment would
            // repeat a value and a double application would skip one
            List<Integer> sorted = new ArrayList<>(returned);
            Collections.sort(sorted);
            assertEquals(sequence(expected), sorted,
                    "the master answered an increment with a value another client had already been given");

            for (Replica replica : replicas) {
                replica.awaitFrames(expected);
                List<String[]> frames = replica.frames();
                assertEquals(expected, frames.size(),
                        "replica " + replica.name + " did not receive every increment");
                for (int i = 0; i < expected; i++) {
                    assertEquals(List.of("INCR", "shared"), List.of(frames.get(i)[0], frames.get(i)[1]),
                            "replica " + replica.name + " received a damaged frame for increment " + i);
                }
            }
        } finally {
            closeAll(replicas);
        }
    }

    @Test
    void aReplicaThatHangsUpMidPropagationIsDroppedAndTheRestKeepGoing() throws Exception {
        List<Replica> replicas = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            replicas.add(Replica.attach(masterPort, 40_010 + i, respSerializer));
        }
        Replica leaving = replicas.get(1);
        try {
            // a first write every replica sees, so the pool is really holding three
            try (RespSocket client = new RespSocket(masterPort, respSerializer)) {
                client.send(frame("SET", "before", "v1"));
                assertEquals("+OK\r\n", client.readReply());
            }
            for (Replica replica : replicas) {
                replica.awaitFrames(1);
            }
            assertEquals(3, connectionPool.getSlaves().size(), "not all three replicas registered");

            // the middle replica disappears between two rounds of propagation
            leaving.hangUp();

            try (RespSocket client = new RespSocket(masterPort, respSerializer)) {
                client.send(frame("SET", "after", "v2"));
                assertEquals("+OK\r\n", client.readReply());
            }
            for (Replica replica : replicas) {
                if (replica != leaving) {
                    replica.awaitFrames(2);
                }
            }
            await(() -> connectionPool.getSlaves().size() == 2,
                    "the replica that hung up was not dropped from the pool");

            // the master still serves its clients and still replicates to what is left
            try (RespSocket client = new RespSocket(masterPort, respSerializer)) {
                client.send(frame("INCR", "after-the-drop"));
                assertEquals(":1\r\n", client.readReply());
                client.send(frame("GET", "after"));
                assertEquals("$2\r\nv2\r\n", client.readReply());
            }
            for (Replica replica : replicas) {
                if (replica != leaving) {
                    replica.awaitFrames(3);
                    List<String[]> frames = replica.frames();
                    assertEquals(List.of("SET", "after", "v2"), Arrays.asList(frames.get(1)),
                            "replica " + replica.name + " missed the write that came after the drop");
                    assertEquals(List.of("INCR", "after-the-drop"), Arrays.asList(frames.get(2)),
                            "replica " + replica.name + " missed the increment that came after the drop");
                }
            }
        } finally {
            closeAll(replicas);
        }
    }

    @Test
    void aDeleteQueuedInATransactionIsAppliedAndPropagatedToEveryReplica() throws Exception {
        List<Replica> replicas = List.of(
                Replica.attach(masterPort, 40_021, respSerializer),
                Replica.attach(masterPort, 40_022, respSerializer));
        try {
            try (RespSocket client = new RespSocket(masterPort, respSerializer)) {
                client.send(frame("SET", "tx:del-before", "v"));
                assertEquals("+OK\r\n", client.readReply());

                client.send(frame("MULTI"));
                assertEquals("+OK\r\n", client.readReply());
                client.send(frame("SET", "tx:del", "value"));
                assertEquals("+QUEUED\r\n", client.readReply());
                client.send(frame("DEL", "tx:del"));
                assertEquals("+QUEUED\r\n", client.readReply());
                client.send(frame("EXEC"));
                assertEquals("*2\r\n+OK\r\n+OK\r\n", client.readReply(),
                        "the transaction did not answer one reply per queued command");

                client.send(frame("GET", "tx:del"));
                assertEquals("$-1\r\n", client.readReply(),
                        "the master still holds the key its own transaction deleted");
            }

            for (Replica replica : replicas) {
                replica.awaitFrames(3);
                List<String[]> frames = replica.frames();
                assertEquals(3, frames.size(),
                        replica.name + " did not receive the transaction's three mutations");
                assertArrayEqualsAsList(new String[]{"SET", "tx:del-before", "v"}, frames.get(0));
                assertArrayEqualsAsList(new String[]{"SET", "tx:del", "value"}, frames.get(1));
                assertArrayEqualsAsList(new String[]{"DEL", "tx:del"}, frames.get(2));
            }
        } finally {
            closeAll(replicas);
        }
    }

    @Test
    void aMixedTransactionOfSetsAndADeleteIsPropagatedAsItWasApplied() throws Exception {
        List<Replica> replicas = List.of(Replica.attach(masterPort, 40_023, respSerializer));
        try {
            try (RespSocket client = new RespSocket(masterPort, respSerializer)) {
                client.send(frame("MULTI"));
                assertEquals("+OK\r\n", client.readReply());
                client.send(frame("SET", "mixed:a", "1"));
                assertEquals("+QUEUED\r\n", client.readReply());
                client.send(frame("SET", "mixed:b", "2"));
                assertEquals("+QUEUED\r\n", client.readReply());
                client.send(frame("DEL", "mixed:a"));
                assertEquals("+QUEUED\r\n", client.readReply());
                client.send(frame("EXEC"));
                assertEquals("*3\r\n+OK\r\n+OK\r\n+OK\r\n", client.readReply(),
                        "the mixed transaction did not answer one reply per queued command");

                client.send(frame("GET", "mixed:a"));
                assertEquals("$-1\r\n", client.readReply(),
                        "the mixed transaction's delete did not take effect on the master");
                client.send(frame("GET", "mixed:b"));
                assertEquals("$1\r\n2\r\n", client.readReply(),
                        "the mixed transaction lost the key it did not delete");
            }

            Replica replica = replicas.get(0);
            replica.awaitFrames(3);
            List<String[]> frames = replica.frames();
            assertEquals(3, frames.size(),
                    replica.name + " did not receive every mutation of the transaction");
            assertArrayEqualsAsList(new String[]{"SET", "mixed:a", "1"}, frames.get(0));
            assertArrayEqualsAsList(new String[]{"SET", "mixed:b", "2"}, frames.get(1));
            assertArrayEqualsAsList(new String[]{"DEL", "mixed:a"}, frames.get(2));
        } finally {
            closeAll(replicas);
        }
    }

    private static void assertArrayEqualsAsList(String[] expected, String[] actual) {
        assertEquals(List.of(expected), Arrays.asList(actual),
                "expected " + String.join(" ", expected) + " but got " + String.join(" ", actual));
    }

    @Test
    void transactionsAndSingleCommandsShareOneCounterWithoutLosingAnIncrement() throws Exception {
        int transactions = 5;
        int perTransaction = 10;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(transactions + WRITERS);
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        List<String> discarded = new CopyOnWriteArrayList<>();

        for (int i = 0; i < transactions; i++) {
            int id = i;
            Thread thread = new Thread(() -> {
                try (RespSocket client = new RespSocket(masterPort, respSerializer)) {
                    awaitLatch(start);
                    client.send(frame("MULTI"));
                    assertEquals("+OK\r\n", client.readReply());
                    for (int n = 0; n < perTransaction; n++) {
                        client.send(frame("INCR", "mixed"));
                        assertEquals("+QUEUED\r\n", client.readReply());
                    }
                    if (id % 2 == 0) {
                        client.send(frame("DISCARD"));
                        discarded.add(client.readReply());
                    } else {
                        client.send(frame("EXEC"));
                        String reply = client.readReply();
                        // one integer per queued increment. The values need not be
                        // consecutive: EXEC applies the queue in order but takes the store
                        // lock per command, so another client's increment can land between
                        // two of them. What must hold is that no value was handed out twice.
                        assertEquals(perTransaction, countIntegers(reply),
                                "the transaction did not answer every queued increment: " + reply);
                        assertEquals(perTransaction, Set.copyOf(parseIntegers(reply)).size(),
                                "the transaction answered the same value twice: " + reply);
                    }
                } catch (Throwable t) {
                    failures.add(t);
                } finally {
                    done.countDown();
                }
            }, "transaction-" + i);
            thread.setDaemon(true);
            thread.start();
        }

        for (int i = 0; i < WRITERS; i++) {
            Thread thread = new Thread(() -> {
                try (RespSocket client = new RespSocket(masterPort, respSerializer)) {
                    awaitLatch(start);
                    for (int n = 0; n < INCREMENTS_PER_WRITER; n++) {
                        client.send(frame("INCR", "mixed"));
                        parseInteger(client.readReply());
                    }
                } catch (Throwable t) {
                    failures.add(t);
                } finally {
                    done.countDown();
                }
            }, "plain-writer-" + i);
            thread.setDaemon(true);
            thread.start();
        }

        start.countDown();
        assertTrue(done.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                "the mixed workload did not finish in time");
        assertTrue(failures.isEmpty(), () -> "a worker failed: " + failures.get(0));
        assertEquals((transactions + 1) / 2, discarded.size(), "not every other transaction was discarded");
        for (String reply : discarded) {
            assertEquals("+OK\r\n", reply, "DISCARD did not answer as expected");
        }

        // the transactions that ran, the ones that were thrown away and the single commands
        // together have to account for exactly the counter the master ended up holding
        int executed = transactions / 2;
        int applied = executed * perTransaction + WRITERS * INCREMENTS_PER_WRITER;
        try (RespSocket client = new RespSocket(masterPort, respSerializer)) {
            client.send(frame("GET", "mixed"));
            // the header carries the length of the value, not the value itself
            assertEquals(respSerializer.serializeBulkString(String.valueOf(applied)), client.readReply(),
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
    private static int countIntegers(String respArray) {
        return parseIntegers(respArray).size();
    }

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

    private static void closeAll(List<Replica> replicas) {
        for (Replica replica : replicas) {
            try {
                replica.close();
            } catch (IOException ignored) {
                // the test is over, the socket goes away regardless
            }
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

    /** A replica that attaches to the master and collects the writes the master streams. */
    private static final class Replica implements AutoCloseable {
        private final RespSocket socket;
        private final List<String[]> received = new CopyOnWriteArrayList<>();
        private final String name;

        private Replica(RespSocket socket, String name) {
            this.socket = socket;
            this.name = name;
        }

        static Replica attach(int masterPort, int listeningPort, RespSerializer respSerializer)
                throws IOException {
            RespSocket socket = new RespSocket(masterPort, respSerializer);
            socket.send(frame("REPLCONF", "listening-port", "" + listeningPort));
            String reply = socket.readReply();
            if (!reply.startsWith("+OK")) {
                throw new IOException("the master refused the replica: " + reply);
            }
            socket.send(frame("REPLCONF", "capa", "psync2"));
            socket.readReply();
            socket.send(frame("PSYNC", "?", "-1"));
            String status = socket.readLine();
            if (!status.startsWith("+FULLRESYNC")) {
                throw new IOException("no FULLRESYNC for the replica: " + status);
            }
            int declaredLength = Integer.parseInt(socket.readLine().substring(1));
            socket.readExactly(declaredLength);

            Replica replica = new Replica(socket, "replica-" + listeningPort);
            replica.startCollecting();
            return replica;
        }

        private void startCollecting() {
            Thread reader = new Thread(() -> {
                while (true) {
                    try {
                        String[] command = socket.readFrame();
                        if (!command[0].equalsIgnoreCase("REPLCONF")) {
                            received.add(command);
                        }
                    } catch (IOException e) {
                        return;
                    }
                }
            }, name + "-reader");
            reader.setDaemon(true);
            reader.start();
        }

        void awaitFrames(int expected) throws InterruptedException {
            await(() -> received.size() >= expected,
                    name + " received " + received.size() + " of " + expected + " writes: " + describe());
        }

        private String describe() {
            List<String> lines = new ArrayList<>();
            for (String[] command : received) {
                lines.add(String.join(" ", command));
            }
            return String.join(" | ", lines);
        }

        List<String[]> frames() {
            return new ArrayList<>(received);
        }

        void hangUp() throws IOException {
            socket.socket.close();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    /** A raw RESP client, so a test can read exactly the bytes the master sent. */
    private static final class RespSocket implements AutoCloseable {
        private final Socket socket;
        private final InputStream in;
        private final OutputStream out;
        private final RespSerializer respSerializer;
        private final List<String[]> decoded = new ArrayList<>();
        private byte[] buffered = new byte[0];
        private int filled;

        RespSocket(int port, RespSerializer respSerializer) throws IOException {
            socket = new Socket("127.0.0.1", port);
            socket.setSoTimeout((int) AWAIT_TIMEOUT_MS);
            socket.setTcpNoDelay(true);
            this.respSerializer = respSerializer;
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

        byte[] readExactly(int length) throws IOException {
            byte[] payload = in.readNBytes(length);
            if (payload.length != length) {
                throw new EOFException("held " + payload.length + " of " + length + " bytes");
            }
            return payload;
        }

        String[] readFrame() throws IOException {
            while (true) {
                // one read can carry a whole batch of writes, so the frames it decoded are
                // queued: taking only the first and dropping the rest would lose writes
                if (!decoded.isEmpty()) {
                    return decoded.remove(0);
                }
                byte[] chunk = new byte[4096];
                int bytesRead = in.read(chunk);
                if (bytesRead == -1) {
                    throw new EOFException("the master hung up");
                }
                buffered = Arrays.copyOf(buffered, filled + bytesRead);
                System.arraycopy(chunk, 0, buffered, filled, bytesRead);
                filled += bytesRead;
                decoded.addAll(takeWholeFrames());
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

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}