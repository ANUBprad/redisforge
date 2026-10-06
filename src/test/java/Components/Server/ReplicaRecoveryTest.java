package Components.Server;

import Components.Infra.ConnectionPool;
import Components.Infra.RespStream;
import Components.Infra.Slave;
import Components.Service.RespSerializer;
import Config.AppConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A replica that cannot reach its master, and a master that goes away.
 *
 * <p>Each server here gets a context of its own. That is what keeps a replica from being
 * able to send its own writes back to the master it is following: the two never share a
 * connection pool, so the only way a write travels the other way is a downstream hop.
 * Every test drives real sockets, and every wait is bounded, so a replica that quietly
 * stops following shows up as a failure rather than as a hang.</p>
 *
 * <p>A reconnect here is a fresh handshake followed by a fresh stream, and this PSYNC ships
 * an empty RDB, so a write the master accepted while the replica was away is not
 * backfilled. What has to hold is the other half: the replica finds the master again by
 * itself, and everything the master accepts after that arrives exactly once.</p>
 */
class ReplicaRecoveryTest {

    private static final long AWAIT_TIMEOUT_MS = 15_000;
    private static final long POLL_MS = 20;
    private static final String FOLLOWER_THREAD = "replication-follower";

    private final List<Node> nodes = new ArrayList<>();

    @AfterEach
    void stopEverything() {
        for (Node node : nodes) {
            node.close();
        }
        nodes.clear();
    }

    @Test
    void aReplicaThatStartsFirstWaitsForTheMasterToAppear() throws Exception {
        int masterPort = freePort();
        // nothing is listening here yet, so this replica has to wait for a master instead of
        // giving up on the first refused connection
        Node replica = startReplica(freePort(), masterPort);
        replica.awaitPortOpen();

        // a replica with no master is still a server, and keeps answering its own clients
        try (RespSocket client = new RespSocket(replica.port)) {
            client.send(frame("PING"));
            assertEquals("+PONG", client.readReply());
            client.send(frame("GET", "recover:early"));
            assertEquals("$-1", client.readReply());
        }

        Node master = startMaster(masterPort);
        master.awaitPortOpen();
        // the replica has to finish attaching before the write, or the write is in the gap
        awaitRegisteredAndReady(master, 1);
        set(master.port, "recover:early", "written once the replica gave up");

        // the replica attaches on its own, with nothing restarting it
        awaitValue(replica, "recover:early", "written once the replica gave up");
    }

    @Test
    void aMasterThatGoesAwayIsFoundAgainAndEveryWriteAfterwardsArrivesOnce() throws Exception {
        int masterPort = freePort();
        Node master = startMaster(masterPort);
        master.awaitPortOpen();
        Node replica = startReplica(freePort(), masterPort);
        replica.awaitPortOpen();

        awaitRegisteredAndReady(master, 1);
        set(master.port, "recover:counter", "0");
        awaitValue(replica, "recover:counter", "0");

        // the master hangs up on the replica, which is what a restarted master looks like
        closeUpstream(master);
        // the old connection is let go of, and a fresh one is made without any nudge. Both
        // halves are waited for in turn, so the new connection cannot be the old one
        awaitAttached(master, 0);
        awaitAttached(master, 1);

        // the reconnect is not a restart, so everything accepted from here on arrives
        set(master.port, "recover:after", "after the master came back");
        awaitValue(replica, "recover:after", "after the master came back");
        for (int i = 1; i <= 3; i++) {
            incr(master.port, "recover:counter");
        }
        // an increment that arrived twice would land the replica at six
        awaitValue(replica, "recover:counter", "3");

        // the stream is still live, so a reconnect did not leave it stalled, and the old
        // connection was let go of instead of being kept alongside the new one
        set(master.port, "recover:still", "and still going");
        awaitValue(replica, "recover:still", "and still going");
        assertEquals(1, master.slaveCount(),
                "the replica is holding more than one connection to a healthy master");
    }

    @Test
    void aMasterThatRefusesIsLookedForAtABoundedRateRatherThanInALoop() throws Exception {
        // a stand in for a master that is not there: it counts what the replica asks for and
        // hangs up at once, so every attempt fails and the retry has to back off
        int attempts;
        try (RefusingMaster refusing = new RefusingMaster()) {
            Node replica = startReplica(freePort(), refusing.port);
            replica.awaitPortOpen();

            Thread.sleep(1_500);
            attempts = refusing.attempts();
        }
        // a bounded number of attempts in a fixed window is the property that matters here:
        // waiting in a loop with no pause would produce hundreds, and a replica that gave up
        // would produce one
        assertTrue(attempts >= 2,
                "the replica asked only " + attempts + " times, so it is not looking for a master at all");
        assertTrue(attempts <= 6,
                "the replica asked " + attempts + " times in 1.5s, so it is asking in a loop");
    }

    @Test
    void aReplicaWithAHealthyMasterIsNotAskedToConnectAgain() throws Exception {
        int masterPort = freePort();
        Node master = startMaster(masterPort);
        master.awaitPortOpen();
        Node replica = startReplica(freePort(), masterPort);
        replica.awaitPortOpen();

        awaitRegisteredAndReady(master, 1);
        set(master.port, "recover:quiet", "first");
        awaitValue(replica, "recover:quiet", "first");

        // a healthy stream is left alone: one follower, one connection, nothing reconnecting
        Thread.sleep(1_500);
        assertEquals(1, master.slaveCount(),
                "the replica attached more than once while its master was healthy");
    }

    @Test
    void aChainKeepsReplicatingAfterTheMiddleHopReconnects() throws Exception {
        int masterPort = freePort();
        int middlePort = freePort();
        int lastPort = freePort();
        Node master = startMaster(masterPort);
        master.awaitPortOpen();
        Node middle = startReplica(middlePort, masterPort);
        middle.awaitPortOpen();
        Node last = startReplica(lastPort, middlePort);
        last.awaitPortOpen();

        awaitRegisteredAndReady(master, 1);
        awaitRegisteredAndReady(middle, 1);
        set(master.port, "chain:before", "before the middle hop lost its master");
        awaitValue(middle, "chain:before", "before the middle hop lost its master");
        awaitValue(last, "chain:before", "before the middle hop lost its master");

        // only the middle hop's upstream goes away; the hop below it is not touched
        closeUpstream(master);
        awaitAttached(master, 0);
        awaitAttached(master, 1);
        set(master.port, "chain:after", "after the middle hop came back");

        // the middle hop reconnects by itself and carries the new write the whole way down
        awaitValue(last, "chain:after", "after the middle hop came back");
        // the last hop never had to reattach, and still gets everything after that
        set(master.port, "chain:still", "and still going");
        awaitValue(last, "chain:still", "and still going");
        assertEquals(1, middle.slaveCount(),
                "the last hop attached to the middle hop more than once");
    }

    @Test
    void aReplicaKeepsItsOffsetInStepWithItsMasterAcrossAReconnect() throws Exception {
        int masterPort = freePort();
        Node master = startMaster(masterPort);
        master.awaitPortOpen();
        Node replica = startReplica(freePort(), masterPort);
        replica.awaitPortOpen();

        awaitRegisteredAndReady(master, 1);
        set(master.port, "recover:offset", "before the link went away");
        awaitValue(replica, "recover:offset", "before the link went away");

        long afterFirstWrite = reportedOffset(master.port);
        assertTrue(afterFirstWrite > 0, "the master never counted the write it propagated");
        await(() -> offsetOrMinusOne(replica.port) == afterFirstWrite,
                "the replica did not report the offset its master counted for the write it applied");

        // the link goes away. The master keeps serving, so it streams past where the replica
        // stands: nothing backfills the gap, but the position both name afterwards must
        // still be the same one
        closeUpstream(master);
        awaitAttached(master, 0);
        set(master.port, "recover:offset", "written while the replica was away");
        awaitAttached(master, 1);

        set(master.port, "recover:offset", "after the link came back");
        awaitValue(replica, "recover:offset", "after the link came back");

        // a full resync starts the replica from the position the master says the stream is
        // at, and every write after that moves both by the same bytes, so a reconnect
        // cannot leave them naming two different positions
        await(() -> {
            long masterOffset = offsetOrMinusOne(master.port);
            long replicaOffset = offsetOrMinusOne(replica.port);
            return masterOffset > 0 && masterOffset == replicaOffset;
        }, "the replica's offset diverged from its master after it reconnected: master "
                + offsetOrMinusOne(master.port) + ", replica " + offsetOrMinusOne(replica.port));
    }

    /**
     * A full resync moves this hop to a new position in a new stream. A replica below is
     * still carrying the old one, so it has to attach again from the new position rather
     * than go on naming a stream that no longer exists.
     */
    @Test
    void aReplicaLetsGoOfTheReplicasBelowItWhenItsStreamIsReplaced() throws Exception {
        int masterPort = freePort();
        int replicaPort = freePort();
        Node replica = startReplica(replicaPort, masterPort);
        replica.awaitPortOpen();

        try (ScriptedMaster scripted = new ScriptedMaster(masterPort, replicaPort)) {
            scripted.attach();
            scripted.handshake();
            scripted.stream(frame("INCR", "replaced:counter"));
            awaitValue(replica, "replaced:counter", "1");

            // a replica below attaches while this hop is still on the stream it started with
            try (RespSocket below = new RespSocket(replicaPort)) {
                below.send(frame("REPLCONF", "listening-port", "" + freePort()));
                assertEquals("+OK", below.readReply(), "the replica below was refused");
                await(() -> replica.slaveCount() == 1, "the replica below never registered");

                // the master hangs up and hands over a stream that starts from somewhere
                // else. This hop takes the new position, and the one below still carries the
                // old one, so it is let go of: kept as it is, an ACK from below would name a
                // stream this hop no longer follows and a WAIT here could never count it
                scripted.hangUp();
                scripted.attach();
                scripted.handshake();

                await(() -> {
                    try {
                        below.send(frame("PING"));
                        below.readReply();
                        return false;
                    } catch (IOException gone) {
                        return true;
                    }
                }, "the replica below was still held on a stream that no longer exists");
                assertEquals(0, replica.slaveCount(),
                        "the connection that was let go of is still held as a replica");
            }
        }
    }

    @Test
    void theHandshakeIsRunAgainOnEveryReconnectAndNoStreamIsAppliedTwice() throws Exception {
        int masterPort = freePort();
        Node replica = startReplica(freePort(), masterPort);
        replica.awaitPortOpen();

        try (ScriptedMaster scripted = new ScriptedMaster(masterPort, replica.port)) {
            // the first attachment: one handshake, one stream, then the master hangs up
            scripted.attach();
            scripted.handshake();
            scripted.stream(frame("INCR", "scripted:counter"));
            awaitValue(replica, "scripted:counter", "1");
            scripted.hangUp();

            // the second attachment, made by the replica on its own
            scripted.attach();
            scripted.handshake();
            scripted.stream(frame("INCR", "scripted:counter"));
            awaitValue(replica, "scripted:counter", "2");

            // nothing is listening any more, so no further attempt can be counted as a
            // connection: two is one per stream, which is what a single follower does
            scripted.close();
            Thread.sleep(500);
            assertEquals(2, scripted.connections(),
                    "the replica opened a connection that no stream used, so it has more than one follower");
            // a stream applied twice would have left the counter at four
            assertEquals("2", get(replica.port, "scripted:counter"),
                    "a replicated command was applied more than once");
        }
    }

    @Test
    void stoppingAReplicaEndsItsWaitingAndClosesItsSockets() throws Exception {
        Set<Thread> followersBefore = liveFollowers();
        Node replica = startReplica(freePort(), freePort());
        replica.awaitPortOpen();

        // it is looking for a master that is not there, so a follower of its own is running
        await(() -> newFollowers(followersBefore) == 1, "the replica never started a follower thread");

        replica.close();

        // the follower is gone rather than left asking for a master nobody asked for
        await(() -> newFollowers(followersBefore) == 0,
                "a follower was still running after the replica was stopped");
        // the accepting socket is closed too, so nothing can reach a stopped server
        assertThrowsConnectRefused(replica.port);
    }

    // ---------------------------------------------------------------- nodes

    private Node startMaster(int port) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
        RedisConfig config = context.getBean(RedisConfig.class);
        config.setRole("master");
        config.setPort(port);
        config.setAppendonly(false);
        Node node = new Node(context, port, null);
        nodes.add(node);
        node.startAcceptLoop();
        return node;
    }

    private Node startReplica(int port, int masterPort) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
        RedisConfig config = context.getBean(RedisConfig.class);
        config.setRole("slave");
        config.setPort(port);
        config.setMasterHost("127.0.0.1");
        config.setMasterPort(masterPort);
        // a replica never opens a file of its own, so this stays off and any file that turns
        // up beside it would be a bug
        config.setAppendonly(false);
        Node node = new Node(context, port, context.getBean(SlaveTcpServer.class));
        nodes.add(node);
        node.startAcceptLoop();
        return node;
    }

    /** One server, with the pieces a test needs to look at it. */
    private static final class Node {
        private final AnnotationConfigApplicationContext context;
        private final int port;
        private final SlaveTcpServer slaveServer;
        private Thread acceptThread;

        private Node(AnnotationConfigApplicationContext context, int port, SlaveTcpServer slaveServer) {
            this.context = context;
            this.port = port;
            this.slaveServer = slaveServer;
        }

        private ConnectionPool pool() {
            return context.getBean(ConnectionPool.class);
        }

        /** the connections this node currently holds as a master */
        private int slaveCount() {
            return pool().getSlaves().size();
        }

        /** the replicas that have asked for their command stream and can be sent writes */
        private int readySlaveCount() {
            int ready = 0;
            for (Slave slave : pool().getSlaves()) {
                if (slave.isReady()) {
                    ready++;
                }
            }
            return ready;
        }

        private void startAcceptLoop() {
            acceptThread = new Thread(() -> {
                if (slaveServer == null) {
                    context.getBean(MasterTcpServer.class).startServer();
                } else {
                    slaveServer.startServer();
                }
            }, "node-" + port);
            acceptThread.setDaemon(true);
            acceptThread.start();
        }

        private void awaitPortOpen() {
            await(() -> {
                try (Socket ignored = new Socket("127.0.0.1", port)) {
                    return true;
                } catch (IOException e) {
                    return false;
                }
            }, "nothing started listening on port " + port);
        }

        private void close() {
            if (slaveServer != null) {
                // ends the retries and closes the sockets the server is holding
                slaveServer.stop();
            }
            context.close();
        }
    }

    /** A master that hangs up on every replica that reaches it, and counts the attempts. */
    private static final class RefusingMaster implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final AtomicInteger attempts = new AtomicInteger();
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final int port;

        private RefusingMaster() throws IOException {
            serverSocket = new ServerSocket(0);
            port = serverSocket.getLocalPort();
            Thread acceptor = new Thread(() -> {
                while (running.get()) {
                    try (Socket replica = serverSocket.accept()) {
                        attempts.incrementAndGet();
                        // closed again straight away, so the handshake cannot finish
                    } catch (IOException e) {
                        return;
                    }
                }
            }, "refusing-master");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        private int attempts() {
            return attempts.get();
        }

        @Override
        public void close() throws IOException {
            running.set(false);
            serverSocket.close();
        }
    }

    /**
     * A master that hands over the RDB and streams whatever a test tells it to, one
     * attachment at a time, so a reconnect can be watched request by request.
     */
    private static final class ScriptedMaster implements AutoCloseable {
        /** the same empty RDB the project's own master sends behind the FULLRESYNC line */
        private static final String EMPTY_RDB_BASE64 =
                "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW1CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
        private final ServerSocket serverSocket;
        private final RespSerializer respSerializer = new RespSerializer();
        private final AtomicInteger connections = new AtomicInteger();
        private final int replicaPort;
        private Socket replica;
        private InputStream in;
        private OutputStream out;
        private RespStream requests;
        private boolean closed;

        private ScriptedMaster(int port, int replicaPort) throws IOException {
            serverSocket = new ServerSocket(port);
            this.replicaPort = replicaPort;
        }

        private int connections() {
            return connections.get();
        }

        private void attach() throws IOException {
            serverSocket.setSoTimeout((int) AWAIT_TIMEOUT_MS);
            replica = serverSocket.accept();
            replica.setTcpNoDelay(true);
            connections.incrementAndGet();
            in = replica.getInputStream();
            out = replica.getOutputStream();
            // one buffer for the whole handshake: the replica's steps can arrive in a single
            // read, and a buffer per request would drop whatever followed the first one
            requests = new RespStream(respSerializer);
        }

        private void handshake() throws IOException {
            assertArrayEquals(new String[]{"PING"}, request(), "the attachment did not start with a ping");
            write("+PONG");
            assertArrayEquals(new String[]{"REPLCONF", "listening-port", "" + replicaPort}, request(),
                    "the attachment did not carry the replica's listening port");
            write("+OK");
            assertArrayEquals(new String[]{"REPLCONF", "capa", "psync2"}, request(),
                    "the attachment did not carry the capability handshake");
            write("+OK");
            assertArrayEquals(new String[]{"PSYNC", "?", "-1"}, request(),
                    "the attachment did not ask for a full resync");
            // the offset is the position in the write stream, as our own master and real Redis
            // both send it: a plain number the replica can pick its own stream up from
            write("+FULLRESYNC scriptedreplid 0");
            byte[] rdb = java.util.Base64.getDecoder().decode(EMPTY_RDB_BASE64);
            write("$" + rdb.length);
            out.write(rdb);
            out.flush();
        }

        private void stream(byte[]... commands) throws IOException {
            for (byte[] command : commands) {
                out.write(command);
            }
            out.flush();
        }

        private void hangUp() throws IOException {
            replica.close();
        }

        private void write(String line) throws IOException {
            out.write((line + "\r\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
        }

        /** Waits for the next whole request the replica sent. */
        private String[] request() throws IOException {
            while (true) {
                List<String[]> whole = requests.drain();
                if (!whole.isEmpty()) {
                    return whole.get(0);
                }
                byte[] buffer = new byte[512];
                int read = in.read(buffer);
                if (read == -1) {
                    throw new IOException("the replica hung up mid handshake");
                }
                requests.append(buffer, read);
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            if (replica != null) {
                replica.close();
            }
            serverSocket.close();
        }
    }

    // ------------------------------------------------------------- actions

    /** The offset a node reports, or -1 when it could not be asked, for use inside a wait. */
    private static long offsetOrMinusOne(int port) {
        try {
            return reportedOffset(port);
        } catch (Exception e) {
            return -1;
        }
    }

    private void set(int port, String key, String value) throws Exception {
        try (RespSocket client = new RespSocket(port)) {
            client.send(frame("SET", key, value));
            assertEquals("+OK", client.readReply(), "the write was refused");
        }
    }

    private void incr(int port, String key) throws Exception {
        try (RespSocket client = new RespSocket(port)) {
            client.send(frame("INCR", key));
            assertTrue(client.readReply().startsWith(":"), "the increment was refused");
        }
    }

    /** Reads one key's value, or null when the node does not hold it. */
    private String get(int port, String key) throws Exception {
        try (RespSocket client = new RespSocket(port)) {
            client.send(frame("GET", key));
            return client.readValue();
        }
    }

    /**
     * The offset a node reports for its write stream, read the way a replica reads it from
     * INFO, so a test compares the two numbers each side actually hands out.
     */
    private static long reportedOffset(int port) throws Exception {
        try (RespSocket client = new RespSocket(port)) {
            client.send(frame("INFO", "replication"));
            String info = client.readValue();
            for (String line : info.split("\r\n")) {
                if (line.startsWith("master_repl_offset:")) {
                    return Long.parseLong(line.substring("master_repl_offset:".length()));
                }
            }
            throw new AssertionError("INFO reported no master_repl_offset: " + info);
        }
    }

    /** Waits for a key to hold a value on a replica, and names the node that never got it. */
    private void awaitValue(Node replica, String key, String expected) {
        await(() -> {
            try {
                return expected.equals(get(replica.port, key));
            } catch (Exception e) {
                return false;
            }
        }, "the replica on port " + replica.port + " never held " + key + "=" + expected);
    }

    /** Hangs up on every replica attached to this node, which is what a master restart looks like. */
    private void closeUpstream(Node master) {
        List<Slave> attached = new ArrayList<>(master.pool().getSlaves());
        assertFalse(attached.isEmpty(), "there was no replica attached to hang up on");
        for (Slave slave : attached) {
            try {
                slave.connection.socket.close();
            } catch (IOException e) {
                throw new IllegalStateException("could not hang up on a replica", e);
            }
        }
    }

    /**
     * Waits until this node is holding the expected number of replicas that are ready to be
     * sent writes.
     *
     * <p>A write the master accepts before a replica has finished attaching is not
     * backfilled, so a test that wants to watch replication has to wait for the attachment
     * rather than hope for it. A count that is expected to come back is only trusted after
     * the drop has been seen, so a stale connection cannot pass for a new one.</p>
     */
    private void awaitAttached(Node master, int expected) {
        await(() -> master.readySlaveCount() == expected,
                "the master on port " + master.port + " never held " + expected
                        + " attached replicas, it held " + master.readySlaveCount());
    }

    /**
     * Waits for a replica to be registered, then for its stream to start, so a write made
     * after this cannot land in the middle of a handshake.
     */
    private void awaitRegisteredAndReady(Node master, int expected) {
        await(() -> master.slaveCount() == expected,
                "the master on port " + master.port + " never held " + expected + " registered replicas");
        awaitAttached(master, expected);
    }

    private static void assertThrowsConnectRefused(int port) {
        try (Socket ignored = new Socket()) {
            ignored.connect(new java.net.InetSocketAddress("127.0.0.1", port), 500);
            org.junit.jupiter.api.Assertions.fail("port " + port + " is still accepting connections");
        } catch (ConnectException expected) {
            // the server is stopped, so its socket is gone
        } catch (IOException e) {
            org.junit.jupiter.api.Assertions.fail("port " + port + " answered " + e);
        }
    }

    private static void assertArrayEquals(String[] expected, String[] actual, String message) {
        org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual, message);
    }

    // ------------------------------------------------------------- waiting

    private static void await(BooleanSupplier condition, String message) {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(POLL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("the test was interrupted", e);
            }
        }
        throw new IllegalStateException(message);
    }

    private static Set<Thread> liveFollowers() {
        Set<Thread> followers = new HashSet<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.isAlive() && FOLLOWER_THREAD.equals(thread.getName())) {
                followers.add(thread);
            }
        }
        return followers;
    }

    private static int newFollowers(Set<Thread> before) {
        Set<Thread> now = liveFollowers();
        now.removeAll(before);
        return now.size();
    }

    // -------------------------------------------------------------- frames

    private static byte[] frame(String... parts) {
        StringBuilder sb = new StringBuilder();
        sb.append('*').append(parts.length).append("\r\n");
        for (String part : parts) {
            sb.append('$').append(part.getBytes(StandardCharsets.UTF_8).length).append("\r\n")
                    .append(part).append("\r\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /** A raw RESP client, so a test reads exactly what the server wrote back. */
    private static final class RespSocket implements AutoCloseable {
        private final Socket socket;
        private final InputStream in;

        private RespSocket(int port) throws IOException {
            socket = new Socket("127.0.0.1", port);
            socket.setSoTimeout((int) AWAIT_TIMEOUT_MS);
            socket.setTcpNoDelay(true);
            in = socket.getInputStream();
        }

        private void send(byte[] bytes) throws IOException {
            socket.getOutputStream().write(bytes);
            socket.getOutputStream().flush();
        }

        /** Reads a whole reply: a status or error line, or a bulk string with its header. */
        private String readReply() throws IOException {
            String header = readLine();
            if (!header.startsWith("$")) {
                return header;
            }
            int length = Integer.parseInt(header.substring(1));
            if (length < 0) {
                return header;
            }
            return header + "\r\n" + readPayload(length) + "\r\n";
        }

        /** Reads a bulk string's payload on its own, or null for a nil reply. */
        private String readValue() throws IOException {
            String header = readLine();
            if ("$-1".equals(header)) {
                return null;
            }
            assertTrue(header.startsWith("$"), "unexpected reply: " + header);
            return readPayload(Integer.parseInt(header.substring(1)));
        }

        private String readPayload(int length) throws IOException {
            StringBuilder value = new StringBuilder();
            for (int i = 0; i < length; i++) {
                value.append((char) readByte());
            }
            readByte();
            readByte();
            return value.toString();
        }

        private String readLine() throws IOException {
            StringBuilder sb = new StringBuilder();
            int b;
            while ((b = readByte()) != -1) {
                if (b == '\r') {
                    readByte();
                    return sb.toString();
                }
                sb.append((char) b);
            }
            throw new IOException("the server hung up");
        }

        private int readByte() throws IOException {
            int b = in.read();
            if (b == -1) {
                throw new IOException("the server hung up");
            }
            return b;
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}
