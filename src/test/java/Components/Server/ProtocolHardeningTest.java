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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * What the server does with commands that are wrong in some way: unknown names, the
 * wrong number of arguments, a SET with an expiry in any case or none at all, the INFO
 * sections, an empty multibulk request, and the transactional control guards. Every one
 * of these used to be a path where the connection died, the server threw, or an error
 * reply turned into a null byte.
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ProtocolHardeningTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;
    private static final int ASSAULT_ROUNDS = 50;

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
        // the upstream handshake is given a closed port; these tests only care about how
        // each server answers a client that speaks to it
        redisConfig.setMasterHost("127.0.0.1");
        redisConfig.setMasterPort(freePort());
        CompletableFuture.runAsync(slaveTcpServer::startServer);
        awaitPortOpen(replicaPort);
    }

    @BeforeEach
    void waitForCleanRegistry() throws InterruptedException {
        await(() -> connectionPool.getClients().isEmpty() && connectionPool.getSlaves().isEmpty(),
                "the server still holds a connection from an earlier test");
    }

    @Test
    void anUnknownCommandIsReflectedAndTheConnectionStaysOpen() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("FOO"));
            assertEquals("-ERR unknown command 'FOO'\r\n", client.readReply(),
                    "an unknown command was not answered with an error naming it");

            // the original spelling goes back, whatever the client used
            client.send(frame("foo", "bar", "baz"));
            assertEquals("-ERR unknown command 'foo'\r\n", client.readReply(),
                    "the error did not echo the command as the client spelled it");

            client.send(frame("PING"));
            assertEquals("+PONG\r\n", client.readReply(),
                    "an unknown command closed the connection instead of being answered");
        }
    }

    @Test
    void commandNamesAreCaseInsensitive() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("ping"));
            assertEquals("+PONG\r\n", client.readReply(), "lowercase ping was not answered");

            client.send(frame("PiNg"));
            assertEquals("+PONG\r\n", client.readReply(), "mixed case ping was not answered");

            client.send(frame("seT", "case:key", "value"));
            assertEquals("+OK\r\n", client.readReply(), "lowercase set was refused");

            client.send(frame("gET", "case:key"));
            assertEquals("$5\r\nvalue\r\n", client.readReply(), "lowercase get lost the value");

            client.send(frame("iNcr", "case:counter"));
            assertEquals(":1\r\n", client.readReply(), "mixed case incr did not run");

            client.send(frame("eCho", "hello"));
            assertEquals("$5\r\nhello\r\n", client.readReply(), "lowercase echo was refused");
        }
    }

    @Test
    void wrongArgumentCountsAreRefusedAndTheConnectionSurvives() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("PING", "too", "many"));
            assertEquals("-ERR wrong number of arguments for 'ping' command\r\n", client.readReply());

            client.send(frame("GET"));
            assertEquals("-ERR wrong number of arguments for 'get' command\r\n", client.readReply());

            client.send(frame("GET", "arity:key", "extra"));
            assertEquals("-ERR wrong number of arguments for 'get' command\r\n", client.readReply());

            client.send(frame("SET", "arity:key"));
            assertEquals("-ERR wrong number of arguments for 'set' command\r\n", client.readReply());

            client.send(frame("SET", "arity:key", "value", "extra"));
            assertEquals("-ERR wrong number of arguments for 'set' command\r\n", client.readReply());

            client.send(frame("INCR"));
            assertEquals("-ERR wrong number of arguments for 'incr' command\r\n", client.readReply());

            client.send(frame("INCR", "arity:counter", "extra"));
            assertEquals("-ERR wrong number of arguments for 'incr' command\r\n", client.readReply());

            client.send(frame("ECHO"));
            assertEquals("-ERR wrong number of arguments for 'echo' command\r\n", client.readReply());

            client.send(frame("INFO", "a", "b"));
            assertEquals("-ERR wrong number of arguments for 'info' command\r\n", client.readReply());

            client.send(frame("MULTI", "now"));
            assertEquals("-ERR wrong number of arguments for 'multi' command\r\n", client.readReply());

            // every refusal above left the connection open, so this answer proves it
            client.send(frame("PING"));
            assertEquals("+PONG\r\n", client.readReply(),
                    "an arity error closed the connection instead of being answered");

            client.send(frame("GET", "arity:key"));
            assertEquals("$-1\r\n", client.readReply(),
                    "a refused SET stored something anyway");
        }
    }

    @Test
    void setWithAnExpiryIsAcceptedInEveryCaseAndThenExpires() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "px:lower", "v", "px", "1000"));
            assertEquals("+OK\r\n", client.readReply(), "a lowercase px option was refused");

            client.send(frame("SET", "px:upper", "v", "PX", "1000"));
            assertEquals("+OK\r\n", client.readReply(), "an uppercase PX option was refused");

            client.send(frame("SET", "px:mixed", "v", "Px", "1000"));
            assertEquals("+OK\r\n", client.readReply(), "a mixed case Px option was refused");

            client.send(frame("GET", "px:lower"));
            assertEquals("$1\r\nv\r\n", client.readReply(), "the px write did not store its value");

            // the deadline is one second out, so a poll with a deadline of several is
            // the answer to when it lapses, not a guess
            for (String key : new String[]{"px:lower", "px:upper", "px:mixed"}) {
                awaitGone(client, key, "$1\r\nv\r\n");
            }
        }
    }

    @Test
    void setWithABadExpiryIsRefusedAndStoresNothing() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "bad:px", "v", "px", "notanumber"));
            assertEquals("-ERR value is not an integer or out of range\r\n", client.readReply());

            client.send(frame("SET", "bad:px", "v", "px", "0"));
            assertEquals("-ERR invalid expire time in 'set' command\r\n", client.readReply(),
                    "an expiry of zero was accepted");

            client.send(frame("SET", "bad:px", "v", "px", "-5"));
            assertEquals("-ERR invalid expire time in 'set' command\r\n", client.readReply(),
                    "a negative expiry was accepted");

            client.send(frame("SET", "bad:px", "v", "ex", "10"));
            assertEquals("-ERR unsupported option 'ex'\r\n", client.readReply(),
                    "an option the command does not know was silently ignored");

            client.send(frame("GET", "bad:px"));
            assertEquals("$-1\r\n", client.readReply(),
                    "a refused SET stored its value anyway");

            client.send(frame("PING"));
            assertEquals("+PONG\r\n", client.readReply(),
                    "a refused SET closed the connection");
        }
    }

    @Test
    void infoSectionsAreAnsweredAsDesigned() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("INFO"));
            String whole = client.readReply();
            assertTrue(whole.contains("# Server"), "bare INFO lost the server section: " + whole);
            assertTrue(whole.contains("# Replication"), "bare INFO lost the replication section: " + whole);
            assertTrue(whole.contains("role:" + redisConfig.getRole()),
                    "bare INFO lost the role: " + whole);
            assertTrue(whole.contains("master_repl_offset:"), "bare INFO lost the offset: " + whole);

            client.send(frame("INFO", "replication"));
            String replication = client.readReply();
            assertTrue(replication.contains("# Replication"), "INFO replication lost its header: " + replication);
            assertTrue(replication.contains("role:" + redisConfig.getRole()),
                    "INFO replication lost the role: " + replication);
            assertTrue(replication.contains("master_replid:"), "INFO replication lost the id: " + replication);

            client.send(frame("INFO", "server"));
            String server = client.readReply();
            assertTrue(server.contains("# Server"), "INFO server lost its header: " + server);
            assertTrue(!server.contains("# Replication"),
                    "INFO server leaked the replication section: " + server);

            client.send(frame("INFO", "all"));
            String all = client.readReply();
            assertTrue(all.contains("# Server") && all.contains("# Replication"),
                    "INFO all did not carry both sections: " + all);

            client.send(frame("INFO", "nonsense"));
            assertEquals("-ERR Invalid INFO section specified\r\n", client.readReply(),
                    "an unknown section was answered with content instead of an error");

            client.send(frame("PING"));
            assertEquals("+PONG\r\n", client.readReply(), "INFO closed the connection");
        }
    }

    @Test
    void anEmptyMultibulkIsAProtocolErrorThatEndsTheConnection() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send("*0\r\n".getBytes(StandardCharsets.UTF_8));
            assertEquals("-ERR Protocol error: empty multibulk request\r\n", client.readReply(),
                    "an empty array was not treated as a protocol error");
            assertEquals("", client.readUntilClosed(),
                    "the connection was kept open after a protocol error, so the stream could "
                            + "never be resynchronised");
        }
        await(() -> connectionPool.getClients().isEmpty(),
                "the connection was not cleaned up after the protocol error");

        try (RespSocket next = new RespSocket(masterPort)) {
            next.send(frame("PING"));
            assertEquals("+PONG\r\n", next.readReply(), "the server did not recover after the protocol error");
        }
    }

    @Test
    void transactionalControlCommandsAreGuarded() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("MULTI"));
            assertEquals("+OK\r\n", client.readReply());

            client.send(frame("EXEC", "now"));
            assertEquals("-ERR wrong number of arguments for 'exec' command\r\n", client.readReply());

            // DISCARD still lands on the open transaction, which proves the bad EXEC
            // did not end it
            client.send(frame("DISCARD"));
            assertEquals("+OK\r\n", client.readReply());

            client.send(frame("EXEC"));
            assertEquals("-ERR EXEC without MULTI\r\n", client.readReply());

            client.send(frame("MULTI"));
            assertEquals("+OK\r\n", client.readReply());

            client.send(frame("DISCARD", "now"));
            assertEquals("-ERR wrong number of arguments for 'discard' command\r\n", client.readReply());

            client.send(frame("DISCARD"));
            assertEquals("+OK\r\n", client.readReply());

            client.send(frame("PING"));
            assertEquals("+PONG\r\n", client.readReply(), "a guarded control command broke the connection");
        }
    }

    @Test
    void aQueuedSetWithPxCarriesItsExpiryThroughExec() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("MULTI"));
            assertEquals("+OK\r\n", client.readReply());

            client.send(frame("SET", "tx:px", "kept", "px", "700"));
            assertEquals("+QUEUED\r\n", client.readReply(),
                    "a SET with px was refused at queue time");

            client.send(frame("EXEC"));
            assertEquals("*1\r\n+OK\r\n", client.readReply(),
                    "the transaction did not answer for its queued SET");

            client.send(frame("GET", "tx:px"));
            assertEquals("$4\r\nkept\r\n", client.readReply(),
                    "the queued SET did not store its value");

            awaitGone(client, "tx:px", "$4\r\nkept\r\n");
        }
    }

    @Test
    void fiftyRoundsOfUnknownArityAndPingKeepOneConnectionAlive() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            for (int round = 0; round < ASSAULT_ROUNDS; round++) {
                client.send(frame("nonsense"));
                assertEquals("-ERR unknown command 'nonsense'\r\n", client.readReply(),
                        "round " + round + ": an unknown command was not answered");

                client.send(frame("get"));
                assertEquals("-ERR wrong number of arguments for 'get' command\r\n", client.readReply(),
                        "round " + round + ": a bare get was not refused");

                client.send(frame("SET", "assault:key"));
                assertEquals("-ERR wrong number of arguments for 'set' command\r\n", client.readReply(),
                        "round " + round + ": a bare set was not refused");

                client.send(frame("PING"));
                assertEquals("+PONG\r\n", client.readReply(),
                        "round " + round + ": the connection died under the assault");
            }
        }
    }

    @Test
    void theReplicaHardensItsProtocolTheSameWay() throws Exception {
        try (RespSocket client = new RespSocket(replicaPort)) {
            client.send(frame("FOO"));
            assertEquals("-ERR unknown command 'FOO'\r\n", client.readReply(),
                    "the replica did not reflect an unknown command");

            client.send(frame("SET", "rep:key", "value"));
            assertEquals("-READONLY You can't write against a replica.\r\n", client.readReply(),
                    "the replica accepted a write");

            // the form is checked first: a malformed SET is a protocol answer, not a
            // refusal of something that was never a valid command
            client.send(frame("SET", "rep:key"));
            assertEquals("-ERR wrong number of arguments for 'set' command\r\n", client.readReply());

            client.send(frame("SET", "rep:key", "v", "px", "-1"));
            assertEquals("-ERR invalid expire time in 'set' command\r\n", client.readReply());

            client.send(frame("PING"));
            assertEquals("+PONG\r\n", client.readReply(),
                    "an error answer closed the replica's connection");
        }
    }

    /** Waits until a key has expired, which a single GET at a fixed moment cannot say. */
    private static void awaitGone(RespSocket client, String key, String aliveReply) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            client.send(frame("GET", key));
            String reply = client.readReply();
            if ("$-1\r\n".equals(reply)) {
                return;
            }
            assertEquals(aliveReply, reply,
                    "the key " + key + " answered with " + reply + " while it was expiring");
            Thread.sleep(50);
        }
        fail("the key " + key + " never expired");
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

    /** A raw RESP client that reads whole replies, arrays included. */
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
