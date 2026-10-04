package Components.Server;

import Components.Infra.Client;
import Components.Infra.RespStream;
import Components.Repository.Store;
import Components.Service.RespSerializer;
import Config.AppConfig;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A replica reads the RDB and then a stream of RESP arrays from its master, and TCP is
 * free to end a read anywhere inside either one. These tests hand that upstream a
 * scripted byte stream so the split point is exact: every position inside a command,
 * one byte at a time, several commands in one read, a payload larger than the read
 * buffer, a master that hangs up mid command, and bytes that are not RESP at all. The
 * last two tests run a whole replica against a master that behaves the way this project
 * does, RDB payload and all.
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ReplicationFramingTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;
    private static final String REPLICATION_ID = "0123456789abcdef0123456789abcdef01234567";
    private static final String RDB_PAYLOAD = "REDIS0009pretend this is a serialized RDB payload";

    @Autowired
    private SlaveTcpServer slaveTcpServer;
    @Autowired
    private RedisConfig redisConfig;
    @Autowired
    private Store store;
    @Autowired
    private RespSerializer respSerializer;

    private int replicaPort;
    private volatile String lastAck;

    @Test
    void commandSplitAtEveryPositionIsAppliedWhole() throws Exception {
        byte[] command = frame("SET", "repl:split", "repl:value");

        for (int split = 1; split < command.length; split++) {
            store.map.remove("repl:split");
            feed(List.of(Arrays.copyOfRange(command, 0, split), Arrays.copyOfRange(command, split, command.length)));

            String stored = value("repl:split");
            assertNotNull(stored, "the replica threw away the command split after " + split + " bytes");
            assertEquals("repl:value", stored,
                    "the replica stored a partial command when the read ended after " + split + " bytes");
        }
    }

    @Test
    void commandDeliveredOneByteAtATimeIsAppliedWhole() throws Exception {
        byte[] command = frame("SET", "repl:drip", "one byte at a time");
        List<byte[]> oneBytePerRead = new ArrayList<>();
        for (byte b : command) {
            oneBytePerRead.add(new byte[]{b});
        }

        String reply = feed(oneBytePerRead);

        assertEquals("one byte at a time", value("repl:drip"));
        assertEquals("", reply, "a write streams no reply upstream");
    }

    @Test
    void severalCommandsInOneReadAreAppliedInOrder() throws Exception {
        byte[] third = frame("SET", "repl:third", "3");
        byte[] first = frame("SET", "repl:first", "1");
        byte[] second = frame("SET", "repl:second", "2");

        feed(List.of(concat(first, second, third), frame("SET", "repl:fourth", "4")));

        assertEquals("1", value("repl:first"));
        assertEquals("2", value("repl:second"));
        assertEquals("3", value("repl:third"));
        assertEquals("4", value("repl:fourth"));
    }

    @Test
    void readThatEndsInsideALengthHeaderIsHeldUntilTheRestArrives() throws Exception {
        byte[] command = frame("SET", "repl:header", "value");
        // cut between the digits and the CRLF of the payload's "$<length>" header
        int cut = new String(command, StandardCharsets.UTF_8).indexOf("$5\r\nvalue") + 2;

        feed(List.of(Arrays.copyOfRange(command, 0, cut), Arrays.copyOfRange(command, cut, command.length)));

        assertEquals("value", value("repl:header"));
    }

    @Test
    void valueLargerThanTheReadBufferIsAppliedWhole() throws Exception {
        String large = "v".repeat(1024 * 1024);
        byte[] command = frame("SET", "repl:large", large);

        List<byte[]> chunks = new ArrayList<>();
        for (int at = 0; at < command.length; at += 5000) {
            chunks.add(Arrays.copyOfRange(command, at, Math.min(at + 5000, command.length)));
        }
        feed(chunks);

        assertEquals(large, value("repl:large"));
    }

    @Test
    void masterHangingUpMidCommandLeavesThatCommandUnapplied() throws Exception {
        byte[] command = frame("SET", "repl:cut", "value");
        byte[] truncated = Arrays.copyOfRange(command, 0, command.length - 4);

        // the master hangs up after a partial command: the loop must end, and what never
        // arrived must not be applied as if it had
        feed(List.of(truncated));

        assertNull(store.getValue("repl:cut"), "a command the master never finished was applied anyway");
    }

    @Test
    void upstreamBytesThatAreNotRespFailTheStream() {
        IOException failure = assertThrows(IOException.class,
                () -> feed(List.of("this is not a RESP command\r\n".getBytes(StandardCharsets.UTF_8))));
        assertTrue(failure.getMessage().contains("malformed"), failure.getMessage());
    }

    @Test
    void getackIsAnsweredWithTheOffsetStreamedSoFar() throws Exception {
        redisConfig.setMasterReplOffset(0L);
        byte[] first = frame("SET", "repl:ack", "a");
        byte[] second = frame("SET", "repl:ack", "b");
        byte[] getack = frame("REPLCONF", "GETACK", "*");
        long offsetBeforeGetack = first.length + second.length;

        String reply = feed(List.of(concat(first, second), getack));

        assertEquals("b", value("repl:ack"));
        assertEquals(respSerializer.respArray(new String[]{"REPLCONF", "ACK", "" + offsetBeforeGetack}), reply);
        assertEquals(offsetBeforeGetack + getack.length, redisConfig.getMasterReplOffset().longValue());
    }

    @Test
    void replicaAppliesWhatTheMasterStreamsAfterTheRdb() throws Exception {
        byte[] first = frame("SET", "repl:one", "one");
        byte[] second = frame("SET", "repl:two", "two");
        byte[] third = frame("SET", "repl:three", "three");
        byte[] getack = frame("REPLCONF", "GETACK", "*");
        for (String key : List.of("repl:one", "repl:two", "repl:three")) {
            store.map.remove(key);
        }
        lastAck = null;
        redisConfig.setMasterReplOffset(0L);

        replicaPort = freePort();
        int masterPort = freePort();
        CompletableFuture<Void> master = CompletableFuture.runAsync(() -> runMaster(masterPort));

        redisConfig.setRole("slave");
        redisConfig.setPort(replicaPort);
        redisConfig.setMasterHost("127.0.0.1");
        redisConfig.setMasterPort(masterPort);
        CompletableFuture.runAsync(slaveTcpServer::startServer);
        awaitPortOpen(replicaPort);

        // the first command is delivered a byte at a time, the rest in one write
        await(() -> "three".equals(value("repl:three")),
                "the replica never applied the commands that followed the RDB payload");

        assertEquals("one", value("repl:one"), "the byte at a time command was applied in part");
        assertEquals("two", value("repl:two"));
        assertEquals("three", value("repl:three"));

        // the replica keeps answering its own clients while it is being fed
        try (RespSocket client = new RespSocket(replicaPort)) {
            client.send(frame("PING"));
            assertEquals("+PONG\r\n", client.readReply());
            client.send(frame("GET", "repl:two"));
            assertEquals("$3\r\ntwo\r\n", client.readReply());
        }

        await(() -> lastAck != null, "the replica never answered the master's GETACK");
        assertEquals("" + (first.length + second.length + third.length), lastAck);
        assertEquals(first.length + second.length + third.length + getack.length,
                redisConfig.getMasterReplOffset().longValue());

        // the master hangs up once it has been acknowledged, and the replica notices
        master.get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Test
    void replicaStaysUsableWhenTheRdbEndsEarly() throws Exception {
        replicaPort = freePort();
        int masterPort = freePort();
        CompletableFuture<Void> master = CompletableFuture.runAsync(() -> runMasterWithShortRdb(masterPort));

        redisConfig.setRole("slave");
        redisConfig.setPort(replicaPort);
        redisConfig.setMasterHost("127.0.0.1");
        redisConfig.setMasterPort(masterPort);
        CompletableFuture.runAsync(slaveTcpServer::startServer);
        awaitPortOpen(replicaPort);

        master.get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        try (RespSocket client = new RespSocket(replicaPort)) {
            client.send(frame("PING"));
            assertEquals("+PONG\r\n", client.readReply());
            client.send(frame("GET", "repl:never-streamed"));
            assertEquals("$-1\r\n", client.readReply());
        }
    }

    /** A master that hands over the RDB and then streams commands in awkward chunks. */
    private void runMaster(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port); Socket replica = serverSocket.accept()) {
            replica.setTcpNoDelay(true);
            InputStream in = replica.getInputStream();
            OutputStream out = replica.getOutputStream();
            RespStream requests = new RespStream(respSerializer);

            assertEquals("PING", readRequest(requests, in)[0]);
            write(out, "+PONG\r\n");

            String[] listeningPort = readRequest(requests, in);
            assertEquals("REPLCONF", listeningPort[0]);
            assertEquals("listening-port", listeningPort[1]);
            assertEquals("" + replicaPort, listeningPort[2]);
            write(out, "+OK\r\n");

            String[] capa = readRequest(requests, in);
            assertEquals("capa", capa[1]);
            assertEquals("psync2", capa[2]);
            write(out, "+OK\r\n");

            String[] psync = readRequest(requests, in);
            assertEquals("PSYNC", psync[0]);
            assertEquals("?", psync[1]);
            assertEquals("-1", psync[2]);

            byte[] rdb = RDB_PAYLOAD.getBytes(StandardCharsets.UTF_8);
            write(out, "+FULLRESYNC " + REPLICATION_ID + " 0\r\n$" + rdb.length + "\r\n");
            out.write(rdb);
            out.flush();

            for (byte b : frame("SET", "repl:one", "one")) {
                out.write(new byte[]{b});
                out.flush();
            }
            out.write(concat(frame("SET", "repl:two", "two"),
                    frame("SET", "repl:three", "three"),
                    frame("REPLCONF", "GETACK", "*")));
            out.flush();

            byte[] buffer = new byte[4096];
            while (lastAck == null) {
                int bytesRead = in.read(buffer);
                if (bytesRead == -1) {
                    break;
                }
                requests.append(buffer, bytesRead);
                for (String[] command : requests.drain()) {
                    if (command[0].equalsIgnoreCase("REPLCONF") && command[1].equalsIgnoreCase("ACK")) {
                        lastAck = command[2];
                    }
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("the fake master failed: " + e, e);
        }
    }

    /** A master that dies in the middle of the RDB payload it promised. */
    private void runMasterWithShortRdb(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port); Socket replica = serverSocket.accept()) {
            replica.setTcpNoDelay(true);
            InputStream in = replica.getInputStream();
            OutputStream out = replica.getOutputStream();
            RespStream requests = new RespStream(respSerializer);

            assertEquals("PING", readRequest(requests, in)[0]);
            write(out, "+PONG\r\n");
            assertEquals("REPLCONF", readRequest(requests, in)[0]);
            write(out, "+OK\r\n");
            assertEquals("REPLCONF", readRequest(requests, in)[0]);
            write(out, "+OK\r\n");
            assertEquals("PSYNC", readRequest(requests, in)[0]);

            byte[] rdb = RDB_PAYLOAD.getBytes(StandardCharsets.UTF_8);
            write(out, "+FULLRESYNC " + REPLICATION_ID + " 0\r\n$" + rdb.length + "\r\n");
            out.write(rdb, 0, rdb.length / 2);
            out.flush();
        } catch (Exception e) {
            throw new IllegalStateException("the fake master failed: " + e, e);
        }
    }

    private static String[] readRequest(RespStream requests, InputStream in) throws IOException {
        byte[] buffer = new byte[4096];
        while (true) {
            int bytesRead = in.read(buffer);
            if (bytesRead == -1) {
                throw new EOFException("the replica hung up during the handshake");
            }
            requests.append(buffer, bytesRead);
            List<String[]> frames = requests.drain();
            if (!frames.isEmpty()) {
                return frames.get(0);
            }
        }
    }

    /** Runs the replica's upstream loop over a scripted read pattern; returns what it wrote back. */
    private String feed(List<byte[]> reads) throws IOException {
        ByteArrayOutputStream writtenBack = new ByteArrayOutputStream();
        Client master = new Client(new Socket(), new ChunkedInputStream(reads), writtenBack, -1);
        slaveTcpServer.streamFromMaster(master);
        return writtenBack.toString(StandardCharsets.UTF_8);
    }

    private String value(String key) {
        return store.getValue(key) == null ? null : store.getValue(key).val;
    }

    private static void write(OutputStream out, String text) throws IOException {
        out.write(text.getBytes(StandardCharsets.UTF_8));
        out.flush();
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

    /** Hands out one scripted chunk per read, which is what a socket does. */
    private static final class ChunkedInputStream extends InputStream {
        private final List<byte[]> chunks;
        private int next;

        ChunkedInputStream(List<byte[]> chunks) {
            this.chunks = chunks;
        }

        @Override
        public int read() {
            if (next >= chunks.size()) {
                return -1;
            }
            byte[] chunk = chunks.get(next++);
            return chunk.length == 0 ? read() : chunk[0] & 0xFF;
        }

        @Override
        public int read(byte[] destination, int offset, int length) {
            while (next < chunks.size() && chunks.get(next).length == 0) {
                next++;
            }
            if (next >= chunks.size()) {
                return -1;
            }
            byte[] chunk = chunks.get(next++);
            int count = Math.min(length, chunk.length);
            System.arraycopy(chunk, 0, destination, offset, count);
            return count;
        }
    }

    private static final class RespSocket implements AutoCloseable {
        private final Socket socket;
        private final InputStream in;
        private final OutputStream out;

        RespSocket(int port) throws IOException {
            socket = new Socket("127.0.0.1", port);
            socket.setSoTimeout((int) AWAIT_TIMEOUT_MS);
            in = socket.getInputStream();
            out = socket.getOutputStream();
        }

        void send(byte[] bytes) throws IOException {
            out.write(bytes);
            out.flush();
        }

        String readReply() throws IOException {
            String header = readLine();
            if (!header.startsWith("$")) {
                return header + "\r\n";
            }
            int length = Integer.parseInt(header.substring(1));
            if (length < 0) {
                return header + "\r\n";
            }
            byte[] payload = in.readNBytes(length);
            if (payload.length != length) {
                throw new EOFException("reply held " + payload.length + " of " + length + " bytes");
            }
            if (in.read() != '\r' || in.read() != '\n') {
                throw new IOException("bulk string was not closed by CRLF");
            }
            return header + "\r\n" + new String(payload, StandardCharsets.UTF_8) + "\r\n";
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