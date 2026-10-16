package com.redisforge.server;

import com.redisforge.network.Client;
import com.redisforge.protocol.RespStream;
import com.redisforge.store.StoreSnapshot;
import com.redisforge.store.Store;
import com.redisforge.store.Value;
import com.redisforge.protocol.RespSerializer;
import com.redisforge.config.AppConfig;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A replica's typed half: the dataset a full resync installs carries hash, list and set
 * kinds, the writes that follow the snapshot apply against those kinds, and a client of
 * the replica can read a hash or list or set but is refused a write to one.
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TypedReplicationTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;
    private static final String REPLICATION_ID = "0123456789abcdef0123456789abcdef01234567";

    @Autowired
    private SlaveTcpServer slaveTcpServer;
    @Autowired
    private RedisConfig redisConfig;
    @Autowired
    private Store store;
    @Autowired
    private RespSerializer respSerializer;

    @Test
    void aFullResyncInstallsEveryKindAndFollowsWithTypedWrites() throws Exception {
        int replicaPort = freePort();
        int masterPort = freePort();
        byte[] extraHashSet = frame("HSET", "repl:extra", "field", "value");
        byte[] extraSrem = frame("SREM", "repl:removed", "m");
        long expectedAck = extraHashSet.length + extraSrem.length;

        CompletableFuture<Void> master = CompletableFuture.runAsync(() -> runTypedMaster(masterPort, expectedAck));

        redisConfig.setRole("slave");
        redisConfig.setPort(replicaPort);
        redisConfig.setMasterHost("127.0.0.1");
        redisConfig.setMasterPort(masterPort);
        CompletableFuture.runAsync(slaveTcpServer::startServer);
        awaitPortOpen(replicaPort);

        // the snapshot carries one entry of every kind
        await(() -> store.peekValue("repl:hash") != null, "the resync never installed the snapshot");
        Value hash = store.getValue("repl:hash");
        assertEquals(Value.Kind.HASH, hash.kind);
        assertEquals("v", hash.hash.get("f"));
        assertEquals(Value.Kind.LIST, store.getValue("repl:list").kind);
        assertEquals(Value.Kind.SET, store.getValue("repl:set").kind);

        // the writes after the snapshot applied with their kinds
        await(() -> store.getValue("repl:extra") != null, "the write after the snapshot was never applied");
        assertEquals("value", store.getValue("repl:extra").hash.get("field"));
        await(() -> store.peekValue("repl:removed") == null, "the SREM after the snapshot was not applied");
        assertEquals(Value.Kind.SET, store.getValue("repl:set").kind,
                "the SREM took members from the wrong key");

        master.get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        // a client of the replica reads a typed key but cannot write one
        try (RespSocket client = new RespSocket(replicaPort)) {
            client.send(frame("HGET", "repl:hash", "f"));
            assertEquals("$1\r\nv\r\n", client.readReply());
            client.send(frame("SMEMBERS", "repl:removed"));
            assertTrue(client.readReply().startsWith("*0"));
            client.send(frame("SADD", "repl:removed", "new"));
            String refused = client.readReply();
            assertTrue(refused.contains("READONLY"),
                    "a typed write was not refused to a replica's client: " + refused);
            assertTrue(store.peekValue("repl:removed") == null,
                    "a refused typed write still changed the replica");
        }
    }

    @Test
    void typedUpstreamFramesAreAppliedFromTheStream() throws Exception {
        redisConfig.setRole("slave");
        redisConfig.setMasterHost("127.0.0.1");
        redisConfig.setMasterPort(freePort());
        store.map.clear();

        feed(frame("HSET", "repl:fedhash", "f", "v"),
                frame("RPUSH", "repl:fedlist", "a", "b"),
                frame("SADD", "repl:fedset", "x", "y"),
                concat(frame("SREM", "repl:fedset", "x"), frame("LPOP", "repl:fedlist", "1")));

        Value hash = store.getValue("repl:fedhash");
        assertEquals(Value.Kind.HASH, hash.kind);
        assertEquals("v", hash.hash.get("f"));
        assertEquals(List.of("b"), new ArrayList<>(store.getValue("repl:fedlist").list),
                "the LPOP was not applied as the master made it");
        assertEquals(1, store.getValue("repl:fedset").set.size(), "the SREM was not applied");
    }

    // ------------------------------------------------------------- fake master

    /**
     * Runs the other end of the replica's handshake and hands it a typed dataset, then the
     * writes that follow and a GETACK, and waits for the acknowledgement of exactly those
     * writes.
     */
    private void runTypedMaster(int port, long expectedAck) {
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

            byte[] dataset = StoreSnapshot.serializeDataset(List.of(
                    new StoreSnapshot.TypedEntry("repl:hash", StoreSnapshot.Kind.HASH,
                            List.of("f", "v"), null),
                    new StoreSnapshot.TypedEntry("repl:list", StoreSnapshot.Kind.LIST,
                            List.of("a", "b"), null),
                    new StoreSnapshot.TypedEntry("repl:set", StoreSnapshot.Kind.SET,
                            List.of("m"), null),
                    new StoreSnapshot.TypedEntry("repl:removed", StoreSnapshot.Kind.SET,
                            List.of("m"), null)));
            write(out, "+FULLRESYNC " + REPLICATION_ID + " 0\r\n$" + dataset.length + "\r\n");
            out.write(dataset);
            out.write(frame("HSET", "repl:extra", "field", "value"));
            out.write(frame("SREM", "repl:removed", "m"));
            out.write(frame("REPLCONF", "GETACK", "*"));
            out.flush();

            byte[] buffer = new byte[4096];
            while (true) {
                int bytesRead = in.read(buffer);
                if (bytesRead == -1) {
                    throw new EOFException("the replica hung up before acknowledging");
                }
                requests.append(buffer, bytesRead);
                for (String[] command : requests.drain()) {
                    if (command[0].equalsIgnoreCase("REPLCONF") && command[1].equalsIgnoreCase("ACK")) {
                        assertEquals("" + expectedAck, command[2],
                                "the replica acknowledged a different byte count than the writes made");
                        return;
                    }
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("the fake master failed: " + e, e);
        }
    }

    // ----------------------------------------------------------------- harness

    /** Runs the upstream stream the way the master's socket delivers it. */
    private void feed(byte[]... reads) throws IOException {
        Client master = new Client(new Socket(), new ChunkedInputStream(List.of(reads)),
                new ByteArrayOutputStream(), -1);
        slaveTcpServer.streamFromMaster(master);
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

    private static byte[] concat(byte[]... arrays) {
        int total = 0;
        for (byte[] array : arrays) {
            total += array.length;
        }
        byte[] all = new byte[total];
        int at = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, all, at, array.length);
            at += array.length;
        }
        return all;
    }

    private static void write(OutputStream out, String text) throws IOException {
        out.write(text.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private static byte[] frame(String... parts) {
        StringBuilder sb = new StringBuilder("*").append(parts.length).append("\r\n");
        for (String part : parts) {
            byte[] payload = part.getBytes(StandardCharsets.UTF_8);
            sb.append("$").append(payload.length).append("\r\n").append(part).append("\r\n");
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

    /** Hands out one scripted chunk per read, which is what a socket does. */
    private static final class ChunkedInputStream extends InputStream {
        private final List<byte[]> chunks;
        private int next;
        private int position;

        ChunkedInputStream(List<byte[]> chunks) {
            this.chunks = chunks;
        }

        private byte[] current() throws IOException {
            while (next < chunks.size()) {
                byte[] chunk = chunks.get(next);
                if (position < chunk.length) {
                    return chunk;
                }
                next++;
                position = 0;
            }
            return null;
        }

        @Override
        public int read() throws IOException {
            byte[] chunk = current();
            return chunk == null ? -1 : chunk[position++] & 0xFF;
        }

        @Override
        public int read(byte[] destination, int offset, int length) throws IOException {
            byte[] chunk = current();
            if (chunk == null) {
                return -1;
            }
            int count = Math.min(length, chunk.length - position);
            System.arraycopy(chunk, position, destination, offset, count);
            position += count;
            return count;
        }
    }

    /** A raw RESP client that decodes whole replies into plain Java values. */
    private final class RespSocket implements AutoCloseable {
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
            char type = (char) readByte();
            String line = readLine();
            return switch (type) {
                case '+', '-', ':' -> line;
                case '$' -> {
                    int length = Integer.parseInt(line);
                    if (length < 0) {
                        yield line;
                    }
                    byte[] payload = in.readNBytes(length);
                    expectCrlf();
                    yield "$" + length + "\r\n" + new String(payload, StandardCharsets.UTF_8) + "\r\n";
                }
                case '*' -> {
                    int count = Integer.parseInt(line);
                    if (count < 0) {
                        yield line;
                    }
                    List<String> items = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        items.add(readReply());
                    }
                    yield "*" + count;
                }
                default -> throw new IOException("not RESP: " + type);
            };
        }

        private int readByte() throws IOException {
            int b = in.read();
            if (b == -1) {
                throw new EOFException("connection closed");
            }
            return b;
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

        private void expectCrlf() throws IOException {
            if (in.read() != '\r' || in.read() != '\n') {
                throw new IOException("expected CRLF after a bulk payload");
            }
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}