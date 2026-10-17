package com.redisforge.server;

import com.redisforge.persistence.AppendOnlyPersistence;
import com.redisforge.store.Store;
import com.redisforge.store.Value;
import com.redisforge.protocol.RespSerializer;
import com.redisforge.config.AppConfig;
import org.junit.jupiter.api.BeforeAll;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The hash, list and set commands over a real socket: what a client reads back, what the
 * append only file records, what the key's own type survives, and what a transaction does
 * with a random pop.
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class HashListSetServerTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;

    @Autowired
    private MasterTcpServer masterTcpServer;
    @Autowired
    private RedisConfig redisConfig;
    @Autowired
    private Store store;
    @Autowired
    private AppendOnlyPersistence appendOnly;
    @Autowired
    private RespSerializer respSerializer;

    private int masterPort;
    private Path file;

    @BeforeAll
    void startMaster(@TempDir Path dir) throws Exception {
        masterPort = freePort();
        file = dir.resolve("appendonly.aof");
        redisConfig.setRole("master");
        redisConfig.setPort(masterPort);
        redisConfig.setAppendonly(true);
        redisConfig.setAppendfilename(file.toString());
        redisConfig.setAppendfsync("no");
        appendOnly.start();
        Thread server = new Thread(masterTcpServer::startServer, "hash-list-set-master");
        server.setDaemon(true);
        server.start();
        awaitPortOpen(masterPort);
    }

    @Test
    void hashesListsAndSetsAnswerOverTheSocket() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            assertEquals(2L, client.send(frame("HSET", "srv:hash", "a", "1", "b", "2")).readReply());
            assertEquals("1", client.send(frame("HGET", "srv:hash", "a")).readReply());
            assertEquals(Arrays.asList("a", "1", "b", "2"),
                    client.send(frame("HGETALL", "srv:hash")).readReply());

            assertEquals(3L, client.send(frame("RPUSH", "srv:list", "a", "b", "c")).readReply());
            assertEquals(Arrays.asList("a", "b", "c"), client.send(frame("LRANGE", "srv:list", "0", "-1")).readReply());

            assertEquals(2L, client.send(frame("SADD", "srv:set", "x", "y")).readReply());
            assertEquals(1L, client.send(frame("SISMEMBER", "srv:set", "x")).readReply());
            assertTrue(client.send(frame("SMEMBERS", "srv:set")).readReply() instanceof List);
        }
    }

    @Test
    void aCommandAgainstTheWrongTypeIsRefusedOverTheSocket() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "srv:str", "v")).readReply();
            assertEquals("WRONGTYPE Operation against a key holding the wrong kind of value",
                    client.send(frame("HGET", "srv:str", "f")).readReply());

            client.send(frame("HSET", "srv:hash2", "f", "v")).readReply();
            assertEquals("WRONGTYPE Operation against a key holding the wrong kind of value",
                    client.send(frame("GET", "srv:hash2")).readReply());
            assertEquals("WRONGTYPE Operation against a key holding the wrong kind of value",
                    client.send(frame("INCR", "srv:hash2")).readReply());
        }
    }

    @Test
    void anExpirationOnAHashKeepsTheHash() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("HSET", "srv:exphash", "f", "v")).readReply();
            assertEquals(1L, client.send(frame("EXPIRE", "srv:exphash", "100")).readReply());
            assertTrue((Long) client.send(frame("TTL", "srv:exphash")).readReply() > 90);
            // the deadline must not have turned the hash into an empty string
            assertEquals("v", client.send(frame("HGET", "srv:exphash", "f")).readReply());
        }
        assertEquals(Value.Kind.HASH, store.peekValue("srv:exphash").kind);
        assertEquals(Value.Kind.HASH, replayIntoNewStore().peekValue("srv:exphash").kind,
                "the hash did not come back as a hash after a restart");
    }

    @Test
    void aPushedPopExpiryRemovesTheKey() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("RPUSH", "srv:pop", "a", "b")).readReply();
            client.send(frame("PEXPIRE", "srv:pop", "20")).readReply();
        }
        Thread.sleep(120);
        try (RespSocket client = new RespSocket(masterPort)) {
            Object reply = client.send(frame("LRANGE", "srv:pop", "0", "-1")).readReply();
            assertTrue(reply instanceof List && ((List<?>) reply).isEmpty(),
                    "an expired list still held elements: " + reply);
        }
    }

    @Test
    void typedWritesAreRecordedAndReadsAreNot() throws Exception {
        int before = frames().size();
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("HSET", "srv:aofhash", "f", "v")).readReply();
            client.send(frame("LPUSH", "srv:aoflist", "x")).readReply();
            client.send(frame("SADD", "srv:aofset", "m")).readReply();
            client.send(frame("HGET", "srv:aofhash", "f")).readReply();
            client.send(frame("SMEMBERS", "srv:aofset")).readReply();
        }
        List<String> names = new ArrayList<>();
        for (String[] written : framesSince(before)) {
            names.add(written[0]);
        }
        assertTrue(names.contains("HSET"), "the hash write was not recorded: " + names);
        assertTrue(names.contains("LPUSH"), "the list write was not recorded: " + names);
        assertTrue(names.contains("SADD"), "the set write was not recorded: " + names);
        assertFalse(names.contains("HGET"), "a read reached the append only file");
        assertFalse(names.contains("SMEMBERS"), "a read reached the append only file");
    }

    @Test
    void asPopIsRecordedAsTheSremOfTheMembersItTook() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SADD", "srv:spop", "only")).readReply();
        }
        int before = frames().size();
        try (RespSocket client = new RespSocket(masterPort)) {
            assertEquals("only", client.send(frame("SPOP", "srv:spop")).readReply());
        }
        List<String[]> written = framesSince(before);
        assertEquals(1, written.size(), "a random pop did not record exactly one frame");
        assertEquals("SREM", written.get(0)[0], "SPOP was recorded as: " + String.join(" ", written.get(0)));
        assertEquals("srv:spop", written.get(0)[1]);
        assertEquals("only", written.get(0)[2]);
    }

    @Test
    void aPopThroughATransactionIsAlsoRecordedAsSrem() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SADD", "srv:txspop", "only")).readReply();
        }
        int before = frames().size();
        long streamMark = redisConfig.getMasterReplOffset();
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("MULTI")).readReply();
            assertEquals("QUEUED", client.send(frame("SPOP", "srv:txspop")).readReply());
            List<?> replies = (List<?>) client.send(frame("EXEC")).readReply();
            assertEquals(1, replies.size());
            assertEquals("only", replies.get(0));
        }
        List<String[]> written = framesSince(before);
        assertEquals("MULTI", written.get(0)[0]);
        assertEquals("SREM", written.get(1)[0], "a SPOP inside a transaction was not written down as SREM");
        assertEquals("only", written.get(1)[2]);

        List<String[]> propagated = propagatedSince(streamMark);
        assertEquals(1, propagated.size(),
                "the transaction passed on something other than its one pop: " + propagated.size());
        assertEquals("SREM", propagated.get(0)[0],
                "a SPOP inside a transaction was passed to replicas raw: "
                        + String.join(" ", propagated.get(0)));
        assertEquals("srv:txspop", propagated.get(0)[1]);
        assertEquals("only", propagated.get(0)[2]);
    }

    @Test
    void aTransactionAppliesEveryTypedKind() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("MULTI")).readReply();
            client.send(frame("HSET", "srv:txhash", "f", "v")).readReply();
            client.send(frame("RPUSH", "srv:txlist", "e")).readReply();
            client.send(frame("SADD", "srv:txset", "m")).readReply();
            List<?> replies = (List<?>) client.send(frame("EXEC")).readReply();
            assertEquals(Arrays.asList(1L, 1L, 1L), replies);
        }
        assertEquals(Value.Kind.HASH, store.peekValue("srv:txhash").kind);
        assertEquals(Value.Kind.LIST, store.peekValue("srv:txlist").kind);
        assertEquals(Value.Kind.SET, store.peekValue("srv:txset").kind);
    }

    @Test
    void aHashIsChargedToMemoryAndReportedByInfo() throws Exception {
        long before;
        try (RespSocket client = new RespSocket(masterPort)) {
            before = usedMemory(client);
            client.send(frame("HSET", "srv:mem", "field", "a-value")).readReply();
        }
        try (RespSocket client = new RespSocket(masterPort)) {
            long after = usedMemory(client);
            assertTrue(after > before, "a hash was not charged to memory: " + before + " -> " + after);
            String section = (String) client.send(frame("INFO", "memory")).readReply();
            assertTrue(section.contains("# Memory"), "INFO memory has no Memory section");
            assertTrue(section.contains("used_memory:"), "INFO memory has no used_memory");
        }
    }

    private long usedMemory(RespSocket client) throws IOException {
        String info = (String) client.send(frame("INFO", "memory")).readReply();
        for (String line : info.split("\r\n")) {
            if (line.startsWith("used_memory:")) {
                return Long.parseLong(line.substring("used_memory:".length()));
            }
        }
        throw new AssertionError("INFO memory carried no used_memory line");
    }

    /** A fresh store replayed from the file, which is what a restarted server would hold. */
    private Store replayIntoNewStore() throws IOException {
        Store fresh = new Store();
        fresh.respSerializer = new RespSerializer();
        AppendOnlyPersistence replayed = new AppendOnlyPersistence(redisConfig, fresh);
        try {
            replayed.start();
        } finally {
            replayed.close();
        }
        return fresh;
    }

    private List<String[]> framesSince(int mark) {
        List<String[]> all = frames();
        return all.subList(mark, all.size());
    }

    /** The frames the master passed to replicas since the stream stood at this offset. */
    private List<String[]> propagatedSince(long fromOffset) {
        byte[] bytes = redisConfig.getReplicationBacklog().read(fromOffset);
        List<String[]> frames = new ArrayList<>();
        int offset = 0;
        while (offset < bytes.length) {
            int length = respSerializer.frameLength(bytes, offset, bytes.length);
            assertTrue(length > 0, "the replication stream has a partial frame at " + offset);
            frames.addAll(respSerializer.deseralize(bytes, offset, offset + length));
            offset += length;
        }
        return frames;
    }

    private List<String[]> frames() {
        byte[] contents;
        try {
            contents = Files.readAllBytes(file);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        List<String[]> frames = new ArrayList<>();
        int offset = 0;
        while (offset < contents.length) {
            int length = respSerializer.frameLength(contents, offset, contents.length);
            assertTrue(length > 0, "byte " + offset + " of the append only file is not a whole frame");
            frames.addAll(respSerializer.deseralize(contents, offset, offset + length));
            offset += length;
        }
        return frames;
    }

    private static byte[] frame(String... parts) {
        StringBuilder sb = new StringBuilder("*" + parts.length + "\r\n");
        for (String part : parts) {
            sb.append("$").append(part.getBytes(StandardCharsets.UTF_8).length)
                    .append("\r\n").append(part).append("\r\n");
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

        RespSocket send(byte[] bytes) throws IOException {
            out.write(bytes);
            out.flush();
            return this;
        }

        Object readReply() throws IOException {
            char type = (char) readByte();
            String line = readLine();
            return switch (type) {
                case '+', '-' -> line;
                case ':' -> Long.parseLong(line);
                case '$' -> {
                    int length = Integer.parseInt(line);
                    if (length < 0) {
                        yield null;
                    }
                    byte[] payload = in.readNBytes(length);
                    expectCrlf();
                    yield new String(payload, StandardCharsets.UTF_8);
                }
                case '*' -> {
                    int count = Integer.parseInt(line);
                    if (count < 0) {
                        yield null;
                    }
                    List<Object> items = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        items.add(readReply());
                    }
                    yield items;
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
