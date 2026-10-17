package com.redisforge.server;

import com.redisforge.network.Client;
import com.redisforge.persistence.AppendOnlyPersistence;
import com.redisforge.store.Store;
import com.redisforge.protocol.RespSerializer;
import com.redisforge.config.AppConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Standalone {@code DEL} over a real socket: it removes every key it names and counts
 * only the ones that existed, whatever kind they hold. A delete that removed nothing
 * changes nothing, so nothing is written to the append only file; one that removed
 * something is recorded as a single {@code DEL} frame and replays as such. A multi-key
 * {@code DEL} arriving on the replication stream removes every key it names, not just
 * the first.
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class DelServerTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;

    @Autowired
    private MasterTcpServer masterTcpServer;
    @Autowired
    private SlaveTcpServer slaveTcpServer;
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
        Thread server = new Thread(masterTcpServer::startServer, "del-master");
        server.setDaemon(true);
        server.start();
        awaitPortOpen(masterPort);
    }

    @Test
    void anExistingKeyIsRemovedAndCounted() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "del:one", "v")).readReply();
            assertEquals(1L, client.send(frame("DEL", "del:one")).readReply());
            assertEquals(null, client.send(frame("GET", "del:one")).readReply());
        }
        assertNull(store.peekValue("del:one"), "the deleted key was still in the store");
    }

    @Test
    void aMissingKeyIsSkippedAndNotCounted() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            assertEquals(0L, client.send(frame("DEL", "del:never:there")).readReply());
            assertEquals(null, client.send(frame("GET", "del:never:there")).readReply());
        }
    }

    @Test
    void severalKeysAreCountedAndEveryKindIsRemoved() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "del:str", "v")).readReply();
            client.send(frame("HSET", "del:hash", "f", "v")).readReply();
            client.send(frame("RPUSH", "del:list", "a")).readReply();
            client.send(frame("SADD", "del:set", "m")).readReply();

            assertEquals(4L, client.send(frame("DEL", "del:str", "del:hash", "del:list", "del:set")).readReply());
        }
        assertNull(store.peekValue("del:str"));
        assertNull(store.peekValue("del:hash"));
        assertNull(store.peekValue("del:list"));
        assertNull(store.peekValue("del:set"));
    }

    @Test
    void aMixedDelCountsOnlyTheKeysItActuallyRemoved() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "del:mix:a", "1")).readReply();
            client.send(frame("SET", "del:mix:b", "2")).readReply();

            assertEquals(2L, client.send(frame("DEL", "del:mix:a", "del:mix:absent:1",
                    "del:mix:b", "del:mix:absent:2")).readReply());
        }
        assertNull(store.peekValue("del:mix:a"));
        assertNull(store.peekValue("del:mix:b"));
        assertNull(store.peekValue("del:mix:absent:1"));
        assertNull(store.peekValue("del:mix:absent:2"));
    }

    @Test
    void aDelNeedsAtLeastOneKey() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            assertEquals("ERR wrong number of arguments for 'del' command",
                    client.send(frame("DEL")).readReply());
            // an arity mistake is a reply, not a disconnect: the connection keeps working
            assertEquals("PONG", client.send(frame("PING")).readReply());
        }
    }

    @Test
    void aDelThatRemovedSomethingIsRecordedAsOneWholeFrameAndReplays() throws Exception {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "del:aof:a", "1")).readReply();
            client.send(frame("SET", "del:aof:b", "2")).readReply();
        }
        int before = frames().size();
        try (RespSocket client = new RespSocket(masterPort)) {
            assertEquals(2L, client.send(frame("DEL", "del:aof:a", "del:aof:b")).readReply());
        }
        List<String[]> written = framesSince(before);
        assertEquals(1, written.size(), "the delete was not recorded as one frame: " + written);
        assertEquals("DEL", written.get(0)[0]);
        assertEquals(List.of("del:aof:a", "del:aof:b"),
                List.of(written.get(0)[1], written.get(0)[2]),
                "the frame did not carry every key the delete removed");
        Store replayed = replayIntoNewStore();
        assertNull(replayed.peekValue("del:aof:a"), "the key came back after a restart");
        assertNull(replayed.peekValue("del:aof:b"), "the key came back after a restart");
    }

    @Test
    void aDelThatRemovedNothingIsNotRecorded() throws Exception {
        int before = frames().size();
        try (RespSocket client = new RespSocket(masterPort)) {
            assertEquals(0L, client.send(frame("DEL", "del:aof:missing:1", "del:aof:missing:2")).readReply());
        }
        List<String[]> written = framesSince(before);
        assertTrue(written.isEmpty(), "a delete that removed nothing was recorded: " + written);
    }

    @Test
    void aReadInTheMiddleOfWritesIsNotRecorded() throws Exception {
        int before = frames().size();
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "del:read:key", "v")).readReply();
            client.send(frame("GET", "del:read:key")).readReply();
            client.send(frame("DEL", "del:read:key")).readReply();
            List<String[]> written = framesSince(before);
            List<String> names = written.stream().map(frame -> frame[0]).toList();
            assertTrue(names.contains("DEL"), "the delete was not recorded: " + names);
            assertEquals(2, written.size(),
                    "the read in between was recorded: " + written);
        }
    }

    @Test
    void aMultiKeyDelOnTheStreamRemovesEveryKeyItNames() throws Exception {
        store.set("del:repl:a", "1");
        store.set("del:repl:b", "2");
        feed(frame("DEL", "del:repl:a", "del:repl:b"));
        assertNull(store.peekValue("del:repl:a"),
                "the replica kept the first key a multi-key delete named");
        assertNull(store.peekValue("del:repl:b"),
                "a multi-key delete dropped the keys after the first");
    }

    @Test
    void aTransactionalDelCountsOnlyTheKeysItActuallyRemoved() throws Exception {
        store.set("del:tx:a", "1");
        store.set("del:tx:b", "2");

        int before = frames().size();
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("MULTI"));
            assertEquals("OK", client.readReply());
            client.send(frame("DEL", "del:tx:a", "del:tx:absent", "del:tx:b"));
            assertEquals("QUEUED", client.readReply());
            client.send(frame("EXEC"));
            assertEquals(List.of(2L), client.readReply(),
                    "a transactional delete did not answer with the number of keys it removed");
        }
        assertNull(store.peekValue("del:tx:a"), "the first key a transactional delete named survived");
        assertNull(store.peekValue("del:tx:b"), "a key a transactional delete named survived");

        List<String[]> written = framesSince(before);
        assertEquals(3, written.size(),
                "the transactional delete was not written as one block: " + describe(written));
        assertArrayEquals(new String[]{"MULTI"}, written.get(0));
        assertArrayEquals(new String[]{"DEL", "del:tx:a", "del:tx:absent", "del:tx:b"}, written.get(1));
        assertArrayEquals(new String[]{"EXEC"}, written.get(2));

        Store replayed = replayIntoNewStore();
        assertNull(replayed.peekValue("del:tx:a"), "a key a transactional delete removed came back after a restart");
        assertNull(replayed.peekValue("del:tx:b"), "a key a transactional delete removed came back after a restart");
    }

    @Test
    void aTransactionalDelOfAKeyThatWasNotThereChangesNothing() throws Exception {
        store.map.remove("del:tx:missing");

        int before = frames().size();
        long streamMark = redisConfig.getMasterReplOffset();
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("MULTI"));
            assertEquals("OK", client.readReply());
            client.send(frame("DEL", "del:tx:missing"));
            assertEquals("QUEUED", client.readReply());
            client.send(frame("EXEC"));
            assertEquals(List.of(0L), client.readReply(),
                    "a transactional delete of a missing key was not answered with a count of zero");
        }
        assertTrue(framesSince(before).isEmpty(),
                "a transactional delete that removed nothing was recorded: " + describe(framesSince(before)));
        assertTrue(propagatedSince(streamMark).isEmpty(),
                "a transactional delete that removed nothing was passed to replicas: "
                        + describe(propagatedSince(streamMark)));
    }

    @Test
    void aTransactionPassesOnOnlyTheCommandsItApplied() throws Exception {
        store.map.remove("del:tx:prop");

        long streamMark = redisConfig.getMasterReplOffset();
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("MULTI"));
            assertEquals("OK", client.readReply());
            client.send(frame("SET", "del:tx:prop", "written"));
            assertEquals("QUEUED", client.readReply());
            client.send(frame("GET", "del:tx:prop"));
            assertEquals("QUEUED", client.readReply());
            client.send(frame("DEL", "del:tx:absent"));
            assertEquals("QUEUED", client.readReply());
            client.send(frame("INCR", "del:tx:prop"));
            assertEquals("QUEUED", client.readReply());
            client.send(frame("EXEC"));
            assertEquals(4, ((List<?>) client.readReply()).size(),
                    "the transaction did not answer with one reply per queued command");
        }

        List<String[]> propagated = propagatedSince(streamMark);
        assertEquals(1, propagated.size(),
                "a read, a refused or a no-op command was passed to replicas: " + describe(propagated));
        assertArrayEquals(new String[]{"SET", "del:tx:prop", "written"}, propagated.get(0));
    }

    // ----------------------------------------------------------------- harness

    /** Runs the upstream stream the way the master's socket delivers it. */
    private void feed(byte[]... reads) throws IOException {
        Client master = new Client(new Socket(), new ChunkedInputStream(List.of(reads)),
                new ByteArrayOutputStream(), -1);
        slaveTcpServer.streamFromMaster(master);
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

    private static String describe(List<String[]> frames) {
        StringBuilder sb = new StringBuilder("[");
        for (String[] frame : frames) {
            if (sb.length() > 1) {
                sb.append(", ");
            }
            sb.append(String.join(" ", frame));
        }
        return sb.append("]").toString();
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

    /** Hands out one scripted chunk per read, which is what a socket does. */
    private static final class ChunkedInputStream extends InputStream {
        private final List<byte[]> chunks;
        private int next;
        private int position;

        ChunkedInputStream(List<byte[]> chunks) {
            this.chunks = chunks;
        }

        private byte[] current() {
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
        public int read() {
            byte[] chunk = current();
            return chunk == null ? -1 : chunk[position++] & 0xFF;
        }

        @Override
        public int read(byte[] destination, int offset, int length) {
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
