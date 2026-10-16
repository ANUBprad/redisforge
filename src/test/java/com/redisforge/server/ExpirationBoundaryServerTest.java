package com.redisforge.server;

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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The boundary of the expiry counters, over a real socket. Redis answers TTL and PTTL as
 * signed 64-bit numbers, so a key with weeks or years still on the clock has to come back
 * as a large positive number rather than the negative an overflow would make of it, and a
 * deadline thrown far enough out to stop fitting a date has to keep the key instead of
 * killing the connection that set it.
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ExpirationBoundaryServerTest {

    private static final long AWAIT_TIMEOUT_MS = 10_000;

    @Autowired
    private MasterTcpServer masterTcpServer;
    @Autowired
    private RedisConfig redisConfig;
    @Autowired
    private com.redisforge.persistence.AppendOnlyPersistence appendOnly;

    private int masterPort;

    @BeforeAll
    void startMaster(@TempDir Path dir) throws Exception {
        masterPort = freePort();
        redisConfig.setRole("master");
        redisConfig.setPort(masterPort);
        redisConfig.setAppendonly(true);
        redisConfig.setAppendfilename(dir.resolve("appendonly.aof").toString());
        redisConfig.setAppendfsync("no");
        appendOnly.start();
        Thread server = new Thread(masterTcpServer::startServer, "expiry-boundary-master");
        server.setDaemon(true);
        server.start();
        awaitPortOpen(masterPort);
    }

    @Test
    void pttlAndTtlAnswerAcrossTheirWholeRange() throws IOException {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "bound:weeks", "v")).readReply();
            assertEquals(1L, client.send(frame("PEXPIRE", "bound:weeks", "3000000000")).readReply());
            // about 34 days out: the milliseconds no longer fit an int, the seconds do
            long pttl = (Long) client.send(frame("PTTL", "bound:weeks")).readReply();
            long ttl = (Long) client.send(frame("TTL", "bound:weeks")).readReply();
            assertTrue(pttl > 2_900_000_000L, "PTTL came back as: " + pttl);
            assertTrue(ttl > 2_900_000L, "TTL came back as: " + ttl);
            assertEquals("v", client.send(frame("GET", "bound:weeks")).readReply());
        }
    }

    @Test
    void pexpireWithTheLargestAllowedDelayKeepsTheKeyAlive() throws IOException {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "bound:max", "v")).readReply();
            assertEquals(1L, client.send(frame("PEXPIRE", "bound:max", "9223372036854775807")).readReply());
            // a span of essentially forever still reads as a very long positive time left
            long pttl = (Long) client.send(frame("PTTL", "bound:max")).readReply();
            assertTrue(pttl > 1_000_000_000_000L, "PTTL came back as: " + pttl);
            assertTrue((Long) client.send(frame("TTL", "bound:max")).readReply() > 1_000_000_000L);
            assertEquals("v", client.send(frame("GET", "bound:max")).readReply());
        }
    }

    @Test
    void pexpireatThatCrashesNoOneStillAnswers() throws IOException {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "bound:at", "v")).readReply();
            assertEquals(1L, client.send(frame("PEXPIREAT", "bound:at", "9223372036854775807")).readReply());
            assertTrue((Long) client.send(frame("PTTL", "bound:at")).readReply() > 0,
                    "a key with a far deadline read as expired");
            assertEquals("v", client.send(frame("GET", "bound:at")).readReply());
        }
    }

    @Test
    void aFarDeadlineInsideATransactionCommitsInsteadOfKillingTheExec() throws IOException {
        try (RespSocket client = new RespSocket(masterPort)) {
            client.send(frame("SET", "bound:tx", "v")).readReply();
            client.send(frame("MULTI")).readReply();
            assertEquals("QUEUED",
                    client.send(frame("EXPIRE", "bound:tx", "9223372036854775807")).readReply());
            assertEquals(List.of(1L), client.send(frame("EXEC")).readReply(),
                    "an overflow deadline stopped the transaction instead of committing");
            try {
                // beyond the range of a date the deadline is kept as "no deadline", which
                // is what a date that far in the future means; the key stays and reads as
                // never expiring
                assertEquals(-1L, client.send(frame("TTL", "bound:tx")).readReply());
            } catch (EOFException closed) {
                fail("the connection died answering TTL after a far EXEC: " + closed);
            }
            assertEquals("v", client.send(frame("GET", "bound:tx")).readReply());
            assertEquals("PONG", client.send(frame("PING")).readReply(),
                    "the connection stayed usable after the far EXEC");
        }
    }

    @Test
    void aLargeValueRoundTripsEvenWhenItArrivesInTinyPieces() throws IOException {
        final int size = 2_000_000;
        String value = "x".repeat(size);
        try (RespSocket client = new RespSocket(masterPort)) {
            byte[] command = frame("SET", "bound:large", value);
            for (int offset = 0; offset < command.length; offset += 1024) {
                int end = Math.min(command.length, offset + 1024);
                client.out.write(command, offset, end - offset);
                client.out.flush();
            }
            assertEquals("OK", client.readReply(),
                    "a large SET did not answer +OK");
            String echoed = (String) client.send(frame("GET", "bound:large")).readReply();
            assertEquals(size, echoed.length(), "the value did not come back whole");
            assertEquals(value, echoed);
        }
    }

    private static byte[] frame(String... parts) throws IOException {
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