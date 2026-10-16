package com.redisforge.benchmark;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A plain-JDK load harness to run against a RedisForge server that is already up.
 * It is not a test: the name does not match surefire's naming, and it is kept out of
 * the test suite on purpose. It measures, over real sockets, what a client actually
 * experiences: sequential round trips, pipelined round trips, typed reads, large
 * values and concurrent connections.
 *
 * <p>Invocation from the repository root, once {@code mvn test} (or
 * {@code mvn test-compile}) has built {@code target/test-classes}:</p>
 *
 * <pre>{@code
 * java -cp target/test-classes Components.Benchmark.BenchmarkMain [host] [port]
 * }</pre>
 *
 * <p>The numbers are for the machine and the dataset they ran on; a busy machine, or a
 * server sharing its port with other traffic, reads lower. The workload is fixed so the
 * run is reproducible rather than absolute: same commands, same counts, one connection
 * for the single-threaded runs.</p>
 */
public final class BenchmarkMain {

    private static final int SEQUENTIAL_OPS = 50_000;
    private static final int PIPE_BATCHES = 10;
    private static final int PIPE_BATCH_SIZE = 5_120;
    private static final int TYPED_CELLS = 20_000;
    private static final int LARGE_VALUE_BYTES = 1_048_576;
    private static final int LARGE_VALUE_OPS = 100;
    private static final int CONCURRENT_CLIENTS = 8;
    private static final int CONCURRENT_OPS_PER_CLIENT = 2_000;

    private BenchmarkMain() {
    }

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 6379;
        System.out.println("redisforge benchmark  host=" + host + " port=" + port);

        try (Conn warm = new Conn(host, port)) {
            for (int i = 0; i < 1_000; i++) {
                warm.send("PING");
                warm.readReply();
            }
        }

        try (Conn conn = new Conn(host, port)) {
            sequential(conn, "SET", "set:%d", SEQUENTIAL_OPS);
            sequential(conn, "GET", "set:%d", SEQUENTIAL_OPS);
            pipelined(conn, "SET", "pipe:%d", PIPE_BATCHES, PIPE_BATCH_SIZE);
            pipelined(conn, "GET", "pipe:%d", PIPE_BATCHES, PIPE_BATCH_SIZE);

            hashCells(conn, "HSET (20,000 field cells)", "hsh");
            listRead(conn, "LRANGE (200-element list)", "lst");
            setRead(conn, "SMEMBERS (200-member set)", "sss");

            byte[] large = new byte[LARGE_VALUE_BYTES];
            java.util.Arrays.fill(large, (byte) 'x');
            String payload = new String(large, StandardCharsets.ISO_8859_1);
            always(conn, "SET 1MiB value", LARGE_VALUE_OPS, () -> {
                try {
                    conn.send("SET", "large", payload);
                    conn.readReply();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            always(conn, "GET 1MiB value", LARGE_VALUE_OPS, () -> {
                try {
                    conn.send("GET", "large");
                    conn.readReply();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            concurrent(host, port, CONCURRENT_CLIENTS, CONCURRENT_OPS_PER_CLIENT);
        }
    }

    private static void sequential(Conn conn, String cmd, String keyPattern, int ops) {
        always(conn, cmd + " (sequential)", ops, () -> {
            try {
                for (int i = 0; i < ops; i++) {
                    if (cmd.equals("SET")) {
                        conn.send("SET", String.format(keyPattern, i), "value-" + i);
                    } else {
                        conn.send("GET", String.format(keyPattern, i));
                    }
                    conn.readReply();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void pipelined(Conn conn, String cmd, String keyPattern, int batches, int batchSize) {
        always(conn, cmd + " (pipelined x" + batchSize + ")", batches * batchSize, () -> {
            try {
                for (int b = 0; b < batches; b++) {
                    conn.beginPipeline();
                    for (int i = 0; i < batchSize; i++) {
                        int key = b * 100_000 + i;
                        if (cmd.equals("SET")) {
                            conn.queue("SET", String.format(keyPattern, key), "value-" + key);
                        } else {
                            conn.queue("GET", String.format(keyPattern, key));
                        }
                    }
                    conn.flushPipeline();
                    conn.readReplies(batchSize);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void hashCells(Conn conn, String label, String key) {
        always(conn, label, TYPED_CELLS, () -> {
            try {
                for (int i = 0; i < TYPED_CELLS; i++) {
                    conn.send("HSET", key, "field-" + i, "value-" + i);
                    conn.readReply();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void listRead(Conn conn, String label, String key) {
        try {
            conn.send("DEL", key);
            conn.readReply();
            for (int i = 0; i < 200; i++) {
                conn.send("RPUSH", key, "e-" + i);
                conn.readReply();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        always(conn, label, 5_000, () -> {
            try {
                for (int i = 0; i < 5_000; i++) {
                    conn.send("LRANGE", key, "0", "-1");
                    conn.readReply();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void setRead(Conn conn, String label, String key) {
        try {
            conn.send("DEL", key);
            conn.readReply();
            for (int i = 0; i < 200; i++) {
                conn.send("SADD", key, "m-" + i);
                conn.readReply();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        always(conn, label, 5_000, () -> {
            try {
                for (int i = 0; i < 5_000; i++) {
                    conn.send("SMEMBERS", key);
                    conn.readReply();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void concurrent(String host, int port, int clients, int opsPerClient) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        try {
            List<Future<?>> futures = new ArrayList<>();
            long start = System.nanoTime();
            for (int c = 0; c < clients; c++) {
                final int index = c;
                futures.add(pool.submit(() -> {
                    try (Conn client = new Conn(host, port)) {
                        for (int i = 0; i < opsPerClient; i++) {
                            client.send("SET", "conc:" + index + ":" + i, "v");
                            client.readReply();
                            client.send("GET", "conc:" + index + ":" + i);
                            client.readReply();
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
            long done = (long) clients * opsPerClient * 2;
            report("concurrent x" + clients, done, System.nanoTime() - start);
        } finally {
            pool.shutdownNow();
        }
    }

    private static void always(Conn conn, String label, int ops, Runnable body) {
        long start = System.nanoTime();
        try {
            body.run();
        } catch (RuntimeException e) {
            throw new RuntimeException(label + " failed: " + e, e);
        }
        report(label, ops, System.nanoTime() - start);
    }

    private static void report(String label, long ops, long nanos) {
        double perSecond = ops * 1_000_000_000.0 / nanos;
        System.out.printf("%-28s %8d ops  %12.1f ops/s  %8.2f us/op%n",
                label, ops, perSecond, 1_000_000.0 / perSecond);
    }

    /** A raw RESP client that writes whole commands and decodes whole replies away. */
    private static final class Conn implements AutoCloseable {
        private final Socket socket;
        private final byte[] pipeline = new byte[PIPE_BATCH_SIZE * 64];
        private int pipelineFilled = 0;
        private int queued = 0;

        Conn(String host, int port) throws IOException {
            socket = new Socket(host, port);
            socket.setSoTimeout(30_000);
            socket.setTcpNoDelay(true);
        }

        void send(String... parts) throws IOException {
            OutputStream out = socket.getOutputStream();
            out.write(frame(parts));
            out.flush();
        }

        void beginPipeline() {
            pipelineFilled = 0;
            queued = 0;
        }

        void queue(String... parts) throws IOException {
            byte[] frame = frame(parts);
            if (pipelineFilled + frame.length > pipeline.length) {
                throw new IOException("pipeline frame too large: " + frame.length);
            }
            System.arraycopy(frame, 0, pipeline, pipelineFilled, frame.length);
            pipelineFilled += frame.length;
            queued++;
        }

        void flushPipeline() throws IOException {
            OutputStream out = socket.getOutputStream();
            out.write(pipeline, 0, pipelineFilled);
            out.flush();
            pipelineFilled = 0;
            queued = 0;
        }

        /** A whole <code>*count\r\n</code> array of bulk strings as a byte array. */
        private static byte[] frame(String[] parts) throws IOException {
            int total = Integer.toString(parts.length).length() + 3;
            for (String part : parts) {
                int bytes = part == null ? 0 : part.getBytes(StandardCharsets.UTF_8).length;
                total += Integer.toString(bytes).length() + 5 + bytes;
            }
            byte[] buffer = new byte[total];
            int at = 0;
            buffer[at++] = '*';
            at += writeAscii(buffer, at, parts.length);
            buffer[at++] = '\r';
            buffer[at++] = '\n';
            for (String part : parts) {
                byte[] bytes = part == null ? new byte[0] : part.getBytes(StandardCharsets.UTF_8);
                buffer[at++] = '$';
                at += writeAscii(buffer, at, bytes.length);
                buffer[at++] = '\r';
                buffer[at++] = '\n';
                System.arraycopy(bytes, 0, buffer, at, bytes.length);
                at += bytes.length;
                buffer[at++] = '\r';
                buffer[at++] = '\n';
            }
            return buffer;
        }

        private static int writeAscii(byte[] buffer, int at, int value) {
            String text = Integer.toString(value);
            for (int i = 0; i < text.length(); i++) {
                buffer[at + i] = (byte) text.charAt(i);
            }
            return text.length();
        }

        void readReplies(int count) throws IOException {
            for (int i = 0; i < count; i++) {
                readReply();
            }
        }

        void readReply() throws IOException {
            InputStream in = socket.getInputStream();
            int type = in.read();
            if (type == -1) {
                throw new EOFException("connection closed");
            }
            String line = readLine(in);
            switch (type) {
                case '+', '-', ':' -> {
                }
                case '$' -> {
                    int length = Integer.parseInt(line);
                    if (length >= 0) {
                        in.readNBytes(length);
                        expectCrlf(in);
                    }
                }
                case '*' -> {
                    int count = Integer.parseInt(line);
                    for (int i = 0; i < count; i++) {
                        readReply();
                    }
                }
                default -> throw new IOException("not RESP: " + (char) type);
            }
        }

        private static String readLine(InputStream in) throws IOException {
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

        private static void expectCrlf(InputStream in) throws IOException {
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