package Components.Server;

import Components.Infra.Client;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The listening socket, admission gate and handler pool one server runs its clients on.
 *
 * <p>Every accepted connection is admitted through a gate of at most
 * {@code maxClients} permits and served on this server's own bounded pool of threads,
 * so a flood of idle or slow clients can never exhaust the JVM's common pool, and the
 * server never grows one thread per connection without a limit. A connection beyond the
 * gate is refused with an error straight away rather than queued for a thread that may
 * never come.</p>
 *
 * <p>Each start call binds its own listener and builds its own pool, and stop closes
 * every one of them, so a server that is stopped and started again starts from fresh
 * resources instead of a pool whose threads belong to an old run.</p>
 */
final class ClientListener {
    private static final Logger logger = Logger.getLogger(ClientListener.class.getName());
    /** keeps thread names unique when one server holds more than one listener at once */
    private static final AtomicInteger threadSequence = new AtomicInteger();
    private static final String MAX_CLIENTS_REPLY = "-ERR max number of clients reached\r\n";
    /** how long a stop waits for in-flight handlers to unwind before it gives up */
    private static final long SHUTDOWN_GRACE_MS = 2_000;

    /** One connection's whole handling loop, from the first byte to the cleanup. */
    @FunctionalInterface
    interface ClientTask {
        void handle(Client client) throws IOException;
    }

    private final String threadPrefix;
    private final ClientTask task;
    private final Object lifecycleLock = new Object();
    /** every listener this server has bound and not yet ended; guarded by lifecycleLock */
    private final List<Cycle> cycles = new ArrayList<>();
    private volatile boolean running;

    ClientListener(String threadPrefix, ClientTask task) {
        this.threadPrefix = threadPrefix;
        this.task = task;
    }

    /**
     * Binds the port and serves clients until the server is stopped. Returns only once
     * the listener has ended.
     *
     * @param onListening run right after the socket is bound, before the first accept,
     *        which is where a replica starts the thread that follows its master
     */
    void start(int port, int maxClients, int idleTimeoutMs, Runnable onListening) {
        Cycle cycle;
        synchronized (lifecycleLock) {
            try {
                // reuse must be set before the bind to have any effect, which is what lets
                // a server rebind its port right after a stop closed live connections
                ServerSocket listener = new ServerSocket();
                listener.setReuseAddress(true);
                listener.bind(new java.net.InetSocketAddress(port));
                cycle = new Cycle(listener, maxClients, idleTimeoutMs);
            } catch (IOException e) {
                logger.log(Level.SEVERE, "could not listen on port " + port + ": " + e.getMessage());
                return;
            }
            running = true;
            cycles.add(cycle);
        }
        onListening.run();
        try {
            acceptLoop(cycle);
        } finally {
            endCycle(cycle);
        }
    }

    /**
     * Stops every listener this server holds: no new connections are admitted, the
     * sockets are closed so blocked reads end, and the handler pool is shut down.
     * Idempotent, and safe to call when nothing was ever started.
     */
    void stop() {
        List<Cycle> stopped;
        synchronized (lifecycleLock) {
            running = false;
            stopped = new ArrayList<>(cycles);
            cycles.clear();
        }
        for (Cycle cycle : stopped) {
            closeQuietly(cycle.listener);
            cycle.executor.shutdownNow();
        }
        for (Cycle cycle : stopped) {
            try {
                if (!cycle.executor.awaitTermination(SHUTDOWN_GRACE_MS, TimeUnit.MILLISECONDS)) {
                    logger.log(Level.WARNING, "client handlers did not stop within "
                            + SHUTDOWN_GRACE_MS + "ms");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    boolean isRunning() {
        return running;
    }

    /** The handlers currently running, which is what shows a stop has finished. */
    int activeClientHandlers() {
        synchronized (lifecycleLock) {
            int active = 0;
            for (Cycle cycle : cycles) {
                active += cycle.executor.getActiveCount();
            }
            return active;
        }
    }

    private void acceptLoop(Cycle cycle) {
        int id = 0;
        try {
            while (running) {
                Socket socket = cycle.listener.accept();
                id++;
                admit(cycle, socket, id);
            }
        } catch (IOException e) {
            // a stop closes the listener to end this loop, and that is not a failure
            if (running) {
                logger.log(Level.SEVERE, "the accept loop ended: " + e.getMessage());
            }
        }
    }

    /** Takes a connection in, or turns it away when the server is already full. */
    private void admit(Cycle cycle, Socket socket, int id) {
        if (!cycle.admission.tryAcquire()) {
            reject(socket);
            return;
        }
        boolean submitted = false;
        try {
            if (cycle.idleTimeoutMs > 0) {
                // a connection that sends nothing is reclaimed by this read limit; a
                // registered replica clears it again when it introduces itself
                socket.setSoTimeout(cycle.idleTimeoutMs);
            }
            Client client = new Client(socket, socket.getInputStream(), socket.getOutputStream(), id);
            cycle.executor.execute(() -> {
                try {
                    serve(client, cycle);
                } finally {
                    cycle.admission.release();
                }
            });
            submitted = true;
        } catch (IOException | RejectedExecutionException refused) {
            logger.log(Level.WARNING, "could not start a handler for client #" + id
                    + ": " + refused.getMessage());
        } finally {
            if (!submitted) {
                cycle.admission.release();
                closeQuietly(socket);
            }
        }
    }

    private void serve(Client client, Cycle cycle) {
        try {
            task.handle(client);
        } catch (SocketTimeoutException idle) {
            logger.log(Level.INFO, "reclaimed the connection to client #" + client.id
                    + " after " + cycle.idleTimeoutMs + "ms of silence");
        } catch (IOException failure) {
            String message = String.valueOf(failure.getMessage());
            if (wentAwayQuietly(message)) {
                logger.log(Level.FINE, "client #" + client.id + " went away: " + message);
            } else {
                logger.log(Level.WARNING, "client #" + client.id
                        + " connection failed: " + message);
            }
        } catch (RuntimeException unexpected) {
            // a failure in one connection's handling must be visible and must stop
            // nowhere near the server itself
            logger.log(Level.SEVERE, "client #" + client.id + " failed unexpectedly", unexpected);
        }
    }

    /** Whether the failure is just a peer that closed its side in a hurry. */
    private static boolean wentAwayQuietly(String message) {
        String lower = message.toLowerCase(Locale.ROOT);
        return lower.contains("connection reset")
                || lower.contains("broken pipe")
                || lower.contains("socket closed")
                || lower.contains("stream closed");
    }

    private void reject(Socket socket) {
        try {
            OutputStream out = socket.getOutputStream();
            out.write(MAX_CLIENTS_REPLY.getBytes(StandardCharsets.UTF_8));
            out.flush();
        } catch (IOException e) {
            logger.log(Level.FINE, "a refused client went away before it could be told: "
                    + e.getMessage());
        } finally {
            closeQuietly(socket);
        }
    }

    private void endCycle(Cycle cycle) {
        synchronized (lifecycleLock) {
            cycles.remove(cycle);
        }
        closeQuietly(cycle.listener);
    }

    private static void closeQuietly(Closeable socket) {
        if (socket == null) {
            return;
        }
        try {
            socket.close();
        } catch (IOException e) {
            logger.log(Level.FINE, e.getMessage());
        }
    }

    /** One bound listener together with the gate and pool that belong to it. */
    private final class Cycle {
        final ServerSocket listener;
        final Semaphore admission;
        final ThreadPoolExecutor executor;
        final int idleTimeoutMs;

        Cycle(ServerSocket listener, int maxClients, int idleTimeoutMs) {
            this.listener = listener;
            this.idleTimeoutMs = idleTimeoutMs;
            this.admission = new Semaphore(maxClients);
            // one bounded pool per server: at most maxClients handler threads, named
            // after the server, dying off again after a minute of idleness
            this.executor = new ThreadPoolExecutor(
                    maxClients, maxClients,
                    60L, TimeUnit.SECONDS,
                    new SynchronousQueue<>(),
                    runnable -> {
                        Thread thread = new Thread(runnable,
                                threadPrefix + "-client-" + threadSequence.incrementAndGet());
                        thread.setDaemon(true);
                        return thread;
                    });
            this.executor.allowCoreThreadTimeOut(true);
        }
    }
}
