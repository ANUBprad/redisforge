package Components.Infra;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class Slave {
    /**
     * How many bytes of the write stream this replica may fall behind by before it is let
     * go of.
     *
     * <p>A replica's frames are written by a sender thread of its own, so no thread that
     * applies or propagates a write ever waits on the socket. What a replica has not read
     * yet is held here, and it is bounded: once more than this many bytes are waiting to be
     * written, the replica is disconnected and recovers through the ordinary PSYNC / full
     * resync path instead of the master buffering an unbounded amount of the stream on its
     * behalf. A single frame is always admitted even when it is larger than this, so the
     * largest mutation a master can make is never dropped for being large.</p>
     */
    public static final long DEFAULT_OUTBOUND_BUFFER_BYTES = 2L * 1024 * 1024;

    public Client connection;
    public List<String> capabilities;
    /** the port the replica listens on, which is how it is told apart from another one */
    public int listeningPort;
    /**
     * Whether this replica's command stream has started, which is only true once its PSYNC
     * has been answered. It is registered earlier than that, when it gives its listening
     * port, and a write sent in between would arrive in the middle of its handshake.
     * Read and written from different threads, hence volatile.
     */
    private volatile boolean ready;
    /**
     * Whether this replica has acknowledged since the last WAIT reported. Read and written
     * from different threads, hence volatile.
     */
    private volatile boolean acknowledgedSinceLastWait;
    /** the frames waiting to be written, drained by this replica's own sender thread */
    private final Outbound outbound;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public Slave(Client client, int listeningPort){
        this(client, listeningPort, DEFAULT_OUTBOUND_BUFFER_BYTES);
    }

    public Slave(Client client, int listeningPort, long outboundBufferBytes){
        this.connection = client;
        this.listeningPort = listeningPort;
        this.capabilities = new ArrayList<>();
        this.outbound = new Outbound(outboundBufferBytes);
        startSender();
    }

    /**
     * One thread writes this replica's frames for as long as it is attached. It is the only
     * thread that touches the socket's output once the connection has registered, so the
     * frames arrive in the order they were queued and a thread applying a write never blocks
     * on this peer. The thread ends when the buffer is closed, which a disconnect does.
     */
    private void startSender(){
        Thread sender = new Thread(this::drainOutbound,
                "replica-outbound-" + listeningPort + "-" + System.identityHashCode(this));
        sender.setDaemon(true);
        sender.start();
    }

    private void drainOutbound(){
        try {
            byte[] frame;
            while ((frame = outbound.take()) != null) {
                connection.outputStream.write(frame);
            }
        } catch (IOException replicaGone) {
            // the replica stopped reading or its socket was closed underneath it. Closing
            // here lets the connection's own handler take the registration down in cleanup
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            close();
        }
    }

    public boolean isReady(){
        return ready;
    }

    public void markReady(){
        ready = true;
    }

    /**
     * Records this replica's acknowledgement for the current WAIT, and reports whether it
     * was the first one.
     *
     * <p>One replica acknowledging twice still counts once: WAIT asks how many replicas are
     * caught up, and one cannot stand in for two. The flag is cleared when a WAIT reports,
     * so the same replica counts again for the next one.</p>
     */
    public boolean markAcknowledged(){
        if(acknowledgedSinceLastWait){
            return false;
        }
        acknowledgedSinceLastWait = true;
        return true;
    }

    public void clearAcknowledgement(){
        acknowledgedSinceLastWait = false;
    }

    /**
     * Queues one frame for this replica, to be written by its sender thread.
     *
     * @return false when the replica has fallen further behind than its outbound buffer
     *         allows, which is the point at which it is dropped and left to resynchronise
     *         rather than silently missing writes
     */
    public boolean offer(byte[] bytes){
        if(bytes == null || bytes.length == 0){
            return true;
        }
        if(closed.get() || !outbound.offer(bytes)){
            close();
            return false;
        }
        return true;
    }

    /**
     * Queues one frame, throwing when the replica has fallen too far behind. Kept for the
     * callers that treat a replica that cannot be written to as an I/O failure to be
     * dropped, which is what propagation does.
     */
    public void send(byte[] bytes) throws IOException {
        if(!offer(bytes)){
            throw new IOException("the replica has fallen behind its outbound buffer and is being dropped");
        }
    }

    /** Stops the sender thread and closes the socket. Idempotent. */
    public void close(){
        if(!closed.compareAndSet(false, true)){
            return;
        }
        // waking the sender makes it leave, and closing underneath a write that is still in
        // flight makes that write fail, so either way the thread does not outlive the socket
        outbound.close();
        connection.close();
    }

    /**
     * A bounded, in-memory queue of whole frames for one replica.
     *
     * <p>It is entered by whichever thread is propagating and drained by the replica's
     * sender thread, so the queue is what decouples the socket from the stream. A frame is
     * always admitted onto an empty queue, so a single large mutation fits even when it is
     * larger than the bound; once frames are waiting, one more is refused as soon as it
     * would put the queue past the bound. Refusing is what drops the replica.</p>
     */
    private static final class Outbound {
        private final long maxBytes;
        private final Deque<byte[]> frames = new ArrayDeque<>();
        private long queuedBytes;
        private boolean closed;

        private Outbound(long maxBytes){
            this.maxBytes = maxBytes;
        }

        synchronized boolean offer(byte[] frame){
            if(closed){
                return false;
            }
            if(!frames.isEmpty() && queuedBytes + frame.length > maxBytes){
                return false;
            }
            frames.addLast(frame);
            queuedBytes += frame.length;
            notifyAll();
            return true;
        }

        synchronized byte[] take() throws InterruptedException {
            while(frames.isEmpty() && !closed){
                wait();
            }
            if(closed){
                return null;
            }
            byte[] frame = frames.pollFirst();
            queuedBytes -= frame.length;
            return frame;
        }

        synchronized void close(){
            closed = true;
            notifyAll();
        }
    }
}
