package com.redisforge.network;

import com.redisforge.protocol.ResponseDto;
import com.redisforge.replication.Slave;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class Client {
    public Socket socket;
    public InputStream inputStream;
    public OutputStream outputStream;
    public int id;

    private boolean transactionalContext;
    public Queue<String[]> commandQueue;
    public List<String> transactionResponse; // the strings will be in resp format we just need to make a resp array out of it
    /**
     * The frame each queued command must be written down and passed on as, when that
     * differs from the command itself. Only SPOP needs it: it pops members at random, so
     * the transaction records the SREM of the members it actually popped, keyed by the
     * exact command array so the order of the queue is preserved.
     */
    public Map<String[], String[]> transactionPropagation = new IdentityHashMap<>();
    /**
     * The queued commands that were applied but changed nothing, so they must not be
     * written to the append-only file or passed to replicas. A refused command is already
     * recognisable from its error reply; a no-op command - a {@code DEL} of a key that was
     * not there, an expiration on a missing key, a typed write that left its value as it
     * was - answers a normal reply and is recorded here instead, keyed by the exact command
     * array so the order of the queue is preserved.
     */
    public Set<String[]> transactionNoOps = Collections.newSetFromMap(new IdentityHashMap<>());

    /**
     * The replica this connection registered as, if it did. Set once when the connection
     * gives its listening port, and read by whichever thread is propagating at that moment,
     * hence volatile.
     *
     * <p>Once set, every frame for this connection is handed to the replica's own sender
     * thread instead of being written here. A master applying or propagating a write must
     * never block on a replica that has stopped reading, and a thread that writes under the
     * backlog's lock must never touch the socket at all: queueing is what lets both hold.</p>
     */
    private volatile Slave replica;


    public boolean getTransactionalContext(){return transactionalContext;}

    public boolean beginTransaction(){
        if(transactionalContext)
            return false;
        transactionalContext = true;
        transactionResponse = new ArrayList<>();
        commandQueue = new LinkedList<>();
        transactionPropagation = new IdentityHashMap<>();
        transactionNoOps = Collections.newSetFromMap(new IdentityHashMap<>());
        return transactionalContext;
    }

    public void endTransaction(){
        commandQueue = null;
        transactionPropagation = new IdentityHashMap<>();
        transactionNoOps = Collections.newSetFromMap(new IdentityHashMap<>());
        transactionalContext=false;
    }

    public Client(Socket socket,
                  InputStream inputStream,
                  OutputStream outputStream,
                  int id){
        this.socket = socket;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.id = id;
    }

    /** Records the replica this connection registered as, so its writes go through it. */
    public void attachReplica(Slave replica){
        this.replica = replica;
    }

    /** The replica this connection registered as, or null when it never registered. */
    public Slave replica(){
        return replica;
    }

    // A replica connection is written to by whichever client thread is propagating at that
    // moment, so the frames have to be serialized per connection: two threads sharing an
    // output stream can otherwise interleave halves of a frame. Once the connection has
    // registered as a replica there is only ever one writer - its own sender thread - so the
    // frame is queued instead, and the text and the payload travel as one frame so a large
    // snapshot is never split across two queue entries.
    public synchronized void send(String res, byte[] data) throws IOException {
        deliver(frame(res, data));
    }

    public synchronized void send(ResponseDto res) throws IOException {
        deliver(frame(res.response, res.data));
    }

    public synchronized void send(byte[] data) throws IOException {
        deliver(frame(null, data));
    }

    public synchronized void send(String data) throws IOException {
        deliver(frame(data, null));
    }

    /**
     * Writes one whole frame, either onto the connection's output stream or - once the
     * connection is a replica - into its bounded outbound buffer for its sender thread.
     *
     * <p>Queueing throws when the replica has fallen further behind than its buffer allows,
     * which is the same I/O failure every caller already treats as a dropped replica.</p>
     */
    private void deliver(byte[] frame) throws IOException {
        if(frame == null || frame.length == 0)
            return;
        Slave replica = this.replica;
        if(replica != null){
            replica.send(frame);
            return;
        }
        outputStream.write(frame);
    }

    /** Joins the text of a reply and its payload into the one frame that travels it. */
    private static byte[] frame(String res, byte[] data){
        byte[] head = (res == null || res.isEmpty()) ? null : res.getBytes();
        if(head == null)
            return data;
        if(data == null || data.length == 0)
            return head;
        byte[] frame = new byte[head.length + data.length];
        System.arraycopy(head, 0, frame, 0, head.length);
        System.arraycopy(data, 0, frame, head.length, data.length);
        return frame;
    }

    public void close() {
        if(transactionalContext){
            endTransaction();
        }
        closeQuietly(inputStream);
        closeQuietly(outputStream);
        closeQuietly(socket);
    }

    private static void closeQuietly(AutoCloseable closeable){
        try {
            closeable.close();
        } catch (Exception e) {
            // the connection is being torn down, there is nothing left to recover
        }
    }
}
