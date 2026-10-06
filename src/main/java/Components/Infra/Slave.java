package Components.Infra;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Slave {
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

    public Slave(Client client, int listeningPort){
        this.connection = client;
        this.listeningPort = listeningPort;
        this.capabilities = new ArrayList<>();
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

    public void send(byte[] bytes) throws IOException {
        if(bytes!=null){
            this.connection.outputStream.write(bytes);
        }
    }
}
