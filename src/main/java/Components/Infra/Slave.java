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
    /**
     * Whether this replica's command stream has started, which is only true once its PSYNC
     * has been answered. It is registered earlier than that, when it gives its listening
     * port, and a write sent in between would arrive in the middle of its handshake.
     * Read and written from different threads, hence volatile.
     */
    private volatile boolean ready;

    public Slave(Client client){
        this.connection = client;
        this.capabilities = new ArrayList<>();
    }

    public boolean isReady(){
        return ready;
    }

    public void markReady(){
        ready = true;
    }

    public void send(byte[] bytes) throws IOException {
        if(bytes!=null){
            this.connection.outputStream.write(bytes);
        }
    }
}
