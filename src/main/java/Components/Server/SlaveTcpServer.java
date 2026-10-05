package Components.Server;

import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
import Components.Repository.Store;
import Components.Service.CommandHandler;
import Components.Service.RespSerializer;
import Components.Infra.Client;
import Components.Infra.RespStream;
import Components.Service.ResponseDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
public class SlaveTcpServer {
    private static final Logger logger = Logger.getLogger(SlaveTcpServer.class.getName());

    /** how long to wait before the first retry, and how far that wait may grow */
    private static final long FIRST_RETRY_DELAY_MS = 250;
    private static final long MAX_RETRY_DELAY_MS = 2_000;
    /** how long to wait for the master's socket to answer, so a dead host cannot hang us */
    private static final int CONNECT_TIMEOUT_MS = 2_000;

    @Autowired
    private RespSerializer respSerializer;
    @Autowired
    private CommandHandler commandHandler;
    @Autowired
    private RedisConfig redisConfig;
    @Autowired
    private ConnectionPool connectionPool;
    @Autowired
    private Store store;

    private volatile boolean running = true;
    /** the socket currently following the master, so shutting down can close it */
    private volatile Socket masterConnection;
    /** the retry waits on this, so stopping the server does not have to wait them out */
    private final Object retryLock = new Object();
    private volatile ServerSocket serverSocket;

    public void startServer(){
        Socket clientSocket = null;
        int port = redisConfig.getPort();

        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

            // one thread follows the master for as long as this server runs. It hands the
            // stream over when the master hangs up and looks for it again when there is
            // none, so a replica that starts too early, or loses its master, recovers by
            // itself. Exactly one thread and one socket at a time, which is what keeps a
            // reconnect from duplicating the stream or applying a write twice.
            Thread follower = new Thread(this::followMasterUntilStopped, "replication-follower");
            follower.setDaemon(true);
            follower.start();

            int id = 0;
            while (running) {
                clientSocket = serverSocket.accept();
                id++;
                Socket finalClientSocket = clientSocket;

                InputStream inputStream = clientSocket.getInputStream();
                OutputStream outputStream = clientSocket.getOutputStream();

                Client client = new Client(finalClientSocket, inputStream, outputStream, id );
                CompletableFuture.runAsync(() -> {
                    try {
                        handleClient(client);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

        } catch (IOException e) {
            if (running) {
                logger.log(Level.SEVERE, e.getMessage());
            }
        } finally {
            closeQuietly(clientSocket);
        }
    }

    /**
     * Stops the server: no further retries, and the sockets it is holding are closed so
     * neither the accept loop nor a read that is waiting on the master can hold the
     * process open.
     */
    public void stop() {
        running = false;
        synchronized (retryLock) {
            retryLock.notifyAll();
        }
        closeQuietly(masterConnection);
        closeQuietly(serverSocket);
    }

    /**
     * Keeps trying to follow the master until the server is stopped.
     *
     * <p>A master that is not there yet is waited for rather than given up on, and a master
     * that goes away is looked for again. The wait grows each time an attempt fails, so a
     * master that stays down is not asked for in a tight loop, and it starts over as soon
     * as an attempt gets far enough to be talking to a real master.</p>
     */
    private void followMasterUntilStopped() {
        long delay = FIRST_RETRY_DELAY_MS;
        while (running) {
            try {
                if (followMasterOnce()) {
                    // the handshake worked, so the next wait starts at the floor again
                    delay = FIRST_RETRY_DELAY_MS;
                }
            } catch (IOException e) {
                if (running) {
                    logger.log(Level.WARNING, "the master is not available ("
                            + e.getMessage() + "), looking for it again in " + delay + "ms");
                }
            }
            if (!running || !waitBeforeRetry(delay)) {
                return;
            }
            delay = Math.min(delay * 2, MAX_RETRY_DELAY_MS);
        }
    }

    /**
     * One attempt at attaching to the master: connect, handshake, then stay on the stream
     * until the master hangs up.
     *
     * @return true once the handshake completed, which is the point at which this replica is
     *         really following a master
     * @throws IOException if the master could not be reached or the handshake did not
     *         finish, in which case the next attempt starts from a new socket
     */
    private boolean followMasterOnce() throws IOException {
        Socket master = new Socket();
        try {
            master.connect(new InetSocketAddress(redisConfig.getMasterHost(), redisConfig.getMasterPort()),
                    CONNECT_TIMEOUT_MS);
            masterConnection = master;

            InputStream inputStream = master.getInputStream();
            OutputStream outputStream = master.getOutputStream();

            //part 1 of the handshake
            outputStream.write("*1\r\n$4\r\nPING\r\n".getBytes());
            logger.log(Level.FINE, readLine(inputStream));

            //part 2 of the handshake
            int lenListeningPort = (redisConfig.getPort()+"").length();
            int listeningPort = redisConfig.getPort();
            String replconf = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" +
                    (lenListeningPort+"") + "\r\n" + (listeningPort+"") +
                    "\r\n";
            outputStream.write(replconf.getBytes());
            logger.log(Level.FINE, readLine(inputStream));

            replconf = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
            outputStream.write(replconf.getBytes());
            logger.log(Level.FINE, readLine(inputStream));

            // part 3 of the handshake
            outputStream.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".getBytes());
            readFullResync(inputStream);

            streamFromMaster(new Client(master, inputStream, outputStream, -1));
            // the master hung up, which is not a failure: the loop looks for it again
            return true;
        } finally {
            masterConnection = null;
            closeQuietly(master);
        }
    }

    /**
     * Waits before the next attempt, unless the server is stopped first.
     *
     * @return true when it is time to try again
     */
    private boolean waitBeforeRetry(long delay) {
        synchronized (retryLock) {
            if (!running) {
                return false;
            }
            try {
                retryLock.wait(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            return running;
        }
    }

    private void closeQuietly(Socket socket) {
        if (socket == null) {
            return;
        }
        try {
            socket.close();
        } catch (IOException e) {
            logger.log(Level.FINE, e.getMessage());
        }
    }

    private void closeQuietly(ServerSocket socket) {
        if (socket == null) {
            return;
        }
        try {
            socket.close();
        } catch (IOException e) {
            logger.log(Level.FINE, e.getMessage());
        }
    }

    /**
     * Applies the commands the master streams down once the RDB payload is out of the way.
     * The upstream is framed exactly the way a client's is: only whole RESP arrays are
     * decoded, in arrival order, and a partly received array waits for the rest of it
     * instead of being read as if the socket had ended a message there.
     */
    void streamFromMaster(Client master) throws IOException {
        RespStream replicated = new RespStream(respSerializer);
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = master.inputStream.read(buffer)) != -1) {
            replicated.append(buffer, bytesRead);
            for (String[] command : replicated.drain()) {
                String response = handleCommandFromMaster(command, master);
                if(response != null && !response.isEmpty())
                    master.outputStream.write(response.getBytes());
                if(!isReplicationControlCommand(command[0])){
                    // the offset follows the write stream only, because that is what the
                    // sending side counted; a control frame is not part of it, so an ACK
                    // from here compares equal against the master's own count. It counts
                    // the bytes the command travels as, in the same encoding, so the two
                    // sides of a hop can never disagree about how far the other has got
                    redisConfig.setMasterReplOffset(redisConfig.getMasterReplOffset()
                            + replicatedBytes(command).length);
                }
            }
        }
    }

    /**
     * Consumes the answer to PSYNC: the "+FULLRESYNC" status line, the header of the RDB
     * bulk string behind it, and exactly as many payload bytes as that header declares.
     * This master sends no CRLF after the payload, so the first replicated command picks
     * up on the very next byte.
     */
    private void readFullResync(InputStream inputStream) throws IOException {
        String status = readLine(inputStream);
        if(status == null || !status.startsWith("+FULLRESYNC")){
            throw new IOException("expected +FULLRESYNC from the master but read: " + status);
        }
        logger.log(Level.FINE, status);

        String header = readLine(inputStream);
        if(header == null || !header.startsWith("$")){
            throw new IOException("expected the length of the RDB but read: " + header);
        }
        int rdbLength = Integer.parseInt(header.substring(1));

        // the RDB is raw bytes, so it is counted off rather than scanned for a delimiter
        byte[] rdb = inputStream.readNBytes(rdbLength);
        if(rdb.length != rdbLength){
            throw new EOFException("the RDB ended after " + rdb.length + " of " + rdbLength + " bytes");
        }
    }

    /** Reads one CRLF terminated line, or null once the master has hung up. */
    private String readLine(InputStream inputStream) throws IOException {
        StringBuilder line = new StringBuilder();
        int b;
        while ((b = inputStream.read()) != -1) {
            if(b == '\r'){
                if(inputStream.read() != '\n'){
                    throw new IOException("expected LF after CR");
                }
                return line.toString();
            }
            line.append((char) b);
        }
        return line.length() == 0 ? null : line.toString();
    }

    /** Replication plumbing: consumed on the way in, never applied and never counted. */
    private boolean isReplicationControlCommand(String command) {
        return command.equalsIgnoreCase("REPLCONF");
    }

    private String handleCommandFromMaster(String[] command, Client master) {
        System.out.println("================================= received command from master =================================");
        for(String c: command){
            System.out.print(c+" ");
        }
        String cmd = command[0];
        cmd = cmd.toUpperCase();

        String res = "";
        switch (cmd){
            case "SET":
                commandHandler.set(command);
                countAndPropagate(command);
                break;
            case "INCR":
                commandHandler.incr(command);
                countAndPropagate(command);
                break;
            case "DEL":
                applyDeleteFromMaster(command);
                break;
            case "REPLCONF":
                res = commandHandler.replconf(command, master);
                break;
        }
        return res;
    }

    /**
     * Applies a delete the master made, and only then passes it on.
     *
     * <p>A replica that kept the key would go on answering with a value its master has
     * already removed, and would pass that key down to its own replicas as well. The
     * delete is idempotent, so a delete of a key this replica never had leaves it in step
     * with a master that refused the same command.</p>
     *
     * <p>{@link Store#delete} takes the key's own lock for the whole removal, which is the
     * same indivisible delete the master made: a client reading this replica cannot see
     * the key half way through being gone.</p>
     */
    private void applyDeleteFromMaster(String[] command) {
        if (command.length < 2) {
            // a frame the master should not have sent. Ignored rather than allowed to
            // throw, because this runs on the stream every other write arrives on, and one
            // bad frame must not take the rest of the replication down with it
            logger.log(Level.WARNING, "ignored a DEL with no key: " + String.join(" ", command));
            return;
        }
        store.delete(command[1]);
        countAndPropagate(command);
    }

    /**
     * Counts the bytes this replica sent onwards and passes the write to the next hop, so
     * a chained replica sees every write exactly once and WAIT counts what actually went
     * over the wire.
     */
    private void countAndPropagate(String[] command) {
        connectionPool.bytesSentToSlaves.addAndGet(replicatedBytes(command).length);
        propagate(command);
    }

    /** The bytes a replicated command travels as, which is what the offset has to count. */
    private byte[] replicatedBytes(String[] command) {
        return respSerializer.respArray(command).getBytes(StandardCharsets.UTF_8);
    }

    private void propagate(String[] command) {
        // a copy, because a replica that cannot be written to is dropped on the way past
        byte[] propagated = replicatedBytes(command);
        for(Slave slave: new ArrayList<>(connectionPool.getSlaves())){
            if(!slave.isReady()){
                // registered by a downstream replica, but still handshaking: a write now
                // would be read by one that is waiting for its FULLRESYNC, so it waits too
                logger.log(Level.FINE, "not sending to a replica whose stream has not started yet");
                continue;
            }
            System.out.println("========================= sending command down to slave ==============================");
            System.out.println("command: "+new String(propagated, StandardCharsets.UTF_8));
            System.out.println(slave.connection.id);
            InetAddress remoteAddress = slave.connection.socket.getInetAddress();
            System.out.println("Remote IP address: " + remoteAddress.getHostAddress() +": "+slave.connection.socket.getPort());
            try {
                slave.send(propagated);
            } catch (IOException e) {
                // propagation runs on the upstream loop's thread, so a downstream replica
                // that stops reading must not be allowed to break that loop
                logger.log(Level.WARNING, "dropping a replica that could not be written to: " + e.getMessage());
                connectionPool.removeSlave(slave);
            }
        }
    }

    private void handleClient(Client client) throws IOException {
        connectionPool.addClient(client);
        RespStream respStream = new RespStream(respSerializer);
        try {
            byte[] buffer = new byte[client.socket.getReceiveBufferSize()];
            int bytesRead;
            // read() blocks until data arrives; it returns -1 once the peer has
            // disconnected, which is what ends this loop. isConnected() is not a
            // liveness check - it stays true forever after the peer goes away.
            while ((bytesRead = client.inputStream.read(buffer)) != -1) {
                // a read is not a command: respStream keeps a partial frame until
                // the rest of it arrives, and splits out every whole one
                respStream.append(buffer, bytesRead);
                for (String[] command : respStream.drain()) {
                    handleCommand(command, client);
                }
            }
        } finally {
            // runs for a normal disconnect and for an I/O failure alike, so the
            // registries never keep a dead connection around
            connectionPool.removeClient(client);
            connectionPool.removeSlave(client);
            client.close();
        }
    }

    private void handleCommand(String[] command, Client client) throws IOException {
        String res = "";
        byte[] data = null;
        switch (command[0]){
            case "PING":
                res = commandHandler.ping(command);
                break;
            case "ECHO":
                res = commandHandler.echo(command);
                break;
            case "SET":
                res = "-READONLY You can't write against a replica.\r\n";
                break;
            case "GET":
                res = commandHandler.get(command);
                break;
            case "INFO":
                res = commandHandler.info(command);
                break;
            case "PSYNC":
                ResponseDto resDto = commandHandler.psync(command, client);
                res = resDto.response;
                data = resDto.data;
                break;
            case "REPLCONF":
                // a replica runs the same handshake as a master, which is what lets it be
                // the upstream master of another replica
                res = commandHandler.replconf(command, client);
                break;
            case "WAIT":
                if(connectionPool.bytesSentToSlaves.get() == 0){
                    res = respSerializer.respInteger(connectionPool.slavesThatAreCaughtUp.get());
                    break;
                }

                Instant start = Instant.now();
                res = commandHandler.wait(command, start);
                connectionPool.slavesThatAreCaughtUp.set(0);
                break;
        }
        client.send(res, data);
    }
}
