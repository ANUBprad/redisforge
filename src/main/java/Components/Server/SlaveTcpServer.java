package Components.Server;

import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
import Components.Repository.Store;
import Components.Service.CommandHandler;
import Components.Service.RespSerializer;
import Components.Infra.Client;
import Components.Infra.RespStream;
import Components.Service.ResponseDto;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Locale;
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

    /** the listener, gate and handler pool this server runs its clients on */
    private final ClientListener listener = new ClientListener("replica", this::handleClient);
    /** the socket currently following the master, so shutting down can close it */
    private volatile Socket masterConnection;
    /** the retry waits on this, so stopping the server does not have to wait them out */
    private final Object retryLock = new Object();

    public void startServer(){
        listener.start(redisConfig.getPort(), redisConfig.getMaxClients(),
                redisConfig.getClientTimeoutMs(), this::startFollower);
    }

    /**
     * One thread follows the master for as long as this server runs. It hands the
     * stream over when the master hangs up and looks for it again when there is
     * none, so a replica that starts too early, or loses its master, recovers by
     * itself. Exactly one thread and one socket at a time, which is what keeps a
     * reconnect from duplicating the stream or applying a write twice.
     */
    private void startFollower() {
        Thread follower = new Thread(this::followMasterUntilStopped, "replication-follower");
        follower.setDaemon(true);
        follower.start();
    }

    /**
     * Stops the server: no further retries, and the sockets it is holding are closed so
     * neither the accept loop nor a read that is waiting on the master can hold the
     * process open. Idempotent, and Spring calls it when the context closes.
     */
    public void stop() {
        connectionPool.closeAllConnections();
        listener.stop();
        // a connection admitted in the moment the listener was closing is picked up here
        connectionPool.closeAllConnections();
        synchronized (retryLock) {
            retryLock.notifyAll();
        }
        closeQuietly(masterConnection);
    }

    /** The client handlers currently running, which a shutdown can be waited on. */
    public int activeClientHandlers() {
        return listener.activeClientHandlers();
    }

    @PreDestroy
    public void shutdown() {
        stop();
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
        while (listener.isRunning()) {
            try {
                if (followMasterOnce()) {
                    // the handshake worked, so the next wait starts at the floor again
                    delay = FIRST_RETRY_DELAY_MS;
                }
            } catch (IOException e) {
                if (listener.isRunning()) {
                    logger.log(Level.WARNING, "the master is not available ("
                            + e.getMessage() + "), looking for it again in " + delay + "ms");
                }
            } catch (RuntimeException unexpected) {
                // one bad frame or one failed apply must not take the follower down for
                // good: the connection is dropped and the next attempt starts clean
                if (listener.isRunning()) {
                    logger.log(Level.SEVERE, "following the master failed unexpectedly", unexpected);
                }
            }
            if (!listener.isRunning() || !waitBeforeRetry(delay)) {
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

            // part 3 of the handshake. A replica that has never had a stream asks for the
            // whole thing; one that has asks to carry its own on from where it stands, which
            // is answered with the bytes it missed instead of the dataset it already holds
            outputStream.write(psyncRequest());
            readPsyncReply(inputStream);

            streamFromMaster(new Client(master, inputStream, outputStream, -1));
            // the master hung up, which is not a failure: the loop looks for it again
            return true;
        } finally {
            masterConnection = null;
            closeQuietly(master);
        }
    }

    /**
     * What this replica asks for when it attaches: the whole stream if it has never had
     * one, and its own position in the stream it already follows if it has. The position is
     * the offset it counted for the writes it applied, so the master can hand back exactly
     * the bytes that stand between them - which is only ever a whole number of frames, since
     * a frame is the unit the offset is counted in.
     */
    private byte[] psyncRequest() {
        if (!redisConfig.hasAdoptedStream()) {
            return "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".getBytes(StandardCharsets.UTF_8);
        }
        String[] psync = new String[]{"PSYNC", redisConfig.getMasterReplId(),
                String.valueOf(redisConfig.getMasterReplOffset())};
        return respSerializer.respArray(psync).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Waits before the next attempt, unless the server is stopped first.
     *
     * @return true when it is time to try again
     */
    private boolean waitBeforeRetry(long delay) {
        synchronized (retryLock) {
            if (!listener.isRunning()) {
                return false;
            }
            try {
                retryLock.wait(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            return listener.isRunning();
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
                if (command.length == 0) {
                    // the master cannot be trusted with the framing any more: the stream
                    // cannot be resynchronised, so this connection ends and the follower
                    // looks for the master again
                    throw new IOException("the master sent an empty multibulk request");
                }
                String response = handleCommandFromMaster(command, master);
                if(response != null && !response.isEmpty())
                    master.outputStream.write(response.getBytes());
                // what a write is counted in is decided by propagate: a write is counted
                // and passed on as one step, and a control frame such as an answer to
                // GETACK never reaches it, so the offset goes on counting the write stream
                // only - in the same bytes both sides of a hop travel in, so an ACK from
                // here compares equal against the master's own count
            }
        }
    }

    /**
     * Consumes the answer to PSYNC, which is one of two things.
     *
     * <p>A "+CONTINUE" means the master still holds this replica's stream: no dataset
     * arrives, no position is taken over, nothing below is let go of, and the writes that
     * follow pick up exactly where this replica stopped, because the bytes in between were
     * written with the answer. Anything else is a "+FULLRESYNC" and the RDB behind it, which
     * is where a new position is taken.</p>
     */
    private void readPsyncReply(InputStream inputStream) throws IOException {
        String status = readLine(inputStream);
        if(status == null){
            throw new IOException("the master hung up before answering PSYNC");
        }
        if(status.startsWith("+CONTINUE")){
            logger.log(Level.FINE, status);
            String[] continueReply = status.split(" ");
            if(continueReply.length >= 2 && !continueReply[1].equals(redisConfig.getMasterReplId())){
                // a master carrying on a stream this replica does not follow would leave the
                // two naming different streams under one id, so this is treated as a failed
                // attempt and the next one starts again from a new socket
                throw new IOException("+CONTINUE for a replication id this replica does not follow: " + status);
            }
            return;
        }
        if(!status.startsWith("+FULLRESYNC")){
            throw new IOException("expected +FULLRESYNC or +CONTINUE from the master but read: " + status);
        }
        logger.log(Level.FINE, status);

        String[] fullResync = status.split(" ");
        if(fullResync.length < 3){
            throw new IOException("+FULLRESYNC without a replication id and offset: " + status);
        }
        long adopted;
        try {
            adopted = Long.parseLong(fullResync[2]);
        } catch (NumberFormatException notAPosition) {
            // a line this replica cannot place itself by is not a stream it can follow, and
            // letting the parse escape would kill the one thread that looks for the master
            throw new IOException("+FULLRESYNC with a position that is not a number: " + status);
        }

        String header = readLine(inputStream);
        if(header == null || !header.startsWith("$")){
            throw new IOException("expected the length of the RDB but read: " + header);
        }
        int rdbLength;
        try {
            rdbLength = Integer.parseInt(header.substring(1));
        } catch (NumberFormatException notALength) {
            throw new IOException("the dataset did not declare a length: " + header);
        }

        // the RDB is raw bytes, so it is counted off rather than scanned for a delimiter.
        // This master sends no CRLF after the payload, so the first replicated command
        // picks up on the very next byte
        byte[] rdb = inputStream.readNBytes(rdbLength);
        if(rdb.length != rdbLength){
            throw new EOFException("the RDB ended after " + rdb.length + " of " + rdbLength + " bytes");
        }

        // the position is taken only once the dataset behind it has arrived whole: half a
        // resync is not a stream this replica can resume from later, and taking the
        // position anyway would have it asking the master to carry on a dataset it never
        // received. Everything the master has streamed so far, including what this replica
        // missed while it was away, is behind that number, so this is where its own stream
        // now stands - and the backlog is emptied onto it, because what it held belongs to
        // the position this hop has just left
        if(adopted != redisConfig.getMasterReplOffset()){
            // a position that moved means a new stream, not a resumed one: a master that
            // restarted counts from nothing again, and what was streamed here while this
            // hop was away is behind it. The replicas below this hop still carry the old
            // stream, so they are let go of and attach again from this position, which is
            // what keeps a chain naming one stream instead of two
            connectionPool.dropDownstreamReplicas();
        }
        redisConfig.setMasterReplId(fullResync[1]);
        redisConfig.setMasterReplOffset(adopted);
        redisConfig.markAdoptedStream();
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

    private String handleCommandFromMaster(String[] command, Client master) {
        String cmd = command[0].toUpperCase(Locale.ROOT);

        String res = "";
        switch (cmd){
            case "SET":
                commandHandler.set(command);
                passOn(command);
                break;
            case "INCR":
                commandHandler.incr(command);
                passOn(command);
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
        passOn(command);
    }

    /**
     * Passes an applied write to the next hop, so a chained replica sees every write
     * exactly once.
     *
     * <p>The offset is not counted here. This hop counts the bytes it received from its
     * master, which are the same bytes it passes on, so counting on both sides would count
     * every write of a chain twice and leave a replica's ACK naming a number its own master
     * never reached.</p>
     */
    private void passOn(String[] command) {
        propagate(command);
    }

    /** The bytes a replicated command travels as, which is what the offset has to count. */
    private byte[] replicatedBytes(String[] command) {
        return respSerializer.respArray(command).getBytes(StandardCharsets.UTF_8);
    }

    private void propagate(String[] command) {
        // a copy, because a replica that cannot be written to is dropped on the way past
        byte[] propagated = replicatedBytes(command);
        // one hold of the backlog's lock: what this hop counts, what it keeps for a replica
        // to resume from and what it sends are one step, so a replica below can never be
        // offered a position whose bytes were counted here but not yet passed on
        synchronized (redisConfig.getReplicationBacklog()) {
            redisConfig.recordReplicatedBytes(propagated);
            for(Slave slave: new ArrayList<>(connectionPool.getSlaves())){
                if(!slave.isReady()){
                    // registered by a downstream replica, but still handshaking: a write now
                    // would be read by one that is waiting for its answer to PSYNC, so it waits too
                    logger.log(Level.FINE, "not sending to a replica whose stream has not started yet");
                    continue;
                }
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
                    if (command.length == 0) {
                        // an empty array names no command and can never become a valid
                        // one, so the stream cannot be resynchronised and must end
                        client.send("-ERR Protocol error: empty multibulk request\r\n");
                        return;
                    }
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
        switch (command[0].toUpperCase(Locale.ROOT)){
            case "PING":
                res = commandHandler.ping(command);
                break;
            case "ECHO":
                res = commandHandler.echo(command);
                break;
            case "SET": {
                // the form of the command is checked before the replica says what it
                // thinks of the write itself, so a malformed SET is a protocol answer
                // rather than a refusal of something that was never a valid command
                CommandHandler.SetForm form = commandHandler.parseSetForm(command);
                res = form.error() != null
                        ? form.error()
                        : "-READONLY You can't write against a replica.\r\n";
                break;
            }
            case "GET":
                res = commandHandler.get(command);
                break;
            case "INFO":
                res = commandHandler.info(command);
                break;
            case "PSYNC":
                // both answers are written by the handler itself while the backlog is held,
                // so null means the socket already has its reply
                ResponseDto resDto = commandHandler.psync(command, client);
                if(resDto == null){
                    return;
                }
                res = resDto.response;
                data = resDto.data;
                break;
            case "REPLCONF":
                // a replica runs the same handshake as a master, which is what lets it be
                // the upstream master of another replica
                res = commandHandler.replconf(command, client);
                break;
            case "WAIT":
                res = commandHandler.wait(command);
                break;
            case "BGREWRITEAOF":
                if (command.length != 1) {
                    res = commandHandler.wrongNumberOfArguments("bgrewriteaof");
                    break;
                }
                // a replica keeps no file of its own: what it holds came from its master's
                // stream, so there is nothing here for a local rewrite to compact. The reply
                // matters, because a client that gets no answer waits forever.
                res = "-ERR no append only file to rewrite\r\n";
                break;
            default:
                res = "-ERR unknown command '" + command[0] + "'\r\n";
                break;
        }
        client.send(res, data);
    }
}
