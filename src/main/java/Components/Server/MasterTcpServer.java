package Components.Server;

import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
import Components.Persistence.AppendOnlyPersistence;
import Components.Repository.Store;
import Components.Repository.Value;
import Components.Service.CommandHandler;
import Components.Service.RespSerializer;
import Components.Infra.Client;
import Components.Infra.RespStream;
import Components.Service.ResponseDto;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
public class MasterTcpServer {
    private static final Logger logger = Logger.getLogger(MasterTcpServer.class.getName());
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
    @Autowired
    private AppendOnlyPersistence appendOnlyPersistence;
    /** the listener, gate and handler pool this server runs its clients on */
    private final ClientListener listener = new ClientListener("master", this::handleClient);

    /**
     * Every so often the store is asked to drop a bounded sample of keys whose deadline
     * has passed, so a key that is never read again still leaves the keyspace. The task
     * runs only while the server does, and only once no matter how often start() is
     * called.
     */
    private static final long ACTIVE_EXPIRY_PERIOD_MILLIS = 100;
    private final AtomicBoolean activeExpiryStarted = new AtomicBoolean(false);
    private ScheduledExecutorService activeExpiry;

    public void startServer(){
        listener.start(redisConfig.getPort(), redisConfig.getMaxClients(),
                redisConfig.getClientTimeoutMs(), () -> { });
        startActiveExpiry();
    }

    private void startActiveExpiry() {
        if (!activeExpiryStarted.compareAndSet(false, true)) {
            return;
        }
        activeExpiry = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "active-expiry");
            thread.setDaemon(true);
            return thread;
        });
        activeExpiry.scheduleWithFixedDelay(store::removeExpired,
                ACTIVE_EXPIRY_PERIOD_MILLIS, ACTIVE_EXPIRY_PERIOD_MILLIS, TimeUnit.MILLISECONDS);
    }

    /**
     * Stops the server: the listener ends, every connection this context holds is
     * closed so no handler stays blocked on a read, and the handler pool shuts down.
     * Idempotent, and Spring calls it when the context closes.
     */
    public void stop() {
        if (activeExpiry != null) {
            activeExpiry.shutdownNow();
            activeExpiry = null;
            // lets a server that is started again after a stop get its sweep back
            activeExpiryStarted.set(false);
        }
        connectionPool.closeAllConnections();
        listener.stop();
        // a connection admitted in the moment the listener was closing is picked up here
        connectionPool.closeAllConnections();
    }

    /** The client handlers currently running, which a shutdown can be waited on. */
    public int activeClientHandlers() {
        return listener.activeClientHandlers();
    }

    @PreDestroy
    public void shutdown() {
        stop();
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
        if(!client.getTransactionalContext()){
            ResponseDto responseDto = caseHandler(command, client);
            // null means the command answered the socket itself: PSYNC writes both of its
            // answers while the backlog is held, and writing one anywhere else would let a
            // write overtake it or land ahead of the bytes it follows
            if(responseDto != null){
                client.send(responseDto);
            }
        }else if(!isTransactionalControlCommand(command[0])){
            // we are in the transactional context and the command is a normal command
            addCommandToTransaction(command, client);
        }else{
            // we are in the transactional context and the command is a transaction control command, EXEC or DISCARD
            transactionController(command, client);
        }

    }

    private void transactionController(String[] command, Client client) throws IOException {
        //control only comes here in the transaction context
        switch (command[0].toUpperCase(Locale.ROOT)){
            case "EXEC":
                if (command.length != 1) {
                    client.send(commandHandler.wrongNumberOfArguments("exec"));
                    return;
                }
                if(client.commandQueue==null || client.commandQueue.isEmpty()){
                    client.send("*0\r\n");
                    client.endTransaction();
                    return;
                }

                Queue<String[]> commands = new LinkedList<>(client.commandQueue);

                //execute the transaction
                BiFunction<String[], Map<String, Value>, String> transactionCacheApplier = commandHandler.getTransactionCommandCacheApplier();
                // the whole transaction is recorded under one hold of the file lock, right
                // after the store applied it, so it lands in the file as one block and in
                // the same order the store was changed in
                appendOnlyPersistence.locked(() -> {
                    store.executeTransaction(client, transactionCacheApplier);
                    appendOnlyPersistence.appendAppliedTransaction(appliedCommands(client, commands));
                    return null;
                });

                client.endTransaction();
                while(!commands.isEmpty()){
                    // each command is counted and sent as one step, so a replica that asks
                    // to carry on between two of them is resumed at a whole frame. An
                    // expiration is sent in its absolute form, so a replica expiring the
                    // key does not start the clock over on its own.
                    propagate(appendOnlyPersistence.replicationFrame(commands.poll()));
                }

                String response = respSerializer.respArray(client.transactionResponse);

                client.send(response);

                break;
            case "DISCARD":
                if (command.length != 1) {
                    client.send(commandHandler.wrongNumberOfArguments("discard"));
                    return;
                }
                client.endTransaction();
                client.send("+OK\r\n");
                break;
        }
    }

    /**
     * The queued commands the transaction really applied, in order.
     *
     * <p>A transaction commits the commands that worked and skips the ones the store
     * refused, so its replies say which those were. Recording a refused command would
     * write down a change that never happened, and replaying it would fail all over
     * again against a keyspace that is already correct.</p>
     */
    private List<String[]> appliedCommands(Client client, Queue<String[]> queued) {
        List<String> replies = client.transactionResponse;
        List<String[]> applied = new ArrayList<>(queued.size());
        int index = 0;
        for (String[] command : queued) {
            if (index < replies.size() && !replies.get(index).startsWith("-")) {
                applied.add(command);
            }
            index++;
        }
        return applied;
    }

    private void addCommandToTransaction(String[] command, Client client) throws IOException {
        client.commandQueue.offer(command);
        client.send("+QUEUED\r\n");
    }

    private boolean isTransactionalControlCommand(String command) {
        return switch (command.toUpperCase(Locale.ROOT)) {
            case "EXEC", "DISCARD" -> true;
            default -> false;
        };
    }

    public ResponseDto caseHandler(String[] command, Client client) throws IOException {
        //control comes here only when the client is not in a transaction
        String res = "";
        byte[] data = null;
        switch (command[0].toUpperCase(Locale.ROOT)){
            case "PING":
                res = commandHandler.ping(command);
                break;
            case "EXEC":
                if (command.length != 1) {
                    res = commandHandler.wrongNumberOfArguments("exec");
                    break;
                }
                res = "-ERR EXEC without MULTI\r\n";
                break;
            case "DISCARD":
                if (command.length != 1) {
                    res = commandHandler.wrongNumberOfArguments("discard");
                    break;
                }
                res = "-ERR DISCARD without MULTI\r\n";
                break;
            case "MULTI":
                if (command.length != 1) {
                    res = commandHandler.wrongNumberOfArguments("multi");
                    break;
                }
                client.beginTransaction();
                res = "+OK\r\n";
                break;
            case "INCR": {
                if (command.length != 2) {
                    res = commandHandler.wrongNumberOfArguments("incr");
                    break;
                }
                // the file lock comes first and the key's lock inside it, which is the
                // order a transaction takes them: applied, recorded and propagated under
                // both, so replicas see the increments of a key in the order this master
                // applied them, and an increment can never end up waiting for the file
                // while a transaction holds it and waits for the key
                res = appendOnlyPersistence.locked(() -> {
                    ReentrantLock keyLock = store.lockFor(command[1]);
                    keyLock.lock();
                    try {
                        String increment = commandHandler.incr(command);
                        // an increment that was refused is not a mutation, so it is neither
                        // written to the file nor replicated
                        if (!increment.startsWith("-")) {
                            appendOnlyPersistence.appendApplied(command);
                            propagate(command);
                        }
                        return increment;
                    } finally {
                        keyLock.unlock();
                    }
                });
                break;
            }
            case "ECHO":
                res = commandHandler.echo(command);
                break;
            case "SET":
                res = appendOnlyPersistence.locked(() -> {
                    String set = commandHandler.set(command);
                    if (set.startsWith("+")) {
                        // the store has the deadline the command asked for, so the entry
                        // can be written with an absolute one
                        appendOnlyPersistence.appendApplied(command);
                    }
                    // on this thread, so replicas receive writes in the order they arrived
                    propagate(command);
                    return set;
                });
                break;
            case "GET":
                res = commandHandler.get(command);
                break;
            case "EXPIRE":
                res = applyExpiration(command, () -> commandHandler.expire(command));
                break;
            case "PEXPIRE":
                res = applyExpiration(command, () -> commandHandler.pexpire(command));
                break;
            case "EXPIREAT":
                res = applyExpiration(command, () -> commandHandler.expireAt(command));
                break;
            case "PEXPIREAT":
                res = applyExpiration(command, () -> commandHandler.pexpireAt(command));
                break;
            case "PERSIST":
                res = applyExpiration(command, () -> commandHandler.persist(command));
                break;
            case "TTL":
                res = commandHandler.ttl(command);
                break;
            case "PTTL":
                res = commandHandler.pttl(command);
                break;
            case "INFO":
                res = commandHandler.info(command);
                break;
            case "REPLCONF":
                res = commandHandler.replconf(command, client);
                break;
            case "WAIT":
                res = commandHandler.wait(command);
                break;
            case "PSYNC":
                // both answers are written by the handler itself while the backlog is held,
                // so null means the socket already has its reply
                ResponseDto resDto = commandHandler.psync(command, client);
                if(resDto == null){
                    return null;
                }
                res = resDto.response;
                data = resDto.data;
                break;
            case "BGREWRITEAOF":
                if (command.length != 1) {
                    res = commandHandler.wrongNumberOfArguments("bgrewriteaof");
                    break;
                }
                res = rewriteAppendOnlyFile();
                break;
            default:
                // the original spelling goes back in the reply, and the connection stays
                // open: an unknown name is a client mistake, not a broken stream
                res = "-ERR unknown command '" + command[0] + "'\r\n";
                break;
        }
        return new ResponseDto(res, data);
    }

    /**
     * The explicit trigger for a rewrite. The work is done before the reply, so a client
     * that has its +OK knows the file on disk is already the compact one. The name is the
     * one Redis uses; nothing here runs in the background.
     */
    private String rewriteAppendOnlyFile() {
        if (!appendOnlyPersistence.isEnabled()) {
            return "-ERR no append only file to rewrite\r\n";
        }
        try {
            appendOnlyPersistence.rewrite();
            return "+OK\r\n";
        } catch (RuntimeException e) {
            // the client gets told what went wrong, rather than the connection dying on it
            logger.log(Level.WARNING, "could not rewrite the append only file: " + e.getMessage());
            return "-ERR " + e.getMessage().replaceAll("[\\r\\n]+", " ") + "\r\n";
        }
    }

    /**
     * A command that hangs a deadline on a key, or takes one off. The store applies it,
     * and only a mutation that really happened ({@code :1}) is written down and passed
     * on: a command refused because the key is gone changes nothing, so replaying it or
     * sending it downstream would describe a change that never occurred. The frame is the
     * absolute one, so replicas and the file expire the key at the same instant.
     */
    private String applyExpiration(String[] command, Supplier<String> action) {
        return appendOnlyPersistence.locked(() -> {
            String applied = action.get();
            if (applied.equals(":1\r\n")) {
                appendOnlyPersistence.appendApplied(command);
                propagate(appendOnlyPersistence.replicationFrame(command));
            }
            return applied;
        });
    }

    private void propagate(String[] command) {
        String commandRespString = respSerializer.respArray(command);
        // the same bytes the replicas are sent, so the offset counts what went over the wire
        byte[] propagated = commandRespString.getBytes(StandardCharsets.UTF_8);
        // one hold of the backlog's lock, because counting the bytes, keeping them and
        // sending them is one step: a replica that asks to resume in between would
        // otherwise be told a position whose bytes were counted but never sent, and would
        // carry on from a stream with a hole in it
        synchronized (redisConfig.getReplicationBacklog()) {
            redisConfig.recordReplicatedBytes(propagated);
            // a copy, because a replica that cannot be written to is dropped on the way past
            for(Slave slave: new ArrayList<>(connectionPool.getSlaves())){
                if(!slave.isReady()){
                    // registered, but still handshaking: a write now would be read by a replica
                    // that is waiting for its answer to PSYNC, so it is left until the stream starts
                    logger.log(Level.FINE, "not sending to a replica whose stream has not started yet");
                    continue;
                }
                try {
                    slave.send(propagated);
                } catch (IOException e) {
                    // propagation runs on the connection's thread, so a replica that stops
                    // reading must not be allowed to break that connection
                    logger.log(Level.WARNING, "dropping a replica that could not be written to: " + e.getMessage());
                    connectionPool.removeSlave(slave);
                }
            }
        }
    }
}
