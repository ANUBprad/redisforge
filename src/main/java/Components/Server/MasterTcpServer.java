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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
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
    public void startServer(){
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = redisConfig.getPort();
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            int id = 0;
            while (true) {
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
            logger.log(Level.SEVERE, e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.getMessage());
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
        switch (command[0]){
            case "EXEC":
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
                    // to carry on between two of them is resumed at a whole frame
                    propagate(commands.poll());
                }

                String response = respSerializer.respArray(client.transactionResponse);

                client.send(response);

                break;
            case "DISCARD":
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
        return switch (command) {
            case "EXEC", "DISCARD" -> true;
            default -> false;
        };
    }

    public ResponseDto caseHandler(String[] command, Client client) throws IOException {
        //control comes here only when the client is not in a transaction
        String res = "";
        byte[] data = null;
        switch (command[0]){
            case "PING":
                res = commandHandler.ping(command);
                break;
            case "EXEC":
                res = "-ERR EXEC without MULTI\r\n";
                break;
            case "DISCARD":
                res = "-ERR DISCARD without MULTI\r\n";
                break;
            case "MULTI":
                client.beginTransaction();
                res = "+OK\r\n";
                break;
            case "INCR": {
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
            case "INFO":
                res = commandHandler.info(command);
                break;
            case "REPLCONF":
                res = commandHandler.replconf(command, client);
                break;
            case "WAIT":
                if(redisConfig.getMasterReplOffset() == 0){
                    res = respSerializer.respInteger(connectionPool.slavesThatAreCaughtUp.get());
                    break;
                }
                Instant start = Instant.now();
                res = commandHandler.wait(command, start);
                connectionPool.resetCaughtUpAccounting();
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
                res = rewriteAppendOnlyFile();
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
