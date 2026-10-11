package Components.Service;

import Components.Infra.Client;
import Components.Infra.ConnectionPool;
import Components.Infra.ReplicationBacklog;
import Components.Infra.Slave;
import Components.Infra.StoreSnapshot;
import Components.Repository.Store;
import Components.Repository.TypedOps;
import Components.Repository.Value;
import Components.Server.RedisConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.SocketException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
public class CommandHandler {
    private static final Logger logger = Logger.getLogger(CommandHandler.class.getName());
    @Autowired
    public RespSerializer respSerializer;
    @Autowired
    public Store store;
    @Autowired
    public RedisConfig redisConfig;
    @Autowired
    public ConnectionPool connectionPool;

    /** What a command with the wrong number of arguments is answered with. */
    public String wrongNumberOfArguments(String command) {
        return "-ERR wrong number of arguments for '" + command + "' command\r\n";
    }

    public String ping(String[] command){
        if (command.length > 2) {
            return wrongNumberOfArguments("ping");
        }
        return "+PONG\r\n";
    }

    public String echo(String[] command){
        if (command.length != 2) {
            return wrongNumberOfArguments("echo");
        }
        return respSerializer.serializeBulkString(command[1]);
    }

    /**
     * The one form {@code SET} is parsed into: key, value, an optional expiry in
     * milliseconds, or the error the command must be answered with. Commands beyond
     * {@code SET key value} and {@code SET key value px <millis>} are refused here
     * rather than having an option silently ignored.
     */
    public record SetForm(String key, String value, Integer expiryMillis, String error) {
    }

    /** Parses a SET command, whatever its case, without touching the store. */
    public SetForm parseSetForm(String[] command) {
        if (command.length < 3 || command.length == 4 || command.length > 5) {
            return new SetForm(null, null, null, wrongNumberOfArguments("set"));
        }
        if (command.length == 3) {
            return new SetForm(command[1], command[2], null, null);
        }
        String option = command[3];
        if (!"px".equalsIgnoreCase(option)) {
            return new SetForm(null, null, null,
                    "-ERR unsupported option '" + option + "'\r\n");
        }
        int millis;
        try {
            millis = Integer.parseInt(command[4]);
        } catch (NumberFormatException notANumber) {
            return new SetForm(null, null, null,
                    "-ERR value is not an integer or out of range\r\n");
        }
        if (millis <= 0) {
            return new SetForm(null, null, null,
                    "-ERR invalid expire time in 'set' command\r\n");
        }
        return new SetForm(command[1], command[2], millis, null);
    }

    public String set(String[] command){
        SetForm form = parseSetForm(command);
        if (form.error() != null) {
            return form.error();
        }
        if (form.expiryMillis() == null) {
            return store.set(form.key(), form.value());
        }
        return store.set(form.key(), form.value(), form.expiryMillis());
    }

    public String get(String[] command){
        if (command.length != 2) {
            return wrongNumberOfArguments("get");
        }
        return store.get(command[1]);
    }

    /**
     * Answers a hash, list or set read. Every typed read names its key first, so a missing
     * key is the wrong number of arguments; the read itself runs under the store's read
     * lock, so a collection cannot be read while a writer is changing it.
     */
    public String typedRead(String[] command) {
        if (command.length < 2) {
            return wrongNumberOfArguments(command[0].toLowerCase(Locale.ROOT));
        }
        return store.read(command[1], value -> TypedOps.read(command, value));
    }

    public String expire(String[] command) {
        if (command.length != 3) return wrongNumberOfArguments("expire");
        try {
            int seconds = Integer.parseInt(command[2]);
            int res = store.expire(command[1], seconds);
            return respSerializer.respInteger(res);
        } catch (NumberFormatException e) {
            return "-ERR value is not an integer or out of range\r\n";
        }
    }

    public String pexpire(String[] command) {
        if (command.length != 3) return wrongNumberOfArguments("pexpire");
        try {
            long millis = Long.parseLong(command[2]);
            int res = store.pexpire(command[1], millis);
            return respSerializer.respInteger(res);
        } catch (NumberFormatException e) {
            return "-ERR value is not an integer or out of range\r\n";
        }
    }

    public String expireAt(String[] command) {
        if (command.length != 3) return wrongNumberOfArguments("expireat");
        try {
            long unix = Long.parseLong(command[2]);
            int res = store.expireAt(command[1], unix);
            return respSerializer.respInteger(res);
        } catch (NumberFormatException e) {
            return "-ERR value is not an integer or out of range\r\n";
        }
    }

    public String pexpireAt(String[] command) {
        if (command.length != 3) return wrongNumberOfArguments("pexpireat");
        try {
            long unix = Long.parseLong(command[2]);
            int res = store.pexpireAt(command[1], unix);
            return respSerializer.respInteger(res);
        } catch (NumberFormatException e) {
            return "-ERR value is not an integer or out of range\r\n";
        }
    }

    public String ttl(String[] command) {
        if (command.length != 2) return wrongNumberOfArguments("ttl");
        return store.ttl(command[1]);
    }

    public String pttl(String[] command) {
        if (command.length != 2) return wrongNumberOfArguments("pttl");
        return store.pttl(command[1]);
    }

    public String persist(String[] command) {
        if (command.length != 2) return wrongNumberOfArguments("persist");
        int res = store.persist(command[1]);
        return respSerializer.respInteger(res);
    }

    public String info(String[] command){
        if (command.length > 2) {
            return wrongNumberOfArguments("info");
        }
        if (command.length == 2) {
            return switch (command[1].toLowerCase(Locale.ROOT)) {
                case "replication" -> respSerializer.serializeBulkString(replicationInfo());
                case "server" -> respSerializer.serializeBulkString(serverInfo());
                case "memory" -> respSerializer.serializeBulkString(memoryInfo());
                case "all" -> respSerializer.serializeBulkString(
                        serverInfo() + "\r\n" + replicationInfo() + "\r\n" + memoryInfo());
                default -> "-ERR Invalid INFO section specified\r\n";
            };
        }
        return respSerializer.serializeBulkString(
                serverInfo() + "\r\n" + replicationInfo() + "\r\n" + memoryInfo());
    }

    private String serverInfo() {
        return "# Server\r\nredis_version:1.0";
    }

    private String replicationInfo() {
        String role = "role:" + redisConfig.getRole();
        String masterReplId = "master_replid:" + redisConfig.getMasterReplId();
        String masterReplOffset = "master_repl_offset:" + redisConfig.getMasterReplOffset();
        return "# Replication\r\n" + String.join("\r\n",
                new String[]{role, masterReplId, masterReplOffset});
    }

    /**
     * The memory section: how many bytes the keyspace is charged, the ceiling the server
     * was given, the policy it evicts under, and how many keys that policy has removed.
     * The numbers are the same logical estimate the eviction decision uses, so a client
     * reading {@code used_memory} sees the quantity {@code maxmemory} is compared against.
     */
    private String memoryInfo() {
        long used = store.currentBytes();
        long max = redisConfig.getMaxmemory();
        return "# Memory\r\n" + String.join("\r\n", new String[]{
                "used_memory:" + used,
                "used_memory_human:" + humanBytes(used),
                "maxmemory:" + max,
                "maxmemory_human:" + humanBytes(max),
                "maxmemory_policy:" + redisConfig.getMaxmemoryPolicy().configName(),
                "evicted_keys:" + store.evictedKeys()
        });
    }

    /** A byte count in the approximate form Redis prints, such as {@code 1.00M}. */
    private static String humanBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + "B";
        }
        String[] units = {"K", "M", "G", "T", "P"};
        double value = bytes;
        int unit = -1;
        while (value >= 1024 && unit < units.length - 1) {
            value /= 1024;
            unit++;
        }
        return String.format(Locale.ROOT, "%.2f%s", value, units[unit]);
    }

    public String replconf(String[] command, Client client) {
        if (command.length < 2) {
            return wrongNumberOfArguments("replconf");
        }

        switch (command[1]){
            case "GETACK":
                if (command.length < 3) {
                    return wrongNumberOfArguments("replconf");
                }
                String [] replconfAck = new String[]{"REPLCONF", "ACK", redisConfig.getMasterReplOffset()+""};
                return respSerializer.respArray(replconfAck);
            case "ACK": {
                if (command.length < 3) {
                    return wrongNumberOfArguments("replconf");
                }
                long ackOffset;
                try {
                    ackOffset = Long.parseLong(command[2]);
                } catch (NumberFormatException notAnOffset) {
                    return "-ERR value is not an integer or out of range\r\n";
                }
                // an acknowledgement only means something from a replica whose stream has
                // started, and only for the position the write stream has actually reached:
                // one naming an earlier position is a replica that has not caught up yet
                Slave replica = connectionPool.slaveFor(client);
                if(replica != null && replica.isReady()
                        && ackOffset == redisConfig.getMasterReplOffset()
                        && replica.markAcknowledged()){
                    connectionPool.countCaughtUp();
                }
                return "";
            }
            case "listening-port": {
                if (command.length < 3) {
                    return wrongNumberOfArguments("replconf");
                }
                int listeningPort;
                try {
                    listeningPort = Integer.parseInt(command[2]);
                } catch (NumberFormatException notAPort) {
                    return "-ERR value is not an integer or out of range\r\n";
                }
                // a registered replica is exempt from the idle timeout from here on: it
                // sends nothing between the writes its master has to push at it, and being
                // dropped for that silence would only make it reconnect and resync
                try {
                    client.socket.setSoTimeout(0);
                } catch (SocketException e) {
                    logger.log(Level.WARNING, "could not lift the idle timeout of a "
                            + "registered replica: " + e.getMessage());
                }
                // one registration per replica. A reconnect can arrive while the connection it
                // replaces is still being cleaned up, and two entries for one replica would
                // mean every write was sent to it twice, so the earlier one is dropped and its
                // socket closed: the replica ends up with exactly one stream
                Slave previous = connectionPool.slaveAtPort(listeningPort, client);
                connectionPool.removeClient(client);
                Slave s = new Slave(client, listeningPort);
                // the connection is now written to only through this replica's own sender
                // thread, so no write a master applies can block on it
                client.attachReplica(s);
                connectionPool.addSlave(s);
                if(previous != null){
                    // removing the replica that this one replaces closes its socket, which
                    // ends its read and unwinds its own handler
                    connectionPool.removeSlave(previous);
                }
                return "+OK\r\n";
            }
            case "capa": {
                if (command.length < 3) {
                    return wrongNumberOfArguments("replconf");
                }
                Slave slave = connectionPool.slaveFor(client);
                // capabilities are recorded against the registered replica, and a connection
                // that never registered has none to record them on
                if(slave == null){
                    return "+OK\r\n";
                }
                for(int i=0; i + 1 < command.length; i++){
                    if(command[i].equals("capa")){
                        slave.capabilities.add(command[i+1]);
                    }
                }
                return "+OK\r\n";
            }
        }

        return "+OK\r\n";
    }
    public byte[] concatenate(byte[] a, byte[] b){
        byte[] result = new byte[a.length +b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }
    /**
     * Answers a replica's request for the command stream.
     *
     * <p>There are two answers. A replica that names this master's replication id and a
     * position the backlog still holds is told to carry on from there, and the bytes it
     * missed are written out with the answer. Anything else - a replica that has never
     * synced, a replication id this master does not have, a position ahead of the stream or
     * one that has already fallen out of the backlog, or a position that does not parse -
     * is answered with a full resync, which is always safe because the replica adopts
     * whatever position this master's stream has reached.</p>
     *
     * <p>The resume is queued from inside this method, with the backlog's lock held, so its
     * three parts - the position it names, the bytes behind it and the moment the replica
     * becomes eligible for writes - are one step. Handing the reply back to the caller would
     * open a gap between them: a write made in that gap would be counted after a position
     * that does not include it, or reach a replica before the answer that tells it where it
     * stands. Holding the lock across the whole thing is also what makes the answer
     * indivisible against propagation, which counts and queues under the same lock. The
     * frame is only queued here, never written to the socket: sending it is left to the
     * replica's own sender thread, so no socket write ever happens under this lock.</p>
     *
     * <p>The refusal is decided on facts checked under the same hold of the lock. A full
     * resync cuts its snapshot and the position it names under one hold of the lock, so the
     * catch-up bytes replayed after it never include a command the snapshot already carries
     * and never miss one that landed while it was being cut. Cutting the snapshot is work,
     * not a socket write, so it holds the stream up the way an AOF write does - never by
     * blocking on a peer.</p>
     *
     * @return the reply to write, or null when this method has already queued it
     */
    public ResponseDto psync(String[] command, Client client) throws IOException {
        if (command.length < 3) {
            return new ResponseDto("-ERR wrong number of arguments for 'psync' command\r\n");
        }
        String replicationIdMaster = command[1];
        String replicationOffSetMaster = command[2];

        if (replicationIdMaster.equals("?")) {
            // a replica with no stream of its own, whatever it says about a position
            fullResync(client);
            return null;
        }

        long requested;
        try {
            requested = Long.parseLong(replicationOffSetMaster);
        } catch (NumberFormatException notAPosition) {
            fullResync(client);
            return null;
        }

        ReplicationBacklog backlog = redisConfig.getReplicationBacklog();
        synchronized (backlog) {
            if (requested >= 0
                    && requested <= redisConfig.getMasterReplOffset()
                    && backlog.covers(requested)
                    && replicationIdMaster.equals(redisConfig.getMasterReplId())) {
                byte[] missed = backlog.read(requested);
                // PSYNC is control traffic: it says a replica has attached or asked to carry
                // on, not that it has consumed a write, so it must not count towards a WAIT
                startStream(client);
                client.send("+CONTINUE " + replicationIdMaster + "\r\n", missed);
                return null;
            }
            // the position was refused on facts checked under this same hold of the lock
        }
        // the snapshot is made outside the lock so a large dataset does not stall the
        // stream; its catch-up is queued under the lock to keep the handover atomic
        fullResync(client);
        return null;
    }

    /**
     * Hands over the whole stream from this position: a full resync line carrying the
     * replication id and offset the replica should take on, and a snapshot of the dataset
     * behind it.
     *
     * <p>Queued, never written to the socket: the replica's own sender thread writes it.
     * The snapshot is cut under the backlog's lock, same as an AOF write - it is work, not a
     * socket write, so nothing ever blocks on a peer. Cutting it under the same hold as the
     * position it is handed over with is what keeps the handover whole: a command that
     * landed before the cut is already counted and caught by the read at the end, and one
     * that lands while the answer is being queued has to wait for the hold, and reaches this
     * replica after the step that tells it where it stands. Only the expensive serialization
     * of a live copy could be spared the lock, and reading the offset apart from it would
     * leave a window in which a command is either carried twice or lost.</p>
     *
     * <p>A backlog so small that the position fell out of its window while the snapshot was
     * being made means this master can no longer bridge the gap alone, and nothing has been
     * written yet, so the connection is closed without a byte going out: the replica's
     * connect-and-read loop fails the attempt and comes back to ask again, landing on a
     * resume if the position is still covered or on another full resync if not, never on a
     * gap this master cannot fill.</p>
     */
    private void fullResync(Client client) throws IOException {
        ReplicationBacklog backlog = redisConfig.getReplicationBacklog();
        byte[] snapshot;
        String replicationId;
        long replicationOffset;
        synchronized (backlog) {
            replicationId = redisConfig.getMasterReplId();
            replicationOffset = redisConfig.getMasterReplOffset();
            snapshot = StoreSnapshot.serializeDataset(store.liveTypedSnapshot());
        }

        String res = "+FULLRESYNC " + replicationId + " " + replicationOffset + "\r\n";
        byte[] header = ("$" + snapshot.length + "\r\n").getBytes();

        synchronized (backlog) {
            byte[] catchUp = backlog.read(replicationOffset);
            if (catchUp == null) {
                // the position was evicted while the snapshot was being made: closing the
                // socket aborts the attempt before any of it goes out, so the replica asks
                // again from a position that is either covered or one full resync away
                client.close();
                return;
            }
            // the stream opens and the answer is queued as one step under the lock - never
            // written to the socket, which the replica's sender thread does outside it - so a
            // write can neither reach this replica ahead of the snapshot that tells it where
            // it stands nor disappear into a replica that was never told it was eligible
            startStream(client);
            client.send(res, concatenate(header, snapshot));
            if (catchUp.length > 0) {
                client.send(catchUp);
            }
        }
    }

    /**
     * Opens a replica's command stream, which is the point from which it may be sent writes.
     *
     * <p>It registered itself earlier, when it gave its listening port, and anything sent
     * before this would arrive in the middle of its handshake and take the whole stream out
     * of step. A master and a replica that is itself a master to another replica both come
     * through here, so neither can forget it. PSYNC is control traffic either way: it says
     * a replica has attached, not that it has consumed a write, so it counts towards no WAIT
     * and never moves the offset.</p>
     */
    private void startStream(Client client) {
        Slave slave = connectionPool.slaveFor(client);
        if(slave != null){
            slave.markReady();
        }
    }
    /**
     * Answers WAIT: broadcasts a GETACK to every replica whose stream has started, then
     * waits until the asked-for number of them has acknowledged the position the write
     * stream has reached, or the timeout passes, and reports the lower of the two.
     *
     * <p>The acknowledgement wait blocks on the pool's monitor instead of spinning, and
     * the accounting is reset only after a wait that really happened: a WAIT on a stream
     * that has not moved reports what stands and leaves it standing.</p>
     */
    public String wait(String[] command) {
        if (command.length != 3) {
            return wrongNumberOfArguments("wait");
        }
        int required;
        int time;
        try {
            required = Integer.parseInt(command[1]);
            time = Integer.parseInt(command[2]);
        } catch (NumberFormatException notANumber) {
            return "-ERR value is not an integer or out of range\r\n";
        }
        if (required < 0 || time < 0) {
            return "-ERR value is not an integer or out of range\r\n";
        }
        if (redisConfig.getMasterReplOffset() == 0) {
            return respSerializer.respInteger(connectionPool.slavesThatAreCaughtUp.get());
        }

        String[] getackarr = new String[] { "REPLCONF", "GETACK", "*" };
        byte[] bytearr = respSerializer.respArray(getackarr).getBytes();

        // queued behind whatever that replica has not read yet, so it cannot answer this
        // GETACK before the writes that were propagated ahead of it
        for(Slave slave: new ArrayList<>(connectionPool.getSlaves())){
            if(!slave.isReady()){
                // the same reason writes wait: this replica is registered but its stream has
                // not started, and a GETACK now would land in the middle of its handshake
                continue;
            }
            try {
                slave.send(bytearr);
            } catch (IOException e) {
                logger.log(Level.WARNING, "dropping a replica that could not be written to: " + e.getMessage());
                connectionPool.removeSlave(slave);
            }
        }

        int res = connectionPool.awaitCaughtUp(required, time);
        connectionPool.resetCaughtUpAccounting();
        // the offset counts replicated writes only. Folding the GETACK into it would
        // push it past the offset a replica reports back, and the next ACK would never match
        return respSerializer.respInteger(Math.min(res, required));
    }

    public String incr(String[] command) {
        if (command.length != 2) {
            return wrongNumberOfArguments("incr");
        }
        // the store owns the read, parse, increment and write, so two clients cannot read
        // the same value and both store the same successor
        return store.increment(command[1]);
    }

    public BiFunction<String[], Map<String, Value>, String> getTransactionCommandCacheApplier(){
        return getTransactionCommandCacheApplier(null);
    }

    /**
     * The applier a queued transaction is run through. When the applying client is known,
     * a typed mutation records the frame it must be written down and passed on as - which
     * matters for {@code SPOP}, whose popped members are only known at apply time - so the
     * transaction can be persisted and replicated deterministically.
     */
    public BiFunction<String[], Map<String, Value>, String> getTransactionCommandCacheApplier(Client client){
        final Store localStore = this.store;
        final RespSerializer localSerializer = this.respSerializer;
        return (String[] command, Map<String, Value> map)->{
            String name = command[0].toUpperCase(Locale.ROOT);
            if (TypedOps.handles(name)) {
                if (TypedOps.isRead(name)) {
                    Value current = command.length >= 2
                            ? resolveTransactionalValue(command[1], map, localStore)
                            : null;
                    return TypedOps.read(command, current);
                }
                return applyTypedTransactional(command, map, localStore, client);
            }
            String res = "";
            switch (name) {
                case "SET":
                    res = handleSetCommandTransactional(command, map, localSerializer, localStore);
                    break;
                case "GET":
                    res = handleGetCommandTransactional(command, map, localSerializer, localStore);
                    break;
                case "INCR":
                    res = handleIncrCommandTransactional(command, map, localSerializer, localStore);
                    break;
                case "DEL":
                    res = handleDelCommandTransactional(command, map, localSerializer, localStore);
                    break;
                case "EXPIRE":
                    res = handleExpireCommandTransactional(command, map, localStore, "EXPIRE");
                    break;
                case "PEXPIRE":
                    res = handleExpireCommandTransactional(command, map, localStore, "PEXPIRE");
                    break;
                case "EXPIREAT":
                    res = handleExpireCommandTransactional(command, map, localStore, "EXPIREAT");
                    break;
                case "PEXPIREAT":
                    res = handleExpireCommandTransactional(command, map, localStore, "PEXPIREAT");
                    break;
                case "PERSIST":
                    res = handlePersistCommandTransactional(command, map, localStore);
                    break;
                case "TTL":
                    res = handleTtlCommandTransactional(command, map, localStore, false);
                    break;
                case "PTTL":
                    res = handleTtlCommandTransactional(command, map, localStore, true);
                    break;
                default:
                    res = "-ERR unknown command '"+command[0]+"'\r\n";
                    break;
            }
            return res;
        };
    }

    /**
     * The value a queued command should work on, taken from the transaction's own cache
     * when an earlier command already touched the key, and copied from the store
     * otherwise. A key the store no longer holds is absent, so the command sees what the
     * transaction will see.
     */
    private Value resolveTransactionalValue(String key, Map<String, Value> map, Store localStore) {
        Value cached = map.get(key);
        if (cached != null) {
            return cached.isDeletedInTransaction ? null : cached;
        }
        Value storeValue = localStore.getValue(key);
        if (storeValue == null) {
            return null;
        }
        Value copy = storeValue.copy();
        map.put(key, copy);
        return copy;
    }

    /**
     * A queued hash, list or set write, applied to the transaction's view of the key. The
     * mutation is computed against a copy and stored into the view, so EXEC commits it
     * with the rest only if the whole transaction is admitted. A mutation that empties a
     * collection marks the key for deletion at commit time.
     */
    private String applyTypedTransactional(String[] command, Map<String, Value> map,
                                           Store localStore, Client client) {
        if (command.length < 2) {
            return wrongNumberOfArguments(command[0].toLowerCase(Locale.ROOT));
        }
        String key = command[1];
        Value current = resolveTransactionalValue(key, map, localStore);
        TypedOps.Mutation mutation = TypedOps.mutate(command, current);
        if (mutation.reply().startsWith("-") || !mutation.changed()) {
            return mutation.reply();
        }
        if (mutation.value() == null) {
            Value tombstone = current != null
                    ? current
                    : new Value("", LocalDateTime.now(), LocalDateTime.MAX);
            tombstone.isDeletedInTransaction = true;
            map.put(key, tombstone);
        } else {
            map.put(key, mutation.value());
        }
        if (client != null && mutation.propagation() != null) {
            client.transactionPropagation.put(command, mutation.propagation());
        }
        return mutation.reply();
    }

    /**
     * A queued EXPIRE and its siblings, applied to the transaction's view of the key. A
     * non-positive or already-passed deadline deletes the key when the transaction
     * commits, exactly as the same command would outside one.
     */
    private String handleExpireCommandTransactional(String[] command, Map<String, Value> map,
                                                    Store localStore, String kind) {
        if (command.length != 3) {
            return wrongNumberOfArguments(kind.toLowerCase(Locale.ROOT));
        }
        long amount;
        try {
            amount = Long.parseLong(command[2]);
        } catch (NumberFormatException e) {
            return "-ERR value is not an integer or out of range\r\n";
        }
        Value value = resolveTransactionalValue(command[1], map, localStore);
        if (value == null) {
            return respSerializer.respInteger(0);
        }
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime exp;
        try {
            exp = switch (kind) {
                case "EXPIRE" -> amount <= 0 ? null : now.plusSeconds(amount);
                case "PEXPIRE" -> amount <= 0 ? null : now.plus(amount, ChronoUnit.MILLIS);
                case "EXPIREAT" -> Store.absoluteOrClamp(Instant.ofEpochSecond(Store.wrapSeconds(amount)));
                case "PEXPIREAT" -> Store.absoluteOrClamp(Instant.ofEpochMilli(amount));
                default -> null;
            };
        } catch (ArithmeticException | DateTimeException deadlineTooFar) {
            // a relative deadline so far out that it is not a date keeps the key forever,
            // the same reading the absolute commands give a date beyond their range
            exp = amount <= 0 ? null : LocalDateTime.MAX;
        }
        if (exp == null || !exp.isAfter(now)) {
            value.isDeletedInTransaction = true;
        } else {
            value.expiry = exp;
        }
        return respSerializer.respInteger(1);
    }

    private String handlePersistCommandTransactional(String[] command, Map<String, Value> map, Store localStore) {
        if (command.length != 2) {
            return wrongNumberOfArguments("persist");
        }
        Value value = resolveTransactionalValue(command[1], map, localStore);
        if (value == null || value.expiry.equals(LocalDateTime.MAX)) {
            return respSerializer.respInteger(0);
        }
        value.expiry = LocalDateTime.MAX;
        return respSerializer.respInteger(1);
    }

    private String handleTtlCommandTransactional(String[] command, Map<String, Value> map,
                                                 Store localStore, boolean millis) {
        if (command.length != 2) {
            return wrongNumberOfArguments(millis ? "pttl" : "ttl");
        }
        Value value = resolveTransactionalValue(command[1], map, localStore);
        if (value == null) {
            return respSerializer.respInteger(-2);
        }
        if (value.expiry.equals(LocalDateTime.MAX)) {
            return respSerializer.respInteger(-1);
        }
        long remaining;
        try {
            remaining = Duration.between(LocalDateTime.now(), value.expiry).toMillis();
        } catch (ArithmeticException tooFarToMeasure) {
            remaining = Long.MAX_VALUE;
        }
        if (remaining < 0) {
            return respSerializer.respInteger(-2);
        }
        return respSerializer.respInteger(millis ? remaining : Store.roundedSeconds(remaining));
    }

    private String handleDelCommandTransactional(String[] command, Map<String, Value> map, RespSerializer localSerializer, Store localStore) {
        if (command.length != 2) {
            return wrongNumberOfArguments("del");
        }
        String key = command[1];
        Value valueToUse;
        Value cachedValue = map.getOrDefault(key, null);
        if(cachedValue==null){
            Value storeValue = localStore.getValue(key);
            if(storeValue == null){
                //both are null
                return "-ERR deleting and invalid key\r\n";
            }else{
                valueToUse = storeValue.copy();
            }
        }else{
            valueToUse = cachedValue;
        }
        valueToUse.isDeletedInTransaction =true;
        map.put(key, valueToUse);
        return "+OK\r\n";
    }

    private String handleIncrCommandTransactional(
            String[] command,
            Map<String, Value> map,
            RespSerializer localSerializer,
            Store localStore) {
        if (command.length != 2) {
            return wrongNumberOfArguments("incr");
        }
        try{
            String key = command[1];
            Value valueToUse;
            Value cachedValue = map.getOrDefault(key, null);
            if(cachedValue==null){
                Value storeValue = localStore.getValue(key);
                if(storeValue==null) {
                    valueToUse = new Value("0", LocalDateTime.now(), LocalDateTime.MAX);
                }else{
                    valueToUse = storeValue.copy();
                }
            }else{
                valueToUse=cachedValue;
            }
            if (valueToUse.kind != Value.Kind.STRING) {
                return TypedOps.WRONGTYPE;
            }
            int val = Integer.parseInt(valueToUse.val);
            val++;
            valueToUse.val = String.valueOf(val);
            map.put(key, valueToUse);
            return respSerializer.respInteger(val);
        } catch (Exception e) {
            return "-ERR value is not an integer or out of range\r\n";
        }
    }
    private String handleGetCommandTransactional(
            String[] command,
            Map<String, Value> map,
            RespSerializer localSerializer,
            Store localStore) {
        if (command.length != 2) {
            return wrongNumberOfArguments("get");
        }
        String key = command[1];
        Value valueToUse;
        Value cachedValue = map.getOrDefault(key, null);
        if(cachedValue==null){
            Value storeValue = localStore.getValue(key);
            if(storeValue ==null){
                //both are null
                return localStore.get(key);
            }else{
                valueToUse = storeValue.copy();
                map.put(key, valueToUse);
            }
        }else{
            valueToUse = cachedValue;
        }
        if (valueToUse.kind != Value.Kind.STRING) {
            return TypedOps.WRONGTYPE;
        }
        return respSerializer.serializeBulkString(valueToUse.val);
    }
    private String handleSetCommandTransactional(
            String[] command,
            Map<String, Value> map,
            RespSerializer localSerializer,
            Store localStore) {
        // the same form a direct SET must have, so a queued write carries its expiry
        // into the transaction instead of having the option dropped on the way in
        SetForm form = parseSetForm(command);
        if (form.error() != null) {
            return form.error();
        }
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime expiry = form.expiryMillis() == null
                ? LocalDateTime.MAX
                : now.plus(form.expiryMillis(), ChronoUnit.MILLIS);
        map.put(form.key(), new Value(form.value(), now, expiry));
        return "+OK\r\n";
    }
}
