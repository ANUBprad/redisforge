package Components.Repository;

import Components.Infra.Client;
import Components.Service.RespSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
public class Store {
    private static final Logger logger = Logger.getLogger(Store.class.getName());
    /**
     * How many locks the keys of this store are spread over. A fixed number: the lock
     * striping never grows with the keyspace, so a server that has written millions of
     * one-off keys holds no more lock objects than one that has written a handful.
     */
    private static final int KEY_LOCK_COUNT = 256;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    // one stripe of locks guards a share of the keys, so a compound operation on a key
    // cannot interleave with another one on the same key while keys keep running
    // independently of each other. A fixed array rather than a per-key map, because a
    // lock per key would be one more unbounded map to grow and then try to shrink. A
    // ReentrantLock rather than a monitor, because a transaction has to hold the locks
    // of several stripes at once and release them one by one at the end.
    private final ReentrantLock[] keyLocks = createKeyLocks();
    public ConcurrentHashMap<String, Value> map;
    @Autowired
    public RespSerializer respSerializer;
    public Store(){
        map = new ConcurrentHashMap<>();
    }

    private static ReentrantLock[] createKeyLocks() {
        ReentrantLock[] stripes = new ReentrantLock[KEY_LOCK_COUNT];
        for (int i = 0; i < stripes.length; i++) {
            stripes[i] = new ReentrantLock();
        }
        return stripes;
    }

    /** The stripe a key's lock lives on. */
    private static int stripeOf(String key) {
        // & 0x7fffffff keeps a hash of Integer.MIN_VALUE from producing a negative
        // index, whatever the key's hash happens to be
        return (key.hashCode() & 0x7fffffff) % KEY_LOCK_COUNT;
    }

    /** The lock that guards a single key. Held across a read followed by a write. */
    public ReentrantLock lockFor(String key) {
        return keyLocks[stripeOf(key)];
    }

    /** How many distinct locks the store spreads its keys over, which never grows. */
    public int keyLockCount() {
        return KEY_LOCK_COUNT;
    }

    /**
     * A snapshot of the keys the store currently holds, safe to iterate without
     * holding a lock of any kind: a key written after this call is not in it.
     */
    public Set<String> getKeys(){
        rwLock.readLock().lock();
        try{
            return Set.copyOf(map.keySet());
        }finally{
            rwLock.readLock().unlock();
        }
    }

    public String set(String key, String val){
        ReentrantLock keyLock = lockFor(key);
        keyLock.lock();
        try {
            rwLock.writeLock().lock();
            try{
                Value value = new Value(val, LocalDateTime.now(), LocalDateTime.MAX);
                map.put(key, value);
                return "+OK\r\n";
            } catch (Exception e) {
                logger.log(Level.SEVERE, e.getMessage());
                return "$-1\r\n";
            }finally{
                rwLock.writeLock().unlock();
            }
        }finally{
            keyLock.unlock();
        }
    }

    public String set(String key, String val, int expiryMilliseconds){
        ReentrantLock keyLock = lockFor(key);
        keyLock.lock();
        try {
            rwLock.writeLock().lock();
            try{
                LocalDateTime now = LocalDateTime.now();
                LocalDateTime exp = now.plus(expiryMilliseconds, ChronoUnit.MILLIS);
                Value value = new Value(val, now, exp);
                map.put(key, value);
                return "+OK\r\n";
            } catch (Exception e) {
                logger.log(Level.SEVERE, e.getMessage());
                return "$-1\r\n";
            }finally{
                rwLock.writeLock().unlock();
            }
        }finally{
            keyLock.unlock();
        }
    }

    /**
     * Sets a key with a deadline that is already fixed, which is how an expiry that was
     * running when the server stopped is restored: the deadline was chosen when the key
     * was written, so it must survive the restart instead of starting over.
     */
    public void setAt(String key, String val, long expiryEpochMillis){
        ReentrantLock keyLock = lockFor(key);
        keyLock.lock();
        try {
            rwLock.writeLock().lock();
            try{
                LocalDateTime expiry = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(expiryEpochMillis), ZoneId.systemDefault());
                Value value = new Value(val, LocalDateTime.now(), expiry);
                map.put(key, value);
            } finally{
                rwLock.writeLock().unlock();
            }
        }finally{
            keyLock.unlock();
        }
    }

    /** Removes a key outright, the way a committed DEL inside a transaction does. */
    public void delete(String key) {
        ReentrantLock keyLock = lockFor(key);
        keyLock.lock();
        try {
            rwLock.writeLock().lock();
            try{
                map.remove(key);
            } finally{
                rwLock.writeLock().unlock();
            }
        }finally{
            keyLock.unlock();
        }
    }

    /**
     * Reads, parses, adds one and stores again as one step. A concurrent map would still
     * lose increments here: two callers can both read the same value and both store the
     * same successor. The key's lock is what makes the whole thing indivisible.
     */
    public String increment(String key) {
        ReentrantLock keyLock = lockFor(key);
        keyLock.lock();
        try {
            LocalDateTime now = LocalDateTime.now();
            Value current = map.get(key);
            if (current != null && current.expiry.isBefore(now)) {
                map.remove(key, current);
                current = null;
            }
            if (current == null) {
                map.put(key, new Value("1", now, LocalDateTime.MAX));
                return respSerializer.respInteger(1);
            }
            int incremented;
            try {
                incremented = Integer.parseInt(current.val) + 1;
            } catch (NumberFormatException notAnInteger) {
                return "-ERR value is not an integer or out of range\r\n";
            }
            // a counter keeps the expiry the key already carried
            map.put(key, new Value(String.valueOf(incremented), current.created, current.expiry));
            return respSerializer.respInteger(incremented);
        }finally{
            keyLock.unlock();
        }
    }

    public String get(String key){
        rwLock.readLock().lock();
        try{
            LocalDateTime now = LocalDateTime.now();
            Value value = map.get(key);

            if(value == null){
                return "$-1\r\n";
            }
            if(value.expiry.isBefore(now)){
                // remove this exact entry: a set that lands between the read and here must
                // not have its fresh value dropped by the cleanup
                map.remove(key, value);
                return "$-1\r\n";
            }
            return respSerializer.serializeBulkString(value.val);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
            return "$-1\r\n";
        }finally{
            rwLock.readLock().unlock();
        }
    }

    public Value getValue(String key) {
        rwLock.readLock().lock();
        try{
            LocalDateTime now = LocalDateTime.now();
            Value value = map.getOrDefault(key, null);

            if(value != null && value.expiry.isBefore(now)){
                map.remove(key, value);
                return null;
            }
            return value;
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
            return null;
        }finally{
            rwLock.readLock().unlock();
        }
    }

    /**
 * The entry as it is stored, whatever its deadline says. Unlike {@link #getValue(String)}
 * this does not treat a key that has expired as gone, because the caller here wants the
 * deadline itself: an expiry that has just passed still has to be recorded faithfully.
 */
public Value peekValue(String key) {
        rwLock.readLock().lock();
        try{
            return map.get(key);
        }finally{
            rwLock.readLock().unlock();
        }
    }

    public void executeTransaction(
            Client client,
            BiFunction<String[], Map<String, Value>, String> transactionCacheApplier
    ){
        Map<String, Value> localCache = new HashMap<>();
        List<String> responses = new ArrayList<>();
        // Every key this transaction touches is held for the whole apply and commit. A
        // single INCR or SET on the same key would otherwise be able to read the value the
        // transaction started from, and then write its result after the transaction had
        // already committed its own.
        //
        // The key locks come before the write lock, which is the order set and increment
        // take as well. Taking them the other way round would let a transaction and a
        // single command on a shared key wait for each other.
        List<ReentrantLock> held = lockKeysInOrder(client.commandQueue);
        rwLock.writeLock().lock();
        try{
            while(!client.commandQueue.isEmpty()){
                String[] command = client.commandQueue.poll();
                String response = transactionCacheApplier.apply(command, localCache);
                responses.add(response);
            }
            //control will only come here when the queue is empty, that means no other commands in the transaction left to be applied

            for(Map.Entry<String, Value> entry : localCache.entrySet()){
                String key = entry.getKey();
                Value value = entry.getValue();

                if(value.isDeletedInTransaction){
                    this.map.remove(key);
                }else{
                    this.map.put(key, value);
                }
            }

            client.transactionResponse.addAll(responses);
        }finally {
            rwLock.writeLock().unlock();
            for(int i = held.size() - 1; i >= 0; i--){
                held.get(i).unlock();
            }
        }
    }

    /**
     * Locks every stripe the queued commands name, in a fixed order, and returns the
     * locks so the caller can release them again. The fixed order is what keeps two
     * transactions that touch the same keys in opposite orders from waiting on each
     * other forever.
     *
     * <p>The stripes, not the keys, are what is acquired: two keys can share a stripe
     * and so one lock, which a thread must take only once, because it has to be
     * released the same number of times it was taken. Sorted by stripe index rather
     * than by key, because striped locks are shared between keys - ordering keys alone
     * would let two transactions walk into the same pair of locks from opposite sides.</p>
     */
    private List<ReentrantLock> lockKeysInOrder(Queue<String[]> commandQueue) {
        Set<Integer> stripes = new TreeSet<>();
        for (String[] command : new ArrayList<>(commandQueue)) {
            if (command.length > 1) {
                stripes.add(stripeOf(command[1]));
            }
        }

        List<ReentrantLock> held = new ArrayList<>(stripes.size());
        for (int stripe : stripes) {
            ReentrantLock keyLock = keyLocks[stripe];
            keyLock.lock();
            held.add(keyLock);
        }
        return held;
    }
}
