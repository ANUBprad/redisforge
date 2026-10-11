package Components.Repository;

import Components.Infra.Client;
import Components.Infra.StoreSnapshot;
import Components.Service.RespSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;
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
    // volatile so that a snapshot install, which swaps in a fresh map, is seen whole by
    // a reader that never took a lock of this store, and so a reference is never torn
    public volatile ConcurrentHashMap<String, Value> map;
    /**
     * The bytes the keyspace is charged, by the same account {@link MemoryEstimator}
     * keeps. Every write that puts, replaces or removes an entry moves it so it always
     * describes what {@link #map} holds, even for keys whose deadline has passed but
     * has not been cleaned up yet.
     */
    private final AtomicLong datasetBytes = new AtomicLong();
    /**
     * The last step of the access sequence that was handed out. Every touch of a key
     * takes the next step, so the sequence never moves other than forwards and the
     * least recently used key is the one with the smallest {@link Value#lastAccess}.
     */
    private final AtomicLong accessSequence = new AtomicLong();
    /** How many keys the eviction policy has removed to admit a write. */
    private final AtomicLong evictedKeys = new AtomicLong();
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

    /** The logical bytes the keyspace is currently charged, used to judge eviction. */
    public long currentBytes() {
        return datasetBytes.get();
    }

    /** How many keys the eviction policy has removed to make room since the store started. */
    public long evictedKeys() {
        return evictedKeys.get();
    }

    /** Counts one key removed by the eviction policy, for {@code INFO memory}. */
    public void recordEviction() {
        evictedKeys.incrementAndGet();
    }

    /**
     * The logical bytes one key contributes right now: the size of its current entry, or
     * zero when the key is not in the map. The expiry is deliberately ignored, because
     * a key whose deadline has passed may still be in the map, and its bytes are still
     * counted in {@link #currentBytes()} until the cleanup takes them out.
     */
    public long entryBytes(String key) {
        Value value = map.get(key);
        return value == null ? 0 : bytesOf(key, value);
    }

    /** The logical bytes of one concrete entry. */
    private long bytesOf(String key, Value value) {
        return MemoryEstimator.entryBytes(key, value.val, value.expiry);
    }

    /** Records that a key was used, at the next step of the access sequence. */
    private void touch(Value value) {
        if (value != null) {
            value.lastAccess = accessSequence.incrementAndGet();
        }
    }

    public String set(String key, String val){
        ReentrantLock keyLock = lockFor(key);
        keyLock.lock();
        try {
            rwLock.writeLock().lock();
            try{
                Value value = new Value(val, LocalDateTime.now(), LocalDateTime.MAX);
                Value previous = map.put(key, value);
                datasetBytes.addAndGet(bytesOf(key, value) - (previous == null ? 0 : bytesOf(key, previous)));
                touch(value);
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
                Value previous = map.put(key, value);
                datasetBytes.addAndGet(bytesOf(key, value) - (previous == null ? 0 : bytesOf(key, previous)));
                touch(value);
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
                Value previous = map.put(key, value);
                datasetBytes.addAndGet(bytesOf(key, value) - (previous == null ? 0 : bytesOf(key, previous)));
                touch(value);
            } finally{
                rwLock.writeLock().unlock();
            }
        }finally{
            keyLock.unlock();
        }
    }

    /**
     * Removes a key outright; {@code true} when the key was actually there. The eviction
     * policy and a committed DEL inside a transaction are the two callers.
     */
    public boolean delete(String key) {
        ReentrantLock keyLock = lockFor(key);
        keyLock.lock();
        try {
            rwLock.writeLock().lock();
            try{
                Value removed = map.remove(key);
                if (removed != null) {
                    datasetBytes.addAndGet(-bytesOf(key, removed));
                }
                return removed != null;
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
                if (map.remove(key, current)) {
                    datasetBytes.addAndGet(-bytesOf(key, current));
                }
                current = null;
            }
            if (current == null) {
                Value created = new Value("1", now, LocalDateTime.MAX);
                map.put(key, created);
                datasetBytes.addAndGet(bytesOf(key, created));
                touch(created);
                return respSerializer.respInteger(1);
            }
            int incremented;
            try {
                incremented = Integer.parseInt(current.val) + 1;
            } catch (NumberFormatException notAnInteger) {
                return "-ERR value is not an integer or out of range\r\n";
            }
            // a counter keeps the expiry the key already carried
            Value successor = new Value(String.valueOf(incremented), current.created, current.expiry);
            map.put(key, successor);
            datasetBytes.addAndGet(bytesOf(key, successor) - bytesOf(key, current));
            touch(successor);
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
                if (map.remove(key, value)) {
                    datasetBytes.addAndGet(-bytesOf(key, value));
                }
                return "$-1\r\n";
            }
            touch(value);
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
                if (map.remove(key, value)) {
                    datasetBytes.addAndGet(-bytesOf(key, value));
                }
                return null;
            }
            touch(value);
            return value;
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
            return null;
        }finally{
            rwLock.readLock().unlock();
        }
    }

    /**
     * The seconds left on a key, rounded the way Redis rounds them. A key that is gone,
     * or whose deadline has already passed, answers {@code -2}; one with no deadline
     * answers {@code -1}.
     */
    public String ttl(String key) {
        Long remaining = remainingMillis(key);
        if (remaining == null) return respSerializer.respInteger(-2);
        if (remaining < 0) return respSerializer.respInteger(-1);
        // Redis rounds the seconds to the nearest one rather than truncating
        return respSerializer.respInteger((int) ((remaining + 500) / 1000));
    }

    public String pttl(String key) {
        Long remaining = remainingMillis(key);
        if (remaining == null) return respSerializer.respInteger(-2);
        if (remaining < 0) return respSerializer.respInteger(-1);
        return respSerializer.respInteger((int) (long) remaining);
    }

    /**
     * Milliseconds left on a key, or {@code null} when the key is gone (including one
     * whose deadline has already passed and is cleaned up here), or {@code -1} when it
     * carries no deadline at all.
     */
    private Long remainingMillis(String key) {
        LocalDateTime now = LocalDateTime.now();
        Value value = map.get(key);
        if (value == null) return null;
        if (value.expiry.isBefore(now)) {
            if (map.remove(key, value)) {
                datasetBytes.addAndGet(-bytesOf(key, value));
            }
            return null;
        }
        if (value.expiry.equals(LocalDateTime.MAX)) return -1L;
        touch(value);
        return Duration.between(now, value.expiry).toMillis();
    }

    public int expire(String key, int seconds) {
        return setExpire(key, seconds <= 0 ? null : LocalDateTime.now().plusSeconds(seconds));
    }

    public int pexpire(String key, long milliseconds) {
        return setExpire(key, milliseconds <= 0 ? null
                : LocalDateTime.now().plus(milliseconds, ChronoUnit.MILLIS));
    }

    public int expireAt(String key, long unixTimeSeconds) {
        return setExpire(key, absoluteOrClamp(Instant.ofEpochSecond(wrapSeconds(unixTimeSeconds))));
    }

    public int pexpireAt(String key, long unixTimeMillis) {
        return setExpire(key, absoluteOrClamp(Instant.ofEpochMilli(unixTimeMillis)));
    }

    /**
     * A deadline far enough out to overflow a calendar date is kept as "no deadline" rather
     * than allowed to throw: the key lives, which is what a date that far in the future
     * means. A deadline before the epoch is pinned to the start so it reads as passed.
     */
    public static LocalDateTime absoluteOrClamp(Instant instant) {
        try {
            return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        } catch (RuntimeException outOfRange) {
            return instant.isAfter(Instant.now()) ? LocalDateTime.MAX : LocalDateTime.MIN;
        }
    }

    /** {@link Instant#ofEpochSecond} throws on overflow, so an absurd second count is clamped. */
    public static long wrapSeconds(long unixTimeSeconds) {
        long maxSeconds = Instant.MAX.getEpochSecond();
        long minSeconds = Instant.MIN.getEpochSecond();
        if (unixTimeSeconds > maxSeconds) return maxSeconds;
        if (unixTimeSeconds < minSeconds) return minSeconds;
        return unixTimeSeconds;
    }

    /**
     * Puts a deadline on an existing, unexpired key. A {@code null} deadline, or one that
     * is not in the future, means the key must go now. Returns the number of keys the
     * command changed: {@code 1} when it acted, {@code 0} when the key was absent.
     */
    private int setExpire(String key, LocalDateTime exp) {
        ReentrantLock keyLock = lockFor(key);
        keyLock.lock();
        try {
            rwLock.writeLock().lock();
            try {
                Value value = map.get(key);
                if (value == null) return 0;
                LocalDateTime now = LocalDateTime.now();
                if (value.expiry.isBefore(now)) {
                    if (map.remove(key, value)) {
                        datasetBytes.addAndGet(-bytesOf(key, value));
                    }
                    return 0;
                }
                if (exp == null || !exp.isAfter(now)) {
                    if (map.remove(key, value)) {
                        datasetBytes.addAndGet(-bytesOf(key, value));
                    }
                    return 1;
                }
                Value successor = new Value(value.val, value.created, exp);
                map.put(key, successor);
                datasetBytes.addAndGet(bytesOf(key, successor) - bytesOf(key, value));
                touch(successor);
                return 1;
            } finally {
                rwLock.writeLock().unlock();
            }
        } finally {
            keyLock.unlock();
        }
    }

    public int persist(String key) {
        ReentrantLock keyLock = lockFor(key);
        keyLock.lock();
        try {
            rwLock.writeLock().lock();
            try {
                Value value = map.get(key);
                if (value == null) return 0;
                LocalDateTime now = LocalDateTime.now();
                if (value.expiry.isBefore(now)) {
                    if (map.remove(key, value)) {
                        datasetBytes.addAndGet(-bytesOf(key, value));
                    }
                    return 0;
                }
                if (value.expiry.equals(LocalDateTime.MAX)) return 0;
                Value successor = new Value(value.val, value.created, LocalDateTime.MAX);
                map.put(key, successor);
                datasetBytes.addAndGet(bytesOf(key, successor) - bytesOf(key, value));
                touch(successor);
                return 1;
            } finally {
                rwLock.writeLock().unlock();
            }
        } finally {
            keyLock.unlock();
        }
    }

    /**
     * The epoch millis of a value's deadline, or {@code null} for one that has none.
     * A deadline so far out it stopped meaning "sometime" is treated as none, the same
     * way {@link #persist} and the AOF treat it.
     */
    public Long absoluteExpiryMillis(Value value) {
        if (value == null || value.expiry == null || value.expiry.equals(LocalDateTime.MAX)) return null;
        return value.expiry.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    /**
     * The live keys as snapshot entries, captured under the read lock at one instant so
     * every deadline is the one it carried then. Keys whose deadline had already passed
     * are left out: a full resync hands over what a fresh connection would read, not
     * what the collection still happens to hold, and the entries carry absolute
     * deadlines so the replica's clock replaces the master's once they are installed.
     *
     * <p>Concurrent reads can still clean an expired key out from under this, but never a
     * live one: a key that is not in the snapshot was already gone, and a key whose
     * deadline this passes over has no business in it. The caller is expected to hold
     * the replication backlog while it runs, which excludes every other data write, so
     * the map reference cannot be swapped out from under the iteration.</p>
     */
    public List<StoreSnapshot.Entry> liveSnapshot() {
        LocalDateTime now = LocalDateTime.now();
        List<StoreSnapshot.Entry> entries = new ArrayList<>();
        rwLock.readLock().lock();
        try {
            for (Map.Entry<String, Value> entry : map.entrySet()) {
                Value value = entry.getValue();
                if (value == null || value.expiry.isBefore(now)) continue;
                entries.add(new StoreSnapshot.Entry(
                        entry.getKey(), value.val, absoluteExpiryMillis(value)));
            }
        } finally {
            rwLock.readLock().unlock();
        }
        entries.sort(Comparator.comparing(StoreSnapshot.Entry::key));
        return entries;
    }

    /**
     * Replaces the whole contents of the store with the keys a full resync carried. The
     * fresh map is built outside the write lock and swapped in under it, so readers see
     * either the old keys or the new ones, never a mixture, while a long installation
     * does not stall reads of the keys that are still in place.
     *
     * <p>Only the entries the install arrives with survive: keys the master did not hand
     * over are gone, which is what a full resync means. A deadline that has already
     * passed by the time the snapshot lands is not resurrected - the master would
     * consider it expired too by then. The caller is expected to hold the replication
     * backlog while it runs, so a downstream resync that serializes this store and an
     * install can never interleave.</p>
     */
    public void installSnapshot(List<StoreSnapshot.Entry> entries) {
        LocalDateTime now = LocalDateTime.now();
        ConcurrentHashMap<String, Value> fresh = new ConcurrentHashMap<>(
                Math.max(16, entries.size() * 2));
        long bytes = 0;
        for (StoreSnapshot.Entry entry : entries) {
            if (entry.deadlineEpochMillis() == null) {
                Value value = new Value(entry.value(), now, LocalDateTime.MAX);
                fresh.put(entry.key(), value);
                bytes += bytesOf(entry.key(), value);
            } else {
                LocalDateTime expiry = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(entry.deadlineEpochMillis()), ZoneId.systemDefault());
                if (!expiry.isBefore(now)) {
                    Value value = new Value(entry.value(), now, expiry);
                    fresh.put(entry.key(), value);
                    bytes += bytesOf(entry.key(), value);
                }
            }
        }
        rwLock.writeLock().lock();
        try {
            this.map = fresh;
            // the swapped map is what the store counts from now on: the old bytes are gone
            // with the old map, and the new ones are the sum of the entries that landed
            datasetBytes.set(bytes);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void removeExpired() {
        List<String> candidates = new ArrayList<>();
        rwLock.readLock().lock();
        try {
            int count = 0;
            for (String k : map.keySet()) {
                if (count++ >= 100) break;
                candidates.add(k);
            }
        } finally {
            rwLock.readLock().unlock();
        }
        LocalDateTime now = LocalDateTime.now();
        for (String k : candidates) {
            ReentrantLock lock = lockFor(k);
            lock.lock();
            try {
                rwLock.writeLock().lock();
                try {
                    Value v = map.get(k);
                    if (v != null && v.expiry.isBefore(now) && map.remove(k, v)) {
                        datasetBytes.addAndGet(-bytesOf(k, v));
                    }
                } finally {
                    rwLock.writeLock().unlock();
                }
            } finally {
                lock.unlock();
            }
        }
    }
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
        executeTransaction(client, transactionCacheApplier, null);
    }

    /**
     * Applies a whole transaction, or refuses it whole.
     *
     * <p>The queued commands are applied to a private view first, so the replies are known
     * before anything is committed. When an {@code admission} is given it is asked once,
     * against that finished view, whether the resulting keyspace would fit the memory
     * limit. A refusal commits nothing and answers the transaction with the error the
     * admission returned: a transaction is admitted as a unit or not at all, because
     * partially applying it to make it fit would leave a state no command asked for. A
     * {@code null} admission admits everything, which is what a store without a limit asks
     * for.</p>
     *
     * @return the error the admission refused with, or {@code null} when the transaction
     *         committed
     */
    public String executeTransaction(
            Client client,
            BiFunction<String[], Map<String, Value>, String> transactionCacheApplier,
            Function<Map<String, Value>, String> admission
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

            if(admission != null){
                String error = admission.apply(localCache);
                if(error != null){
                    // the whole transaction is refused: nothing is committed and no reply
                    // is recorded, so the caller answers EXEC with this error
                    return error;
                }
            }

            for(Map.Entry<String, Value> entry : localCache.entrySet()){
                String key = entry.getKey();
                Value value = entry.getValue();

                if(value.isDeletedInTransaction){
                    Value removed = this.map.remove(key);
                    if(removed != null){
                        datasetBytes.addAndGet(-bytesOf(key, removed));
                    }
                }else{
                    Value previous = this.map.put(key, value);
                    datasetBytes.addAndGet(bytesOf(key, value) - (previous == null ? 0 : bytesOf(key, previous)));
                    touch(value);
                }
            }

            client.transactionResponse.addAll(responses);
            return null;
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
