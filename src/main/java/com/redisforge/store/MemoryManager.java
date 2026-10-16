package com.redisforge.store;

import com.redisforge.server.RedisConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Decides whether a write fits the configured memory ceiling, and which keys to evict
 * when it does not.
 *
 * <p>A write is judged by the size the keyspace <em>would</em> have once it lands:
 * {@code current - the bytes the target key already holds + the bytes the new entry
 * would take}. When that is within {@code maxmemory} nothing is touched. When it is over
 * and the policy is {@link EvictionPolicy#NOEVICTION} the write is refused with
 * {@code -OOM}. Otherwise the policy's candidates are removed one at a time until the
 * write fits, and each removal is reported so the caller can write it down and pass it
 * on as the {@code DEL} it is.</p>
 *
 * <p>The manager never decides for a replica or for a replay: those paths apply what
 * their master or their file says, and re-applying a policy decision there would let a
 * replica drop keys its master still holds. Only the master's client writes come here.</p>
 *
 * <p>The admission runs inside the same hold of the replication backlog that applies and
 * records the write, so the keys it removes and the write they made room for reach the
 * file and the replicas in the one order they happened in.</p>
 */
@Component
public class MemoryManager {

    /** The refusal a write past {@code maxmemory} is answered with, spelled the way Redis spells it. */
    public static final String OOM_ERROR =
            "-OOM command not allowed when used memory > 'maxmemory'.\r\n";

    @Autowired
    public Store store;
    @Autowired
    public RedisConfig redisConfig;

    /**
     * The outcome of judging one write: an error to answer with, or the keys that were
     * evicted to admit it. Exactly one of the two is ever set.
     */
    public record Admission(String error, List<String> evictedKeys) {

        public boolean rejected() {
            return error != null;
        }
    }

    /**
     * Judges a write of {@code newEntryBytes} to {@code targetKey}.
     *
     * @return an admission with no keys when the write fits, an admission naming the keys
     *         to remove when it only fits after eviction, or a rejection when it cannot
     *         be admitted at all
     */
    public Admission admit(String targetKey, long newEntryBytes) {
        long max = redisConfig.getMaxmemory();
        if (max <= 0) {
            return admitDirectly();
        }
        EvictionPolicy policy = redisConfig.getMaxmemoryPolicy();
        long projected = store.currentBytes() - store.entryBytes(targetKey) + newEntryBytes;
        if (projected <= max) {
            return admitDirectly();
        }
        if (policy == EvictionPolicy.NOEVICTION) {
            return rejected();
        }

        List<String> evicted = new ArrayList<>();
        // the target and everything already considered are off limits: the target is the
        // write being admitted, and a key that did not free enough must not be looked at
        // twice or the loop would never make progress
        Set<String> unavailable = new HashSet<>();
        unavailable.add(targetKey);
        LocalDateTime now = LocalDateTime.now();
        while (projected > max) {
            String victim = selectVictim(policy, unavailable, now);
            if (victim == null) {
                // the policy has nothing left it may remove, so the write cannot be made
                // to fit and is refused exactly as under noeviction
                return rejected();
            }
            Value value = store.peekValue(victim);
            long victimBytes = store.entryBytes(victim);
            boolean live = value != null && !value.expiry.isBefore(now);
            store.delete(victim);
            projected -= victimBytes;
            unavailable.add(victim);
            if (live) {
                // a key whose deadline had already passed is cleanup, not an eviction, so
                // it frees the bytes without counting towards evicted_keys
                store.recordEviction();
                evicted.add(victim);
            }
        }
        return new Admission(null, evicted);
    }

    /**
     * Judges a whole transaction against the ceiling, from the view it would commit.
     *
     * <p>No key is evicted: a transaction is admitted as a unit or refused as one. When
     * the state it would leave is over the limit the transaction is refused with
     * {@code -OOM} and commits nothing, rather than being made to fit by removing keys
     * the queued commands never mentioned.</p>
     *
     * @return {@code null} when the transaction fits, otherwise the error to answer EXEC
     *         with
     */
    public String admitTransaction(Map<String, Value> view) {
        long max = redisConfig.getMaxmemory();
        if (max <= 0) {
            return null;
        }
        long projected = store.currentBytes();
        for (Map.Entry<String, Value> entry : view.entrySet()) {
            String key = entry.getKey();
            Value value = entry.getValue();
            long previous = store.entryBytes(key);
            long next = value.isDeletedInTransaction ? 0
                    : MemoryEstimator.entryBytes(key, value);
            projected += next - previous;
        }
        return projected <= max ? null : OOM_ERROR;
    }

    private Admission admitDirectly() {
        return new Admission(null, List.of());
    }

    private Admission rejected() {
        return new Admission(OOM_ERROR, List.of());
    }

    /**
     * The one key the policy would remove next, or {@code null} when it has no candidate.
     *
     * <p>The candidates are the keys the policy may touch - every key for an
     * {@code allkeys-} policy, only keyed-with-a-deadline for a {@code volatile-} one -
     * less the target and anything already considered. The oldest touch decides an LRU
     * policy, the nearest deadline a TTL one, and a uniformly drawn key a random one.</p>
     */
    private String selectVictim(EvictionPolicy policy, Set<String> unavailable, LocalDateTime now) {
        String chosen = null;
        long oldest = Long.MAX_VALUE;
        LocalDateTime nearestDeadline = null;
        long seen = 0;
        for (Map.Entry<String, Value> entry : store.map.entrySet()) {
            String key = entry.getKey();
            if (unavailable.contains(key)) {
                continue;
            }
            Value value = entry.getValue();
            if (value == null) {
                continue;
            }
            if (policy.volatileOnly() && !MemoryEstimator.hasDeadline(value.expiry)) {
                continue;
            }
            switch (policy) {
                case ALLKEYS_LRU, VOLATILE_LRU -> {
                    if (value.lastAccess < oldest) {
                        oldest = value.lastAccess;
                        chosen = key;
                    }
                }
                case VOLATILE_TTL -> {
                    if (nearestDeadline == null || value.expiry.isBefore(nearestDeadline)) {
                        nearestDeadline = value.expiry;
                        chosen = key;
                    }
                }
                case ALLKEYS_RANDOM, VOLATILE_RANDOM -> {
                    // reservoir sampling: after n candidates each has had a 1/n chance of
                    // being the one held, so the survivor is uniform over all of them
                    seen++;
                    if (ThreadLocalRandom.current().nextLong(seen) == 0) {
                        chosen = key;
                    }
                }
                default -> {
                    // NOEVICTION never asks for a victim
                }
            }
        }
        return chosen;
    }
}
