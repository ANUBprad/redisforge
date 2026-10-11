package Components.Repository;

import java.util.Locale;

/**
 * The rules under which a write that would put the store past {@code maxmemory} is
 * admitted by evicting keys first, named the way Redis names them.
 *
 * <p>Every policy except {@link #NOEVICTION} first removes the keys it is allowed to,
 * one at a time, until the write fits. {@link #VOLATILE_LRU}, {@link #VOLATILE_RANDOM}
 * and {@link #VOLATILE_TTL} may only remove keys that carry a deadline; the other two
 * may remove any key. When the policy's candidate set is empty — there is nothing to
 * evict, or no volatile key exists — the write is refused with the same error
 * {@link #NOEVICTION} always answers with.</p>
 */
public enum EvictionPolicy {

    NOEVICTION("noeviction", false),
    ALLKEYS_LRU("allkeys-lru", false),
    ALLKEYS_RANDOM("allkeys-random", false),
    VOLATILE_LRU("volatile-lru", true),
    VOLATILE_RANDOM("volatile-random", true),
    VOLATILE_TTL("volatile-ttl", true);

    /** The value {@code INFO memory} and the command line spell this policy as. */
    private final String configName;
    /** Whether this policy may only evict keys that carry a deadline. */
    private final boolean volatileOnly;

    EvictionPolicy(String configName, boolean volatileOnly) {
        this.configName = configName;
        this.volatileOnly = volatileOnly;
    }

    public String configName() {
        return configName;
    }

    /**
     * Whether this policy limits eviction to keys that carry a deadline.
     */
    public boolean volatileOnly() {
        return volatileOnly;
    }

    /**
     * Reads the name a configuration carries, in any letter case.
     *
     * @param name a policy name such as {@code allkeys-lru}
     * @return the policy, or {@code null} when the name is not one this server knows
     */
    public static EvictionPolicy parse(String name) {
        if (name == null) {
            return null;
        }
        String normalized = name.trim().toLowerCase(Locale.ROOT);
        for (EvictionPolicy policy : values()) {
            if (policy.configName.equals(normalized)) {
                return policy;
            }
        }
        return null;
    }
}