package Components.Server;

import Components.Repository.EvictionPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The memory ceiling and eviction policy as configuration: their defaults, and the values a
 * bad one is refused rather than quietly replaced by.
 */
class RedisConfigMemoryTest {

    @Test
    void theDefaultsAreNoLimitAndNoEviction() {
        RedisConfig config = new RedisConfig();
        assertEquals(0, config.getMaxmemory());
        assertEquals(EvictionPolicy.NOEVICTION, config.getMaxmemoryPolicy());
    }

    @Test
    void aNegativeCeilingIsRefused() {
        RedisConfig config = new RedisConfig();
        assertThrows(IllegalArgumentException.class, () -> config.setMaxmemory(-1));
    }

    @Test
    void aCeilingIsKeptAsGiven() {
        RedisConfig config = new RedisConfig();
        config.setMaxmemory(1024);
        assertEquals(1024, config.getMaxmemory());
    }

    @Test
    void aPolicyIsSetByNameInAnyCase() {
        RedisConfig config = new RedisConfig();
        config.setMaxmemoryPolicy("ALLKEYS-LRU");
        assertEquals(EvictionPolicy.ALLKEYS_LRU, config.getMaxmemoryPolicy());
    }

    @Test
    void anUnknownPolicyIsRefusedRatherThanDefaulted() {
        RedisConfig config = new RedisConfig();
        assertThrows(IllegalArgumentException.class, () -> config.setMaxmemoryPolicy("allkeys-mru"));
        // a refused name leaves the safe default in place
        assertEquals(EvictionPolicy.NOEVICTION, config.getMaxmemoryPolicy());
    }
}
