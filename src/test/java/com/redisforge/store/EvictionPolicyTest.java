package com.redisforge.store;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The names a policy is spelled with, and which of them may only remove keys that carry
 * a deadline.
 */
class EvictionPolicyTest {

    @Test
    void parsesEveryPolicyName() {
        assertEquals(EvictionPolicy.NOEVICTION, EvictionPolicy.parse("noeviction"));
        assertEquals(EvictionPolicy.ALLKEYS_LRU, EvictionPolicy.parse("allkeys-lru"));
        assertEquals(EvictionPolicy.ALLKEYS_RANDOM, EvictionPolicy.parse("allkeys-random"));
        assertEquals(EvictionPolicy.VOLATILE_LRU, EvictionPolicy.parse("volatile-lru"));
        assertEquals(EvictionPolicy.VOLATILE_RANDOM, EvictionPolicy.parse("volatile-random"));
        assertEquals(EvictionPolicy.VOLATILE_TTL, EvictionPolicy.parse("volatile-ttl"));
    }

    @Test
    void parsingIgnoresCaseAndSurroundingSpace() {
        assertEquals(EvictionPolicy.ALLKEYS_LRU, EvictionPolicy.parse("ALLKEYS-LRU"));
        assertEquals(EvictionPolicy.VOLATILE_TTL, EvictionPolicy.parse("  Volatile-TTL  "));
    }

    @Test
    void anUnknownNameIsNotAPolicy() {
        assertNull(EvictionPolicy.parse("allkeys-mru"));
        assertNull(EvictionPolicy.parse(""));
        assertNull(EvictionPolicy.parse(null));
    }

    @Test
    void onlyTheVolatilePoliciesAreLimitedToKeysWithADeadline() {
        assertFalse(EvictionPolicy.NOEVICTION.volatileOnly());
        assertFalse(EvictionPolicy.ALLKEYS_LRU.volatileOnly());
        assertFalse(EvictionPolicy.ALLKEYS_RANDOM.volatileOnly());
        assertTrue(EvictionPolicy.VOLATILE_LRU.volatileOnly());
        assertTrue(EvictionPolicy.VOLATILE_RANDOM.volatileOnly());
        assertTrue(EvictionPolicy.VOLATILE_TTL.volatileOnly());
    }

    @Test
    void configNameRoundTripsThroughParse() {
        for (EvictionPolicy policy : EvictionPolicy.values()) {
            assertEquals(policy, EvictionPolicy.parse(policy.configName()));
            assertEquals(policy.configName(), policy.configName().toLowerCase());
        }
    }
}
