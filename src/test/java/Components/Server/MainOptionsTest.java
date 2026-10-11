package Components.Server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The command line parser: what a set of options changes, and how an option the server
 * cannot use is refused with a message that names it instead of being ignored or leaving
 * the parse to throw past the end of the argument list.
 */
class MainOptionsTest {

    private RedisConfig config;

    @BeforeEach
    void freshConfig() {
        config = new RedisConfig();
        config.setPort(6379);
        config.setRole("master");
    }

    @Test
    void theDefaultsStandWhenNothingIsPassed() {
        RedisForgeArgs.apply(config, new String[0]);
        assertEquals(6379, config.getPort());
        assertEquals("master", config.getRole());
    }

    @Test
    void aPortIsParsed() {
        RedisForgeArgs.apply(config, new String[]{"--port", "6380"});
        assertEquals(6380, config.getPort());
    }

    @Test
    void aReplicaPairIsParsed() {
        RedisForgeArgs.apply(config, new String[]{"--replicaof", "127.0.0.1 6379"});
        assertEquals("slave", config.getRole());
        assertEquals("127.0.0.1", config.getMasterHost());
        assertEquals(6379, config.getMasterPort());
    }

    @Test
    void aYesAndANumberAndAPolicyAllLand() {
        RedisForgeArgs.apply(config,
                new String[]{"--appendonly", "yes", "--maxmemory", "1024", "--max-clients", "5"});
        assertTrue(config.isAppendonly());
        assertEquals(1024, config.getMaxmemory());
        assertEquals(5, config.getMaxClients());
    }

    @Test
    void anUnknownOptionIsRefusedWithItsName() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> RedisForgeArgs.apply(config, new String[]{"--nope"}));
        assertTrue(e.getMessage().contains("--nope"), e.getMessage());
    }

    @Test
    void anOptionMissingItsValueIsRefusedWithItsName() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> RedisForgeArgs.apply(config, new String[]{"--port"}));
        assertTrue(e.getMessage().contains("--port"), e.getMessage());
    }

    @Test
    void aNonNumberIsRefusedWithTheOptionNamed() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> RedisForgeArgs.apply(config, new String[]{"--port", "not-a-port"}));
        assertTrue(e.getMessage().contains("--port"), e.getMessage());
    }

    @Test
    void aBadPolicyIsRefusedRatherThanIgnored() {
        assertThrows(IllegalArgumentException.class,
                () -> RedisForgeArgs.apply(config, new String[]{"--maxmemory-policy", "allkeys-mru"}));
    }

    @Test
    void aBadFsyncPolicyIsRefusedRatherThanIgnored() {
        assertThrows(IllegalArgumentException.class,
                () -> RedisForgeArgs.apply(config, new String[]{"--appendfsync", "sometimes"}));
    }

    @Test
    void aBadBooleanIsRefusedWithTheOptionNamed() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> RedisForgeArgs.apply(config, new String[]{"--appendonly", "maybe"}));
        assertTrue(e.getMessage().contains("--appendonly"), e.getMessage());
    }

    @Test
    void aBadReplicaPairIsRefused() {
        assertThrows(IllegalArgumentException.class,
                () -> RedisForgeArgs.apply(config, new String[]{"--replicaof", "127.0.0.1"}));
        assertThrows(IllegalArgumentException.class,
                () -> RedisForgeArgs.apply(config, new String[]{"--replicaof", "127.0.0.1 not-a-port"}));
    }

    @Test
    void anAbsurdTimeoutIsRefusedInsteadOfOverflowing() {
        assertThrows(IllegalArgumentException.class,
                () -> RedisForgeArgs.apply(config, new String[]{"--timeout", "9999999999"}));
    }
}