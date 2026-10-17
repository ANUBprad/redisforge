package com.redisforge;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Guards the packaged entry point.
 *
 * <p>The assembly manifest names {@code com.redisforge.Main} as its {@code Main-Class}. If
 * the source ever loses its {@code package} line it compiles into the default package, the
 * manifest stops resolving, and {@code java -jar redisforge.jar} fails at startup with a
 * {@code ClassNotFoundException} - which no ordinary test notices, because the tests drive
 * the Spring context directly. This test fails in that case instead.</p>
 */
class MainPackagingTest {

    @Test
    void theEntryPointNamedByTheManifestExistsInThePackagedNamespace() throws Exception {
        Class<?> main = Class.forName("com.redisforge.Main");
        assertEquals("com.redisforge", main.getPackageName(),
                "the manifest Main-Class is com.redisforge.Main but the class is not in that package");
        assertNotNull(main.getMethod("main", String[].class),
                "com.redisforge.Main has no main(String[]) method");
    }
}
