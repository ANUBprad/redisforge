package com.redisforge.store;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The size account one entry is charged: a fixed overhead, the UTF-8 bytes of the key
 * and the value, and the deadline metadata only when the key really carries a deadline.
 */
class MemoryEstimatorTest {

    @Test
    void countsTheOverheadAndTheKeyAndValueBytes() {
        assertEquals(MemoryEstimator.ENTRY_OVERHEAD_BYTES + 1 + 1,
                MemoryEstimator.entryBytes("k", "v", LocalDateTime.MAX));
    }

    @Test
    void countsUtf8BytesRatherThanCharacters() {
        // 'e' with an acute accent is one character but two UTF-8 bytes, and the estimate
        // sizes what the bytes cost, not what the string counts
        String twoByteCharacter = "\u00e9";
        assertEquals(MemoryEstimator.ENTRY_OVERHEAD_BYTES + 1 + 2,
                MemoryEstimator.entryBytes("k", twoByteCharacter, LocalDateTime.MAX));
    }

    @Test
    void addsTheDeadlineMetadataOnlyForARealDeadline() {
        long base = MemoryEstimator.ENTRY_OVERHEAD_BYTES + 1 + 1;
        assertEquals(base + MemoryEstimator.EXPIRY_METADATA_BYTES,
                MemoryEstimator.entryBytes("k", "v", LocalDateTime.now().plusSeconds(30)));
        assertEquals(base, MemoryEstimator.entryBytes("k", "v", null));
        assertEquals(base, MemoryEstimator.entryBytes("k", "v", LocalDateTime.MAX));
    }

    @Test
    void aNullValueContributesNoValueBytes() {
        assertEquals(MemoryEstimator.ENTRY_OVERHEAD_BYTES + 1,
                MemoryEstimator.entryBytes("k", null, LocalDateTime.MAX));
    }

    @Test
    void hasDeadlineTreatsTheSentinelAndNullAsNoDeadline() {
        assertFalse(MemoryEstimator.hasDeadline(null));
        assertFalse(MemoryEstimator.hasDeadline(LocalDateTime.MAX));
        assertTrue(MemoryEstimator.hasDeadline(LocalDateTime.now().plusSeconds(1)));
    }
}
