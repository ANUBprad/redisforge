package Components.Infra;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The window a replica resumes from, on its own: what it keeps, what it lets go of, what
 * it hands back for a position at the edge of either, and what it may change about itself.
 *
 * <p>Positions here are replication offsets, so every append moves the window forward by
 * exactly the bytes it took in and a byte of the stream always lives at its own position
 * modulo the capacity. The tests work through the edges a real stream reaches - the window
 * filling, wrapping, an append bigger than the window, and a reader running alongside a
 * writer - because those are the places a resume would be handed the wrong bytes.</p>
 */
class ReplicationBacklogTest {

    private static final int DEFAULT_CAPACITY = 1 << 10;

    @Test
    void aPositionInsideTheWindowIsReadBackByteForByte() {
        ReplicationBacklog backlog = new ReplicationBacklog(DEFAULT_CAPACITY);
        byte[] first = bytes("SET before the window moved");
        byte[] second = bytes("SET after it");
        backlog.append(first);
        backlog.append(second);

        // a position reads through to the end of the stream, which is what a replica that
        // stopped there is given: everything it has not seen, in the order it was written
        assertArrayEquals(concat(first, second), backlog.read(0),
                "the stream did not come back whole from the start");
        assertArrayEquals(second, backlog.read(first.length),
                "the second frame did not come back whole from its own position");
        byte[] whole = concat(first, second);
        assertArrayEquals(
                Arrays.copyOfRange(whole, first.length + 3, whole.length),
                backlog.read(first.length + 3),
                "a position in the middle of a frame did not read on to the end of the stream");
        assertEquals(0, backlog.startOffset(), "nothing had fallen out, so the window should start at the stream's start");
        assertEquals(whole.length, backlog.endOffset());
    }

    @Test
    void thePositionTheStreamHasReachedIsCoveredAndCarriesNoBytes() {
        ReplicationBacklog backlog = new ReplicationBacklog(DEFAULT_CAPACITY);
        byte[] frame = bytes("SET a 1");
        backlog.append(frame);
        long end = backlog.endOffset();

        assertTrue(backlog.covers(end), "the newest position is not resumable");
        assertArrayEquals(new byte[0], backlog.read(end),
                "a replica that stopped on the newest byte should be resumed with nothing to carry");
        assertFalse(backlog.covers(end + 1), "a position past the end of the stream was offered");
        assertNull(backlog.read(end + 1), "a position past the end of the stream was read");
    }

    @Test
    void aPositionThatHasAlreadyFallenOutOfTheWindowIsNotCovered() {
        ReplicationBacklog backlog = new ReplicationBacklog(DEFAULT_CAPACITY);
        backlog.append(bytes("SET a 1"));

        assertFalse(backlog.covers(-1), "a position before the stream was offered");
        assertNull(backlog.read(-1), "a position before the stream was read");

        // enough to push the window past the position the stream started at
        byte[] filler = new byte[DEFAULT_CAPACITY + 64];
        Arrays.fill(filler, (byte) 'x');
        backlog.append(filler);
        long start = backlog.startOffset();
        assertTrue(start > 1, "the window did not slide past the start of the stream");

        assertFalse(backlog.covers(start - 1), "a position whose byte has fallen out was still offered");
        assertNull(backlog.read(start - 1), "a position whose byte has fallen out was read");
        assertTrue(backlog.covers(start), "the oldest position the window holds was refused");
        assertNotNull(backlog.read(start));
        assertFalse(backlog.covers(backlog.endOffset() + 1), "a position past the end of the stream was offered");
    }

    @Test
    void theOldestBytesFallOutAsTheWindowFills() {
        ReplicationBacklog backlog = new ReplicationBacklog(10);
        backlog.append(bytes("abcde"));
        assertEquals(0, backlog.startOffset(), "the window should still start at the stream");
        assertEquals(5, backlog.endOffset());

        backlog.append(bytes("fghij"));
        assertEquals(0, backlog.startOffset(), "the window is exactly full, so nothing has fallen out yet");
        assertArrayEquals(bytes("abcdefghij"), backlog.read(0));

        backlog.append(bytes("klmno"));
        assertEquals(5, backlog.startOffset(), "the window did not let go of its oldest bytes");
        assertEquals(15, backlog.endOffset());
        assertFalse(backlog.covers(4), "a position whose byte has fallen out was still offered");
        assertNull(backlog.read(4), "a position whose byte has fallen out was read");
        assertArrayEquals(bytes("fghijklmno"), backlog.read(5), "the window does not hold the ten bytes it should");
        assertArrayEquals(bytes("o"), backlog.read(14));
        assertArrayEquals(new byte[0], backlog.read(15));
    }

    @Test
    void anAppendLargerThanTheWindowKeepsOnlyItsTail() {
        ReplicationBacklog backlog = new ReplicationBacklog(10);
        backlog.append(bytes("abcdefghijklmnop"));

        assertEquals(6, backlog.startOffset(), "the window should have slid to what the append left in it");
        assertEquals(16, backlog.endOffset());
        assertFalse(backlog.covers(5), "the window still reaches further back than its own capacity");
        assertArrayEquals(bytes("ghijklmnop"), backlog.read(6), "the tail of the append did not come back whole");
        assertArrayEquals(bytes("klmnop"), backlog.read(10));
    }

    @Test
    void anAppendLargerThanTheWindowDisplacesWhatWasHeldBeforeIt() {
        ReplicationBacklog backlog = new ReplicationBacklog(4);
        backlog.append(bytes("abcd"));
        assertArrayEquals(bytes("abcd"), backlog.read(0));

        backlog.append(bytes("efghijkl"));
        assertEquals(8, backlog.startOffset(), "the old contents should be gone along with the append's own head");
        assertEquals(12, backlog.endOffset());
        assertNull(backlog.read(0), "the window went back to serving bytes it has already let go of");
        assertArrayEquals(bytes("ijkl"), backlog.read(8));
    }

    @Test
    void theWindowWrapsAndEveryByteStillComesBackInOrder() {
        ReplicationBacklog backlog = new ReplicationBacklog(8);
        StringBuilder stream = new StringBuilder();

        for (int i = 0; i < 8; i++) {
            String chunk = String.format("%03d", i);
            stream.append(chunk);
            backlog.append(chunk.getBytes(StandardCharsets.UTF_8));

            byte[] whole = stream.toString().getBytes(StandardCharsets.UTF_8);
            int heldFrom = Math.max(0, whole.length - 8);
            assertEquals(heldFrom, backlog.startOffset(), "the window did not slide to what it still holds");
            assertArrayEquals(
                    Arrays.copyOfRange(whole, heldFrom, whole.length),
                    backlog.read(heldFrom),
                    "after " + (i + 1) * 3 + " bytes the window does not hold the tail it should");
            if (heldFrom > 0) {
                assertNull(backlog.read(heldFrom - 1), "the window claims a byte it has already wrapped past");
            }
        }
        assertEquals(24, backlog.endOffset());
    }

    @Test
    void bytesSpanningTheWrapPointComeBackWhole() {
        ReplicationBacklog backlog = new ReplicationBacklog(8);
        // ten bytes into an eight byte window: two of them fall out and the rest sit at the
        // end of the buffer, so the window's own contents are split across the wrap
        backlog.append(bytes("ABCDEFGHIJ"));
        assertArrayEquals(bytes("CDEFGHIJ"), backlog.read(2));

        backlog.append(bytes("K"));
        assertEquals(3, backlog.startOffset());
        assertEquals(11, backlog.endOffset());
        assertArrayEquals(bytes("DEFGHIJK"), backlog.read(3), "a frame spanning the wrap point came back out of order");
        assertArrayEquals(bytes("HIJK"), backlog.read(7));
        assertNull(backlog.read(2), "the byte before the window came back anyway");
    }

    @Test
    void multibyteBytesComeBackByteForByte() {
        ReplicationBacklog backlog = new ReplicationBacklog(9);
        byte[] first = bytes("\uD83C\uDF0D\u0905");  // four bytes for the emoji, three for the letter
        byte[] second = bytes("\uD83C\uDF0D");
        backlog.append(first);
        backlog.append(second);

        byte[] whole = concat(first, second);
        assertArrayEquals(Arrays.copyOfRange(whole, 2, whole.length), backlog.read(2),
                "the window mangled bytes when it wrapped inside a multibyte character");
        // the character itself is whole, so what a replica is handed decodes to what was sent
        assertEquals("\uD83C\uDF0D", new String(backlog.read(first.length), StandardCharsets.UTF_8),
                "the character at the start of the resume did not survive the window");
    }

    @Test
    void aFrameLargerThanTheWindowIsReadBackWhole() {
        ReplicationBacklog backlog = new ReplicationBacklog(1 << 20);
        byte[] big = new byte[500_000];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i * 31 + 7);
        }
        backlog.append(big);

        assertEquals(0, backlog.startOffset(), "a frame that fits should not push anything out");
        assertEquals(big.length, backlog.endOffset());
        assertArrayEquals(big, backlog.read(0), "a frame larger than a small frame does not come back whole");
    }

    @Test
    void resetEmptiesTheWindowOntoThePositionItIsGiven() {
        ReplicationBacklog backlog = new ReplicationBacklog(DEFAULT_CAPACITY);
        backlog.append(bytes("SET a 1"));
        backlog.reset(1_000);

        assertTrue(backlog.isEmpty(), "the window still holds bytes from a stream it no longer follows");
        assertEquals(1_000, backlog.startOffset());
        assertEquals(1_000, backlog.endOffset());
        assertTrue(backlog.covers(1_000), "the position a new stream starts at is not resumable");
        assertArrayEquals(new byte[0], backlog.read(1_000));
        assertFalse(backlog.covers(999), "a position before the new stream was offered");
        assertFalse(backlog.covers(1_001), "a position past the new stream was offered");
        assertNull(backlog.read(999));

        backlog.append(bytes("xy"));
        assertEquals(1_000, backlog.startOffset(), "the new stream should start where the window was placed");
        assertEquals(1_002, backlog.endOffset());
        assertArrayEquals(bytes("xy"), backlog.read(1_000));
        assertArrayEquals(bytes("y"), backlog.read(1_001));
    }

    @Test
    void everyPositionInTheWindowIsCoveredAndNoPositionBeforeItIs() {
        ReplicationBacklog backlog = new ReplicationBacklog(4);
        backlog.append(bytes("abcdefgh"));  // twice the window, so half of it is gone
        long start = backlog.startOffset();
        long end = backlog.endOffset();
        assertEquals(4, start);

        for (long position = start; position <= end; position++) {
            assertNotNull(backlog.read(position), "position " + position + " is in the window but was not read");
            assertEquals((int) (end - position), backlog.read(position).length,
                    "position " + position + " did not read through to the end of the stream");
        }
        assertNull(backlog.read(start - 1), "the position before the window was served");
        assertFalse(backlog.covers(end + 1), "a position past the end of the window was served");
    }

    @Test
    void aReaderFollowingAnAppendingStreamNeverSeesAHole() throws Exception {
        ReplicationBacklog backlog = new ReplicationBacklog(1 << 16);
        int recordLength = 17;          // prime against the capacity, so every append wraps
        int records = 400;
        int total = recordLength * records;
        AtomicReference<Throwable> failure = new AtomicReference<>();
        boolean[] writerDone = {false};

        Thread writer = new Thread(() -> {
            try {
                for (int record = 0; record < records; record++) {
                    byte[] chunk = new byte[recordLength];
                    for (int i = 0; i < recordLength; i++) {
                        int position = record * recordLength + i;
                        chunk[i] = (byte) (position * 31 + 7);
                    }
                    backlog.append(chunk);
                }
            } catch (Throwable t) {
                failure.set(t);
            } finally {
                writerDone[0] = true;
            }
        }, "backlog-writer");

        Thread reader = new Thread(() -> {
            try {
                long position = 0;
                while (position < total || !writerDone[0]) {
                    byte[] available = backlog.read(position);
                    if (available == null) {
                        Thread.sleep(1);
                        continue;
                    }
                    for (byte b : available) {
                        byte expected = (byte) ((position * 31 + 7) & 0xFF);
                        if (b != expected) {
                            throw new AssertionError("byte " + position + " came back as " + b + " not " + expected);
                        }
                        position++;
                    }
                }
                if (position != total) {
                    throw new AssertionError("the reader stopped at " + position + " of " + total);
                }
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "backlog-reader");

        writer.start();
        reader.start();
        writer.join(30_000);
        reader.join(30_000);
        if (writer.isAlive() || reader.isAlive()) {
            fail("a reader and a writer on the window did not finish");
        }
        if (failure.get() != null) {
            fail("reading while the window was being appended to: " + failure.get());
        }
        assertEquals(total, backlog.endOffset(), "the window lost or repeated bytes while both were running");
        assertEquals(0, backlog.startOffset(),
                "the window is larger than the stream it was given, so nothing should have fallen out");
        assertNotNull(backlog.read(0));
        assertEquals(total, backlog.read(0).length, "the whole stream did not come back after the reads settled");
    }

    @Test
    void aWindowThatCannotHoldOneByteIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> new ReplicationBacklog(0),
                "a window with no room in it was built");
        assertThrows(IllegalArgumentException.class, () -> new ReplicationBacklog(-5),
                "a window with negative room in it was built");
    }

    @Test
    void resizingAnEmptyWindowKeepsThePositionItNames() {
        ReplicationBacklog backlog = new ReplicationBacklog(DEFAULT_CAPACITY);
        backlog.reset(1_000);

        backlog.resize(64);
        assertEquals(1_000, backlog.startOffset(),
                "the window started again at 0 and no longer names where the stream is standing");
        assertEquals(1_000, backlog.endOffset(), "a change of room moved the end of the stream");
        assertTrue(backlog.covers(1_000), "the position the window was placed on was refused after the change");
        assertArrayEquals(new byte[0], backlog.read(1_000));
        assertFalse(backlog.covers(999), "a position before the stream was offered after the change");

        backlog.resize(1 << 20);
        assertEquals(1_000, backlog.startOffset(), "a wider window moved the position it names");
        backlog.append(bytes("xy"));
        assertEquals(1_000, backlog.startOffset(), "the stream did not resume at the position the window names");
        assertEquals(1_002, backlog.endOffset());
        assertArrayEquals(bytes("xy"), backlog.read(1_000),
                "the bytes written after the window changed did not come back whole");
    }

    @Test
    void resizingAWindowThatHoldsTheStreamIsRefused() {
        ReplicationBacklog backlog = new ReplicationBacklog(DEFAULT_CAPACITY);
        backlog.append(bytes("SET a 1"));

        assertThrows(IllegalStateException.class, () -> backlog.resize(64),
                "a window holding the stream was resized, and its bytes would have nowhere to go");

        assertEquals(7, backlog.endOffset(), "the refused change still touched the window");
        assertArrayEquals(bytes("SET a 1"), backlog.read(0),
                "the window lost the stream it holds over a change that was refused");
    }

    @Test
    void resizingToNoRoomIsRefused() {
        ReplicationBacklog backlog = new ReplicationBacklog(DEFAULT_CAPACITY);

        assertThrows(IllegalArgumentException.class, () -> backlog.resize(0),
                "a window with no room in it was made");
        assertThrows(IllegalArgumentException.class, () -> backlog.resize(-5),
                "a window with negative room in it was made");

        backlog.append(bytes("still fine"));
        assertEquals(10, backlog.endOffset(), "the window stopped working after a refused change");
        assertArrayEquals(bytes("still fine"), backlog.read(0),
                "the window lost its own stream over a change that was refused");
    }

    // ------------------------------------------------------------- helpers

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] joined = new byte[first.length + second.length];
        System.arraycopy(first, 0, joined, 0, first.length);
        System.arraycopy(second, 0, joined, first.length, second.length);
        return joined;
    }
}
