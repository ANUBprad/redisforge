package Components.Infra;

/**
 * A bounded, in-memory copy of the tail of the replication stream.
 *
 * <p>It keeps the bytes that have gone out, so a replica that comes back can be told to
 * carry on from where it stopped instead of being handed the dataset again. The positions
 * it works in are the replication offset's own: position 0 is the first byte of the
 * stream, and every append moves the window forward by exactly the bytes it took in.</p>
 *
 * <p>The window is a fixed size, so the oldest bytes fall out as new ones arrive. A
 * replica that was away for longer than the window holds cannot be resumed from where it
 * stopped and is answered with a full resync instead. Byte {@code n} of the stream lives
 * at index {@code n % capacity}, which lets the window slide without moving the bytes it
 * still has to serve.</p>
 *
 * <p>Every method is synchronized, and the callers that matter hold the backlog itself as
 * the lock: counting the bytes, keeping them and sending them has to be one indivisible
 * step, or a replica could be offered a position whose bytes were counted but never sent.
 * That makes the backlog's own identity part of the lock, so it is never replaced -
 * {@link #resize} gives it a wider or narrower buffer in place, and only while it holds
 * nothing, which is what keeps one lock guarding the stream for the whole time a server
 * runs.</p>
 */
public class ReplicationBacklog {

    private byte[] buffer;
    private int capacity;
    /** the position of the oldest byte still held, which is the first one a replica can resume from */
    private long startOffset;
    /** the position one past the newest byte held, which is the furthest a replica can resume from */
    private long endOffset;

    public ReplicationBacklog(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("a replication backlog has to hold at least one byte, got " + capacity);
        }
        this.capacity = capacity;
        this.buffer = new byte[capacity];
    }

    /**
     * Takes in bytes of the stream, letting the oldest ones fall out when the window is full.
     *
     * <p>One append larger than the window keeps its own tail: everything before it is
     * further back than the window can reach, so what it displaces is the window's contents
     * as well as the append's head.</p>
     */
    public synchronized void append(byte[] streamBytes) {
        int length = streamBytes.length;
        if (length == 0) {
            return;
        }
        if (length >= capacity) {
            // the whole window is taken over by the append's own tail. The tail still has to
            // land where each of its positions reads from - byte at position p lives at index
            // p modulo the capacity - and the tail rarely starts on such a boundary, so it is
            // written round the buffer rather than straight across it
            startOffset = endOffset + length - capacity;
            for (int i = 0; i < capacity; i++) {
                buffer[(int) ((startOffset + i) % capacity)] = streamBytes[length - capacity + i];
            }
            endOffset += length;
            return;
        }
        long newEnd = endOffset + length;
        long oldestKept = newEnd - capacity;
        if (startOffset < oldestKept) {
            startOffset = oldestKept;
        }
        for (int i = 0; i < length; i++) {
            buffer[(int) ((endOffset + i) % capacity)] = streamBytes[i];
        }
        endOffset = newEnd;
    }

    /**
     * Whether a replica standing at this position could be resumed from here, which is
     * true exactly when every byte from there to the end of the stream is still held.
     *
     * <p>The end counts as covered: a replica that stopped on the newest byte has missed
     * nothing, and is answered with a resume that carries no bytes rather than with the
     * dataset it already has.</p>
     */
    public synchronized boolean covers(long offset) {
        return offset >= startOffset && offset <= endOffset;
    }

    /**
     * Everything from this position to the end of the stream.
     *
     * @return the bytes, an empty array when the position is the end of the stream, or
     *         null when they have already fallen out of the window
     */
    public synchronized byte[] read(long fromOffset) {
        if (!covers(fromOffset)) {
            return null;
        }
        int length = (int) (endOffset - fromOffset);
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = buffer[(int) ((fromOffset + i) % capacity)];
        }
        return bytes;
    }

    /**
     * Empties the window onto one position, which is what a hop does when it takes on a
     * new stream: what the window held was counted from a position it no longer follows,
     * and a replica asking about it would be asking about the wrong stream.
     */
    public synchronized void reset(long offset) {
        startOffset = offset;
        endOffset = offset;
    }

    /** True while nothing has been recorded, which is the only time the window may be resized. */
    public synchronized boolean isEmpty() {
        return startOffset == endOffset;
    }

    /**
     * Changes how much of the stream the window keeps, in place.
     *
     * <p>It may only be done while the window holds nothing: what it holds is laid out
     * around the buffer it has, and a new buffer would have nowhere to find it again. An
     * empty window carries no bytes to lose, so the positions it names - which are the
     * stream's own - carry over untouched, and the window goes on covering exactly what
     * the offset says it does.</p>
     *
     * <p>It is done in place rather than by building a new window, because the window is
     * also the lock the stream is counted and sent under: a server that swapped it would
     * let one thread count into the old window while another answered a resume from the
     * new one.</p>
     */
    public synchronized void resize(int newCapacity) {
        if (newCapacity <= 0) {
            throw new IllegalArgumentException(
                    "a replication backlog has to hold at least one byte, got " + newCapacity);
        }
        if (!isEmpty()) {
            throw new IllegalStateException(
                    "the replication backlog already holds the stream, so its size cannot be changed");
        }
        this.buffer = new byte[newCapacity];
        this.capacity = newCapacity;
    }

    public synchronized long startOffset() {
        return startOffset;
    }

    public synchronized long endOffset() {
        return endOffset;
    }

    public int capacity() {
        return capacity;
    }
}
