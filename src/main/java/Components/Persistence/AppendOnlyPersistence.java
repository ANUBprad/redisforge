package Components.Persistence;

import Components.Repository.Store;
import Components.Repository.Value;
import Components.Server.RedisConfig;
import Components.Service.RespSerializer;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Append only file: the durable record of the mutations a master has applied.
 *
 * <p>Every mutation that reaches the store is written to the file as a standard
 * RESP command, and the file is replayed into the store before the server starts
 * accepting clients. This component owns the file, the flush policy and the
 * replay rules; the store keeps no knowledge of the disk.
 *
 * <p>All writes go through one lock, so a RESP frame from one client can never be
 * spliced into a frame from another client, and the order of entries in the file is
 * the order in which the store was changed. Holding that same lock across a mutation
 * and its entry is what makes the second part true under concurrency, which is what
 * {@link #locked(Supplier)} is for.
 */
@Component
public class AppendOnlyPersistence {

    private static final Logger logger = Logger.getLogger(AppendOnlyPersistence.class.getName());

    /** How often the everysec policy forces pending writes to disk. */
    private static final long FSYNC_PERIOD_SECONDS = 1;

    private final RedisConfig redisConfig;
    private final Store store;
    private final RespSerializer respSerializer = new RespSerializer();

    private final ReentrantLock lock = new ReentrantLock();

    /** null while appendonly is off, which is when this class does nothing at all. */
    private volatile FileChannel channel;
    private FsyncPolicy policy = FsyncPolicy.EVERYSEC;
    private ScheduledExecutorService fsyncTask;

    public AppendOnlyPersistence(RedisConfig redisConfig, Store store) {
        this.redisConfig = redisConfig;
        this.store = store;
    }

    public boolean isEnabled() {
        return channel != null;
    }

    /**
     * Opens the file and replays it into the store, before the master accepts clients,
     * so recovery is complete before any client can observe the dataset. A replica
     * never does this: its state comes from its master, and it keeps no local file.
     *
     * @return the number of commands replayed
     */
    public int start() throws IOException {
        if (!redisConfig.isMaster() || !redisConfig.isAppendonly()) {
            return 0;
        }
        policy = FsyncPolicy.parse(redisConfig.getAppendfsync());

        File file = new File(redisConfig.getAppendfilename());
        File directory = file.getAbsoluteFile().getParentFile();
        if (directory != null && !directory.isDirectory()) {
            Files.createDirectories(directory.toPath());
        }

        byte[] contents = file.isFile() ? Files.readAllBytes(file.toPath()) : new byte[0];
        Parsed parsed = parse(contents);

        channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        if (parsed.truncatedAt != contents.length) {
            // a frame that stops mid way at the end of the file is a write that did not
            // survive, so it is dropped and the file is cut back to the last whole frame
            channel.truncate(parsed.truncatedAt);
        }
        channel.position(parsed.truncatedAt);

        int replayed = replay(parsed.frames);

        if (policy == FsyncPolicy.EVERYSEC) {
            startFsyncTask();
        }
        logger.info("appendonly replayed " + replayed + " commands from " + file.getPath()
                + " (fsync=" + redisConfig.getAppendfsync() + ")");
        return replayed;
    }

    /** Stops the periodic flush, pushes pending writes to disk and closes the file. */
    public void close() {
        lock.lock();
        try {
            if (fsyncTask != null) {
                fsyncTask.shutdownNow();
                fsyncTask = null;
            }
            if (channel != null) {
                channel.force(true);
                channel.close();
                channel = null;
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "could not close the append only file", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Runs a mutation, and everything the caller does to publish it, while the file is
     * locked, so entries land in the order the store was changed in. When appendonly is
     * off there is no file and no locking at all.
     */
    public <T> T locked(Supplier<T> work) {
        if (channel == null) {
            return work.get();
        }
        lock.lock();
        try {
            return work.get();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Records one command the store has just applied. A relative expiry is written as
     * an absolute deadline, because a PX measured when the command ran says nothing
     * about how long the key should live once the file is replayed.
     */
    public void appendApplied(String[] command) {
        if (channel == null || !isMutation(command)) {
            return;
        }
        lock.lock();
        try {
            append(respSerializer.respArray(frameFor(command)));
        } finally {
            lock.unlock();
        }
    }

    /**
 * Records an executed transaction as one MULTI block, so it is replayed as the single
 * transaction it was instead of as loose commands. The whole block is written under one
 * hold of the lock, so no other client's command can land between its commands.
 *
 * <p>Only the mutations are written. A command the store refused, and a command that only
 * read, changed nothing, so replaying them would either fail a second time or describe a
 * change that never happened.</p>
 */
    public void appendAppliedTransaction(List<String[]> commands) {
        if (channel == null) {
            return;
        }
        List<String[]> mutations = commands.stream()
                .filter(AppendOnlyPersistence::isMutation)
                .toList();
        if (mutations.isEmpty()) {
            return;
        }
        lock.lock();
        try {
            StringBuilder block = new StringBuilder(respSerializer.respArray(new String[]{"MULTI"}));
            for (String[] command : mutations) {
                block.append(respSerializer.respArray(frameFor(command)));
            }
            block.append(respSerializer.respArray(new String[]{"EXEC"}));
            append(block.toString());
        } finally {
            lock.unlock();
        }
    }

    /** Only these change the keyspace, so only these are ever written down. */
    private static boolean isMutation(String[] command) {
        if (command.length < 2) {
            return false;
        }
        return switch (command[0].toUpperCase()) {
            case "SET", "INCR", "DEL" -> true;
            default -> false;
        };
    }

    /** Writes one entry, flushing it when the policy asks for that. */
    private void append(String resp) {
        try {
            channel.write(ByteBuffer.wrap(resp.getBytes(StandardCharsets.UTF_8)));
            if (policy == FsyncPolicy.ALWAYS) {
                channel.force(false);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("could not append to the append only file", e);
        }
    }

    private void startFsyncTask() {
        fsyncTask = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "appendonly-fsync");
            thread.setDaemon(true);
            return thread;
        });
        fsyncTask.scheduleAtFixedRate(this::forceQuietly,
                FSYNC_PERIOD_SECONDS, FSYNC_PERIOD_SECONDS, TimeUnit.SECONDS);
    }

    private void forceQuietly() {
        lock.lock();
        try {
            if (channel != null) {
                channel.force(false);
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "could not flush the append only file", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * The entry to keep for a command that was applied: a SET keeps the deadline the
     * store ended up with, every other command is kept exactly as it arrived.
     */
    private String[] frameFor(String[] command) {
        if (command.length < 3 || !command[0].equalsIgnoreCase("SET")) {
            return command;
        }
        Long deadline = absoluteExpiryMillis(store.peekValue(command[1]));
        if (deadline == null) {
            return new String[]{"SET", command[1], command[2]};
        }
        return new String[]{"SET", command[1], command[2], "PXAT", String.valueOf(deadline)};
    }

    /** null when the key does not expire. */
    private static Long absoluteExpiryMillis(Value value) {
        if (value == null || value.expiry == null || value.expiry.equals(LocalDateTime.MAX)) {
            return null;
        }
        return value.expiry.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    /**
     * Splits the file into whole RESP frames. A frame that stops early is only tolerated
     * at the very end, where it is an interrupted write; bytes that are not RESP at all
     * mean the file cannot be trusted and stop startup.
     */
    private Parsed parse(byte[] contents) throws IOException {
        List<String[]> frames = new ArrayList<>();
        int offset = 0;
        while (offset < contents.length) {
            int length = respSerializer.frameLength(contents, offset, contents.length);
            if (length == RespSerializer.INCOMPLETE_FRAME) {
                return new Parsed(frames, offset);
            }
            if (length == RespSerializer.MALFORMED_FRAME) {
                throw corrupt("no RESP frame at byte " + offset);
            }
            frames.add(respSerializer.deserializeFrame(contents, offset, offset + length));
            offset += length;
        }
        return new Parsed(frames, offset);
    }

    /** Applies the log to the store, keeping a MULTI block together while it does. */
    private int replay(List<String[]> frames) throws IOException {
        List<String[]> queued = null;
        for (String[] frame : frames) {
            switch (frame[0].toUpperCase()) {
                case "MULTI" -> queued = new ArrayList<>();
                case "DISCARD" -> queued = null;
                case "EXEC" -> {
                    if (queued == null) {
                        throw corrupt("EXEC without MULTI");
                    }
                    replayTransaction(queued);
                    queued = null;
                }
                case "SET", "INCR", "DEL" -> {
                    if (queued == null) {
                        replayCommand(frame);
                    } else {
                        queued.add(frame);
                    }
                }
                default -> throw corrupt("unknown command '" + frame[0] + "'");
            }
        }
        if (queued != null) {
            throw corrupt("MULTI without EXEC");
        }
        return frames.size();
    }

    /**
     * The commands of one transaction were applied together when they were written, so
     * they are applied together here too. A command that cannot be applied means the log
     * disagrees with the data it describes, which is not something to guess at.
     */
    private void replayTransaction(List<String[]> commands) throws AppendOnlyCorruptedException {
        for (String[] command : commands) {
            try {
                replayCommand(command);
            } catch (IOException | RuntimeException e) {
                throw corrupt("could not replay transaction command '" + command[0]
                        + "': " + e.getMessage());
            }
        }
    }

    private void replayCommand(String[] command) throws AppendOnlyCorruptedException {
        switch (command[0].toUpperCase()) {
            case "SET" -> replaySet(command);
            case "INCR" -> {
                requireArity(command, 2);
                store.increment(command[1]);
            }
            case "DEL" -> {
                requireArity(command, 2);
                for (int i = 1; i < command.length; i++) {
                    store.delete(command[i]);
                }
            }
            default -> throw corrupt("unknown command '" + command[0] + "'");
        }
    }

    private void replaySet(String[] frame) throws AppendOnlyCorruptedException {
        if (frame.length < 3) {
            throw corrupt("SET without a value");
        }
        if (frame.length == 3) {
            store.set(frame[1], frame[2]);
            return;
        }
        if (frame.length != 5 || !frame[3].equalsIgnoreCase("PXAT")) {
            throw corrupt("SET with unsupported options: " + String.join(" ", frame));
        }
        long deadline;
        try {
            deadline = Long.parseLong(frame[4]);
        } catch (NumberFormatException e) {
            throw corrupt("SET with an unreadable deadline '" + frame[4] + "'");
        }
        if (LocalDateTime.ofInstant(Instant.ofEpochMilli(deadline), ZoneId.systemDefault())
                .isBefore(LocalDateTime.now())) {
            store.delete(frame[1]);
            return;
        }
        store.setAt(frame[1], frame[2], deadline);
    }

    private static void requireArity(String[] command, int minimum) throws AppendOnlyCorruptedException {
        if (command.length < minimum) {
            throw new AppendOnlyCorruptedException(
                    command[0] + " in the append only file has no key");
        }
    }

    private AppendOnlyCorruptedException corrupt(String reason) {
        return new AppendOnlyCorruptedException(
                "append only file " + redisConfig.getAppendfilename() + " is corrupt: " + reason);
    }

    /** The frames found in the file, and the offset up to which they are whole. */
    private record Parsed(List<String[]> frames, int truncatedAt) { }
}