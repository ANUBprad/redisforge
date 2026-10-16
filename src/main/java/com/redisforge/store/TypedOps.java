package com.redisforge.store;

import com.redisforge.protocol.RespSerializer;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The hash, list and set commands, in one place.
 *
 * <p>Every path that applies a typed command - a client write on the master, a queued
 * command inside a transaction, a replica applying its master's stream and a replayed
 * append only file - comes through here. There is exactly one implementation of each
 * command's semantics, so a hash field, a list element or a set member cannot mean one
 * thing on the master and another on a replica or after a restart.</p>
 *
 * <p>A mutation is computed against a copy and handed back whole: {@link #mutate} never
 * changes the value it is given. The caller decides whether to commit what comes back,
 * which is what lets a write be judged against the memory limit before the keyspace is
 * touched. A mutation that leaves a collection empty returns a {@code null} value, which
 * is how "drop the key" is spelled.</p>
 *
 * <p>A reply that starts with {@code -} is an error and carries no change. A change that
 * did not happen - a field set to the value it already had, a member already present -
 * is reported with {@code changed == false}, so the caller can answer without writing it
 * down or passing it on.</p>
 */
public final class TypedOps {

    /** The reply a command gets when the key holds another kind of value. */
    public static final String WRONGTYPE =
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

    private static final RespSerializer RESP = new RespSerializer();
    private static final String NOT_INTEGER = "-ERR value is not an integer or out of range\r\n";
    private static final String OUT_OF_RANGE = "-ERR value is out of range, must be positive\r\n";
    private static final String NO_SUCH_KEY = "-ERR no such key\r\n";
    private static final String INDEX_OUT_OF_RANGE = "-ERR index out of range\r\n";
    private static final String NULL_BULK = "$-1\r\n";
    private static final String NULL_ARRAY = "*-1\r\n";
    private static final String EMPTY_ARRAY = "*0\r\n";
    private static final String OK = "+OK\r\n";

    private static final Set<String> READS = Set.of(
            "HGET", "HMGET", "HLEN", "HEXISTS", "HGETALL", "HKEYS", "HVALS", "HSTRLEN",
            "LLEN", "LRANGE", "LINDEX",
            "SCARD", "SISMEMBER", "SMISMEMBER", "SMEMBERS", "SRANDMEMBER");

    private static final Set<String> WRITES = Set.of(
            "HSET", "HDEL", "HSETNX",
            "LPUSH", "RPUSH", "LPOP", "RPOP", "LSET", "LREM", "LTRIM",
            "SADD", "SREM", "SPOP");

    private TypedOps() {
    }

    /** Whether the name is one of the hash, list or set commands. */
    public static boolean handles(String name) {
        return READS.contains(name) || WRITES.contains(name);
    }

    /** Whether the name only reads, so a replica may serve it. */
    public static boolean isRead(String name) {
        return READS.contains(name);
    }

    /**
     * One applied mutation: the reply, whether the keyspace really changed, the value to
     * commit ({@code null} to drop the key) and the frame to record and pass on - which
     * is the command itself except where the effect must be made deterministic, as
     * {@code SPOP} is written down as the {@code SREM} of the members it popped.
     */
    public record Mutation(String reply, boolean changed, Value value, String[] propagation) {
    }

    /** Applies a typed write against {@code current}, which may be null. */
    public static Mutation mutate(String[] command, Value current) {
        String name = command[0].toUpperCase(Locale.ROOT);
        return switch (name) {
            case "HSET" -> hset(command, current);
            case "HDEL" -> hdel(command, current);
            case "HSETNX" -> hsetnx(command, current);
            case "LPUSH" -> push(command, current, true);
            case "RPUSH" -> push(command, current, false);
            case "LPOP" -> pop(command, current, true);
            case "RPOP" -> pop(command, current, false);
            case "LSET" -> lset(command, current);
            case "LREM" -> lrem(command, current);
            case "LTRIM" -> ltrim(command, current);
            case "SADD" -> sadd(command, current);
            case "SREM" -> srem(command, current);
            case "SPOP" -> spop(command, current);
            default -> new Mutation("-ERR unknown command '" + command[0] + "'\r\n", false, null, null);
        };
    }

    /** Answers a typed read against {@code current}, which may be null. */
    public static String read(String[] command, Value current) {
        String name = command[0].toUpperCase(Locale.ROOT);
        return switch (name) {
            case "HGET" -> hget(command, current);
            case "HMGET" -> hmget(command, current);
            case "HLEN" -> hlen(command, current);
            case "HEXISTS" -> hexists(command, current);
            case "HGETALL" -> hgetall(command, current);
            case "HKEYS" -> hkeys(command, current);
            case "HVALS" -> hvals(command, current);
            case "HSTRLEN" -> hstrlen(command, current);
            case "LLEN" -> llen(command, current);
            case "LRANGE" -> lrange(command, current);
            case "LINDEX" -> lindex(command, current);
            case "SCARD" -> scard(command, current);
            case "SISMEMBER" -> sismember(command, current);
            case "SMISMEMBER" -> smismember(command, current);
            case "SMEMBERS" -> smembers(command, current);
            case "SRANDMEMBER" -> srandmember(command, current);
            default -> "-ERR unknown command '" + command[0] + "'\r\n";
        };
    }

    // ---------------------------------------------------------------- hashes

    private static Mutation hset(String[] command, Value current) {
        if (command.length < 4 || (command.length & 1) != 0) {
            return error(wrongArity(command[0]));
        }
        if (wrongKind(current, Value.Kind.HASH)) {
            return error(WRONGTYPE);
        }
        Value next = current == null ? Value.hash(now(), LocalDateTime.MAX) : current.copy();
        int added = 0;
        boolean changed = false;
        for (int i = 2; i + 1 < command.length; i += 2) {
            String previous = next.hash.put(command[i], command[i + 1]);
            if (previous == null) {
                added++;
                changed = true;
            } else if (!previous.equals(command[i + 1])) {
                changed = true;
            }
        }
        if (!changed) {
            return unchanged(RESP.respInteger(0));
        }
        return new Mutation(RESP.respInteger(added), true, next, command);
    }

    private static Mutation hdel(String[] command, Value current) {
        if (command.length < 3) {
            return error(wrongArity(command[0]));
        }
        if (current == null) {
            return unchanged(RESP.respInteger(0));
        }
        if (wrongKind(current, Value.Kind.HASH)) {
            return error(WRONGTYPE);
        }
        Value next = current.copy();
        int removed = 0;
        for (int i = 2; i < command.length; i++) {
            if (next.hash.remove(command[i]) != null) {
                removed++;
            }
        }
        if (removed == 0) {
            return unchanged(RESP.respInteger(0));
        }
        return new Mutation(RESP.respInteger(removed), true, next.hash.isEmpty() ? null : next, command);
    }

    private static Mutation hsetnx(String[] command, Value current) {
        if (command.length != 4) {
            return error(wrongArity(command[0]));
        }
        if (wrongKind(current, Value.Kind.HASH)) {
            return error(WRONGTYPE);
        }
        Value next = current == null ? Value.hash(now(), LocalDateTime.MAX) : current.copy();
        if (next.hash.containsKey(command[2])) {
            return unchanged(RESP.respInteger(0));
        }
        next.hash.put(command[2], command[3]);
        return new Mutation(RESP.respInteger(1), true, next, command);
    }

    private static String hget(String[] command, Value current) {
        if (command.length != 3) {
            return wrongArity(command[0]);
        }
        if (wrongKind(current, Value.Kind.HASH)) {
            return WRONGTYPE;
        }
        if (current == null) {
            return NULL_BULK;
        }
        String value = current.hash.get(command[2]);
        return value == null ? NULL_BULK : RESP.serializeBulkString(value);
    }

    private static String hmget(String[] command, Value current) {
        if (command.length < 3) {
            return wrongArity(command[0]);
        }
        if (wrongKind(current, Value.Kind.HASH)) {
            return WRONGTYPE;
        }
        List<String> out = new ArrayList<>(command.length - 2);
        for (int i = 2; i < command.length; i++) {
            String value = current == null ? null : current.hash.get(command[i]);
            out.add(value == null ? NULL_BULK : RESP.serializeBulkString(value));
        }
        return RESP.respArray(out);
    }

    private static String hlen(String[] command, Value current) {
        if (command.length != 2) {
            return wrongArity(command[0]);
        }
        if (wrongKind(current, Value.Kind.HASH)) {
            return WRONGTYPE;
        }
        return RESP.respInteger(current == null ? 0 : current.hash.size());
    }

    private static String hexists(String[] command, Value current) {
        if (command.length != 3) {
            return wrongArity(command[0]);
        }
        if (wrongKind(current, Value.Kind.HASH)) {
            return WRONGTYPE;
        }
        return RESP.respInteger(current != null && current.hash.containsKey(command[2]) ? 1 : 0);
    }

    private static String hgetall(String[] command, Value current) {
        if (command.length != 2) {
            return wrongArity(command[0]);
        }
        if (wrongKind(current, Value.Kind.HASH)) {
            return WRONGTYPE;
        }
        if (current == null) {
            return EMPTY_ARRAY;
        }
        List<String> out = new ArrayList<>(current.hash.size() * 2);
        for (Map.Entry<String, String> field : current.hash.entrySet()) {
            out.add(RESP.serializeBulkString(field.getKey()));
            out.add(RESP.serializeBulkString(field.getValue()));
        }
        return RESP.respArray(out);
    }

    private static String hkeys(String[] command, Value current) {
        if (command.length != 2) {
            return wrongArity(command[0]);
        }
        if (wrongKind(current, Value.Kind.HASH)) {
            return WRONGTYPE;
        }
        if (current == null) {
            return EMPTY_ARRAY;
        }
        List<String> out = new ArrayList<>(current.hash.size());
        for (String field : current.hash.keySet()) {
            out.add(RESP.serializeBulkString(field));
        }
        return RESP.respArray(out);
    }

    private static String hvals(String[] command, Value current) {
        if (command.length != 2) {
            return wrongArity(command[0]);
        }
        if (wrongKind(current, Value.Kind.HASH)) {
            return WRONGTYPE;
        }
        if (current == null) {
            return EMPTY_ARRAY;
        }
        List<String> out = new ArrayList<>(current.hash.size());
        for (String value : current.hash.values()) {
            out.add(RESP.serializeBulkString(value));
        }
        return RESP.respArray(out);
    }

    private static String hstrlen(String[] command, Value current) {
        if (command.length != 3) {
            return wrongArity(command[0]);
        }
        if (wrongKind(current, Value.Kind.HASH)) {
            return WRONGTYPE;
        }
        String value = current == null ? null : current.hash.get(command[2]);
        return RESP.respInteger(value == null ? 0 : utf8(value));
    }

    // ----------------------------------------------------------------- lists

    private static Mutation push(String[] command, Value current, boolean left) {
        if (command.length < 3) {
            return error(wrongArity(command[0]));
        }
        if (wrongKind(current, Value.Kind.LIST)) {
            return error(WRONGTYPE);
        }
        Value next = current == null ? Value.list(now(), LocalDateTime.MAX) : current.copy();
        for (int i = 2; i < command.length; i++) {
            if (left) {
                next.list.addFirst(command[i]);
            } else {
                next.list.addLast(command[i]);
            }
        }
        return new Mutation(RESP.respInteger(next.list.size()), true, next, command);
    }

    private static Mutation pop(String[] command, Value current, boolean left) {
        if (command.length != 2 && command.length != 3) {
            return error(wrongArity(command[0]));
        }
        Integer count = null;
        if (command.length == 3) {
            count = parseCount(command[2]);
            if (count == null) {
                return error(NOT_INTEGER);
            }
            if (count < 0) {
                return error(OUT_OF_RANGE);
            }
        }
        if (wrongKind(current, Value.Kind.LIST)) {
            return error(WRONGTYPE);
        }
        if (count == null) {
            if (current == null || current.list.isEmpty()) {
                return unchanged(NULL_BULK);
            }
            Value next = current.copy();
            String popped = left ? next.list.pollFirst() : next.list.pollLast();
            return new Mutation(RESP.serializeBulkString(popped), true, next.list.isEmpty() ? null : next, command);
        }
        if (current == null) {
            return unchanged(NULL_ARRAY);
        }
        Value next = current.copy();
        List<String> popped = new ArrayList<>();
        for (int i = 0; i < count && !next.list.isEmpty(); i++) {
            popped.add(left ? next.list.pollFirst() : next.list.pollLast());
        }
        if (popped.isEmpty()) {
            return unchanged(EMPTY_ARRAY);
        }
        return new Mutation(RESP.respArray(popped.toArray(new String[0])), true,
                next.list.isEmpty() ? null : next, command);
    }

    private static Mutation lset(String[] command, Value current) {
        if (command.length != 4) {
            return error(wrongArity(command[0]));
        }
        Integer index = parseCount(command[2]);
        if (index == null) {
            return error(NOT_INTEGER);
        }
        if (current == null) {
            return error(NO_SUCH_KEY);
        }
        if (wrongKind(current, Value.Kind.LIST)) {
            return error(WRONGTYPE);
        }
        Value next = current.copy();
        List<String> elements = new ArrayList<>(next.list);
        int resolved = index >= 0 ? index : elements.size() + index;
        if (resolved < 0 || resolved >= elements.size()) {
            return error(INDEX_OUT_OF_RANGE);
        }
        String previous = elements.set(resolved, command[3]);
        if (previous.equals(command[3])) {
            return unchanged(OK);
        }
        next.list = new ArrayDeque<>(elements);
        return new Mutation(OK, true, next, command);
    }

    private static Mutation lrem(String[] command, Value current) {
        if (command.length != 4) {
            return error(wrongArity(command[0]));
        }
        Integer count = parseCount(command[2]);
        if (count == null) {
            return error(NOT_INTEGER);
        }
        if (current == null) {
            return unchanged(RESP.respInteger(0));
        }
        if (wrongKind(current, Value.Kind.LIST)) {
            return error(WRONGTYPE);
        }
        Value next = current.copy();
        int removed = 0;
        if (count >= 0) {
            int limit = count == 0 ? Integer.MAX_VALUE : count;
            Iterator<String> it = next.list.iterator();
            while (it.hasNext() && removed < limit) {
                if (it.next().equals(command[3])) {
                    it.remove();
                    removed++;
                }
            }
        } else {
            int limit = -count;
            Iterator<String> it = next.list.descendingIterator();
            while (it.hasNext() && removed < limit) {
                if (it.next().equals(command[3])) {
                    it.remove();
                    removed++;
                }
            }
        }
        if (removed == 0) {
            return unchanged(RESP.respInteger(0));
        }
        return new Mutation(RESP.respInteger(removed), true, next.list.isEmpty() ? null : next, command);
    }

    private static Mutation ltrim(String[] command, Value current) {
        if (command.length != 4) {
            return error(wrongArity(command[0]));
        }
        Integer start = parseCount(command[2]);
        Integer stop = parseCount(command[3]);
        if (start == null || stop == null) {
            return error(NOT_INTEGER);
        }
        if (current == null) {
            return unchanged(OK);
        }
        if (wrongKind(current, Value.Kind.LIST)) {
            return error(WRONGTYPE);
        }
        Value next = current.copy();
        List<String> elements = new ArrayList<>(next.list);
        int size = elements.size();
        int from = start < 0 ? size + start : start;
        int to = stop < 0 ? size + stop : stop;
        if (from < 0) {
            from = 0;
        }
        if (to >= size) {
            to = size - 1;
        }
        if (from > to || from >= size) {
            return new Mutation(OK, true, null, command);
        }
        List<String> kept = new ArrayList<>(elements.subList(from, to + 1));
        if (kept.size() == size) {
            return unchanged(OK);
        }
        next.list = new ArrayDeque<>(kept);
        return new Mutation(OK, true, next, command);
    }

    private static String llen(String[] command, Value current) {
        if (command.length != 2) {
            return wrongArity(command[0]);
        }
        if (wrongKind(current, Value.Kind.LIST)) {
            return WRONGTYPE;
        }
        return RESP.respInteger(current == null ? 0 : current.list.size());
    }

    private static String lrange(String[] command, Value current) {
        if (command.length != 4) {
            return wrongArity(command[0]);
        }
        Integer start = parseCount(command[2]);
        Integer stop = parseCount(command[3]);
        if (start == null || stop == null) {
            return NOT_INTEGER;
        }
        if (wrongKind(current, Value.Kind.LIST)) {
            return WRONGTYPE;
        }
        if (current == null) {
            return EMPTY_ARRAY;
        }
        List<String> elements = new ArrayList<>(current.list);
        int size = elements.size();
        int from = start < 0 ? size + start : start;
        int to = stop < 0 ? size + stop : stop;
        if (from < 0) {
            from = 0;
        }
        if (to >= size) {
            to = size - 1;
        }
        if (from > to || from >= size) {
            return EMPTY_ARRAY;
        }
        return RESP.respArray(elements.subList(from, to + 1).toArray(new String[0]));
    }

    private static String lindex(String[] command, Value current) {
        if (command.length != 3) {
            return wrongArity(command[0]);
        }
        Integer index = parseCount(command[2]);
        if (index == null) {
            return NOT_INTEGER;
        }
        if (wrongKind(current, Value.Kind.LIST)) {
            return WRONGTYPE;
        }
        if (current == null) {
            return NULL_BULK;
        }
        List<String> elements = new ArrayList<>(current.list);
        int resolved = index >= 0 ? index : elements.size() + index;
        if (resolved < 0 || resolved >= elements.size()) {
            return NULL_BULK;
        }
        return RESP.serializeBulkString(elements.get(resolved));
    }

    // ------------------------------------------------------------------ sets

    private static Mutation sadd(String[] command, Value current) {
        if (command.length < 3) {
            return error(wrongArity(command[0]));
        }
        if (wrongKind(current, Value.Kind.SET)) {
            return error(WRONGTYPE);
        }
        Value next = current == null ? Value.set(now(), LocalDateTime.MAX) : current.copy();
        int added = 0;
        for (int i = 2; i < command.length; i++) {
            if (next.set.add(command[i])) {
                added++;
            }
        }
        if (added == 0) {
            return unchanged(RESP.respInteger(0));
        }
        return new Mutation(RESP.respInteger(added), true, next, command);
    }

    private static Mutation srem(String[] command, Value current) {
        if (command.length < 3) {
            return error(wrongArity(command[0]));
        }
        if (current == null) {
            return unchanged(RESP.respInteger(0));
        }
        if (wrongKind(current, Value.Kind.SET)) {
            return error(WRONGTYPE);
        }
        Value next = current.copy();
        int removed = 0;
        for (int i = 2; i < command.length; i++) {
            if (next.set.remove(command[i])) {
                removed++;
            }
        }
        if (removed == 0) {
            return unchanged(RESP.respInteger(0));
        }
        return new Mutation(RESP.respInteger(removed), true, next.set.isEmpty() ? null : next, command);
    }

    private static Mutation spop(String[] command, Value current) {
        if (command.length != 2 && command.length != 3) {
            return error(wrongArity(command[0]));
        }
        Integer count = null;
        if (command.length == 3) {
            count = parseCount(command[2]);
            if (count == null) {
                return error(NOT_INTEGER);
            }
            if (count < 0) {
                return error(OUT_OF_RANGE);
            }
        }
        if (wrongKind(current, Value.Kind.SET)) {
            return error(WRONGTYPE);
        }
        if (current == null) {
            return unchanged(count == null ? NULL_BULK : NULL_ARRAY);
        }
        Value next = current.copy();
        List<String> members = new ArrayList<>(next.set);
        Collections.shuffle(members, ThreadLocalRandom.current());
        if (count == null) {
            if (members.isEmpty()) {
                return unchanged(NULL_BULK);
            }
            String popped = members.get(0);
            next.set.remove(popped);
            String[] propagation = new String[]{"SREM", command[1], popped};
            return new Mutation(RESP.serializeBulkString(popped), true,
                    next.set.isEmpty() ? null : next, propagation);
        }
        int take = Math.min(count, members.size());
        if (take == 0) {
            return unchanged(EMPTY_ARRAY);
        }
        List<String> popped = new ArrayList<>(members.subList(0, take));
        String[] propagation = new String[2 + take];
        propagation[0] = "SREM";
        propagation[1] = command[1];
        for (int i = 0; i < take; i++) {
            next.set.remove(popped.get(i));
            propagation[2 + i] = popped.get(i);
        }
        return new Mutation(RESP.respArray(popped.toArray(new String[0])), true,
                next.set.isEmpty() ? null : next, propagation);
    }

    private static String scard(String[] command, Value current) {
        if (command.length != 2) {
            return wrongArity(command[0]);
        }
        if (wrongKind(current, Value.Kind.SET)) {
            return WRONGTYPE;
        }
        return RESP.respInteger(current == null ? 0 : current.set.size());
    }

    private static String sismember(String[] command, Value current) {
        if (command.length != 3) {
            return wrongArity(command[0]);
        }
        if (wrongKind(current, Value.Kind.SET)) {
            return WRONGTYPE;
        }
        return RESP.respInteger(current != null && current.set.contains(command[2]) ? 1 : 0);
    }

    private static String smismember(String[] command, Value current) {
        if (command.length < 3) {
            return wrongArity(command[0]);
        }
        if (wrongKind(current, Value.Kind.SET)) {
            return WRONGTYPE;
        }
        StringBuilder out = new StringBuilder();
        out.append('*').append(command.length - 2).append("\r\n");
        for (int i = 2; i < command.length; i++) {
            out.append(':').append(current != null && current.set.contains(command[i]) ? 1 : 0).append("\r\n");
        }
        return out.toString();
    }

    private static String smembers(String[] command, Value current) {
        if (command.length != 2) {
            return wrongArity(command[0]);
        }
        if (wrongKind(current, Value.Kind.SET)) {
            return WRONGTYPE;
        }
        if (current == null) {
            return EMPTY_ARRAY;
        }
        return RESP.respArray(current.set.toArray(new String[0]));
    }

    private static String srandmember(String[] command, Value current) {
        if (command.length != 2 && command.length != 3) {
            return wrongArity(command[0]);
        }
        Integer count = null;
        if (command.length == 3) {
            count = parseCount(command[2]);
            if (count == null) {
                return NOT_INTEGER;
            }
        }
        if (wrongKind(current, Value.Kind.SET)) {
            return WRONGTYPE;
        }
        List<String> members = current == null ? List.of() : new ArrayList<>(current.set);
        if (count == null) {
            if (members.isEmpty()) {
                return NULL_BULK;
            }
            return RESP.serializeBulkString(members.get(ThreadLocalRandom.current().nextInt(members.size())));
        }
        if (count == 0 || members.isEmpty()) {
            return EMPTY_ARRAY;
        }
        if (count > 0) {
            List<String> shuffled = new ArrayList<>(members);
            Collections.shuffle(shuffled, ThreadLocalRandom.current());
            return RESP.respArray(shuffled.subList(0, Math.min(count, shuffled.size())).toArray(new String[0]));
        }
        List<String> out = new ArrayList<>(-count);
        for (int i = 0; i < -count; i++) {
            out.add(members.get(ThreadLocalRandom.current().nextInt(members.size())));
        }
        return RESP.respArray(out.toArray(new String[0]));
    }

    // --------------------------------------------------------------- helpers

    private static boolean wrongKind(Value current, Value.Kind expected) {
        return current != null && current.kind != expected;
    }

    /** Parses a count or index, or {@code null} when it is not an integer. */
    private static Integer parseCount(String text) {
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException notANumber) {
            return null;
        }
    }

    private static Mutation error(String reply) {
        return new Mutation(reply, false, null, null);
    }

    private static Mutation unchanged(String reply) {
        return new Mutation(reply, false, null, null);
    }

    private static String wrongArity(String name) {
        return "-ERR wrong number of arguments for '" + name.toLowerCase(Locale.ROOT) + "' command\r\n";
    }

    private static int utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8).length;
    }

    private static LocalDateTime now() {
        return LocalDateTime.now();
    }
}
