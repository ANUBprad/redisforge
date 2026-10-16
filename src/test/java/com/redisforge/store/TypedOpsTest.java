package com.redisforge.store;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The hash, list and set commands, exercised against a bare {@link TypedOps} with no
 * server around it. Each test drives commands through the same read and mutate entry
 * points every path in the server uses, keeps the resulting keyspace in a plain map, and
 * checks both the reply and the state that was left behind.
 *
 * <p>Replies are decoded into plain values - a {@code Long} for an integer, a {@code
 * String} for a bulk or simple string, a {@code List} for an array, {@code null} for a
 * null bulk or array - so a test says what a command answers without repeating the wire
 * framing.</p>
 */
class TypedOpsTest {

    private final Map<String, Value> keyspace = new HashMap<>();

    @Test
    void hsetCountsOnlyNewFieldsAndOverwritesWithoutAdding() {
        assertEquals(2L, run("HSET", "h", "a", "1", "b", "2"));
        assertEquals(0L, run("HSET", "h", "a", "9"), "an overwrite was counted as a new field");
        assertEquals(2, keyspace.get("h").hash.size());
        assertEquals("9", run("HGET", "h", "a"));
    }

    @Test
    void hsetToTheSameValueReportsNoChange() {
        run("HSET", "h", "a", "1");
        Value before = keyspace.get("h");
        assertEquals(0L, run("HSET", "h", "a", "1"));
        assertTrue(before == keyspace.get("h"), "an unchanged HSET replaced the entry anyway");
    }

    @Test
    void hmgetAnswersNullForMissingFieldsAndKeepsOrder() {
        run("HSET", "h", "a", "1", "b", "2");
        assertEquals(Arrays.asList("1", null, "2"), run("HMGET", "h", "a", "missing", "b"));
    }

    @Test
    void hdelRemovingTheLastFieldDropsTheKey() {
        run("HSET", "h", "a", "1", "b", "2");
        assertEquals(1L, run("HDEL", "h", "a"));
        assertEquals(1L, run("HDEL", "h", "b"));
        assertNull(keyspace.get("h"), "an emptied hash was left in the keyspace");
    }

    @Test
    void hsetnxRefusesToOverwrite() {
        assertEquals(1L, run("HSETNX", "h", "a", "1"));
        assertEquals(0L, run("HSETNX", "h", "a", "2"));
        assertEquals("1", run("HGET", "h", "a"));
    }

    @Test
    void hstrlenCountsBytesNotCharacters() {
        run("HSET", "h", "a", "éé");
        assertEquals(4L, run("HSTRLEN", "h", "a"));
        assertEquals(0L, run("HSTRLEN", "h", "missing"));
    }

    @Test
    void hgetallHkeysHvalsFollowInsertionOrder() {
        run("HSET", "h", "b", "2", "a", "1", "c", "3");
        assertEquals(Arrays.asList("b", "2", "a", "1", "c", "3"), run("HGETALL", "h"));
        assertEquals(Arrays.asList("b", "a", "c"), run("HKEYS", "h"));
        assertEquals(Arrays.asList("2", "1", "3"), run("HVALS", "h"));
    }

    @Test
    void lpushReversesItsArgumentsAtTheHeadAndRpushAppends() {
        assertEquals(3L, run("LPUSH", "l", "a", "b", "c"));
        assertEquals(Arrays.asList("c", "b", "a"), run("LRANGE", "l", "0", "-1"));
        assertEquals(4L, run("RPUSH", "l", "d"));
        assertEquals(Arrays.asList("c", "b", "a", "d"), run("LRANGE", "l", "0", "-1"));
    }

    @Test
    void popWithACountAnswersAnArrayAndDropsTheKeyWhenEmptied() {
        run("RPUSH", "l", "a", "b", "c", "d");
        assertEquals(Arrays.asList("a", "b"), run("LPOP", "l", "2"));
        assertEquals(Arrays.asList("d", "c"), run("RPOP", "l", "2"));
        assertNull(keyspace.get("l"), "an emptied list was left in the keyspace");
    }

    @Test
    void lrangeAndLindexResolveNegativePositions() {
        run("RPUSH", "l", "a", "b", "c", "d", "e");
        assertEquals(Arrays.asList("d", "e"), run("LRANGE", "l", "-2", "-1"));
        assertEquals("e", run("LINDEX", "l", "-1"));
        assertNull(run("LINDEX", "l", "99"));
    }

    @Test
    void lsetRewritesInPlaceAndRefusesAnOutOfRangeIndex() {
        run("RPUSH", "l", "a", "b", "c");
        assertEquals("OK", run("LSET", "l", "1", "B"));
        assertEquals("B", run("LINDEX", "l", "1"));
        assertEquals("ERR index out of range", run("LSET", "l", "9", "x"));
        assertEquals("OK", run("LSET", "l", "-1", "C"));
        assertEquals("C", run("LINDEX", "l", "2"));
    }

    @Test
    void lremCountsFromHeadTailOrBothEnds() {
        run("RPUSH", "l", "x", "a", "x", "a", "x", "a");
        assertEquals(1L, run("LREM", "l", "1", "a"), "a positive count removes from the head");
        assertEquals(Arrays.asList("x", "x", "a", "x", "a"), run("LRANGE", "l", "0", "-1"));
        assertEquals(1L, run("LREM", "l", "-1", "a"), "a negative count removes from the tail");
        assertEquals(Arrays.asList("x", "x", "a", "x"), run("LRANGE", "l", "0", "-1"));
        assertEquals(3L, run("LREM", "l", "0", "x"), "a zero count removes every match");
        assertEquals(Arrays.asList("a"), run("LRANGE", "l", "0", "-1"));
    }

    @Test
    void ltrimKeepsTheWindowAndDropsTheKeyWhenNothingIsLeft() {
        run("RPUSH", "l", "a", "b", "c", "d", "e");
        assertEquals("OK", run("LTRIM", "l", "1", "3"));
        assertEquals(Arrays.asList("b", "c", "d"), run("LRANGE", "l", "0", "-1"));
        assertEquals("OK", run("LTRIM", "l", "5", "9"));
        assertNull(keyspace.get("l"), "an emptied list was left in the keyspace");
    }

    @Test
    void saddCountsOnlyNewMembersAndDeduplicates() {
        assertEquals(3L, run("SADD", "s", "a", "b", "c", "a"));
        assertEquals(0L, run("SADD", "s", "a", "b"));
        assertEquals(3L, run("SCARD", "s"));
    }

    @Test
    void sremRemovingTheLastMemberDropsTheKey() {
        run("SADD", "s", "a", "b");
        assertEquals(2L, run("SREM", "s", "a", "b"));
        assertNull(keyspace.get("s"), "an emptied set was left in the keyspace");
    }

    @Test
    void smembersFollowInsertionOrderAndSmismemberReportsEachMember() {
        run("SADD", "s", "c", "a", "b");
        assertEquals(Arrays.asList("c", "a", "b"), run("SMEMBERS", "s"));
        assertEquals(Arrays.asList(1L, 0L, 1L), run("SMISMEMBER", "s", "a", "x", "c"));
    }

    @Test
    void spopIsWrittenDownAsTheSremOfTheMemberItTook() {
        run("SADD", "s", "a");
        TypedOps.Mutation mutation = TypedOps.mutate(new String[]{"SPOP", "s"}, keyspace.get("s"));
        assertTrue(mutation.changed());
        assertEquals(3, mutation.propagation().length);
        assertEquals("SREM", mutation.propagation()[0]);
        assertEquals("s", mutation.propagation()[1]);
        assertEquals("a", mutation.propagation()[2], "the popped member was not the one named for removal");
    }

    @Test
    void spopWithACountNamesEveryMemberItRemoved() {
        run("SADD", "s", "a", "b", "c");
        TypedOps.Mutation mutation = TypedOps.mutate(new String[]{"SPOP", "s", "2"}, keyspace.get("s"));
        assertEquals(4, mutation.propagation().length);
        assertEquals("SREM", mutation.propagation()[0]);
        assertEquals(1, mutation.value().set.size(), "two members were not taken from a set of three");
    }

    @Test
    void srandmemberWithANegativeCountMayRepeatButNeverChangesTheSet() {
        run("SADD", "s", "a", "b", "c");
        Object reply = run("SRANDMEMBER", "s", "-6");
        assertEquals(6, ((List<?>) reply).size());
        assertEquals(3L, run("SCARD", "s"), "a random read changed the set");
    }

    @Test
    void aCommandAgainstTheWrongKindIsRefusedWhateverTheKind() {
        run("SET", "str", "v");
        assertWrongType(run("HGET", "str", "f"));
        assertWrongType(run("LPUSH", "str", "x"));
        assertWrongType(run("SADD", "str", "x"));
        run("RPUSH", "l", "x");
        assertWrongType(run("HGET", "l", "f"));
        assertWrongType(run("SADD", "l", "x"));
        run("SADD", "s", "x");
        assertWrongType(run("LPUSH", "s", "x"));
        assertWrongType(run("HGETALL", "s"));
    }

    @Test
    void aReadOfAMissingKeyAnswersTheEmptyForm() {
        assertEquals(0L, run("HLEN", "absent"));
        assertEquals(0L, run("LLEN", "absent"));
        assertEquals(0L, run("SCARD", "absent"));
        assertEquals(List.of(), run("HGETALL", "absent"));
        assertNull(run("HGET", "absent", "f"));
        assertEquals(List.of(), run("SMEMBERS", "absent"));
    }

    @Test
    void arityIsCheckedBeforeTheKind() {
        assertTrue(((String) run("HSET", "h", "onlyfield")).startsWith("ERR wrong number of arguments"));
        assertTrue(((String) run("LPUSH", "l")).startsWith("ERR wrong number of arguments"));
    }

    @Test
    void aCountThatIsNotAnIntegerIsRefused() {
        run("RPUSH", "l", "a");
        assertTrue(((String) run("LPOP", "l", "x")).startsWith("ERR value is not an integer"));
        assertTrue(((String) run("LRANGE", "l", "0", "x")).startsWith("ERR value is not an integer"));
        assertTrue(((String) run("SPOP", "s", "x")).startsWith("ERR value is not an integer"));
    }

    // --------------------------------------------------------------- harness

    private Object run(String... command) {
        String name = command[0].toUpperCase(Locale.ROOT);
        if (name.equals("SET")) {
            keyspace.put(command[1], new Value(command[2], LocalDateTime.now(), LocalDateTime.MAX));
            return "OK";
        }
        if (TypedOps.isRead(name)) {
            return decode(TypedOps.read(command, keyspace.get(command[1])));
        }
        TypedOps.Mutation mutation = TypedOps.mutate(command, keyspace.get(command[1]));
        if (mutation.changed()) {
            if (mutation.value() == null) {
                keyspace.remove(command[1]);
            } else {
                keyspace.put(command[1], mutation.value());
            }
        }
        return decode(mutation.reply());
    }

    private static void assertWrongType(Object reply) {
        assertTrue(reply instanceof String && ((String) reply).startsWith("WRONGTYPE"),
                "not a wrong type reply: " + reply);
    }

    /** Decodes one RESP reply into plain Java values: Long, String, List or null. */
    private static Object decode(String raw) {
        return decode(raw, new int[]{0});
    }

    private static Object decode(String raw, int[] position) {
        char type = raw.charAt(position[0]++);
        int lineEnd = raw.indexOf("\r\n", position[0]);
        String line = raw.substring(position[0], lineEnd);
        position[0] = lineEnd + 2;
        return switch (type) {
            case '+', '-' -> line;
            case ':' -> Long.parseLong(line);
            case '$' -> {
                int length = Integer.parseInt(line);
                if (length < 0) {
                    yield null;
                }
                String payload = raw.substring(position[0], position[0] + length);
                position[0] += length + 2;
                yield payload;
            }
            case '*' -> {
                int count = Integer.parseInt(line);
                if (count < 0) {
                    yield null;
                }
                List<Object> items = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    items.add(decode(raw, position));
                }
                yield items;
            }
            default -> throw new IllegalStateException("not RESP: " + raw);
        };
    }
}
