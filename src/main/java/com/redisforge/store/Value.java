package com.redisforge.store;

import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * One entry of the keyspace.
 *
 * <p>A value carries a {@link Kind}, which is the Redis type it answers as: a plain
 * string, a hash, a list or a set. Exactly the field that belongs to the kind is set -
 * {@link #val} for a string, {@link #hash}, {@link #list} or {@link #set} for the three
 * collections - and every command is applied against the kind it names. A command that
 * names another kind is answered with the wrong type error instead of being allowed to
 * reinterpret the bytes.</p>
 *
 * <p>The collections keep insertion order: a hash is a {@link LinkedHashMap}, a list an
 * {@link ArrayDeque} and a set a {@link LinkedHashSet}. That order is what a read such as
 * {@code HGETALL} or {@code LRANGE} returns, and it is stable across the whole server. It
 * is deliberately not Redis's own internal ordering, which is an implementation detail
 * that no client should depend on; what is promised here is only that the order is the
 * insertion order and does not change by itself.</p>
 */
public class Value {

    /** The Redis type of an entry. */
    public enum Kind { STRING, HASH, LIST, SET }

    public Kind kind;
    public String val;
    public Map<String, String> hash;
    public Deque<String> list;
    public Set<String> set;
    public LocalDateTime created;
    public LocalDateTime expiry;
    public boolean isDeletedInTransaction;// flag to delete after transaction
    /**
     * The step of the store's access sequence at which this entry was last touched, so
     * the eviction policies can pick the least recently used key. A higher number is
     * later. Written by whichever thread worked on the key and read, at eviction time,
     * by whichever thread is admitting a write, hence volatile.
     */
    public volatile long lastAccess;

    /** A plain string value, which is the only kind the original store ever held. */
    public Value(String val, LocalDateTime created, LocalDateTime expiry) {
        this.kind = Kind.STRING;
        this.val = val;
        this.created = created;
        this.expiry = expiry;
        this.isDeletedInTransaction = false;
    }

    private Value(Kind kind, LocalDateTime created, LocalDateTime expiry) {
        this.kind = kind;
        this.created = created;
        this.expiry = expiry;
        this.isDeletedInTransaction = false;
    }

    /** An empty hash, insertion ordered. */
    public static Value hash(LocalDateTime created, LocalDateTime expiry) {
        Value value = new Value(Kind.HASH, created, expiry);
        value.hash = new LinkedHashMap<>();
        return value;
    }

    /** An empty list, insertion ordered from head to tail. */
    public static Value list(LocalDateTime created, LocalDateTime expiry) {
        Value value = new Value(Kind.LIST, created, expiry);
        value.list = new ArrayDeque<>();
        return value;
    }

    /** An empty set, insertion ordered. */
    public static Value set(LocalDateTime created, LocalDateTime expiry) {
        Value value = new Value(Kind.SET, created, expiry);
        value.set = new LinkedHashSet<>();
        return value;
    }

    /**
     * A deep copy: a collection's elements are copied into a fresh collection, so a
     * command applied to the copy cannot change the entry the store is still holding.
     * The copy carries the same kind, deadline and access step as the original.
     */
    public Value copy() {
        Value copy = switch (kind) {
            case STRING -> new Value(val, created, expiry);
            case HASH -> {
                Value value = hash(created, expiry);
                value.hash.putAll(hash);
                yield value;
            }
            case LIST -> {
                Value value = list(created, expiry);
                value.list.addAll(list);
                yield value;
            }
            case SET -> {
                Value value = set(created, expiry);
                value.set.addAll(set);
                yield value;
            }
        };
        copy.lastAccess = lastAccess;
        return copy;
    }

    /**
     * Whether a collection entry holds nothing any more. A string is never "empty" this
     * way: the empty string is a value a key can hold, while an empty hash, list or set
     * is not a key any command leaves behind.
     */
    public boolean isEmpty() {
        return switch (kind) {
            case STRING -> false;
            case HASH -> hash.isEmpty();
            case LIST -> list.isEmpty();
            case SET -> set.isEmpty();
        };
    }
}
