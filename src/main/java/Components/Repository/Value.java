package Components.Repository;

import java.time.LocalDateTime;

public class Value {
    public String val;
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
    public Value( String val, LocalDateTime created, LocalDateTime expiry) {
        this.created = created;
        this.val = val;
        this.expiry = expiry;
        this.isDeletedInTransaction = false;
    }
}
