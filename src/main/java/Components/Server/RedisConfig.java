package Components.Server;


import Components.Infra.ReplicationBacklog;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class RedisConfig {

    /** how much of the replication stream is kept for a replica to resume from */
    public static final int DEFAULT_REPL_BACKLOG_SIZE = 1_048_576;

    private String role;
    private int port;
    private String masterHost;
    private int masterPort;
    private boolean appendonly = false;
    private String appendfilename = "appendonly.aof";
    private String appendfsync = "everysec";
    private String masterReplId = null;
    // written by the upstream replication thread and read by whatever thread answers a
    // GETACK or reports on INFO
    private volatile Long masterReplOffset = null;
    /**
     * The tail of the stream, kept so a replica can be told to carry on instead of starting
     * again. It is also the lock the stream is counted and sent under, so its identity has
     * to stay put for as long as the server runs: {@link #setReplBacklogSize} widens or
     * narrows this very window in place rather than building a new one, and only while it
     * holds nothing.
     */
    private ReplicationBacklog replicationBacklog = new ReplicationBacklog(DEFAULT_REPL_BACKLOG_SIZE);
    private int replBacklogSize = DEFAULT_REPL_BACKLOG_SIZE;
    /**
     * Whether this node has taken a stream from a master. It is what decides how it attaches
     * again: one that has asks to carry its stream on from where it stands, and one that has
     * never had a stream asks for the whole thing. Read and written from different threads,
     * hence volatile.
     */
    private volatile boolean adoptedStream = false;

    public String getMasterReplId() {
        if(masterReplId == null){
            masterReplId = UUID.randomUUID().toString().replace("-", "")
                    +  UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        }
        return masterReplId;
    }

    public void setMasterReplId(String masterReplId) {
        this.masterReplId = masterReplId;
    }

    public Long getMasterReplOffset() {
        if(masterReplOffset == null){
            masterReplOffset = 0L;
        }
        return masterReplOffset;
    }

    /**
     * Records that bytes of the replication stream have gone out, keeps them where a
     * replica can be resumed from, and returns the offset the stream has now reached.
     *
     * <p>This is the only place the offset moves, and it counts the same bytes that are
     * written to replicas: a master advances it for the writes it propagates, and a replica
     * for the writes it applies and passes on. Handshake and control traffic never goes
     * through here, so the offset stays a position in the write stream and nothing else.</p>
     *
     * <p>Counting and keeping happen together, under the backlog's own lock, and so does
     * sending: a replica is only ever offered a position whose bytes have both been counted
     * and handed to every replica that was already attached.</p>
     */
    public long recordReplicatedBytes(byte[] streamBytes) {
        ReplicationBacklog backlog = replicationBacklog;
        synchronized (backlog) {
            masterReplOffset = getMasterReplOffset() + streamBytes.length;
            backlog.append(streamBytes);
            return masterReplOffset;
        }
    }

    /**
     * Takes on a position in the stream, which is what a full resync does.
     *
     * <p>The backlog is emptied onto that position at the same moment: what it held was
     * counted from the position this hop has just left, so a replica asking about it would
     * be asking about a stream this hop no longer follows. Keeping the two apart here would
     * let an offset name a position the window does not hold, which is the one thing a
     * resume can never be wrong about.</p>
     */
    public void setMasterReplOffset(Long masterReplOffset) {
        ReplicationBacklog backlog = replicationBacklog;
        synchronized (backlog) {
            this.masterReplOffset = masterReplOffset;
            backlog.reset(masterReplOffset == null ? 0L : masterReplOffset);
        }
    }

    /** The window a replica resumes from, which is also the lock the stream is sent under. */
    public ReplicationBacklog getReplicationBacklog() {
        return replicationBacklog;
    }

    /**
     * How much of the stream is kept for a replica to resume from.
     *
     * <p>It can only be changed while nothing has been recorded, and the window is resized
     * where it stands rather than replaced: an empty window names a position of the stream
     * - the one the offset has reached - and every byte recorded from here on has to be
     * kept at the position the offset counts it at. A fresh window starting at 0 would
     * count the same bytes under different names, and every resume from then on would be
     * answered from a window that disagrees with the offset about where the stream is.</p>
     */
    public void setReplBacklogSize(int bytes) {
        synchronized (replicationBacklog) {
            // resize refuses a window that holds the stream, and refuses a size of nothing
            replicationBacklog.resize(bytes);
            replBacklogSize = bytes;
        }
    }

    public int getReplBacklogSize() {
        return replBacklogSize;
    }

    public boolean hasAdoptedStream() {
        return adoptedStream;
    }

    public void markAdoptedStream() {
        adoptedStream = true;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public void setMasterHost(String masterHost) {
        this.masterHost = masterHost;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public void setMasterPort(int masterPort) {
        this.masterPort = masterPort;
    }

    public String getRole() {
        return role;
    }

    public boolean isMaster() {
        return "master".equals(role);
    }

    public int getPort() {
        return port;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean isAppendonly() {
        return appendonly;
    }

    public void setAppendonly(boolean appendonly) {
        this.appendonly = appendonly;
    }

    public String getAppendfilename() {
        return appendfilename;
    }

    public void setAppendfilename(String appendfilename) {
        this.appendfilename = appendfilename;
    }

    public String getAppendfsync() {
        return appendfsync;
    }

    public void setAppendfsync(String appendfsync) {
        this.appendfsync = appendfsync;
    }

}
