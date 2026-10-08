package Components.Infra;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ConnectionPool {
    private Set<Client> clients;
    private Set<Slave> slaves;
    // connections come and go on their own threads while propagation is walking the
    // replica set, so both sets and both counters have to tolerate that
    public final AtomicInteger slavesThatAreCaughtUp = new AtomicInteger();
    // the lock a WAIT waits on: an acknowledgement increments the count and notifies
    // under this same lock, so a waiter can never check the count and then block through
    // the acknowledgement it was waiting for
    private final Object acknowledgementLock = new Object();

    /**
     * Forgets what every replica has acknowledged, which is what a WAIT does when it
     * reports.
     *
     * <p>Each replica acknowledges once per WAIT, so one replica that answers twice cannot
     * stand in for two, and a replica that re-acknowledges after the next WAIT counts
     * again: an offset does not stop being acknowledged just because it was waited for.</p>
     */
    public void resetCaughtUpAccounting(){
        synchronized (acknowledgementLock) {
            for (Slave slave : slaves) {
                slave.clearAcknowledgement();
            }
            slavesThatAreCaughtUp.set(0);
        }
    }

    /**
     * Records that a replica acknowledged the position the write stream has reached, and
     * wakes any WAIT that is waiting for it.
     */
    public void countCaughtUp() {
        synchronized (acknowledgementLock) {
            slavesThatAreCaughtUp.incrementAndGet();
            acknowledgementLock.notifyAll();
        }
    }

    /**
     * Waits until at least {@code required} replicas have acknowledged, or until
     * {@code timeoutMs} have passed, and returns how many have.
     *
     * <p>The count is checked under the same lock acknowledgements are made under, so
     * the wait cannot start after the acknowledgement it is waiting for already arrived,
     * and the thread blocks on the monitor instead of spinning for the whole timeout.</p>
     */
    public int awaitCaughtUp(int required, long timeoutMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        synchronized (acknowledgementLock) {
            while (true) {
                int caughtUp = slavesThatAreCaughtUp.get();
                if (caughtUp >= required) {
                    return caughtUp;
                }
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (remainingMs <= 0) {
                    return caughtUp;
                }
                try {
                    // at least one millisecond, so a wait for a sub-millisecond remainder
                    // cannot turn into an unbounded wait for a notification
                    acknowledgementLock.wait(Math.max(1, remainingMs));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return slavesThatAreCaughtUp.get();
                }
            }
        }
    }
    public ConnectionPool() {
        clients = ConcurrentHashMap.newKeySet();
        slaves = ConcurrentHashMap.newKeySet();
    }

    public Set<Client> getClients() {
        return clients;
    }

    public Set<Slave> getSlaves() {
        return slaves;
    }

    /** The replica registered on this connection, or null when it never registered. */
    public Slave slaveFor(Client client){
        for(Slave slave: slaves){
            if(slave.connection.equals(client)){
                return slave;
            }
        }
        return null;
    }

    /**
     * The replica already registered on this listening port, other than the given
     * connection.
     *
     * <p>A replica's listening port is its identity: one process cannot listen twice, so
     * two registrations for the same port are one replica that has come back. Finding the
     * earlier one is what lets a reconnect replace it instead of standing beside it.</p>
     */
    public Slave slaveAtPort(int listeningPort, Client otherThan){
        for(Slave slave: slaves){
            if(slave.listeningPort == listeningPort && slave.connection != otherThan){
                return slave;
            }
        }
        return null;
    }

    /**
     * Lets go of every replica below this hop, which is what this hop does when the stream
     * it follows has been replaced by a full resync.
     *
     * <p>A replica below carries a position in the stream it was following from here. Once
     * this hop has taken a new one, the two name different streams, so no ACK from below
     * could ever be equal to a WAIT above: the connection is closed so that replica attaches
     * again and takes the new position the same way this hop just took it.</p>
     */
    public void dropDownstreamReplicas(){
        for(Slave slave: new ArrayList<>(slaves)){
            slaves.remove(slave);
            slave.clearAcknowledgement();
            slave.connection.close();
        }
        slavesThatAreCaughtUp.set(0);
    }

    public void addClient(Client client){
        if(client!=null)
            clients.add(client);
    }

    public void addSlave(Slave slave){
        if(slave!=null)
            slaves.add(slave);
    }

    public boolean removeClient(Client client){
        return clients.remove(client);
    }

    public boolean removeSlave(Slave slave){
        return slaves.remove(slave);
    }

    public boolean removeSlave(Client client){
        Slave slaveToRemove = slaveFor(client);
        // a plain client never registered as a replica, and a concurrent set rejects a
        // null key, so there has to be something to remove before asking
        if(slaveToRemove == null){
            return false;
        }

        return slaves.remove(slaveToRemove);
    }

    /**
     * Closes every connection this pool holds, which is what a server shutdown does so
     * no handler is left blocked on a read. Closing is idempotent and the handlers'
     * own cleanup removes what is left of each entry, so calling this twice is harmless.
     */
    public void closeAllConnections() {
        for (Client client : new ArrayList<>(clients)) {
            client.close();
        }
        for (Slave slave : new ArrayList<>(slaves)) {
            slave.connection.close();
        }
        clients.clear();
        slaves.clear();
        slavesThatAreCaughtUp.set(0);
    }
}
