package Components.Infra;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ConnectionPool {
    private Set<Client> clients;
    private Set<Slave> slaves;
    // connections come and go on their own threads while propagation is walking the
    // replica set, so both sets and both counters have to tolerate that
    public final AtomicInteger slavesThatAreCaughtUp = new AtomicInteger();
    public final AtomicInteger bytesSentToSlaves = new AtomicInteger();

    public void slaveAck(int ackResponse){
        // replconf getack *
        if(this.bytesSentToSlaves.get() == ackResponse){
            slavesThatAreCaughtUp.incrementAndGet();
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
        Slave slaveToRemove = null;
        for(Slave s: slaves){
            if(s.connection.equals(client)){
                slaveToRemove = s;
                break;
            }
        }
        // a plain client never registered as a replica, and a concurrent set rejects a
        // null key, so there has to be something to remove before asking
        if(slaveToRemove == null){
            return false;
        }

        return slaves.remove(slaveToRemove);
    }
}
