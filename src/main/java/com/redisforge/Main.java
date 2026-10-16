import com.redisforge.persistence.AppendOnlyPersistence;
import com.redisforge.server.MasterTcpServer;
import com.redisforge.server.RedisConfig;
import com.redisforge.server.RedisForgeArgs;
import com.redisforge.server.SlaveTcpServer;
import com.redisforge.config.AppConfig;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.IOException;


public class Main {
    public static void main(String[] args) throws IOException {
        AnnotationConfigApplicationContext context =
              new AnnotationConfigApplicationContext(AppConfig.class);
        MasterTcpServer master = context.getBean(MasterTcpServer.class);
        SlaveTcpServer slave = context.getBean(SlaveTcpServer.class);
        RedisConfig redisConfig = context.getBean(RedisConfig.class);
        redisConfig.setPort(6379);
        redisConfig.setRole("master");
        try {
            RedisForgeArgs.apply(redisConfig, args);
        } catch (IllegalArgumentException badOption) {
            System.err.println("redisforge: " + badOption.getMessage());
            System.err.println("options: --port <port> --replicaof \"<host> <port>\" "
                    + "--appendonly yes|no --appendfilename <path> "
                    + "--appendfsync always|everysec|no --repl-backlog-size <bytes> "
                    + "--max-clients <count> --timeout <seconds> "
                    + "--maxmemory <bytes> --maxmemory-policy <policy>");
            System.exit(1);
        }

        AppendOnlyPersistence appendOnly = context.getBean(AppendOnlyPersistence.class);
        if(redisConfig.getRole().equals("slave")){
            // the accept loop and the follower's retries are both ended on the way out,
            // so a signal closes the replica's sockets instead of leaving them open
            Runtime.getRuntime().addShutdownHook(new Thread(slave::stop, "replica-shutdown"));
            slave.startServer();
        }else{
            // recovery runs before the listening socket exists, so the first client to
            // connect already sees the recovered dataset. A file that cannot be trusted
            // stops the server here instead of starting up with a dataset nobody asked for.
            appendOnly.start();
            // one hook for the whole master and one ordering: the listener stops first so
            // no handler keeps writing, and only then is the append only file flushed and
            // closed. This is the same action @PreDestroy runs, and it is idempotent, so a
            // signal and a context close cannot double-close or race each other
            Runtime.getRuntime().addShutdownHook(new Thread(master::shutdown, "master-shutdown"));
            master.startServer();
        }
    }
}
