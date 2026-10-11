import Components.Persistence.AppendOnlyPersistence;
import Components.Server.MasterTcpServer;
import Components.Server.RedisConfig;
import Components.Server.RedisForgeArgs;
import Components.Server.SlaveTcpServer;
import Config.AppConfig;
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
            // the file is flushed and closed on the way out, including on a signal
            Runtime.getRuntime().addShutdownHook(new Thread(appendOnly::close, "appendonly-shutdown"));
            master.startServer();
        }
    }
}
