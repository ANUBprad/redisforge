import Components.Persistence.AppendOnlyPersistence;
import Components.Persistence.FsyncPolicy;
import Components.Server.MasterTcpServer;
import Components.Server.RedisConfig;
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
        int port = 6379;
        redisConfig.setPort(port);
        redisConfig.setRole("master");
        for(int i=0;i<args.length;i++){

            switch(args[i]){
                case "--port":
                    port = Integer.parseInt(args[i+1]);
                    redisConfig.setPort(port);
                    break;
                case "--replicaof":
                    redisConfig.setRole("slave");
//                    "<MASTER_HOST> <MASTER_PORT>"
                    String masterHost = args[i+1].split(" ")[0];
                    int masterPort = Integer.parseInt(args[i+1].split(" ")[1]);

                    redisConfig.setMasterHost(masterHost);
                    redisConfig.setMasterPort(masterPort);

                    break;
                case "--appendonly":
                    redisConfig.setAppendonly(parseBoolean(args[i+1]));
                    break;
                case "--appendfilename":
                    redisConfig.setAppendfilename(args[i+1]);
                    break;
                case "--appendfsync":
                    // parsed here so a bad policy is refused before anything is opened
                    redisConfig.setAppendfsync(FsyncPolicy.parse(args[i+1]).name().toLowerCase());
                    break;
                case "--repl-backlog-size":
                    // how much of the stream is kept in memory for a replica to resume
                    // from, in bytes. It has to be set before the first byte is recorded
                    redisConfig.setReplBacklogSize(Integer.parseInt(args[i+1]));
                    break;
                case "--max-clients":
                    // the ceiling on connections this process holds at once; one beyond it
                    // is refused with an error instead of waiting for a thread
                    redisConfig.setMaxClients(Integer.parseInt(args[i+1]));
                    break;
                case "--timeout":
                    // seconds a connection may stay silent before it is reclaimed,
                    // 0 disables the limit
                    redisConfig.setClientTimeoutMs(Math.multiplyExact(Integer.parseInt(args[i+1]), 1000));
                    break;
            }
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

    private static boolean parseBoolean(String value) {
        if ("yes".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("no".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException("expected yes or no, got '" + value + "'");
    }
}
