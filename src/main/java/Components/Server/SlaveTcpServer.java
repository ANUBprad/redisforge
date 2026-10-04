package Components.Server;

import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
import Components.Service.CommandHandler;
import Components.Service.RespSerializer;
import Components.Infra.Client;
import Components.Infra.RespStream;
import Components.Service.ResponseDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
public class SlaveTcpServer {
    private static final Logger logger = Logger.getLogger(SlaveTcpServer.class.getName());
    @Autowired
    private RespSerializer respSerializer;
    @Autowired
    private CommandHandler commandHandler;
    @Autowired
    private RedisConfig redisConfig;
    @Autowired
    private ConnectionPool connectionPool;
    public void startServer(){
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = redisConfig.getPort();

        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

            CompletableFuture<Void> slaveConnectionFuture = CompletableFuture.runAsync(this::initiateSlavery);
            slaveConnectionFuture.thenRun(()->System.out.println("Replication completed"));

            int id = 0;
            while (true) {
                clientSocket = serverSocket.accept();
                id++;
                Socket finalClientSocket = clientSocket;

                InputStream inputStream = clientSocket.getInputStream();
                OutputStream outputStream = clientSocket.getOutputStream();

                Client client = new Client(finalClientSocket, inputStream, outputStream, id );
                CompletableFuture.runAsync(() -> {
                    try {
                        handleClient(client);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.getMessage());
            }
        }
    }

    private void initiateSlavery() {
        try(Socket master = new Socket(redisConfig.getMasterHost(), redisConfig.getMasterPort())){
            InputStream inputStream = master.getInputStream();
            OutputStream outputStream = master.getOutputStream();

            //part 1 of the handshake
            outputStream.write("*1\r\n$4\r\nPING\r\n".getBytes());
            logger.log(Level.FINE, readLine(inputStream));

            //part 2 of the handshake
            int lenListeningPort = (redisConfig.getPort()+"").length();
            int listeningPort = redisConfig.getPort();
            String replconf = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" +
                    (lenListeningPort+"") + "\r\n" + (listeningPort+"") +
                    "\r\n";
            outputStream.write(replconf.getBytes());
            logger.log(Level.FINE, readLine(inputStream));

            replconf = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
            outputStream.write(replconf.getBytes());
            logger.log(Level.FINE, readLine(inputStream));

            // part 3 of the handshake
            outputStream.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".getBytes());
            readFullResync(inputStream);

            streamFromMaster(new Client(master, inputStream, outputStream, -1));
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    /**
     * Applies the commands the master streams down once the RDB payload is out of the way.
     * The upstream is framed exactly the way a client's is: only whole RESP arrays are
     * decoded, in arrival order, and a partly received array waits for the rest of it
     * instead of being read as if the socket had ended a message there.
     */
    void streamFromMaster(Client master) throws IOException {
        RespStream replicated = new RespStream(respSerializer);
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = master.inputStream.read(buffer)) != -1) {
            replicated.append(buffer, bytesRead);
            for (String[] command : replicated.drain()) {
                String response = handleCommandFromMaster(command, master);
                if(response != null && !response.isEmpty())
                    master.outputStream.write(response.getBytes());
                // the master counts a command by the bytes it occupies, so ack the same
                redisConfig.setMasterReplOffset(redisConfig.getMasterReplOffset()
                        + respSerializer.respArray(command).getBytes().length);
            }
        }
    }

    /**
     * Consumes the answer to PSYNC: the "+FULLRESYNC" status line, the header of the RDB
     * bulk string behind it, and exactly as many payload bytes as that header declares.
     * This master sends no CRLF after the payload, so the first replicated command picks
     * up on the very next byte.
     */
    private void readFullResync(InputStream inputStream) throws IOException {
        String status = readLine(inputStream);
        if(status == null || !status.startsWith("+FULLRESYNC")){
            throw new IOException("expected +FULLRESYNC from the master but read: " + status);
        }
        logger.log(Level.FINE, status);

        String header = readLine(inputStream);
        if(header == null || !header.startsWith("$")){
            throw new IOException("expected the length of the RDB but read: " + header);
        }
        int rdbLength = Integer.parseInt(header.substring(1));

        // the RDB is raw bytes, so it is counted off rather than scanned for a delimiter
        byte[] rdb = inputStream.readNBytes(rdbLength);
        if(rdb.length != rdbLength){
            throw new EOFException("the RDB ended after " + rdb.length + " of " + rdbLength + " bytes");
        }
    }

    /** Reads one CRLF terminated line, or null once the master has hung up. */
    private String readLine(InputStream inputStream) throws IOException {
        StringBuilder line = new StringBuilder();
        int b;
        while ((b = inputStream.read()) != -1) {
            if(b == '\r'){
                if(inputStream.read() != '\n'){
                    throw new IOException("expected LF after CR");
                }
                return line.toString();
            }
            line.append((char) b);
        }
        return line.length() == 0 ? null : line.toString();
    }

    private String handleCommandFromMaster(String[] command, Client master) {
        System.out.println("================================= received command from master =================================");
        for(String c: command){
            System.out.print(c+" ");
        }
        String cmd = command[0];
        cmd = cmd.toUpperCase();

        String res = "";
        switch (cmd){
            case "SET":
                commandHandler.set(command);
                String commandRespString = respSerializer.respArray(command);
                byte[] toCount = commandRespString.getBytes();
                connectionPool.bytesSentToSlaves += toCount.length;
                CompletableFuture.runAsync(()->propagate(command));
                break;
            case "REPLCONF":
                res = commandHandler.replconf(command, master);
                break;
        }
        return res;
    }

    private void propagate(String[] command) {
        String commandRespString = respSerializer.respArray(command);
        try{
            for(Slave slave: connectionPool.getSlaves()){
                System.out.println("========================= sending command down to slave ==============================");
                System.out.println("command: "+commandRespString);
                System.out.println(slave.connection.id);
                InetAddress remoteAddress = slave.connection.socket.getInetAddress();
                System.out.println("Remote IP address: " + remoteAddress.getHostAddress() +": "+slave.connection.socket.getPort());
                slave.send(commandRespString.getBytes());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void handleClient(Client client) throws IOException {
        connectionPool.addClient(client);
        RespStream respStream = new RespStream(respSerializer);
        try {
            byte[] buffer = new byte[client.socket.getReceiveBufferSize()];
            int bytesRead;
            // read() blocks until data arrives; it returns -1 once the peer has
            // disconnected, which is what ends this loop. isConnected() is not a
            // liveness check - it stays true forever after the peer goes away.
            while ((bytesRead = client.inputStream.read(buffer)) != -1) {
                // a read is not a command: respStream keeps a partial frame until
                // the rest of it arrives, and splits out every whole one
                respStream.append(buffer, bytesRead);
                for (String[] command : respStream.drain()) {
                    handleCommand(command, client);
                }
            }
        } finally {
            // runs for a normal disconnect and for an I/O failure alike, so the
            // registries never keep a dead connection around
            connectionPool.removeClient(client);
            connectionPool.removeSlave(client);
            client.close();
        }
    }

    private void handleCommand(String[] command, Client client) throws IOException {
        String res = "";
        byte[] data = null;
        switch (command[0]){
            case "PING":
                res = commandHandler.ping(command);
                break;
            case "ECHO":
                res = commandHandler.echo(command);
                break;
            case "SET":
                res = "-READONLY You can't write against a replica.\r\n";
                break;
            case "GET":
                res = commandHandler.get(command);
                break;
            case "INFO":
                res = commandHandler.info(command);
                break;
            case "PSYNC":
                ResponseDto resDto = commandHandler.psync(command);
                res = resDto.response;
                data = resDto.data;
                break;
            case "WAIT":
                if(connectionPool.bytesSentToSlaves == 0){
                    res = respSerializer.respInteger(connectionPool.slavesThatAreCaughtUp);
                    break;
                }

                Instant start = Instant.now();
                res = commandHandler.wait(command, start);
                connectionPool.slavesThatAreCaughtUp = 0;
                break;
        }
        client.send(res, data);
    }
}
