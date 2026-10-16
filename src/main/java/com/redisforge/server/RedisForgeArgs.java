package com.redisforge.server;

import com.redisforge.persistence.FsyncPolicy;

/**
 * The command line options <code>Main</code> passes on to the server's configuration.
 *
 * <p>Parsing lives here rather than in <code>Main</code> so it can be tested directly.
 * An option that is not known, an option missing its value, or a value the server cannot
 * accept stops the parse with a message naming the option: the server refuses to start
 * instead of silently ignoring what it was told or, worse, reading past the end of the
 * argument list. Every value passes through the same validation the setter performs, so
 * a bad {@code --appendfsync} is refused before any file is opened.</p>
 */
public final class RedisForgeArgs {

    private RedisForgeArgs() {
    }

    public static void apply(RedisConfig redisConfig, String[] args) {
        int i = 0;
        while (i < args.length) {
            switch (args[i]) {
                case "--port":
                    redisConfig.setPort(integerValue(args, i, "port"));
                    i += 2;
                    break;
                case "--replicaof":
                    redisConfig.setRole("slave");
                    // "<MASTER_HOST> <MASTER_PORT>"
                    String pairing = stringValue(args, i, "replicaof");
                    String[] split = pairing.split(" ");
                    if (split.length != 2 || split[0].isEmpty() || split[1].isEmpty()) {
                        throw new IllegalArgumentException(
                                "--replicaof wants \"<host> <port>\", got '" + pairing + "'");
                    }
                    redisConfig.setMasterHost(split[0]);
                    try {
                        redisConfig.setMasterPort(Integer.parseInt(split[1]));
                    } catch (NumberFormatException notANumber) {
                        throw new IllegalArgumentException(
                                "--replicaof wants \"<host> <port>\", got '" + pairing + "'");
                    }
                    i += 2;
                    break;
                case "--appendonly":
                    redisConfig.setAppendonly(booleanValue(args, i, "appendonly"));
                    i += 2;
                    break;
                case "--appendfilename":
                    redisConfig.setAppendfilename(stringValue(args, i, "appendfilename"));
                    i += 2;
                    break;
                case "--appendfsync":
                    // parsed here so a bad policy is refused before anything is opened
                    redisConfig.setAppendfsync(
                            FsyncPolicy.parse(stringValue(args, i, "appendfsync")).name().toLowerCase());
                    i += 2;
                    break;
                case "--repl-backlog-size":
                    redisConfig.setReplBacklogSize(integerValue(args, i, "repl-backlog-size"));
                    i += 2;
                    break;
                case "--max-clients":
                    redisConfig.setMaxClients(integerValue(args, i, "max-clients"));
                    i += 2;
                    break;
                case "--timeout":
                    int timeoutSeconds = integerValue(args, i, "timeout");
                    try {
                        redisConfig.setClientTimeoutMs(Math.multiplyExact(timeoutSeconds, 1000));
                    } catch (ArithmeticException overflow) {
                        throw new IllegalArgumentException("--timeout is too large: " + timeoutSeconds);
                    }
                    i += 2;
                    break;
                case "--maxmemory":
                    redisConfig.setMaxmemory(longValue(args, i, "maxmemory"));
                    i += 2;
                    break;
                case "--maxmemory-policy":
                    redisConfig.setMaxmemoryPolicy(stringValue(args, i, "maxmemory-policy"));
                    i += 2;
                    break;
                default:
                    throw new IllegalArgumentException("unknown option '" + args[i] + "'");
            }
        }
    }

    private static String stringValue(String[] args, int i, String flag) {
        if (i + 1 >= args.length) {
            throw new IllegalArgumentException("--" + flag + " needs a value");
        }
        return args[i + 1];
    }

    private static int integerValue(String[] args, int i, String flag) {
        try {
            return Integer.parseInt(stringValue(args, i, flag));
        } catch (NumberFormatException notANumber) {
            throw new IllegalArgumentException(
                    "--" + flag + " wants a number, got '" + args[i + 1] + "'");
        }
    }

    private static long longValue(String[] args, int i, String flag) {
        try {
            return Long.parseLong(stringValue(args, i, flag));
        } catch (NumberFormatException notANumber) {
            throw new IllegalArgumentException(
                    "--" + flag + " wants a number, got '" + args[i + 1] + "'");
        }
    }

    private static boolean booleanValue(String[] args, int i, String flag) {
        String value = stringValue(args, i, flag);
        if ("yes".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("no".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException("--" + flag + " wants yes or no, got '" + value + "'");
    }
}