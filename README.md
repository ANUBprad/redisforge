# RedisForge

A Redis-compatible in-memory server written in Java 23, speaking the RESP protocol
over TCP. RedisForge implements the core Redis command set along with transactions
and asynchronous master/replica replication.

## Features

- RESP protocol over a plain TCP socket
- `PING`, `ECHO`, `GET`, `SET` (including `PX` millisecond expiry) and `INCR`
- Key expiration: `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`,
  with lazy expiry on read and a bounded active sweep
- Transactions via `MULTI` / `EXEC` / `DISCARD`
- Append-only file persistence on the master, replayed at startup, compacted on demand
  with `BGREWRITEAOF`
- Replication: `REPLCONF`, `PSYNC` (partial and full resync) and `WAIT`
- Master and replica modes, with support for chained replication

## Requirements

- JDK 23
- Maven

## Build

```sh
mvn package
```

This produces a self-contained `target/redisforge.jar`. Add `-DskipTests` to skip the
test suite.

## Test

```sh
mvn test
```

## Run

```sh
java -jar target/redisforge.jar
```

Options:

| Flag | Description | Default |
| --- | --- | --- |
| `--port <port>` | Port to listen on | `6379` |
| `--replicaof "<host> <port>"` | Run as a replica of the given master | master mode |
| `--appendonly yes\|no` | Persist writes to an append-only file and replay it on start | `no` |
| `--appendfilename <path>` | Where the append-only file is kept | `appendonly.aof` |
| `--appendfsync always\|everysec\|no` | How often the file is flushed to disk | `everysec` |
| `--repl-backlog-size <bytes>` | How much of the replication stream a master keeps for partial resyncs | `1048576` |
| `--max-clients <count>` | Connections accepted at once; one more is refused with an error | `256` |
| `--timeout <seconds>` | Close a connection that stays silent this long; `0` disables | `300` |

Start a replica:

```sh
java -jar target/redisforge.jar --port 6480 --replicaof "127.0.0.1 6379"
```

Send a command with any RESP client, for example `redis-cli -p 6379 ping`.

## Persistence

A master can write every applied write to an append-only file, in the RESP format, and
replay it before it starts accepting clients:

```sh
java -jar target/redisforge.jar --appendonly yes --appendfilename data/appendonly.aof
```

- Only the master appends. A replica writes no file of its own and takes its data from
  its master, so a replica and its upstream cannot diverge in what they have on disk.
- Only writes that were applied are written down. A refused increment and a discarded
  transaction leave the keyspace alone, and so leave the file alone.
- A transaction is written as one `MULTI` / `EXEC` block, so it is replayed as the single
  transaction it was.
- `SET` with a relative expiry such as `PX` is written as an absolute deadline in
  milliseconds, `PXAT`. Replay keeps the deadline the client was given instead of
  starting the countdown again, so a key that expired while the server was down comes
  back absent.
- The expiration commands are written the same absolute way: `EXPIRE`, `PEXPIRE`,
  `EXPIREAT` and `PEXPIREAT` become a `PEXPIREAT` with the deadline the store ended up
  holding, `PERSIST` is kept as `PERSIST`, and an expiration that removed the key becomes
  a `DEL`. A refused expiration (the key was not there) is not written at all.
- `--appendfsync always` flushes each write before it is acknowledged, `everysec` flushes
  once a second from a background thread, and `no` leaves flushing to the operating
  system. `no` and `everysec` can lose the last second of writes if the machine stops.
- A file that ends in the middle of a frame, which is what a crash can leave behind, is
  truncated back to its last whole command. A file that is not a whole sequence of
  commands anywhere else is refused, and the server refuses to start rather than serve a
  keyspace it cannot vouch for.
- The file can be compacted at any time with `BGREWRITEAOF`. The reply is `+OK` only
  after the compact file is already on disk: it is built beside the original from the
  state the store is holding now — one `SET` per live key, keeping each key's absolute
  deadline and leaving out keys that have expired — forced whole, and then swapped into
  place in a single step. A failure at any point leaves the file that was already there
  and answers `-ERR` instead. The work happens before the reply, so the server is busy
  for as long as the rewrite takes.
- The rewrite runs under the same lock that records writes, so a write either lands in
  the state the compact file describes or lands in the file the swap left behind; it
  cannot be lost between the two. It is local maintenance: it is not sent to replicas,
  and a replica or a master running with `--appendonly no` answers
  `-ERR no append only file to rewrite`. The fsync policy keeps governing the writes
  that come after the swap.

Restarting the server with the same `--appendfilename` recovers the data:

```sh
java -jar target/redisforge.jar --appendonly yes --appendfilename data/appendonly.aof
```

## Docker

```sh
docker build -t redisforge .
docker run -p 6379:6379 redisforge
```

Run a replica against a master reachable from the container:

```sh
docker run -p 6480:6480 redisforge --port 6480 --replicaof "<master-ip> 6379"
```

## Kubernetes

A Helm chart is included under [`helm/redisforge`](helm/redisforge):

```sh
helm lint helm/redisforge
helm template helm/redisforge
helm install redisforge helm/redisforge
```

Set `image.repository` and `image.tag` in `helm/redisforge/values.yaml` to match the
image you published.

## Supported commands

### Master

`PING`, `ECHO`, `SET`, `GET`, `INCR`, `MULTI`, `EXEC`, `DISCARD`, `INFO replication`,
`REPLCONF`, `PSYNC`, `WAIT`, `BGREWRITEAOF`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`,
`PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`

`DEL` is only handled inside a transaction.

`EXPIRE` / `PEXPIRE` answer `:1` when the deadline was set (or the key removed, for a
non-positive delay) and `:0` when the key does not exist. `TTL` / `PTTL` answer the
remaining time, `:-1` for a key with no deadline, and `:-2` for a key that is not there.
`PERSIST` answers `:1` when a deadline was removed and `:0` when there was none.

### Replica

`PING`, `ECHO`, `GET`, `TTL`, `PTTL`, `INFO replication`, `REPLCONF`, `PSYNC`, `WAIT`

Write commands, including the expiration commands, are rejected with `-READONLY`.
`BGREWRITEAOF` is recognized but refused with `-ERR no append only file to rewrite`,
since a replica keeps no file of its own.

## Errors and protocol limits

- An unknown command is answered with `-ERR unknown command '<name>'` and the connection
  stays open: the original spelling is reflected back, in any letter case.
- A known command with the wrong number of arguments is answered with
  `-ERR wrong number of arguments for '<name>' command` and the connection stays open.
- `SET` accepts the `PX` option in any letter case; an option it does not know is refused
  with `-ERR unsupported option '<option>'` instead of being silently ignored. A
  non-numeric expiry answers `-ERR value is not an integer or out of range`, and zero or
  a negative one answers `-ERR invalid expire time in 'set' command`. A refused `SET`
  stores nothing.
- `INFO` answers `# Server` and `# Replication` for a bare `INFO` or `INFO all`, just
  `# Replication` for `INFO replication`, just `# Server` for `INFO server`, and
  `-ERR Invalid INFO section specified` for anything else.
- An empty multibulk request (`*0`) cannot be resynchronised from, so it is answered with
  `-ERR Protocol error: empty multibulk request` and the connection is closed.
- Errors are replies, not disconnects: after any of the above (except the protocol
  error), the same connection can keep sending commands.

## Concurrency and limits

- Each server (master and replica each own one) serves its clients on its own bounded
  thread pool of at most `--max-clients` threads, so client traffic never competes for
  the JVM's common pool and a slow client cannot grow the thread count without limit.
- At most `--max-clients` connections are admitted at once. One more is refused
  immediately with `-ERR max number of clients reached` and hung up, rather than queued
  for a thread that may never come. Connections already admitted keep being answered
  while the gate is full.
- Keys are protected by a fixed pool of 256 striped locks. Transactions take every
  stripe they need in a single global order, so two transactions that touch the same
  keys in opposite orders cannot deadlock.
- With `--timeout` set above 0, a connection that sends nothing for that long is
  reclaimed by the server; a registered replica is exempt while it follows its master.
- A replica write attempt, an unknown command and an arity mistake are answered with an
  error on the connection that sent them and affect nothing else.

## Shutdown

- The servers stop promptly: new connections stop being admitted, open client
  connections are closed so no handler waits on a read forever, and in-flight handlers
  get a short grace period before being cut off.
- Stopping twice is harmless, and a server that is started again after a stop works
  like a fresh one, including rebinding its port.
- Spring calls `stop()` when the application context closes (`@PreDestroy`), so the
  process does not outlive its context with sockets still open.

## Limitations

- With `--appendonly no`, and always for a replica, data is held in memory only.
  The RDB shipped behind a full resync is empty, so a replica restored that way starts
  empty until its master sends more.
- There is no RDB snapshot: recovery always replays the append-only file.
  `BGREWRITEAOF` compacts it on demand, but nothing rewrites it in the background, so
  between rewrites the file holds the full history of the writes it replays.
- A replica reconnects with a partial resync when it can. A master keeps the newest
  bytes of its stream in a backlog, sized with `--repl-backlog-size` (1 MiB by default),
  and answers a replica that asks to carry on - naming the master's replication id and a
  position the backlog still holds - with `+CONTINUE` and exactly the bytes it missed.
  Anything else: a replica that has never synced, a replication id this master does not
  have, a position ahead of the stream or one that has already fallen out of the backlog,
  or a position that does not parse, is answered with a full resync, which is always safe
  because the replica takes on whatever position the master's stream has reached. Since
  the shipped RDB is empty, a full resync necessarily starts the replica from an empty
  keyspace, and the stream that follows rebuilds it from the writes that come after.
- No failover: a master that goes away is not replaced, and there is no Sentinel or
  Cluster support.
- No authentication and no TLS. Any client that can reach the port has full access.
- Keys expire lazily when they are read, and a bounded background sweep (100 keys per
  tick) removes keys that are never read again. There is no eviction policy, and the
  sweep's removals are not written to the append-only file or propagated: replicas drop
  the same keys lazily or on the master's next deadline-carrying command.
- Only the commands listed under [Supported commands](#supported-commands) are
  implemented; RedisForge is not a drop-in replacement for Redis.

## Project layout

```
src/main/java/Main.java           entry point and argument parsing
src/main/java/Config              Spring configuration used for dependency injection
src/main/java/Components/Server   master and replica TCP servers
src/main/java/Components/Service  RESP codec and command handling
src/main/java/Components/Repository  keyspace storage
src/main/java/Components/Persistence  append-only file writing, replay and fsync
src/main/java/Components/Infra    connection and replica tracking
helm/redisforge                   Helm chart
```

Spring is used only as a dependency-injection container; the server itself is built on
the JDK's blocking socket API.
