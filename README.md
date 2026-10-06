# RedisForge

A Redis-compatible in-memory server written in Java 23, speaking the RESP protocol
over TCP. RedisForge implements the core Redis command set along with transactions
and asynchronous master/replica replication.

## Features

- RESP protocol over a plain TCP socket
- `PING`, `ECHO`, `GET`, `SET` (including `PX` millisecond expiry) and `INCR`
- Transactions via `MULTI` / `EXEC` / `DISCARD`
- Append-only file persistence on the master, replayed at startup, compacted on demand
  with `BGREWRITEAOF`
- Replication: `REPLCONF`, `PSYNC` (full resync) and `WAIT`
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
`REPLCONF`, `PSYNC`, `WAIT`, `BGREWRITEAOF`

`DEL` is only handled inside a transaction.

### Replica

`PING`, `ECHO`, `GET`, `INFO replication`, `REPLCONF`, `PSYNC`, `WAIT`

Write commands are rejected with `-READONLY`. `BGREWRITEAOF` is recognized but refused
with `-ERR no append only file to rewrite`, since a replica keeps no file of its own.

## Limitations

- With `--appendonly no`, and always for a replica, data is held in memory only.
  Replication still performs a full resync and ships an empty RDB payload, so a replica
  restored that way starts empty until its master sends more.
- There is no RDB snapshot: recovery always replays the append-only file.
  `BGREWRITEAOF` compacts it on demand, but nothing rewrites it in the background, so
  between rewrites the file holds the full history of the writes it replays.
- Replication is full-resync only. There is no replication backlog and no partial
  resync: a replica that reconnects, or one that attaches later, is always resynced
  from scratch with an empty payload.
- No failover: a master that goes away is not replaced, and there is no Sentinel or
  Cluster support.
- No authentication and no TLS. Any client that can reach the port has full access.
- Keys expire lazily, when they are read. There is no background expiry sweep and no
  eviction policy.
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
