# RedisForge

A Redis-compatible in-memory server written in Java 23, speaking the RESP protocol
over TCP. RedisForge implements the core Redis command set along with transactions
and asynchronous master/replica replication.

## Features

- RESP protocol over a plain TCP socket
- `PING`, `ECHO`, `GET`, `SET` (including `PX` millisecond expiry) and `INCR`
- Transactions via `MULTI` / `EXEC` / `DISCARD`
- Append-only file persistence on the master, replayed at startup
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
`REPLCONF`, `PSYNC`, `WAIT`

`DEL` is only handled inside a transaction.

### Replica

`PING`, `ECHO`, `GET`, `INFO replication`, `REPLCONF`, `PSYNC`, `WAIT`

Write commands are rejected with `-READONLY`.

## Limitations

- With `--appendonly no`, and always for a replica, data is held in memory only.
  Replication still performs a full resync and ships an empty RDB payload, so a replica
  restored that way starts empty until its master sends more.
- There is no RDB snapshot and no rewrite: the append-only file only ever grows.
- No authentication and no TLS. Any client that can reach the port has full access.
- Keys expire lazily, when they are read. There is no background expiry sweep and no
  eviction policy.

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
