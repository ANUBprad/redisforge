# RedisForge

A Redis-compatible in-memory server written in Java 23, speaking the RESP protocol
over TCP. RedisForge implements the core Redis command set along with transactions
and asynchronous master/replica replication.

## Features

- RESP protocol over a plain TCP socket
- `PING`, `ECHO`, `GET`, `SET` (including `PX` millisecond expiry) and `INCR`
- Transactions via `MULTI` / `EXEC` / `DISCARD`
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

Start a replica:

```sh
java -jar target/redisforge.jar --port 6480 --replicaof "127.0.0.1 6379"
```

Send a command with any RESP client, for example `redis-cli -p 6379 ping`.

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

- Data is held in memory only. There is no persistence; replication always performs a
  full resync and ships an empty RDB payload.
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
src/main/java/Components/Infra    connection and replica tracking
helm/redisforge                   Helm chart
```

Spring is used only as a dependency-injection container; the server itself is built on
the JDK's blocking socket API.
