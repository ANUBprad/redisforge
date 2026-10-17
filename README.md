# RedisForge

A Redis-compatible in-memory server written in Java 23, speaking the RESP protocol
over TCP. RedisForge implements the core Redis command set along with transactions
and asynchronous master/replica replication.

## Source layout

The code is grouped by responsibility under the standard Maven source tree:

```text
src/
  main/java/com/redisforge/
    Main.java
    command/       Command dispatch and command semantics
    config/        Spring wiring
    network/       Client connection primitives
    persistence/   Append-only file, replay and fsync policy
    protocol/      RESP framing, parsing and replies
    replication/   Replica links and replication backlog
    server/        Master/replica listeners and runtime options
    store/         Key/value state, typed values, expiry and memory policy
  test/java/com/redisforge/
    benchmark/     Standalone benchmark harness
    persistence/   AOF recovery tests
    protocol/      RESP tests
    replication/   Replication backlog tests
    server/        Socket, command, concurrency and replication tests
    store/         Store, typed-value and memory tests
```

The package names and directory names match, so classes can be found by their domain
rather than through a generic `Components` bucket. Tests follow the same package boundaries
as the code they exercise.

## Features

- RESP protocol over a plain TCP socket
- `PING`, `ECHO`, `GET`, `SET` (including `PX` millisecond expiry) and `INCR`
- Hashes, lists and sets: `HSET`, `HGET`, `HGETALL`, `HKEYS`, `HVALS`, `HMGET`,
  `HSTRLEN`, `HDEL`, `HSETNX`, `HLEN`, `HEXISTS`; `LPUSH`, `RPUSH`, `LPOP`, `RPOP`,
  `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LREM`, `LTRIM`; `SADD`, `SREM`, `SISMEMBER`,
  `SMISMEMBER`, `SCARD`, `SMEMBERS`, `SPOP`, `SRANDMEMBER`, with strict value-type
  checking across every kind
- Key expiration: `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`,
  with lazy expiry on read and a bounded active sweep
- Transactions via `MULTI` / `EXEC` / `DISCARD`
- Append-only file persistence on the master, replayed at startup, compacted on demand
  with `BGREWRITEAOF`
- Replication: `REPLCONF`, `PSYNC` (partial and full resync) and `WAIT`
- Master and replica modes, with support for chained replication
- Memory accounting with an optional `--maxmemory` ceiling and Redis-style eviction
  policies (`noeviction`, `allkeys-lru`, `allkeys-random`, `volatile-lru`,
  `volatile-random`, `volatile-ttl`), reported through `INFO memory`

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

## Continuous integration and delivery

The GitHub Actions pipeline is split so that a pull request can prove the build without
being able to publish or deploy anything:

- [`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on every pull request and on
  every push to `main` (and on `v*` tags). It runs the full test suite with Java 23 and
  `mvn clean verify`, uploads the surefire reports on failure, lints and renders the Helm
  chart, and builds the production Docker image. On a push to `main` or a version tag it
  also publishes the image to the GitHub Container Registry as
  `ghcr.io/<owner>/redisforge`, tagged with an immutable `sha-<commit>` alongside the
  branch, tag and `latest` tags. Pull requests build the image but never push it, and only
  the `publish` job is granted `packages: write`.
- [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml) deploys to Azure Container
  Apps only after a `ci` run on `main` has finished successfully. It always deploys the
  exact `sha-<commit>` image that build published - never a moving `latest` tag - behind a
  protected `production` environment, and serialises deploys so two never overlap. It can
  also be run manually with an earlier `image_tag`, which is how a known-good build is
  rolled back. See [`deploy/azure/README.md`](deploy/azure/README.md) for the Azure setup,
  cost, persistence limits, smoke tests and cleanup.

Because RedisForge has no AUTH or TLS, the deployed service uses **internal** TCP ingress
only and is never exposed to the public internet.

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
| `--maxmemory <bytes>` | Memory ceiling for the keyspace; `0` means no limit | `0` |
| `--maxmemory-policy <policy>` | How a write past `--maxmemory` is admitted: `noeviction`, `allkeys-lru`, `allkeys-random`, `volatile-lru`, `volatile-random`, `volatile-ttl` | `noeviction` |

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
  state the store is holding now — one `SET` per live string, an `HSET`, `RPUSH` or
  `SADD` per live hash, list or set, each keeping the key's absolute deadline as a
  `PEXPIREAT` and leaving out keys that have expired — forced whole, and then swapped
  into place in a single step. A failure at any point leaves the file that was already
  there and answers `-ERR` instead. The work happens before the reply, so the server is
  busy for as long as the rewrite takes.
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

## Memory limits

A master can be given a ceiling on how large its keyspace is allowed to grow, and a rule
for what to do when a write would put it over:

```sh
java -jar target/redisforge.jar --maxmemory 268435456 --maxmemory-policy allkeys-lru
```

- The size of a key is a logical estimate, not the JVM's resident memory: a fixed
  overhead per entry, the UTF-8 bytes of the key and the value, and a few bytes more for
  a key that carries a deadline. A hash, list or set also charges each of its fields,
  elements or members, with the same overhead per element as the entry itself, so a
  collection's cost grows with its contents. It is stable across runs and platforms,
  which is what a limit has to be to mean the same thing everywhere. `INFO memory`
  reports the same number it compares against, so `used_memory` is the quantity
  `maxmemory` is judged against, not the process's RSS.
- A write is judged by the size the keyspace *would* have once it lands: the bytes the
  target key already holds are given back before the new entry is charged. A write that
  fits is admitted with nothing removed.
- With `--maxmemory-policy noeviction` a write that would not fit is refused with
  `-OOM command not allowed when used memory > 'maxmemory'.` and the keyspace is left
  exactly as it was.
- The other policies remove keys until the write fits. `allkeys-lru` and
  `allkeys-random` may remove any key; `volatile-lru`, `volatile-random` and
  `volatile-ttl` remove only keys that carry a deadline. When the policy has nothing it
  may remove — the keyspace is empty, or a `volatile-` policy finds no key with a
  deadline — the write is refused with the same `-OOM` error.
- A key the policy removes is written to the append-only file and passed to replicas as
  the `DEL` it is, so a replica ends up holding exactly what its master held. A replica
  never evicts on its own account: it applies the writes its master sends, `DEL`s
  included, even when a ceiling is configured on the replica too.
- A transaction is admitted as a unit or refused as one. If the state it would commit
  does not fit the ceiling, `EXEC` answers `-OOM` and commits nothing, rather than being
  made to fit by removing keys the queued commands never mentioned.

## Hashes, lists and sets

Every key carries a type: a string, a hash, a list or a set. Exactly one kind is held
at a key, and a command that names another kind is answered with `-WRONGTYPE Operation
against a key holding the wrong kind of value` instead of being allowed to reinterpret
the bytes. `GET` and `INCR` are string commands, so they answer `-WRONGTYPE` against a
hash, list or set, and the typed reads do the same against a string.

- A hash is a `field -> value` map, a list an ordered sequence of elements, a set a
  collection of distinct members. All three keep insertion order, and that order is what
  `HGETALL`, `HKEYS`, `HVALS` and `SMEMBERS` return. It is stable across the whole
  server, not Redis's own internal ordering, which no client should depend on.
- `HSET` answers the number of fields it added; setting a field to the value it already
  held changes nothing. `LPUSH` pushes each argument one by one, so `LPUSH key a b c`
  leaves the list `[c, b, a]`. `SADD` answers how many members were new; members already
  present are ignored.
- Counts and indexes follow Redis: an `LPOP`/`RPOP`/`SPOP` count that is negative is an
  error while `SRANDMEMBER` accepts it (returning duplicates); zero removes every match
  for `LREM`; ranges and indexes may be negative and are resolved against the tail;
  `LRANGE` with `stop` beyond the end stops at the last element.
- A hash whose last field, a list whose last element or a set whose last member is
  removed stops existing: the empty collection is not a key.
- `SPOP` picks members by random. Because a random pop has to mean the same thing on a
  master, a replica and a replay, it is recorded and propagated as the `SREM` of exactly
  the members it took; it is never written down as the `SPOP` itself. The reply to the
  client is still the member or members popped.
- A typed key takes a deadline like a string: `EXPIRE`, `PEXPIRE` and friends work on a
  hash, list or set and keep the collection intact. A typed command against an expired
  or missing key answers the empty form: `:0` for the length and membership counts,
  `$-1` for a missing single value, and an empty array for a missing whole-key read.

## Benchmark

[`src/test/java/com/redisforge/benchmark/BenchmarkMain.java`](src/test/java/com/redisforge/benchmark/BenchmarkMain.java)
is a plain-JDK harness in the test tree that hammers a RedisForge server that is already
up. It is deliberately not a JUnit test, so `mvn test` does not run it. Point it at a
running server:

```sh
mvn test-compile
java -cp target/test-classes com.redisforge.benchmark.BenchmarkMain [host] [port]
```

It measures what a real client socket sees: 50,000 sequential `SET` and `GET` round
trips; `SET` and `GET` pipelined 5,120 commands at a time; 20,000 `HSET` field-cells
grown one at a time on a single hash; repeated `LRANGE` and `SMEMBERS` reads of a
200-element collection; 1 MiB `SET` and `GET` values; and 8 concurrent clients doing
2,000 `SET`+`GET` pairs each. The workload is fixed, so a repeat run on the same machine
is reproducible; the figures are for the machine they ran on, not a ceiling.

Measured in this repository, on one connection unless the row says concurrency, against
the packaged jar:

| Workload | ops/s | µs/op |
| --- | ---: | ---: |
| `SET` sequential (50,000) | 4,877 | 205 |
| `GET` sequential (50,000) | 5,621 | 178 |
| `SET` pipelined (10 × 5,120) | 8,332 | 120 |
| `GET` pipelined (10 × 5,120) | 8,718 | 115 |
| `HSET` on a 20,000-cell hash | 260 | 3,845 |
| `LRANGE` on a 200-element list | 388 | 2,577 |
| `SMEMBERS` on a 200-member set | 341 | 2,929 |
| `SET` 1 MiB value | 2,947 | 339 |
| `GET` 1 MiB value | 9,637 | 104 |
| 8 concurrent clients (2,000 `SET`+`GET` each) | 14,132 | 71 |

The typed rows are the semantics, not a defect: a mutation is computed against a copy of
the whole collection and swapped in whole (see
[Hashes, lists and sets](#hashes-lists-and-sets)), so an `HSET` into a 20,000-cell hash
costs time in the hash's size, and a 200-element read rebuilds a 200-part reply every
call.

## Docker

```sh
docker build -t redisforge .
docker run -p 6379:6379 redisforge
```

- The image is built in two stages — Maven compiles the jar on a JDK image, and a JRE
  image runs it — and the server runs as a non-root user, `redisforge`, so a compromised
  server does not hold the container's privileges.
- The append-only file is written under `/data`, which the image owns. Mount a volume on
  it to keep the file across container restarts:

```sh
docker run -p 6379:6379 -v redisforge-data:/data \
  redisforge --appendonly yes --appendfilename /data/appendonly.aof
```

Run a replica against a master reachable from the container:

```sh
docker run -p 6480:6480 redisforge --port 6480 --replicaof "<master-ip> 6379"
```

## Kubernetes

The Helm chart under [`helm/redisforge`](helm/redisforge) uses a single StatefulSet,
a headless service for the pod identity, and a client service. The client service is
`ClusterIP` by default, so the unauthenticated TCP server is not exposed outside the
cluster unless you explicitly change the service type.

Build and publish the image first, then set the repository and tag you published:

```sh
helm lint helm/redisforge
helm template redisforge helm/redisforge --namespace redisforge
helm install redisforge helm/redisforge \
  --namespace redisforge --create-namespace \
  --set image.repository=YOUR_REGISTRY/redisforge \
  --set image.tag=YOUR_TAG
```

Persistence is off by default. That keeps local chart installs simple, but data is
ephemeral across pod replacement. For AOF-backed storage, enable the chart's PVC and
AOF settings together:

```sh
helm upgrade --install redisforge helm/redisforge \
  --namespace redisforge --create-namespace \
  --set image.repository=YOUR_REGISTRY/redisforge \
  --set image.tag=YOUR_TAG \
  --set persistence.enabled=true
```

The chart is intentionally a single-instance starting point, not a horizontally
scalable Redis cluster. RedisForge speaks plain TCP and does not provide AUTH or TLS;
keep it on a trusted network and do not expose the service publicly.


## Supported commands

### Master

`PING`, `ECHO`, `DEL`, `SET`, `GET`, `INCR`, `MULTI`, `EXEC`, `DISCARD`, `INFO`
(sections `server`, `replication`, `memory`, `all`), `REPLCONF`, `PSYNC`, `WAIT`,
`BGREWRITEAOF`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`

Hashes: `HSET`, `HGET`, `HMGET`, `HDEL`, `HEXISTS`, `HLEN`, `HGETALL`, `HKEYS`,
`HVALS`, `HSETNX`, `HSTRLEN`

Lists: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LREM`,
`LTRIM`

Sets: `SADD`, `SREM`, `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SMEMBERS`, `SPOP`,
`SRANDMEMBER`

`DEL` removes each named key whatever it holds, inside and outside a transaction, and
answers with the number of keys it actually removed: a key that is not there is skipped,
not counted, and not an error. A `DEL` that removed nothing changes nothing and is
neither written to the append-only file nor passed to replicas. See
[Hashes, lists and sets](#hashes-lists-and-sets) for the semantics.

`EXPIRE` / `PEXPIRE` answer `:1` when the deadline was set (or the key removed, for a
non-positive delay) and `:0` when the key does not exist. `TTL` / `PTTL` answer the
remaining time, `:-1` for a key with no deadline, and `:-2` for a key that is not there.
`PERSIST` answers `:1` when a deadline was removed and `:0` when there was none.

With `--maxmemory` set, a write that would not fit the ceiling is either admitted after
evicting keys by the configured policy or refused with `-OOM`. See
[Memory limits](#memory-limits).

### Replica

`PING`, `ECHO`, `GET`, `TTL`, `PTTL`, `INFO replication`, `REPLCONF`, `PSYNC`, `WAIT`,
and every hash, list and set read: `HGET`, `HMGET`, `HLEN`, `HEXISTS`, `HGETALL`,
`HKEYS`, `HVALS`, `HSTRLEN`, `LLEN`, `LRANGE`, `LINDEX`, `SCARD`, `SISMEMBER`,
`SMISMEMBER`, `SMEMBERS`, `SRANDMEMBER`

Write commands, including the typed writes and the expiration commands, are rejected
with `-READONLY`. `BGREWRITEAOF` is recognized but refused with
`-ERR no append only file to rewrite`, since a replica keeps no file of its own.

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
- `INFO` answers `# Server`, `# Replication` and `# Memory` for a bare `INFO` or
  `INFO all`, a single section for `INFO server`, `INFO replication` or `INFO memory`,
  and `-ERR Invalid INFO section specified` for anything else.
- With a memory ceiling set, a write that cannot be admitted — the policy is
  `noeviction`, or a `volatile-` policy has no key with a deadline to remove — is
  answered with `-OOM command not allowed when used memory > 'maxmemory'.` and nothing
  is stored or written down.
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
- A client write is judged against `--maxmemory` under the same hold of the replication
  backlog that applies and records it, so the keys an eviction removed and the write they
  made room for reach the append-only file and the replicas in the one order they
  happened in. See [Memory limits](#memory-limits).
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

- With `--appendonly no`, and always for a replica, data is held in memory only. A
  replica restored by a full resync is handed its master's current dataset, so it starts
  from the keys its master holds rather than empty.
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
  because the replica takes on whatever position the master's stream has reached. A full
  resync ships the master's current dataset behind the `+FULLRESYNC` line, so the replica
  replaces its keyspace with the master's live keys, keeping each key's type and its
  remaining deadline, and the stream that follows keeps it in step from there. An older
  string-only snapshot is still accepted and installed as the strings it holds.
- A write reaches replicas in the same absolute form the file keeps. An expiration travels
  as the `PEXPIREAT` (or a `PERSIST`, or the `DEL` of a key the expiration removed) that
  reproduces what the store ended up with, and a `SET` carrying a relative `PX` travels as
  a `SET` carrying the absolute deadline as `PXAT`. A replica hangs the master's deadline on
  the key instead of counting the delay again from its own clock, so a key expires on every
  replica at the instant it expires on the master, and the stream and the file never
  describe the same write differently.
- No failover: a master that goes away is not replaced, and there is no Sentinel or
  Cluster support.
- No authentication and no TLS. Any client that can reach the port has full access.
- Keys expire lazily when they are read, and a bounded background sweep (100 keys per
  tick) removes keys that are never read again. These expiration removals are not written
  to the append-only file or propagated: replicas drop the same keys lazily or on the
  master's next deadline-carrying command. Eviction under `--maxmemory` is different: a
  key the policy removes is written down and propagated as a `DEL`, so a replica stays in
  step with its master (see [Memory limits](#memory-limits)).
- An expiration so far out that counting its deadline in milliseconds overflows a long —
  a relative `PEXPIRE` or `EXPIRE` of about 292 million years, or `Long.MAX_VALUE` —
  is clamped to "no deadline": the key is held without one. `TTL` / `PTTL` still report a
  huge remaining time on the live server, but the store, a full resync and the
  append-only file treat the key as permanent, so such a key is kept forever rather than
  expiring absurdly far in the future.
- A typed write is computed against a copy of the whole collection and swapped in whole,
  so mutating a hash, list or set costs time proportional to its size; an `HSET` into a
  20,000-cell hash measures a few milliseconds on a desktop (see
  [Benchmark](#benchmark)).
- Only the commands listed under [Supported commands](#supported-commands) are
  implemented; RedisForge is not a drop-in replacement for Redis.

## Project layout

```
src/main/java/com/redisforge/Main.java          entry point and argument parsing
src/main/java/com/redisforge/config             Spring configuration used for dependency injection
src/main/java/com/redisforge/server             master and replica TCP servers
src/main/java/com/redisforge/command            command execution and transaction semantics
src/main/java/com/redisforge/protocol           RESP codec
src/main/java/com/redisforge/store              keyspace, typed values, expiry and memory policy
src/main/java/com/redisforge/persistence        append-only file writing, replay and fsync
src/main/java/com/redisforge/replication        replica connections and the replication backlog
src/main/java/com/redisforge/network            socket client primitives
helm/redisforge                                 Helm chart
deploy/azure                                    Azure Container Apps provisioning and smoke tests
```

Spring is used only as a dependency-injection container; the server itself is built on
the JDK's blocking socket API.
