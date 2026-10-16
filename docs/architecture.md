# Code map

RedisForge stays a single Maven application. Packages describe the responsibilities
that change for different reasons; they are not separate services.

- `com.redisforge`: process entry point and shutdown hook
- `com.redisforge.config`: Spring component discovery
- `com.redisforge.server`: client listeners, runtime options, master and replica server loops
- `com.redisforge.protocol`: RESP stream framing and reply serialization
- `com.redisforge.command`: command execution and transaction semantics
- `com.redisforge.store`: values, collection operations, expiry, snapshots and memory admission
- `com.redisforge.persistence`: append-only file encoding, recovery and fsync policy
- `com.redisforge.replication`: replica connections and the replication backlog
- `com.redisforge.network`: socket client primitives
- `com.redisforge.benchmark`: the standalone load harness in the test source tree

The package and directory names intentionally use lowercase Java package conventions.
Tests live under the same package boundaries in `src/test/java`, which makes package
visibility explicit and keeps the Maven build's source and test trees parallel.

## Runtime boundaries

A master applies and records writes before propagating them. A replica applies the
master's stream and may forward it when used in a chain. The append-only file belongs to
the master only; replicas recover from their upstream and do not write an independent
AOF. The in-process store is not a distributed database, so the Helm chart starts one
instance by default and does not try to scale the master horizontally.

## Deployment assumptions

- The TCP protocol is plain text RESP without AUTH or TLS; keep network access private.
- Persistence is opt-in. Docker and Helm can enable AOF, but durable storage requires a
  persistent mount or PVC.
- `helm/redisforge` is a minimal single-instance Kubernetes chart. Liveness and readiness
  use a TCP socket check because the server does not expose an HTTP health endpoint.
