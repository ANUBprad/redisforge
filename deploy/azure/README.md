# Deploying RedisForge to Azure Container Apps

This directory holds an idempotent provisioning script for running RedisForge on Azure
Container Apps (ACA). It is written to be safe to run repeatedly and to reuse resources
that already exist instead of creating duplicates.

> **Status:** the manifests here are reviewed but have not been executed against a live
> subscription from this repository's environment. Treat the first run as a rehearsal and
> verify each step's output.

## What gets created

| Resource | Name (default) | Notes |
| --- | --- | --- |
| Resource group | `rg-redisforge` | reused if present |
| Container Apps environment | `redisforge-env` | v2 (workload profiles) with the built-in consumption profile |
| Container app | `redisforge` | internal TCP ingress on port 6379, one replica |

## Why internal TCP ingress

RedisForge speaks plain RESP with **no AUTH and no TLS**. A public TCP port would be an
open database. The app is therefore created with `--ingress internal`, so it is reachable
only from other apps in the same Container Apps environment, by name and port. It is never
reachable from the public internet.

If you need to reach it from outside the environment, put a client in the same
environment or VNet (for example a small jump container app, or an app on a
VNet-integrated environment) rather than making the port public.

## Why not scale to zero

ACA consumption can scale to zero, but a TCP database that scaled to zero would refuse
connections until it cold-started, and a client holding a connection would be dropped.
The script sets `--min-replicas 1 --max-replicas 1`: one warm replica, and a single writer
for the append-only file. `maxReplicas` stays at 1 because two replicas writing one AOF
would corrupt it.

## Cost

These figures are for the **Consumption** profile in East US and exclude the monthly free
grant (per subscription): the first **180,000 vCPU-seconds, 360,000 GiB-seconds, and 2
million requests** each month are free.

| Meter | Published rate | 0.25 vCPU / 0.5 GiB warm replica |
| --- | --- | --- |
| vCPU, idle (kept warm, no traffic) | $0.000003 / vCPU-second | ~648,000 vCPU-s ≈ $1.94/mo |
| vCPU, active | $0.000024 / vCPU-second | only while serving load |
| Memory | $0.000003 / GiB-second | ~1,296,000 GiB-s ≈ $3.89/mo |
| Requests | $0.40 / million | a TCP connection counts as one request |

A dormant but warm instance lands around **$4–6/month after the free grant**; a busy one
costs more because active vCPU is eight times the idle rate. A workload profiles (v2)
environment with only the consumption profile has no fixed environment fee (only the
Dedicated plan has one). Log ingestion into the environment's Log Analytics workspace is
billed separately by Azure Monitor; to avoid it entirely you can create the environment
with `--logs-destination none`, at the cost of losing container logs.

**Do not treat these numbers as a quote.** Check the
[ACA pricing page](https://azure.microsoft.com/en-us/pricing/details/container-apps/) and
the Azure pricing calculator for your region before provisioning, and delete the
resources when you are done (see [Cleanup](#cleanup)).

## Persistence and its limits

The default script runs RedisForge **without AOF** (`--appendonly` is left at its default
`no`), so a restart loses data. That is honest for a demonstration and costs nothing.

Durable AOF on ACA means mounting an Azure Files share at `/data` and passing
`--appendonly yes --appendfilename /data/appendonly.aof`. Before you rely on it, note:

- ACA mounts Azure Files over SMB. The append-only file relies on `fsync` and on an
  atomic rename during `BGREWRITEAOF`. SMB does not provide the same crash-consistency
  guarantees as a local filesystem, and this repository has **not** verified AOF recovery
  on an SMB mount. Treat such a deployment as unverified for durability.
- A single replica must own the file. `maxReplicas` is pinned to 1 for that reason; do not
  raise it while AOF is enabled.
- A more durable alternative is to run RedisForge on a host or platform that can give it a
  real block device (a VM with a managed disk, or any environment with a local volume),
  and keep ACA for stateless front ends. If you need managed persistence with Redis
  semantics and replication, a managed Redis service is the better fit than an
  unverified SMB-mounted AOF.

## Provision

```bash
# one-time: pick the subscription
az login
az account set --subscription "<subscription-id-or-name>"

# deploy
GHCR_PAT="<token with read:packages>" \
IMAGE_REPOSITORY="ghcr.io/<owner>/redisforge" \
IMAGE_TAG="sha-<40-hex-commit>" \
./deploy/azure/provision.sh
```

`IMAGE_TAG` should be the immutable `sha-<commit>` tag CI published. Running the script
again with the same values converges to the same resources.

## Private connectivity and smoke tests

Because the ingress is internal, check it from inside the environment:

```bash
# RESP PING executed inside the running container
./scripts/azure/in-container-smoke.sh redisforge rg-redisforge
```

From a host that can route to the environment's internal address, use the external smoke
test:

```bash
./scripts/azure/smoke-test.sh <fqdn> 6379
```

The `cd` workflow runs the in-container check automatically after each deployment.

## CI/CD

- `.github/workflows/ci.yml` tests, validates Docker and Helm, and on `main`/tags
  publishes `ghcr.io/<owner>/redisforge` with an immutable `sha-<commit>` tag.
- `.github/workflows/deploy.yml` deploys to ACA after a successful `ci` run on `main`,
  always by the exact `sha-<commit>` image.

Configure these once in the repository:

| Kind | Name | Purpose |
| --- | --- | --- |
| Environment | `production` | gates automated deploys (add required reviewers) |
| Secret | `AZURE_CREDENTIALS` | output of `az ad sp create-for-rbac --sdk-auth` for the deploy principal |
| Variable | `AZURE_RESOURCE_GROUP` | e.g. `rg-redisforge` |
| Variable | `AZURE_CONTAINERAPP_NAME` | e.g. `redisforge` |
| Variable | `AZURE_CONTAINER_NAME` | e.g. `redisforge` |
| Variable | `SMOKE_TEST_MODE` | `in-container` (default) or `skip` |

## Rollback

Every image is tagged with the commit it was built from, so rolling back is redeploying an
earlier tag:

```bash
# redeploy a known-good tag
IMAGE_TAG="sha-<earlier-commit>" GHCR_PAT="..." IMAGE_REPOSITORY="ghcr.io/<owner>/redisforge" \
  ./deploy/azure/provision.sh
```

Or run the **cd** workflow manually with `image_tag` set to the earlier `sha-<commit>`. The
workflow prints the previous image in its summary so the rollback target is always known.

## Cleanup

```bash
# remove everything this script created
az group delete --name rg-redisforge --yes --no-wait
```

If you reused an existing resource group, delete only the resources the script added
(the container app, then the environment) instead of the whole group.
