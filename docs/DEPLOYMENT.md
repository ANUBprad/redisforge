# Free deployment options

RedisForge is a raw RESP server over TCP, not an HTTP web app. A host must allow inbound TCP connections on port 6379 to provide a remotely reachable Redis-compatible endpoint. Many free web hosts only route HTTP/HTTPS and are not suitable.

## Zero-billing approach (recommended)

This repository is public, so GitHub Actions can run the CI workflow on standard hosted runners without charging for runner minutes, and the workflow can publish the container image to GitHub Container Registry (GHCR). This gives you a tested, versioned image without needing any cloud account, cloud subscription, or payment method.

The free setup intentionally does **not** promise a permanently running public server. The safest truly no-subscription option is to run RedisForge on your own computer with Docker. It remains available while your computer and container are running.

### Run locally with Docker

Build the image from the repository:

```sh
docker build -t redisforge:local .
docker run --rm --name redisforge -p 127.0.0.1:6379:6379 redisforge:local
```

The port is bound to loopback only, so other machines cannot reach this unauthenticated server. In another terminal, test it with `redis-cli` if installed:

```sh
redis-cli -h 127.0.0.1 -p 6379 ping
```

Expected response: `PONG`.

To enable AOF persistence locally, create a directory and mount it into the container:

```sh
# PowerShell (Windows)
New-Item -ItemType Directory -Force redisforge-data | Out-Null
docker run --rm --name redisforge `
  -p 127.0.0.1:6379:6379 `
  -v "${PWD}/redisforge-data:/data" `
  redisforge:local `
  --appendonly yes --appendfilename /data/appendonly.aof

# Bash (Linux/macOS)
# mkdir -p redisforge-data
# docker run --rm --name redisforge \
#   -p 127.0.0.1:6379:6379 \
#   -v "$PWD/redisforge-data:/data" \
#   redisforge:local \
#   --appendonly yes --appendfilename /data/appendonly.aof
```

Keep the data directory backed up if you care about the contents. AOF is a persistence mechanism, not a backup strategy.

### Pull the published image

After a successful push to `main`, the CI workflow publishes immutable commit-tagged images to GHCR:

```sh
docker pull ghcr.io/anubprad/redisforge:sha-<commit-sha>
```

Replace `<commit-sha>` with the full commit SHA from the successful GitHub Actions run. The image package is associated with this public repository; if GHCR visibility is not public yet, use the repository's Packages settings to make the package public. Do not create a paid registry or cloud resource.

## Hosting alternatives reviewed

| Option | Fits RedisForge TCP? | Strict no-subscription / no-bill assessment |
| --- | --- | --- |
| Local Docker | Yes | Recommended. No hosting account or payment method; only uses your own machine. |
| GitHub Actions + GHCR | Publishes the image; does not run it as a service | Recommended free CI and image distribution for this public repository. |
| Render free web service | Not a safe assumption for arbitrary inbound Redis TCP; also has ephemeral filesystem and sleeps when idle | Not selected. Free web services are designed around web traffic and local files are lost on restart/redeploy. |
| Railway free plan | TCP proxy exists, but the free allowance is limited and plan/credit terms can change | Not selected because you explicitly don't want usage credits or subscription/billing concerns. |
| Azure / AWS / Google Cloud / Oracle Cloud | Potentially, with platform-specific networking | Not selected: account, billing, payment-method, quota, or credit-expiry requirements conflict with your preference. |

Free hosting terms change. This guide intentionally prefers an option whose costs do not depend on a cloud free tier.

## Security

RedisForge currently speaks plain RESP without AUTH or TLS. Do not bind port 6379 to `0.0.0.0`, open it in your router/firewall, or expose it to the public internet. If remote access is needed later, add authentication and encrypted transport before considering a public endpoint.

## CI/CD status

- `.github/workflows/ci.yml` tests and packages the Java application, validates the Helm chart, builds the Docker image, and publishes it to GHCR on pushes to `main` and version tags.
- There is intentionally no cloud deployment workflow. CI and image publishing remain automated; running a live server is a separate, opt-in local Docker step.
