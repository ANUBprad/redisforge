#!/usr/bin/env bash
#
# Smoke tests a deployed RedisForge Container App without needing a route to its private
# TCP port: it asks Azure to run a RESP PING *inside* the container, against the app's
# own loopback interface. That proves the running revision accepted a TCP connection and
# answered a real command on the port clients use.
#
# Usage: in-container-smoke.sh <app-name> <resource-group> [container-name]
set -euo pipefail

app="${1:?usage: in-container-smoke.sh <app-name> <resource-group> [container-name]}"
rg="${2:?usage: in-container-smoke.sh <app-name> <resource-group> [container-name]}"
container="${3:-redisforge}"

# Sent by the container's BusyBox shell; $4 is a RESP bulk length, not a shell variable.
probe='printf "*1\r\n$4\r\nPING\r\n" | nc -w 5 127.0.0.1 6379'

output="$(az containerapp exec \
  --name "${app}" \
  --resource-group "${rg}" \
  --container "${container}" \
  --command "/bin/sh -c '${probe}'" 2>&1)" || true

if printf '%s' "${output}" | grep -q 'PONG'; then
  echo "in-container smoke test passed: the deployed revision answered PONG"
  exit 0
fi

echo "in-container smoke test failed: no PONG from ${app} (${rg})" >&2
echo "${output}" >&2
exit 1
