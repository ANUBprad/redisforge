#!/usr/bin/env bash
#
# Confirms a RedisForge endpoint answers a real RESP command. Run this from a host that
# can reach the app's TCP port (a self-hosted runner, a jump box, or a client inside the
# same network); for a private Container Apps environment the port is not reachable from
# the public internet by design.
#
# Usage: smoke-test.sh <host> <port> [timeout-seconds]
set -euo pipefail

host="${1:?usage: smoke-test.sh <host> <port> [timeout-seconds]}"
port="${2:?usage: smoke-test.sh <host> <port> [timeout-seconds]}"
timeout="${3:-5}"

if ! exec 3<>"/dev/tcp/${host}/${port}" 2>/dev/null; then
  echo "smoke test failed: could not open a TCP connection to ${host}:${port}" >&2
  exit 1
fi

# One RESP PING: *1 $4 PING
printf '*1\r\n$4\r\nPING\r\n' >&3

if read -r -t "${timeout}" reply <&3 && [[ "${reply}" == *PONG* ]]; then
  exec 3<&- 3>&-
  echo "smoke test passed: ${host}:${port} answered ${reply}"
  exit 0
fi

exec 3<&- 3>&-
echo "smoke test failed: no PONG from ${host}:${port} within ${timeout}s" >&2
exit 1
