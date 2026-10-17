#!/usr/bin/env bash
#
# Provisions (or updates) RedisForge on Azure Container Apps, idempotently. Running it
# again converges the resources to the same shape instead of creating duplicates.
#
# The app is exposed with *internal* TCP ingress only: it is reachable from other apps in
# the same Container Apps environment by name, and never from the public internet. This
# is deliberate - RedisForge has no AUTH or TLS, so a public port would be an open
# database. See deploy/azure/README.md for the cost and persistence caveats.
#
# Prerequisites:
#   - az CLI logged in (az login) with a subscription selected that can create resources
#   - the image already published by CI (ghcr.io/<owner>/redisforge:sha-<commit>)
#   - a GHCR personal access token with read:packages, exported as GHCR_PAT
#
# Usage:
#   GHCR_PAT=... IMAGE_TAG=sha-<commit> ./deploy/azure/provision.sh
#
# Environment (all optional except IMAGE_TAG and GHCR_PAT; defaults shown):
#   RESOURCE_GROUP      rg-redisforge
#   LOCATION            eastus2
#   ENVIRONMENT_NAME    redisforge-env
#   APP_NAME            redisforge
#   IMAGE_REPOSITORY    ghcr.io/<owner>/redisforge   (required if not defaulted)
#   IMAGE_TAG           (required) e.g. sha-<40 hex>, or a released version
#   GHCR_USERNAME       the GitHub account the PAT belongs to
#   GHCR_PAT            (required) a token with read:packages
#   CPU                 0.25
#   MEMORY              0.5Gi
set -euo pipefail

RESOURCE_GROUP="${RESOURCE_GROUP:-rg-redisforge}"
LOCATION="${LOCATION:-eastus2}"
ENVIRONMENT_NAME="${ENVIRONMENT_NAME:-redisforge-env}"
APP_NAME="${APP_NAME:-redisforge}"
CPU="${CPU:-0.25}"
MEMORY="${MEMORY:-0.5Gi}"
CONTAINER_PORT=6379

: "${IMAGE_TAG:?set IMAGE_TAG to the image tag to deploy, e.g. sha-<commit>}"
: "${GHCR_PAT:?set GHCR_PAT to a GitHub token with read:packages}"
: "${IMAGE_REPOSITORY:?set IMAGE_REPOSITORY to the image path, e.g. ghcr.io/<owner>/redisforge}"
GHCR_USERNAME="${GHCR_USERNAME:-$(echo "${IMAGE_REPOSITORY}" | cut -d/ -f2)}"

echo "Resource group : ${RESOURCE_GROUP} (${LOCATION})"
echo "Environment    : ${ENVIRONMENT_NAME}"
echo "App            : ${APP_NAME}"
echo "Image          : ${IMAGE_REPOSITORY}:${IMAGE_TAG}"

# The containerapp command group ships as a CLI extension.
az extension add --name containerapp --upgrade --only-show-errors 1>/dev/null

# Idempotent: create the group only if it is not there yet.
az group create --name "${RESOURCE_GROUP}" --location "${LOCATION}" --only-show-errors 1>/dev/null

# A v2 Container Apps environment is created by default. Its built-in consumption
# profile is what this app runs on, so no dedicated workload profile (and no fixed
# plan fee) is involved. If the environment already exists, reuse it.
if az containerapp env show --name "${ENVIRONMENT_NAME}" --resource-group "${RESOURCE_GROUP}" \
     --only-show-errors 1>/dev/null 2>&1; then
  echo "Reusing the existing Container Apps environment."
else
  echo "Creating the Container Apps environment (this can take a few minutes)..."
  az containerapp env create \
    --name "${ENVIRONMENT_NAME}" \
    --resource-group "${RESOURCE_GROUP}" \
    --location "${LOCATION}" \
    --only-show-errors 1>/dev/null
fi

# Shared flags. minReplicas is 1 on purpose: a TCP database that scaled to zero would
# refuse connections while cold, so it stays warm and the cost is predictable. maxReplicas
# is 1 because a single writer owns the append-only file.
common=(
  --name "${APP_NAME}"
  --resource-group "${RESOURCE_GROUP}"
  --environment "${ENVIRONMENT_NAME}"
  --image "${IMAGE_REPOSITORY}:${IMAGE_TAG}"
  --target-port "${CONTAINER_PORT}"
  --exposed-port "${CONTAINER_PORT}"
  --transport tcp
  --ingress internal
  --cpu "${CPU}"
  --memory "${MEMORY}"
  --min-replicas 1
  --max-replicas 1
  --registry-server ghcr.io
  --registry-username "${GHCR_USERNAME}"
  --registry-password "${GHCR_PAT}"
)

# Idempotent: update an existing app, otherwise create it.
if az containerapp show --name "${APP_NAME}" --resource-group "${RESOURCE_GROUP}" \
     --only-show-errors 1>/dev/null 2>&1; then
  echo "Updating the existing container app..."
  az containerapp update "${common[@]}" --only-show-errors 1>/dev/null
else
  echo "Creating the container app..."
  az containerapp create "${common[@]}" --only-show-errors 1>/dev/null
fi

# What was deployed, without printing any secret.
revision=$(az containerapp show --name "${APP_NAME}" --resource-group "${RESOURCE_GROUP}" \
  --query "properties.latestRevisionName" -o tsv)
fqdn=$(az containerapp show --name "${APP_NAME}" --resource-group "${RESOURCE_GROUP}" \
  --query "properties.configuration.ingress.fqdn" -o tsv)
echo
echo "Deployed revision : ${revision}"
echo "Internal address  : ${fqdn}:${CONTAINER_PORT} (only reachable from inside the environment)"
