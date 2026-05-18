# Overview

Fazure implements enough of the Azure Storage REST surface to run integration tests and local stacks without cloud accounts. Data persists in **Pebble** on disk (or ephemeral storage when configured).

## Services

| Service | Port (default) | Notes |
|---------|----------------|-------|
| Blob | 10000 | Block blobs, containers, leases, snapshots (partial) |
| Queue | 10001 | Messages, visibility timeout, peek |
| Table | 10002 | Entity CRUD, batch, OData filters |

## Compared to Azurite

Fazure targets **fgrzl stack compatibility** (used with `azkit`, `kv` Azure backend, mesh-core compose). See the feature matrix in the root [README](../README.md#-feature-comparison).

Notable gaps vs production Azure:

- No Azure AD / OAuth (shared-key style dev accounts)
- No blob tiers, versioning, or soft delete
- Queue/table ACLs not implemented

## Deployment modes

- **Docker / compose** — recommended for mesh-core and CI
- **Binary** — run the server directly with config from environment

## Image

Published as `ghcr.io/fgrzl/fazure:latest` for compose files that reference the fgrzl emulator.
