# Configuration

Fazure is configured through **environment variables** in `cmd/main.go`. There are no command-line flags.

## Supported variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | OS temp dir + `/data` | Pebble data directory |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error` |

## Ports

Blob **10000**, Queue **10001**, and Table **10002** are **hardcoded** in the server binary (Azurite defaults). They are not overridden by environment variables today.

## Persistence

| Deployment | Behavior |
|------------|----------|
| This repo's `compose.yml` | `DATA_DIR=/data` with volume `fazure-data` — data persists across restarts |
| `docker run` without `DATA_DIR` | Uses a temp directory under the OS temp path |
| Local `go run ./cmd` | Same as unset `DATA_DIR` |

## Account / auth

The emulator accepts requests without validating SharedKey or SAS signatures. Connection strings use the usual `devstoreaccount1` development account for client compatibility only.

## Production warning

Fazure is for **development and automated tests only**. Do not expose it to untrusted networks.
