# Getting started

## Docker Compose (recommended)

From the repository root:

```powershell
docker compose up -d
```

Default host ports:

| Service | Port |
|---------|------|
| Blob | 10000 |
| Queue | 10001 |
| Table | 10002 |

## Connection string

Use the standard Azurite-style development connection string (account `devstoreaccount1`) unless you override credentials in configuration. Applications using `azkit` or Azure SDKs should point endpoints at `http://127.0.0.1:1000x`.

## Verify

- Blob: create container and upload a block blob
- Table: insert entity via `azkit/tables` or `kv` Azure backend
- Queue: enqueue and dequeue a message

## mesh-core

mesh-core `compose.yml` references `ghcr.io/fgrzl/fazure:latest` for the `fazure` service — same ports and semantics as this repository's compose file.

## Next steps

- [Configuration](configuration.md) — data directory, logging, bindings
