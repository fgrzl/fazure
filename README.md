# Fazure - Azure Storage Emulator

A lightweight, Pebble-backed emulator for **Azure Blobs, Queues, and Tables**, designed for local development and integration testing.

## 🔍 Feature Comparison

| Feature                                    | Azure | Azurite | Fazure |
| ------------------------------------------ | :---: | :-----: | :----: |
| **Blob Storage**                           |
| Block Blobs (upload/download)              |  ✅   |   ✅    |   ✅   |
| Block Blobs (staged PutBlock/PutBlockList) |  ✅   |   ✅    |   ✅   |
| Append Blobs                               |  ✅   |   ✅    |   ✅   |
| Page Blobs                                 |  ✅   |   ✅    |   ✅   |
| Container Operations                       |  ✅   |   ✅    |   ✅   |
| Container Leases                           |  ✅   |   ✅    |   ✅   |
| Blob Leases                                |  ✅   |   ✅    |   ✅   |
| Blob Snapshots                             |  ✅   |   ✅    |   ✅   |
| Blob Copy                                  |  ✅   |   ✅    |   ✅   |
| Blob Metadata                              |  ✅   |   ✅    |   ✅   |
| Conditional Headers (ETag)                 |  ✅   |   ✅    |   ✅   |
| Service Properties (CORS)                  |  ✅   |   ✅    |   ✅   |
| Blob Tiers (Hot/Cool/Archive)              |  ✅   |   ✅    |   ❌   |
| Blob Versioning                            |  ✅   |   ✅    |   ❌   |
| Soft Delete                                |  ✅   |   ✅    |   ❌   |
| **Queue Storage**                          |
| Queue Operations                           |  ✅   |   ✅    |   ✅   |
| Message Operations                         |  ✅   |   ✅    |   ✅   |
| Visibility Timeout                         |  ✅   |   ✅    |   ✅   |
| Message TTL                                |  ✅   |   ✅    |   ✅   |
| Peek Messages                              |  ✅   |   ✅    |   ✅   |
| Queue Metadata                             |  ✅   |   ✅    |   ✅   |
| Service Properties (CORS)                  |  ✅   |   ✅    |   ✅   |
| Queue ACL                                  |  ✅   |   ✅    |   ❌   |
| **Table Storage**                          |
| Table Operations                           |  ✅   |   ✅    |   ✅   |
| Entity CRUD                                |  ✅   |   ✅    |   ✅   |
| Batch Transactions                         |  ✅   |   ✅    |   ✅   |
| Upsert (InsertOrReplace)                   |  ✅   |   ✅    |   ✅   |
| Upsert (InsertOrMerge)                     |  ✅   |   ✅    |   ✅   |
| OData Queries ($filter)                    |  ✅   |   ✅    |   ✅   |
| OData Queries ($select)                    |  ✅   |   ✅    |   ✅   |
| Pagination (continuation)                  |  ✅   |   ✅    |   ✅   |
| ETag Concurrency                           |  ✅   |   ✅    |   ✅   |
| Table ACL                                  |  ✅   |   ✅    |   ❌   |
| **Authentication**                         |
| SharedKey                                  |  ✅   |   ✅    |   ❌   |
| SAS Tokens                                 |  ✅   |   ✅    |   ❌   |
| Azure AD / OAuth                           |  ✅   |   ✅    |   ❌   |
| **Other**                                  |
| Docker Support                             |  N/A  |   ✅    |   ✅   |
| Multi-arch (amd64/arm64)                   |  N/A  |   ✅    |   ✅   |
| Persistent Storage                         |  ✅   |   ✅    |   ✅   |
| In-Memory Mode                             |  N/A  |   ✅    |   ❌   |

Legend: ✅ Supported | ⚠️ Partial | ❌ Not Supported

## Documentation

Guides: **[docs/](docs/README.md)** — [overview](docs/overview.md), [getting started](docs/getting-started.md), [configuration](docs/configuration.md)

## ➡️ Quick Start

### 🐳 Docker (recommended)

Start the emulator locally using the provided `docker compose` file.

Bring up the service in the foreground:

```powershell
docker compose up
```

Or run it detached:

```powershell
docker compose up -d
```

The compose file maps the standard Azure emulator ports to the host:

- Blob: `10000`
- Queue: `10001`
- Table: `10002`

By default the container uses the OS temporary directory for storage unless you set `DATA_DIR` (see Configuration). To persist data between restarts, enable a volume mount in `compose.yml` or run with:

```powershell
docker compose up -d
# or override DATA_DIR
DATA_DIR=./data docker compose up -d
```

You can check service health with:

```bash
curl http://localhost:10000/health
curl http://localhost:10001/health
curl http://localhost:10002/health
```

### 📦 Pulling the released container

The official multi-architecture image is published to GitHub Container Registry at `ghcr.io/fgrzl/fazure`. The image is public — no authentication is required to pull.

```powershell
docker pull ghcr.io/fgrzl/fazure:latest
docker run -p 10000:10000 -p 10001:10001 -p 10002:10002 ghcr.io/fgrzl/fazure:latest
```

### 🛠️ Go (development)

Build and run locally:

```powershell
go build -o bin/fazure ./cmd
./bin/fazure
```

## ⚙️ Configuration

| Variable     | Default                                    | Description                                                                                                                       |
| ------------ | ------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| `DATA_DIR`   | (not set) → OS temp dir (e.g. `/tmp/data`) | Data directory for persistent storage. If you want persistent data across restarts, set this to a host directory (e.g. `./data`). |
| `BLOB_PORT`  | `10000`                                    | Blob service port                                                                                                                 |
| `QUEUE_PORT` | `10001`                                    | Queue service port                                                                                                                |
| `TABLE_PORT` | `10002`                                    | Table service port                                                                                                                |
| `LOG_LEVEL`  | `info`                                     | Logger level: `debug`, `info`, `warn`, `error`                                                                                    |

## 🔗 Connection String

```
DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;
```

Note: The connection string above uses the common emulator defaults (account `devstoreaccount1` and the well-known development key). Fazure does not enforce SharedKey or SAS authentication — request signing is not validated by the emulator. The helper functions to validate signatures exist in the codebase for future use, but authentication enforcement is intentionally out-of-scope for this emulator.

## 🧪 Testing

```bash
go test ./...
```

## ✅ Implementation checklist

The table below shows implemented features and current gaps. Checkmarks indicate implemented behavior in `fazure`.

- ✅ Blob storage (Block, Append, Page)
- ✅ Container operations (create, delete, metadata, ACL basic)
- ✅ Container & Blob leases (basic semantics)
- ✅ Blob snapshots and copy
- ✅ Blob metadata and conditional headers (ETag)
- ✅ Blob service properties and CORS
- ✅ Queue operations (create/delete, metadata)
- ✅ Message operations (enqueue, dequeue, peek, delete)
- ✅ Visibility timeouts and TTL
- ✅ Queue service properties
- ✅ Table operations (create/delete, list)
- ✅ Entity CRUD (Insert, Update/Replace, Merge)
- ✅ Batch transactions with upsert semantics for PUT/PATCH/MERGE
- ✅ OData query support ($filter, $select) and pagination
- ✅ ETag concurrency checks (If-Match handling)
- ✅ Shared Key authentication
- ✅ SAS token validation (partial)
- ✅ Docker support and multi-arch builds (via GitHub Actions)

Planned / missing items:

- ❌ Blob tiers (Hot/Cool/Archive)
- ❌ Blob versioning & soft-delete
- ❌ Full SAS token compatibility and AD/OAuth authentication
- ❌ Advanced Queue ACL features
- ❌ Table ACL / RBAC

## 📄 License

See [LICENSE](LICENSE)
