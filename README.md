# Fazure - Azure Storage Emulator

A lightweight, Pebble-backed emulator for **Azure Blobs, Queues, and Tables**, designed for local development and integration testing.

## Feature Comparison

| Feature | Azure | Azurite | Fazure |
|---------|:-----:|:-------:|:------:|
| **Blob Storage** |
| Block Blobs (upload/download) | ✅ | ✅ | ✅ |
| Block Blobs (staged PutBlock/PutBlockList) | ✅ | ✅ | ✅ |
| Append Blobs | ✅ | ✅ | ✅ |
| Page Blobs | ✅ | ✅ | ✅ |
| Container Operations | ✅ | ✅ | ✅ |
| Container Leases | ✅ | ✅ | ✅ |
| Blob Leases | ✅ | ✅ | ✅ |
| Blob Snapshots | ✅ | ✅ | ✅ |
| Blob Copy | ✅ | ✅ | ✅ |
| Blob Metadata | ✅ | ✅ | ✅ |
| Conditional Headers (ETag) | ✅ | ✅ | ✅ |
| Service Properties (CORS) | ✅ | ✅ | ✅ |
| Blob Tiers (Hot/Cool/Archive) | ✅ | ✅ | ❌ |
| Blob Versioning | ✅ | ✅ | ❌ |
| Soft Delete | ✅ | ✅ | ❌ |
| **Queue Storage** |
| Queue Operations | ✅ | ✅ | ✅ |
| Message Operations | ✅ | ✅ | ✅ |
| Visibility Timeout | ✅ | ✅ | ✅ |
| Message TTL | ✅ | ✅ | ✅ |
| Peek Messages | ✅ | ✅ | ✅ |
| Queue Metadata | ✅ | ✅ | ✅ |
| Service Properties (CORS) | ✅ | ✅ | ✅ |
| Queue ACL | ✅ | ✅ | ❌ |
| **Table Storage** |
| Table Operations | ✅ | ✅ | ✅ |
| Entity CRUD | ✅ | ✅ | ✅ |
| Batch Transactions | ✅ | ✅ | ✅ |
| Upsert (InsertOrReplace) | ✅ | ✅ | ✅ |
| Upsert (InsertOrMerge) | ✅ | ✅ | ✅ |
| OData Queries ($filter) | ✅ | ✅ | ✅ |
| OData Queries ($select) | ✅ | ✅ | ✅ |
| Pagination (continuation) | ✅ | ✅ | ✅ |
| ETag Concurrency | ✅ | ✅ | ✅ |
| Table ACL | ✅ | ✅ | ❌ |
| **Authentication** |
| SharedKey | ✅ | ✅ | ✅ |
| SAS Tokens | ✅ | ✅ | ⚠️ |
| Azure AD / OAuth | ✅ | ✅ | ❌ |
| **Other** |
| Docker Support | N/A | ✅ | ✅ |
| Multi-arch (amd64/arm64) | N/A | ✅ | ✅ |
| Persistent Storage | ✅ | ✅ | ✅ |
| In-Memory Mode | N/A | ✅ | ❌ |

Legend: ✅ Supported | ⚠️ Partial | ❌ Not Supported

## Quick Start

### Docker

```bash
docker compose up
```

### Go

```bash
go build -o bin/fazure ./cmd
./bin/fazure
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `./data` | Data directory for persistent storage |
| `BLOB_PORT` | `10000` | Blob service port |
| `QUEUE_PORT` | `10001` | Queue service port |
| `TABLE_PORT` | `10002` | Table service port |

## Connection String

```
DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;
```

## Testing

```bash
go test ./...
```

## Specification

See [SPEC.md](SPEC.md) for detailed API documentation.

## License

See [LICENSE](LICENSE)
