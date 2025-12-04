# Fazure - Azure Storage Emulator

A lightweight, Pebble-backed emulator for **Azure Blobs, Queues, and Tables**, designed for local development and integration testing.

## Features

- Proper HTTP response headers (ETag, x-ms-\* headers, request IDs)
- Azure Storage error responses (XML format)
- SAS token and SharedKey authentication support
- Conditional request headers (If-Match, If-None-Match, If-Modified-Since)
- Pagination with continuation tokens
- OData query support ($filter, $select, $top for Tables)
- Docker multi-arch builds (amd64, arm64)

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

- `DATA_DIR` - Data directory (default: `./data`)
- `PORT` - HTTP port (default: `10000`)

## Testing

```bash
go test ./...
```

All unit tests pass. Integration tests require a running Fazure emulator.

## Implementation Status

**Phase 1 (Complete):**

- ✅ Response headers (ETag, Last-Modified, x-ms-version, x-ms-request-id)
- ✅ Error XML responses with Azure error codes
- ✅ SAS token validation (basic signature verification)
- ✅ SharedKey authentication (HMAC-SHA256 validation)
- ✅ Conditional headers (If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since)

**Phase 2 (Partial):**

- ✅ Pagination with continuation tokens
- ✅ OData filtering ($filter for Tables)
- ⏳ Lease operations (TODO)

## Specification

See [SPEC.md](SPEC.md) for detailed API documentation.

## License

See [LICENSE](LICENSE)
