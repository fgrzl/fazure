# Fazure - Azure Storage Emulator

A lightweight, Pebble-backed emulator for **Azure Blobs, Queues, and Tables**, designed for local development and integration testing.

## Quick Start

### Docker

```bash
docker compose up
```

### Go

```bash
go build -o bin/fazure ./cmd/fauxtable
./bin/fazure
```

## Configuration

- `DATA_DIR` - Data directory (default: `./data`)
- `PORT` - HTTP port (default: `10000`)

## Testing

```bash
go test ./...
```

## Specification

See [SPEC.md](SPEC.md) for detailed API documentation.

## License

See [LICENSE](LICENSE)
