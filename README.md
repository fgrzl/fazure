# Fazure - Azure Storage Emulator

A lightweight, deterministic, Pebble-backed emulator for **Azure Blobs, Queues, and Tables**, designed for **local development and integration testing**.

## Features

- ✅ Azure Blob Storage
- ✅ Azure Queue Storage  
- ✅ Azure Table Storage (with batch operations)
- ✅ Pebble-backed persistence
- ✅ Docker support
- ✅ No authentication (dev/test focused)
- ✅ Azure SDK compatible

## Quick Start

### Using Docker

```bash
# Build the image
docker build -t fazure .

# Run the emulator
docker run -p 10000:10000 -v $(pwd)/data:/data fazure
```

### Using Go

```bash
# Build
go build -o bin/fazure ./cmd/fauxtable

# Run
./bin/fazure
```

## Configuration

Environment variables:

- `DATA_DIR` - Data directory path (default: `./data`)
- `PORT` - HTTP port (default: `10000`)

## API Endpoints

### Blobs
- `PUT /blob/{account}/{container}` - Create container
- `GET /blob/{account}?comp=list` - List containers
- `PUT /blob/{account}/{container}/{blob}` - Upload blob
- `GET /blob/{account}/{container}/{blob}` - Download blob
- `DELETE /blob/{account}/{container}/{blob}` - Delete blob
- `GET /blob/{account}/{container}?restype=container&comp=list` - List blobs

### Queues
- `PUT /queue/{account}/{queue}` - Create queue
- `GET /queue/{account}?comp=list` - List queues
- `POST /queue/{account}/{queue}/messages` - Enqueue message
- `GET /queue/{account}/{queue}/messages` - Dequeue messages
- `DELETE /queue/{account}/{queue}/messages/{messageId}` - Delete message

### Tables
- `POST /table/Tables` - Create table
- `GET /table/Tables` - List tables
- `POST /table/{table}` - Insert entity
- `GET /table/{table}(PartitionKey='{pk}',RowKey='{rk}')` - Get entity
- `PUT /table/{table}(PartitionKey='{pk}',RowKey='{rk}')` - Update entity
- `PATCH /table/{table}(PartitionKey='{pk}',RowKey='{rk}')` - Merge entity
- `DELETE /table/{table}(PartitionKey='{pk}',RowKey='{rk}')` - Delete entity
- `GET /table/{table}` - Query entities
- `POST /table/$batch` - Batch operations

## Testing

```bash
# Run tests
go test ./test/...

# Run specific test
go test ./test -run TestTableOperations
```

## Development

See [SPEC.md](SPEC.md) for detailed specification.

## License

See [LICENSE](LICENSE)
