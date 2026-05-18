# Configuration

Fazure is configured through **environment variables** and command-line flags (see server `main` and compose files in the repository).

## Common settings

| Concern | Typical approach |
|---------|------------------|
| Data directory | Volume mount for Pebble files |
| Listen addresses | Bind blob/queue/table ports (default 10000–10002) |
| Account name / key | Development defaults compatible with Azurite clients |

## Persistence

Mount a host directory into the container data path so blobs, queues, and tables survive restarts. Without a volume, data is ephemeral.

## Logging

Adjust log level via environment variables documented in the server package. Use structured logs when running under orchestrators.

## Production warning

Fazure is for **development and automated tests only**. Do not expose it to untrusted networks or use it as a production storage tier.
