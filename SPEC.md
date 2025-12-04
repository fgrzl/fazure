Here is a tight, clean **SPEC.md** for your Go-based Azure Storage Emulator.

---

# **Azure Storage Emulator (Go) – SPEC.md**

A lightweight, deterministic, Pebble-backed emulator for **Azure Blobs, Queues, and Tables**, designed for **local development and integration testing**.
Implements Azure Storage semantics **without authentication**, optimized for simplicity and correctness—not production durability.

---

## **1. Goals**

### ✔ Accurate Azure Semantics

All REST API shapes, status codes, headers, behaviors, continuation tokens, and ordering must match Azure Storage as closely as possible.

### ✔ Unified, Pebble-Backed Persistence

A single Pebble instance rooted at `data/` inside the container:

```
data/
  blobs/
  queues/
  tables/
```

When running via Docker, callers may mount a host volume to `/data`.

### ✔ Deterministic & Developer-Friendly

* No throttling
* No transient failures
* No replication
* No auth (for now)
* Always emit helpful `slog` debugging output

### ✔ Integration Tested Against Real Azure SDKs

Tests run a throwaway Docker container, then use the official Azure Go SDK to verify protocol compatibility.

---

## **2. Project Structure**

```
cmd/
  main.go
  Dockerfile

internal/
  common/     # shared utilities: pebble opener, logging, helpers
  blobs/      # blob REST impl
  queues/     # queue REST impl
  tables/     # table REST impl

test/
  blobs_test.go
  queues_test.go
  tables_test.go
```

All internal packages remain **flat**—no deep subdirectories.

---

## **3. Runtime Model**

### **3.1 Pebble Instance**

Opened once in `main.go`:

```
dataDir := os.Getenv("DATA_DIR") or "./data"
db, err := pebble.Open(dataDir, ...)
```

Shared across all subsystems via dependency injection.

### **3.2 Key Layout**

#### **Blobs**

```
blobs/{container}/{blobName} → value = raw bytes
blobs/{container}/{blobName}/meta → JSON metadata (content-type, etc.)
```

#### **Queues**

```
queues/{queue}/meta → approximate message count, visibility timeout settings
queues/{queue}/messages/{msgID} → content + metadata
```

#### **Tables**

Partition+RowKey ordering preserved lexicographically:

```
tables/{table}/{PartitionKey}/{RowKey} → JSON entity
```

Continuation tokens serialize:

```
{ "nextPK": "...", "nextRK": "..." }
```

---

## **4. API Requirements**

## **4.1 Blobs**

Support:

* Create container
* List containers
* Put block blob
* Get blob
* Delete blob
* Set/Get blob properties
* List blobs

Correct behaviors:

* Overwrites allowed
* 404 for missing blob
* Content-Length, Content-Type, ETag, Last-Modified must be accurate

**Unsupported for now:** block lists, snapshots.

---

## **4.2 Queues**

Support:

* Create queue
* List queues
* Put message
* Get messages (with visibility timeout)
* Delete message
* Clear messages
* Queue metadata

Correct semantics:

* Visibility timeout
* Pop receipts
* Correct dequeue order
* Message TTL
* Approximate message count tracking

---

## **4.3 Tables**

Support:

### **Operations**

* Create table
* Insert entity
* Replace entity
* Merge entity
* InsertOrReplace
* InsertOrMerge
* Delete entity
* Query with `$filter`, `$top`, `$select`
* Continuation tokens

### **Semantics**

* ETags generated with each write
* Timestamp ("odata.etag") updated on mutation
* Insert fails with 409 if entity exists
* Merge/Replace honor ETag rules
* Query must respect lexicographic PK/RK ordering

### **Batch Operations**

Support batch operations via `$batch` endpoint:

* Up to 100 operations per batch
* All operations in a batch must target the same partition key (Azure requirement)
* Atomic execution: all succeed or all fail (for same-partition batches)
* Supported batch operations:
  - Insert
  - Update (Replace)
  - Merge
  - InsertOrReplace
  - InsertOrMerge
  - Delete
* Return multipart MIME response with individual operation results
* Batch request format: multipart/mixed with changeset
* Each operation returns individual status code and response
* If any operation fails, entire batch fails (rollback semantics)
* ETags must be validated for conditional operations within batch

---

## **5. HTTP Server Design**

### Framework: `net/http`

Reason: small surface, easy to debug, no external dependencies.

### Routing

Each subsystem exposes:

```
type Handler struct {
    DB *pebble.DB
    Log *slog.Logger
}
```

Bound in `main.go`:

```
/blob/<...>   
/queue/<...>  
/table/<...>
```

Simple regex routing or `httprouter`.

---

## **6. Logging**

### Logging Requirements:

* Use `slog` everywhere
* Log request method, path, latency, errors
* Debug logs include keys touched and continuation tokens
* No structured secrets (future auth)

### Log Levels:

* INFO: container/queue/table lifecycle
* DEBUG: entity reads/writes, filters applied
* ERROR: protocol errors, decoding failures

---

## **7. Docker Target**

### **cmd/Dockerfile**

* Distroless minimal base
* `FROM gcr.io/distroless/static`
* Binary is fully static (CGO disabled)
* Entrypoint: `/azure-emulator`
* Volume: `/data`

---

## **8. Tests**

All tests run via Go using Docker:

### Common Pattern

1. Build docker image
2. Run container on ephemeral port
3. Use Azure SDK:

```go
client, err := azblob.NewClient(emulatorURL, nil)
```

4. Perform CRUD operations
5. Assert Azure-correct behavior

### Test Coverage

#### **test/blobs_test.go**

* Create container
* Upload blob
* Download blob
* Delete blob
* Metadata
* List

#### **test/queues_test.go**

* Create queue
* Enqueue
* Dequeue with visibility timeout
* Delete message
* Clear queue

#### **test/tables_test.go**

* Create table
* Insert entity
* Replace entity
* Merge entity
* Delete entity
* Query
* Continuation tokens
* ETag correctness

---

## **9. Non-Goals (Explicit)**

* No authentication (`SharedKey`, SAS, OAuth)
* No ACLs
* No Premium/Hot/Cool archive tiers
* No CORS rules
* No server-side encryption
* No batch insert for tables (phase 2)
* No multi-region replication
* No scaling/throttling
* No eventual consistency simulation

This is a **dev tool**, not a service.

---

## **10. Future Extensions**

* Blob snapshots
* Queue poison-message rules
* SAS token parsing (no signature validation)
* Throttling simulation for stress testing

---

# ✔ If you want, I can generate next:

* **Directory scaffolding** (empty `.go` files)
* **Main.go bootstrap** with Pebble + routing
* **Key/value layout helper library**
* **Minimal working Blob implementation**
* **Integration test harness** that builds, runs, and tears down Docker

Just tell me what part you want next.
