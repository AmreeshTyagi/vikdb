# VikDB

A high-performance, persistent Key-Value store built with Go, implementing an LSM-tree architecture.

## Features

- **Persistent Storage**: Data survives restarts via WAL (Write-Ahead Log) and SSTables
- **High Performance**: In-memory MemTable for fast writes, indexed SSTables for efficient reads
- **Crash Recovery**: Automatic recovery from WAL on startup
- **Compaction**: Automatic merging of SSTables to optimize storage
- **RESTful API**: HTTP endpoints for all operations
- **Range Queries**: Efficient range scan operations

## Architecture

- **MemTable**: In-memory sorted key-value store
- **WAL**: Write-Ahead Log for durability and crash recovery
- **SSTables**: Immutable sorted files on disk with in-memory indexes
- **Compactor**: Merges SSTables to reduce read amplification

See [FLOW_DIAGRAM.md](FLOW_DIAGRAM.md) for detailed data flow diagrams (writes, reads, flush, compaction, replication, leader election, and failover).

## Supported store APIs

- **Put** – store or update a key-value pair
- **Read** – get a value by key
- **ReadKeyRange** – get all key-value pairs in a key range (start, end)
- **Delete** – remove a key
- **BatchPut** – write multiple key-value pairs in one call

## Installation

Install dependencies and dev tools (including [air](https://github.com/air-verse/air) for live reload), then build:

```bash
make install   # install project deps and air
make build     # build bin/vikdb
```

Or build without the Makefile:

```bash
mkdir -p bin && go build -o bin/vikdb ./cmd/vikdb
```

## Usage

### Start the Server

```bash
./bin/vikdb -addr :8080 \
        -memtable-max-size 104857600 \
        -wal-path ./data/wal \
        -sstable-dir ./data/sstables
```

Or use `make run` (builds `bin/vikdb` and starts it).

### API Endpoints

#### PUT /kv/:key
Store a key-value pair.

**Request:**
```bash
# dGVzdC1rZXk= is "test-key" in Base64
# dGVzdC12YWx1ZQ== is "test-value" in Base64
curl -X PUT http://localhost:8080/kv/dGVzdC1rZXk= \
  -H "Content-Type: application/json" \
  -d '{"value": "dGVzdC12YWx1ZQ=="}'
```

**Response:**
```json
{
  "success": true,
  "message": "key stored successfully"
}
```

#### GET /kv/:key
Retrieve a value by key.

**Request:**
```bash
# dGVzdC1rZXk= is "test-key" in Base64
curl http://localhost:8080/kv/dGVzdC1rZXk=
```

**Response:**
```json
// "dGVzdC12YWx1ZQ==" is "test-value" in Base64
{
  "success": true,
  "value": "dGVzdC12YWx1ZQ=="
}
```


#### DELETE /kv/:key
Delete a key.

**Request:**
```bash
# dGVzdC1rZXk= is "test-key" in Base64
curl -X DELETE http://localhost:8080/kv/dGVzdC1rZXk=
```

**Response:**
```json
{
  "success": true,
  "message": "key deleted successfully"
}
```

#### GET /kv/range?start=...&end=...
Query a range of keys (GET with query parameters).

**Request:**
```bash
# 'key2' and 'key4' will be used as-is in the query params for this example.
curl "http://localhost:8080/kv/range?start=key2&end=key4"

```

**Response:**
```json
//"a2V5Mg==" is "key2" and "dmFsdWUy" is "value2" in Base64
 // "a2V5Mw==" is "key3" and "dmFsdWUz" is "value3" in Base64
{
  "success": true,
  "items": [
    {"key": "a2V5Mg==", "value": "dmFsdWUy"}
    ,
    {"key": "a2V5Mw==", "value": "dmFsdWUz"}
  ]
}
```

#### POST /kv/range
Query a range of keys (POST with JSON body).

**Request:**
```bash
# "a2V5Mg==" is "key2" in Base64
# "a2V5NA==" is "key4" in Base64
curl -X POST http://localhost:8080/kv/range \
  -H "Content-Type: application/json" \
  -d '{
    "start_key": "a2V5Mg==",   
    "end_key": "a2V5NA=="      
  }'
```

#### POST /kv/batch
Batch put operation.

**Request:**

```bash
# "a2V5MQ==" is "key1", "a2V5Mg==" is "key2"
# "dmFsdWUx" is "value1", "dmFsdWUy" is "value2"
curl -X POST http://localhost:8080/kv/batch \
  -H "Content-Type: application/json" \
  -d '{
    "keys": ["a2V5MQ==", "a2V5Mg=="],      
    "values": ["dmFsdWUx", "dmFsdWUy"]  
  }'
```

**Response:**
```json
{
  "success": true,
  "count": 2
}
```

#### GET /health
Health check endpoint.

**Request:**
```bash
curl http://localhost:8080/health
```

**Response:**
```json
{
  "status": "healthy",
  "memtable_size": 1024,
  "wal_size": 2048,
  "sstable_count": 3
}
```

## See [HA_CLUSTER.md](HA_CLUSTER.md) for usage with replication

## Key Encoding

All keys and values in API requests/responses are Base64 encoded:
- Keys in URLs: Base64 URL encoding
- Keys in JSON: Base64 standard encoding
- Values: Base64 standard encoding

## Error Handling

The API returns appropriate HTTP status codes:
- `200 OK`: Successful operation
- `400 Bad Request`: Invalid request format or parameters
- `404 Not Found`: Key not found (for GET operations)
- `405 Method Not Allowed`: HTTP method not supported
- `500 Internal Server Error`: Server-side error

Error responses follow this format:
```json
{
  "success": false,
  "error": "error message"
}
```

## Testing

Run all tests:
```bash
go test ./...
```

Run with verbose output:
```bash
go test -v ./...
```

## Implementation Status

✅ **Core Storage Engine**
- MemTable implementation
- WAL implementation
- SSTable read/write
- Basic compaction

✅ **Network Layer**
- HTTP API endpoints
- Request/response handling
- Error handling

✅ **Replication**
- Multiple nodes with Raft consensus. Assuming not to be used for cross-region.
- Leader election and heartbeats
- Log replication and automatic failover

🚧 **Scope of optimization** (Future)
- Bloom filters
- Compression
- Caching strategies
- Performance tuning