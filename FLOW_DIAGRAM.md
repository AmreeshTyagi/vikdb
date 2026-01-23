# VikDB Data Flow Diagram

## Write Operation Flow (Put/Delete/BatchPut)

```
┌─────────────────────────────────────────────────────────────────┐
│                         HTTP API Request                         │
│                    PUT /kv/:key {value: "..."}                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                          Store.Put()                            │
│  - Receives key and value                                       │
│  - Validates input                                              │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  WAL.Append()   │
                    │  - Create entry │
                    │  - Write to file│
                    │  - fsync()      │
                    └────────┬─────────┘
                             │
                             │ (Durability Guarantee)
                             │
                             ▼
                    ┌─────────────────┐
                    │  WAL File       │
                    │  (Persistent)    │
                    └─────────────────┘
                             │
                             │ (After successful write)
                             │
                             ▼
                    ┌─────────────────┐
                    │ MemTable.Put()   │
                    │  - Insert/Update │
                    │  - Update size   │
                    └────────┬─────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      HTTP API Response                          │
│                  {success: true, message: "..."}                │
└─────────────────────────────────────────────────────────────────┘
```

## Read Operation Flow (Get/GetRange)

```
┌─────────────────────────────────────────────────────────────────┐
│                         HTTP API Request                         │
│                         GET /kv/:key                             │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                          Store.Get()                             │
│  - No WAL interaction (read-only)                               │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ MemTable.Get()   │
                    │  - Binary search │
                    │  - Return value  │
                    └────────┬─────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      HTTP API Response                          │
│              {success: true, value: "base64..."}                │
└─────────────────────────────────────────────────────────────────┘
```

## Crash Recovery Flow (Startup)

```
┌─────────────────────────────────────────────────────────────────┐
│                    Store Initialization                         │
│                  NewStore(memTableMaxSize, walPath)              │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Open WAL File   │
                    │  (if exists)     │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  WAL.Replay()    │
                    │  - Read entries  │
                    │  - Call callback │
                    └────────┬─────────┘
                             │
                             │ (For each entry)
                             │
                             ▼
                    ┌─────────────────┐
                    │ applyWALEntry()  │
                    │  - Put/Delete    │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  MemTable        │
                    │  (Restored)      │
                    └─────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Store Ready for Operations                   │
└─────────────────────────────────────────────────────────────────┘
```

## Flush Operation Flow (MemTable → SSTable)

```
┌─────────────────────────────────────────────────────────────────┐
│                    MemTable ShouldFlush()                       │
│              Returns true when size >= maxSize                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Get all entries  │
                    │ MemTable.GetAll  │
                    │ Entries()        │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Write to SSTable │
                    │ (Future step)    │
                    └────────┬─────────┘
                             │
                             │ (After successful write)
                             │
                             ▼
                    ┌─────────────────┐
                    │ MemTable.Clear() │
                    │  - Reset entries │
                    │  - Reset size    │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  WAL.Truncate()  │
                    │  - Clear file    │
                    │  - Reset to 0    │
                    └─────────────────┘
```

## Complete System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client/API Layer                         │
│  PUT /kv/:key  |  GET /kv/:key  |  DELETE /kv/:key  |  ...      │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                            Store Layer                           │
│  ┌──────────────┐         ┌──────────────┐                      │
│  │   Put()       │────────▶│   WAL        │                      │
│  │   Get()       │         │   Append()    │                      │
│  │   Delete()    │         │   Replay()    │                      │
│  │   BatchPut()  │         │   Truncate()  │                      │
│  │   GetRange()  │         └──────┬───────┘                      │
│  └──────┬────────┘                │                               │
│         │                         │                               │
│         │                         ▼                               │
│         │                ┌──────────────┐                         │
│         │                │  WAL File    │                         │
│         │                │  (Disk)       │                         │
│         │                └───────────────┘                         │
│         │                                                         │
│         └───────────────────┐                                    │
│                             │                                    │
│                             ▼                                    │
│                    ┌──────────────┐                              │
│                    │  MemTable    │                              │
│                    │  Put()        │                              │
│                    │  Get()        │                              │
│                    │  Delete()     │                              │
│                    │  GetRange()   │                              │
│                    └──────────────┘                              │
└─────────────────────────────────────────────────────────────────┘
```

## Key Design Principles

1. **Write Path (Durability First)**:
   - Write to WAL → Sync to disk → Apply to MemTable
   - Ensures no data loss even if process crashes after WAL write

2. **Read Path (Fast)**:
   - Directly query MemTable (no disk I/O)
   - O(log n) lookup time

3. **Recovery Path**:
   - On startup, replay WAL to rebuild MemTable
   - Ensures consistency after crash

4. **Flush Path**:
   - Write MemTable to SSTable → Clear MemTable → Truncate WAL
   - Prevents WAL from growing unbounded

## Data Structures

```
WAL Entry Format (Binary):
┌─────────┬──────────┬──────────┬──────────┬──────────┐
│ Type    │ Key Len  │   Key    │ Value Len│  Value   │
│ (1 byte)│ (4 bytes)│ (N bytes)│ (4 bytes)│ (M bytes)│
└─────────┴──────────┴──────────┴──────────┴──────────┘

MemTable Structure:
┌─────────────────────────────────────────┐
│  entries: []KeyValue (sorted by key)    │
│  size: int64                            │
│  maxSize: int64                         │
│  mu: sync.RWMutex                       │
└─────────────────────────────────────────┘
```

## Error Handling

- **WAL Write Failure**: Operation fails, MemTable not updated
- **MemTable Write Failure**: WAL already written, but operation fails
- **Recovery Failure**: Store initialization fails if WAL replay fails
- **Partial Writes**: WAL entries are atomic (all-or-nothing per entry)
