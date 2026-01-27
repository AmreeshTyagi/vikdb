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

## Read Operation Flow (Get/GetRange) - With SSTables

```
┌─────────────────────────────────────────────────────────────────┐
│                         HTTP API Request                         │
│                         GET /kv/:key                             │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                          Store.Read()                            │
│  - No WAL interaction (read-only)                               │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ MemTable.Get()   │
                    │  - Binary search │
                    └────────┬─────────┘
                             │
                             │ (if not found)
                             │
                             ▼
                    ┌─────────────────┐
                    │ Check SSTables   │
                    │ (newest to old)  │
                    │  - Use index     │
                    │  - Read value    │
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
                    │ WriteSSTable()   │
                    │  - Sort entries  │
                    │  - Write data    │
                    │  - Build index  │
                    │  - Write footer  │
                    │  - Sync to disk  │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  SSTable File    │
                    │  (Immutable)     │
                    └────────┬─────────┘
                             │
                             │ (After successful write)
                             │
                             ▼
                    ┌─────────────────┐
                    │ OpenSSTable()    │
                    │  - Load index    │
                    │  - Add to list   │
                    └────────┬─────────┘
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

## Compaction Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    Compactor.ShouldCompact()                    │
│          Returns true when SSTable count >= maxFiles            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Collect entries  │
                    │ from all SSTables│
                    └────────┬─────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Merge & Dedupe   │
                    │  - Newest wins    │
                    │  - Sort by key   │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ WriteSSTable()   │
                    │  - Create merged  │
                    │  - Write to disk  │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Delete old        │
                    │ SSTables          │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Update Store      │
                    │  - Replace list   │
                    │  - Keep merged    │
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
│  │   Read()      │         │   Append()    │                      │
│  │   Delete()    │         │   Replay()    │                      │
│  │   BatchPut()  │         │   Truncate()  │                      │
│  │   ReadKeyRange() │     └──────┬───────┘                      │
│  │   Flush()     │                │                               │
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
│                    │  Flush()      │                              │
│                    └──────┬────────┘                              │
│                           │                                       │
│                           │ (when full)                            │
│                           │                                       │
│                           ▼                                       │
│                    ┌──────────────┐                               │
│                    │ WriteSSTable()│                               │
│                    │  - Sort data  │                               │
│                    │  - Build index│                               │
│                    │  - Write file │                               │
│                    └──────┬────────┘                               │
│                           │                                       │
│                           ▼                                       │
│                    ┌──────────────┐                               │
│                    │ SSTable Files │                               │
│                    │ (Immutable)   │                               │
│                    │  - Index in RAM│                               │
│                    │  - Data on disk│                              │
│                    └──────┬────────┘                               │
│                           │                                       │
│                           │ (when too many)                        │
│                           │                                       │
│                           ▼                                       │
│                    ┌──────────────┐                               │
│                    │ Compactor     │                               │
│                    │  - Merge      │                               │
│                    │  - Dedupe     │                               │
│                    │  - Compact    │                               │
│                    └──────────────┘                               │
└─────────────────────────────────────────────────────────────────┘
```

## Key Design Principles

1. **Write Path (Durability First)**:
   - Write to WAL → Sync to disk → Apply to MemTable
   - Ensures no data loss even if process crashes after WAL write

2. **Read Path (Multi-tier)**:
   - Check MemTable first (fastest, O(log n))
   - Then check SSTables newest to oldest (disk I/O, but indexed)
   - First match wins (newer data takes precedence)

3. **Recovery Path**:
   - On startup, replay WAL to rebuild MemTable
   - Load all SSTable indexes into memory
   - Ensures consistency after crash

4. **Flush Path**:
   - Write MemTable to SSTable → Clear MemTable → Truncate WAL
   - Prevents WAL from growing unbounded
   - Creates immutable sorted files for efficient reads

5. **Compaction Path**:
   - Merge multiple SSTables when count exceeds threshold
   - Remove duplicates (newest value wins)
   - Reduce read amplification
   - Free disk space by removing old files

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

SSTable File Format:
┌─────────────────────────────────────────────────────────────┐
│ Data Section                                                │
│ ┌──────────┬──────┬──────────┬────────┐                  │
│ │ Key Len  │ Key  │ Value Len│ Value  │ (repeated)       │
│ │ (4 bytes)│(N)   │ (4 bytes)│ (M)    │                  │
│ └──────────┴──────┴──────────┴────────┘                  │
├─────────────────────────────────────────────────────────────┤
│ Index Section                                               │
│ ┌──────────┬──────┬──────────┐                            │
│ │ Key Len  │ Key  │ Offset   │ (repeated)                │
│ │ (4 bytes)│(N)   │ (8 bytes)│                            │
│ └──────────┴──────┴──────────┘                            │
├─────────────────────────────────────────────────────────────┤
│ Footer                                                      │
│ ┌──────────────┬──────────────┬──────────────┐            │
│ │ Index Offset │ Index Size   │ Data Size    │            │
│ │ (8 bytes)    │ (8 bytes)    │ (8 bytes)    │            │
│ └──────────────┴──────────────┴──────────────┘            │
└─────────────────────────────────────────────────────────────┘

SSTable Structure (in memory):
┌─────────────────────────────────────────┐
│  filePath: string                       │
│  file: *os.File                        │
│  index: map[string]int64 (key->offset) │
│  mu: sync.RWMutex                       │
└─────────────────────────────────────────┘
```

## Error Handling

- **WAL Write Failure**: Operation fails, MemTable not updated
- **MemTable Write Failure**: WAL already written, but operation fails
- **Recovery Failure**: Store initialization fails if WAL replay fails
- **Partial Writes**: WAL entries are atomic (all-or-nothing per entry)
- **SSTable Write Failure**: Flush fails, MemTable remains unchanged
- **SSTable Read Failure**: Returns not found, continues to next SSTable
- **Compaction Failure**: Old SSTables preserved, compaction retried later

---

## Replication Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Client(s)                                       │
│         PUT /kv/:key  │  GET /kv/:key  │  DELETE  │  BatchPut               │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                               │ (writes only to leader; reads from any node)
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Leader Node (Raft Leader)                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │ ReplicatedStore │──│ RaftState       │──│ ReplicationMgr  │             │
│  │ Put/Delete      │  │ term, log       │  │ ReplicateEntry  │             │
│  │ BatchPut        │  │ commitIndex     │  │ AppendEntries   │             │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘             │
│           │                    │                    │                        │
│           │                    │    AppendEntries RPC (log + heartbeats)      │
│           ▼                    ▼                    │                        │
│  ┌─────────────────┐  ┌─────────────────┐         │                        │
│  │ Store           │  │ ElectionManager │         │                        │
│  │ WAL / MemTable  │  │ heartbeats      │         │                        │
│  └─────────────────┘  └─────────────────┘         │                        │
└────────────────────────────────────────────────────┼────────────────────────┘
                                                     │
                     ┌───────────────────────────────┼───────────────────────────────┐
                     │                               │                               │
                     ▼                               ▼                               ▼
            ┌─────────────────┐             ┌─────────────────┐             ┌─────────────────┐
            │  Follower Node  │             │  Follower Node  │             │  Follower Node  │
            │  - AppendEntries│             │  - AppendEntries│             │  - AppendEntries│
            │  - ApplyLogEntry│             │  - ApplyLogEntry│             │  - ApplyLogEntry│
            │  - RequestVote  │             │  - RequestVote  │             │  - RequestVote  │
            │  - Store        │             │  - Store        │             │  - Store        │
            └─────────────────┘             └─────────────────┘             └─────────────────┘
```

## Replicated Write Flow (Put/Delete/BatchPut)

```
┌─────────────────────────────────────────────────────────────────┐
│                    HTTP API Write Request                        │
│              PUT /kv/:key  │  DELETE /kv/:key  │  BatchPut       │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Server: isLeader?│
                    │ (replication on) │
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              │ No (follower)                │ Yes (leader)
              ▼                              ▼
     ┌─────────────────┐          ┌─────────────────────────────────┐
     │ 307 Redirect     │          │ ReplicatedStore.Put/Delete/     │
     │ Location: leader │          │ BatchPut()                        │
     │ (client retries  │          └────────────────┬────────────────┘
     │  to leader)      │                             │
     └─────────────────┘                             ▼
                                            ┌─────────────────┐
                                            │ ReplicateEntry() │
                                            │ - Only leader    │
                                            └────────┬─────────┘
                                                     │
                                                     ▼
                                            ┌─────────────────┐
                                            │ Append to local  │
                                            │ Raft log         │
                                            │ (term, index)    │
                                            └────────┬─────────┘
                                                     │
                                                     ▼
                                            ┌─────────────────┐
                                            │ AppendEntries    │
                                            │ RPC to all       │
                                            │ followers        │
                                            └────────┬─────────┘
                                                     │
                          ┌──────────────────────────┼──────────────────────────┐
                          │                          │                          │
                          ▼                          ▼                          ▼
                   ┌─────────────┐            ┌─────────────┐            ┌─────────────┐
                   │ Follower A  │            │ Follower B  │            │ Follower C  │
                   │ append log  │            │ append log  │            │ append log  │
                   │ ApplyLogEntry│           │ ApplyLogEntry│           │ ApplyLogEntry│
                   │ → MemTable  │            │ → MemTable  │            │ → MemTable  │
                   └──────┬──────┘            └──────┬──────┘            └──────┬──────┘
                          │ Success                  │ Success                  │
                          └──────────────────────────┼──────────────────────────┘
                                                     │
                                                     │ (majority ack)
                                                     ▼
                                            ┌─────────────────┐
                                            │ Update           │
                                            │ commitIndex      │
                                            └────────┬─────────┘
                                                     │
                                                     ▼
                                            ┌─────────────────┐
                                            │ applyCommitted   │
                                            │ Entries()        │
                                            │ (leader: MemTable)│
                                            └────────┬─────────┘
                                                     │
                                                     ▼
                                            ┌─────────────────┐
                                            │ Leader: WAL.Append│
                                            │ (durability)     │
                                            └────────┬─────────┘
                                                     │
                                                     ▼
                                            ┌─────────────────┐
                                            │ ShouldFlush?    │
                                            │ → FlushMemTable │
                                            │   if needed     │
                                            └────────┬─────────┘
                                                     │
                                                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    HTTP API Response (200)                       │
└─────────────────────────────────────────────────────────────────┘
```

## Leader Election Flow

```
┌─────────────────────────────────────────────────────────────────┐
│              Follower / Candidate (per node)                     │
│  ElectionManager.run() → handleElectionTimeout()                 │
└────────────────────────────┬────────────────────────────────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │ Last heartbeat   │
                     │ + random timeout │
                     │ (150–300 ms)     │
                     │ elapsed?         │
                     └────────┬────────┘
                              │ Yes
                              ▼
                     ┌─────────────────┐
                     │ startElection()   │
                     │ - Increment term  │
                     │ - Set Candidate   │
                     │ - votedFor = self │
                     └────────┬─────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │ RequestVote RPC  │
                     │ to all peers     │
                     │ (term, lastLog   │
                     │  Index/Term)     │
                     └────────┬─────────┘
                              │
         ┌────────────────────┼────────────────────┐
         │                    │                    │
         ▼                    ▼                    ▼
  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
  │ Peer A       │     │ Peer B       │     │ Peer C       │
  │ VoteGranted? │     │ VoteGranted? │     │ VoteGranted? │
  │ (term ok,    │     │ (term ok,    │     │ (term ok,    │
  │  log ok)     │     │  log ok)     │     │  log ok)     │
  └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
         │                    │                    │
         └────────────────────┼────────────────────┘
                              │
                              │ (majority votes)
                              ▼
                     ┌─────────────────┐
                     │ Set Role Leader  │
                     │ Reset heartbeat  │
                     │ timer            │
                     └────────┬─────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │ Start heartbeats │
                     │ sendHeartbeatTo  │
                     │ All()            │
                     └─────────────────┘
```

## Heartbeat Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    Leader Node                                   │
│  ElectionManager.handleHeartbeat() → sendHeartbeatToAll()         │
│  (periodic, e.g. every 50 ms)                                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ AppendEntries   │
                    │ RPC (empty      │
                    │ Entries: [])    │
                    │ term, PrevLog,   │
                    │ LeaderCommit    │
                    └────────┬────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
         ▼                   ▼                   ▼
  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
  │ Follower A   │     │ Follower B   │     │ Follower C   │
  │ - term ok    │     │ - term ok    │     │ - term ok    │
  │ - lastHeart  │     │ - lastHeart  │     │ - lastHeart  │
  │   beat = now │     │   beat = now │     │   beat = now │
  └─────────────┘     └─────────────┘     └─────────────┘
                             │
                             │ (followers do not start election
                             │  while they keep receiving heartbeats)
                             ▼
                    Election timeout is reset on each heartbeat.
                    If leader stops, timeout elapses → new election.
```

## Failover and Client Redirect

```
┌─────────────────────────────────────────────────────────────────┐
│  Leader dies or becomes unreachable                              │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Followers stop   │
                    │ receiving        │
                    │ heartbeats       │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Election timeout │
                    │ (150–300 ms)     │
                    │ expires          │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ One or more      │
                    │ startElection()  │
                    │ → RequestVote    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ New leader       │
                    │ elected          │
                    │ (majority votes) │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ New leader sends │
                    │ heartbeats       │
                    └─────────────────┘

  Client write to follower (before it knows who is leader):
┌─────────────────────────────────────────────────────────────────┐
│  Client  →  PUT /kv/:key  →  Follower                            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Server checks    │
                    │ isLeader()       │
                    │ → false          │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ 307 Temporary    │
                    │ Redirect         │
                    │ Location:        │
                    │ http://<leader>  │
                    │ /kv/:key         │
                    └────────┬────────┘
                             │
                             ▼
                    Client retries request to leader URL.
```

## Replication Design Principles

1. **Replicated Write Path**: Leader accepts write → append to Raft log → replicate via AppendEntries RPC → majority ack → update commitIndex → apply to MemTable (leader and followers). Leader then writes to local WAL for durability. Followers apply entries to MemTable only (log is the source of truth for replication).

2. **Leader Election**: Followers and candidates use a random election timeout (150–300 ms). On timeout, node becomes candidate, increments term, votes for self, and sends RequestVote to all peers. Majority votes → become leader. Higher term in replies → step down to follower.

3. **Heartbeats**: Leader sends empty AppendEntries periodically (e.g. 50 ms). Followers treat these as heartbeats and reset their election timeout. Missing heartbeats eventually trigger a new election.

4. **Failover**: When the leader is lost, followers hit election timeout, run an election, and a new leader is chosen. Clients that send writes to a follower receive **307 Temporary Redirect** with `Location: http://<leader>/...` and should retry to the leader.

5. **Reads**: Any node (leader or follower) can serve reads from its local Store; replication does not require redirecting reads.

