# VikDB with Replication

This guide walks you through running VikDB in **replication mode** with multiple nodes, leader election, and automatic failover.

## Prerequisites

- Go 1.19 or later
- VikDB built from source (see [README.md](README.md))

## Overview

In replication mode, VikDB uses a **Raft-based** cluster:

- **Writes** (PUT, DELETE, BatchPut) go only to the **leader**. Followers redirect clients to the leader with `307 Temporary Redirect`, if writes are requested to followers.
- **Reads** (GET, range) can be served by any node (leader or follower).
- The **leader** replicates writes to followers via the Raft log; when a majority acknowledge, the write is committed and applied.

Each node runs two servers:

1. **API server** – HTTP API for clients (KV operations, health). Binds to `-addr`.
2. **Replication server** – Raft RPC (RequestVote, AppendEntries). Binds to `-replication-port`.

## Replication Flags

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `-node-id` | Yes (for replication) | - | Unique ID for this node (e.g. `node1`, `node2`) |
| `-cluster` | Yes (for replication) | - | Comma-separated list of **replication addresses** of all nodes (`host:replicationPort`) |
| `-replication-port` | No | `8081` | Port for Raft RPC on this node |
| `-addr` | No | `:8080` | Port for the HTTP API on this node |

Replication is enabled only when **both** `-node-id` and `-cluster` are set.

## Running a 3-Node Cluster Locally

Use three terminals. Each node uses a different API port and replication port. The `-cluster` value must list every node’s **replication** address (`host:replicationPort`) so nodes can reach each other for Raft RPCs.

**Terminal 1 – Node 1**

```bash
./vikdb \
  -addr :8080 \
  -replication-port 8081 \
  -node-id node1 \
  -cluster "localhost:8081,localhost:8083,localhost:8085" \
  -wal-path ./data/node1/wal \
  -sstable-dir ./data/node1/sstables
```

**Terminal 2 – Node 2**

```bash
./vikdb \
  -addr :8082 \
  -replication-port 8083 \
  -node-id node2 \
  -cluster "localhost:8081,localhost:8083,localhost:8085" \
  -wal-path ./data/node2/wal \
  -sstable-dir ./data/node2/sstables
```

**Terminal 3 – Node 3**

```bash
./vikdb \
  -addr :8084 \
  -replication-port 8085 \
  -node-id node3 \
  -cluster "localhost:8081,localhost:8083,localhost:8085" \
  -wal-path ./data/node3/wal \
  -sstable-dir ./data/node3/sstables
```

Use separate `-wal-path` and `-sstable-dir` per node so data and Raft state don’t clash.

After a short time, one node becomes leader and the others stay followers. You’ll see “Cluster mode enabled” and the replication address on each process.

## Using the API with Replication

### Writing (PUT, DELETE, BatchPut)

Writes must go to the **leader**. If you send a write to a follower, the server responds with **307 Temporary Redirect** and a `Location` header pointing at the leader.

**Option 1: Send writes to a known leader**

Find the leader via `/cluster/status` (see below), then send PUT/DELETE/Batch to that node’s API address (e.g. `http://localhost:8080`).

**Option 2: Let curl follow redirects**

If you hit a follower, it will redirect; use `-L` so curl retries on the `Location` URL:

```bash
# Base64: "my-key" -> "bXkta2V5", "my-value" -> "bXktdmFsdWU"
curl -L -X PUT http://localhost:8082/kv/bXkta2V5 \
  -H "Content-Type: application/json" \
  -d '{"value": "bXktdmFsdWU="}'
```

If `localhost:8082` is a follower, you’ll get a 307 to the leader and curl will re-issue the request there.

**Example – PUT to leader (e.g. node1 on :8080):**

```bash
curl -X PUT http://localhost:8080/kv/dGVzdC1rZXk= \
  -H "Content-Type: application/json" \
  -d '{"value": "dGVzdC12YWx1ZQ=="}'
```

**Example – DELETE:**

```bash
curl -X DELETE http://localhost:8080/kv/dGVzdC1rZXk=
```

**Example – BatchPut:**

```bash
curl -X POST http://localhost:8080/kv/batch \
  -H "Content-Type: application/json" \
  -d '{"keys": ["a2V5MQ==", "a2V5Mg=="], "values": ["dmFsdWUx", "dmFsdWUy"]}'
```

### Reading (GET, range)

Reads can go to **any** node. No redirect is needed.

```bash
# GET
curl http://localhost:8082/kv/dGVzdC1rZXk=

# Range (e.g. start=key2, end=key4)
curl "http://localhost:8084/kv/range?start=key2&end=key4"
```

You can round-robin or always use one node for reads; both are valid.

## Cluster and Health Endpoints

### GET /cluster/status

Returns whether replication is on, node role, term, and node ID.

**Request:**

```bash
curl http://localhost:8080/cluster/status
```

**Response (replication enabled):**

```json
{
  "replication": true,
  "is_leader": true,
  "term": 1,
  "node_id": "node1"
}
```

Use `is_leader` and `node_id` to know which node is the leader and where to send writes if you don’t want to rely on redirects.

### GET /health

Returns store health; in replication mode it also includes `role`.

**Request:**

```bash
curl http://localhost:8080/health
```

**Example response:**

```json
{
  "status": "healthy",
  "memtable_size": 0,
  "wal_size": 0,
  "sstable_count": 0,
  "role": "leader"
}
```

## Failover and Redirects

- If the **leader** exits or becomes unreachable, followers stop receiving heartbeats, run an election, and a new leader is chosen.
- Clients that were using the old leader’s address need to **use the new leader’s URL** (or use `-L` and a follower URL so redirects point to the new leader).
- There is no built-in client discovery; use `/cluster/status` on each node to see who is leader, or rely on 307 redirects from followers.

When you send a write to a follower:

1. The follower responds with **307 Temporary Redirect**.
2. The `Location` header is `http://<leader-address><path>` (e.g. `http://localhost:8080/kv/...`).
3. Retry the same request to the URL in `Location` (e.g. `curl -L` does this for you).

## Multi-Machine Cluster

On different hosts, use each host’s IP (or DNS name) and the same layout:

- **API** on one port (e.g. `:8080`).
- **Replication** on another (e.g. `:8081`).

`-cluster` must list each node’s **replication** address, e.g.:

```text
192.168.1.10:8081,192.168.1.11:8081,192.168.1.12:8081
```

Each node then runs with its own `-addr`, `-replication-port`, `-node-id`, and the same `-cluster` string. Ensure firewalls allow traffic on both API and replication ports between all nodes.

## Quick Reference

| Task | Command / URL |
|------|----------------|
| Start with replication | `-node-id <id> -cluster "addr1,addr2,..."` |
| Cluster status | `GET /cluster/status` |
| Health | `GET /health` |
| Write (prefer leader or use `-L`) | `PUT /kv/:key`, `DELETE /kv/:key`, `POST /kv/batch` |
| Read (any node) | `GET /kv/:key`, `GET /kv/range?start=...&end=...` |
| Follow redirects | `curl -L ...` |

Key and value encoding (Base64) and the rest of the KV API are the same as in single-node mode; see [README.md](README.md) and [FLOW_DIAGRAM.md](FLOW_DIAGRAM.md) for the replication data flow.
