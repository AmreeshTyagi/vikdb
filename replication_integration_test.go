package vikdb

import (
	"path/filepath"
	"testing"
	"time"
)

func TestReplicatedStore_BasicOperations(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")
	statePath := filepath.Join(tmpDir, "raft_state.json")

	// Create cluster config (single node for basic test)
	cluster := &ClusterConfig{
		NodeID:          "node1",
		Address:         "localhost:8080",
		ReplicationPort: "8081",
		Peers:           []Node{},
	}

	// Create store
	store, err := NewStore(1024*1024, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create Raft state
	raft, err := NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create Raft state: %v", err)
	}

	// Make this node the leader for testing
	raft.SetRole(RoleLeader)
	raft.IncrementTerm()

	// Create replicated store
	replicatedStore := NewReplicatedStore(store, raft)
	defer replicatedStore.Close()

	// Test Put operation
	if err := replicatedStore.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Test Get operation
	value, exists := replicatedStore.Read([]byte("key1"))
	if !exists {
		t.Fatal("Key should exist")
	}
	if string(value) != "value1" {
		t.Fatalf("Expected value1, got %s", value)
	}

	// Test Delete operation
	if err := replicatedStore.Delete([]byte("key1")); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	_, exists = replicatedStore.Read([]byte("key1"))
	if exists {
		t.Fatal("Key should not exist after delete")
	}
}

func TestReplicatedStore_NonLeaderRejectsWrites(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")
	statePath := filepath.Join(tmpDir, "raft_state.json")

	cluster := &ClusterConfig{
		NodeID:          "node1",
		Address:         "localhost:8080",
		ReplicationPort: "8081",
		Peers:           []Node{},
	}

	store, err := NewStore(1024*1024, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	raft, err := NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create Raft state: %v", err)
	}

	// Keep as follower
	replicatedStore := NewReplicatedStore(store, raft)
	defer replicatedStore.Close()

	// Try to put as follower - should fail
	err = replicatedStore.Put([]byte("key1"), []byte("value1"))
	if err == nil {
		t.Fatal("Put should fail when not leader")
	}
}

func TestReplicatedStore_BatchPut(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")
	statePath := filepath.Join(tmpDir, "raft_state.json")

	cluster := &ClusterConfig{
		NodeID:          "node1",
		Address:         "localhost:8080",
		ReplicationPort: "8081",
		Peers:           []Node{},
	}

	store, err := NewStore(1024*1024, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	raft, err := NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create Raft state: %v", err)
	}

	raft.SetRole(RoleLeader)
	raft.IncrementTerm()

	replicatedStore := NewReplicatedStore(store, raft)
	defer replicatedStore.Close()

	keys := [][]byte{[]byte("key1"), []byte("key2")}
	values := [][]byte{[]byte("value1"), []byte("value2")}

	if err := replicatedStore.BatchPut(keys, values); err != nil {
		t.Fatalf("Failed to batch put: %v", err)
	}

	// Verify values
	value, exists := replicatedStore.Read([]byte("key1"))
	if !exists || string(value) != "value1" {
		t.Fatal("key1 should exist with value1")
	}

	value, exists = replicatedStore.Read([]byte("key2"))
	if !exists || string(value) != "value2" {
		t.Fatal("key2 should exist with value2")
	}
}

func TestReplicatedStore_GetRange(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")
	statePath := filepath.Join(tmpDir, "raft_state.json")

	cluster := &ClusterConfig{
		NodeID:          "node1",
		Address:         "localhost:8080",
		ReplicationPort: "8081",
		Peers:           []Node{},
	}

	store, err := NewStore(1024*1024, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	raft, err := NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create Raft state: %v", err)
	}

	raft.SetRole(RoleLeader)
	raft.IncrementTerm()

	replicatedStore := NewReplicatedStore(store, raft)
	defer replicatedStore.Close()

	// Add some keys
	replicatedStore.Put([]byte("key1"), []byte("value1"))
	replicatedStore.Put([]byte("key2"), []byte("value2"))
	replicatedStore.Put([]byte("key3"), []byte("value3"))

	// Wait a bit for entries to be applied
	time.Sleep(100 * time.Millisecond)

	// Test range query
	results := replicatedStore.ReadKeyRange([]byte("key1"), []byte("key3"))
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
}
