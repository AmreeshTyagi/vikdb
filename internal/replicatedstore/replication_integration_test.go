package replicatedstore

import (
	"path/filepath"
	"testing"
	"time"

	"vikdb/internal/raft"
	"vikdb/internal/store"
)

func TestReplicatedStore_BasicOperations(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")
	statePath := filepath.Join(tmpDir, "raft_state.json")

	cluster := &raft.ClusterConfig{
		NodeID:          "node1",
		Address:         "localhost:8080",
		ReplicationPort: "8081",
		Peers:           []raft.Node{},
	}

	s, err := store.NewStore(1024*1024, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	r, err := raft.NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create Raft state: %v", err)
	}

	r.SetRole(raft.RoleLeader)
	r.IncrementTerm()

	rs := NewReplicatedStore(s, r)
	defer rs.Close()

	if err := rs.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	value, exists := rs.Read([]byte("key1"))
	if !exists {
		t.Fatal("Key should exist")
	}
	if string(value) != "value1" {
		t.Fatalf("Expected value1, got %s", value)
	}

	if err := rs.Delete([]byte("key1")); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	_, exists = rs.Read([]byte("key1"))
	if exists {
		t.Fatal("Key should not exist after delete")
	}
}

func TestReplicatedStore_NonLeaderRejectsWrites(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")
	statePath := filepath.Join(tmpDir, "raft_state.json")

	cluster := &raft.ClusterConfig{
		NodeID:          "node1",
		Address:         "localhost:8080",
		ReplicationPort: "8081",
		Peers:           []raft.Node{},
	}

	s, err := store.NewStore(1024*1024, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	r, err := raft.NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create Raft state: %v", err)
	}

	rs := NewReplicatedStore(s, r)
	defer rs.Close()

	err = rs.Put([]byte("key1"), []byte("value1"))
	if err == nil {
		t.Fatal("Put should fail when not leader")
	}
}

func TestReplicatedStore_BatchPut(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")
	statePath := filepath.Join(tmpDir, "raft_state.json")

	cluster := &raft.ClusterConfig{
		NodeID:          "node1",
		Address:         "localhost:8080",
		ReplicationPort: "8081",
		Peers:           []raft.Node{},
	}

	s, err := store.NewStore(1024*1024, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	r, err := raft.NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create Raft state: %v", err)
	}

	r.SetRole(raft.RoleLeader)
	r.IncrementTerm()

	rs := NewReplicatedStore(s, r)
	defer rs.Close()

	keys := [][]byte{[]byte("key1"), []byte("key2")}
	values := [][]byte{[]byte("value1"), []byte("value2")}

	if err := rs.BatchPut(keys, values); err != nil {
		t.Fatalf("Failed to batch put: %v", err)
	}

	value, exists := rs.Read([]byte("key1"))
	if !exists || string(value) != "value1" {
		t.Fatal("key1 should exist with value1")
	}

	value, exists = rs.Read([]byte("key2"))
	if !exists || string(value) != "value2" {
		t.Fatal("key2 should exist with value2")
	}
}

func TestReplicatedStore_GetRange(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")
	statePath := filepath.Join(tmpDir, "raft_state.json")

	cluster := &raft.ClusterConfig{
		NodeID:          "node1",
		Address:         "localhost:8080",
		ReplicationPort: "8081",
		Peers:           []raft.Node{},
	}

	s, err := store.NewStore(1024*1024, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	r, err := raft.NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create Raft state: %v", err)
	}

	r.SetRole(raft.RoleLeader)
	r.IncrementTerm()

	rs := NewReplicatedStore(s, r)
	defer rs.Close()

	rs.Put([]byte("key1"), []byte("value1"))
	rs.Put([]byte("key2"), []byte("value2"))
	rs.Put([]byte("key3"), []byte("value3"))

	time.Sleep(100 * time.Millisecond)

	results := rs.ReadKeyRange([]byte("key1"), []byte("key3"))
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
}
