package raft

import (
	"path/filepath"
	"testing"
	"time"

	"vikdb/internal/wal"
)

func TestRaftState_BasicOperations(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "raft_state.json")

	cluster := &ClusterConfig{
		NodeID:          "node1",
		Address:         "localhost:8080",
		ReplicationPort: "8081",
		Peers: []Node{
			{ID: "node2", Address: "localhost:8082", Role: RoleFollower},
			{ID: "node3", Address: "localhost:8083", Role: RoleFollower},
		},
	}

	r, err := NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create Raft state: %v", err)
	}

	if r.GetCurrentTerm() != 0 {
		t.Fatalf("Expected term 0, got %d", r.GetCurrentTerm())
	}

	if r.GetRole() != RoleFollower {
		t.Fatalf("Expected role Follower, got %s", r.GetRole())
	}

	if err := r.IncrementTerm(); err != nil {
		t.Fatalf("Failed to increment term: %v", err)
	}

	if r.GetCurrentTerm() != 1 {
		t.Fatalf("Expected term 1, got %d", r.GetCurrentTerm())
	}

	if err := r.SetRole(RoleLeader); err != nil {
		t.Fatalf("Failed to set role: %v", err)
	}

	if r.GetRole() != RoleLeader {
		t.Fatalf("Expected role Leader, got %s", r.GetRole())
	}
}

func TestRaftState_LogOperations(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "raft_state.json")

	cluster := &ClusterConfig{
		NodeID:          "node1",
		Address:         "localhost:8080",
		ReplicationPort: "8081",
		Peers:           []Node{},
	}

	r, err := NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create Raft state: %v", err)
	}

	entry := LogEntry{
		Term:  1,
		Type:  wal.WALEntryPut,
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}
	r.AppendLogEntry(entry)

	if r.GetLastLogIndex() != 1 {
		t.Fatalf("Expected last log index 1, got %d", r.GetLastLogIndex())
	}

	retrieved, err := r.GetLogEntry(1)
	if err != nil {
		t.Fatalf("Failed to get log entry: %v", err)
	}

	if string(retrieved.Key) != "key1" {
		t.Fatalf("Expected key key1, got %s", retrieved.Key)
	}
}

func TestRaftState_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "raft_state.json")

	cluster := &ClusterConfig{
		NodeID:          "node1",
		Address:         "localhost:8080",
		ReplicationPort: "8081",
		Peers:           []Node{},
	}

	r1, err := NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create Raft state: %v", err)
	}

	r1.IncrementTerm()
	r1.SetVotedFor("node2")
	r1.AppendLogEntry(LogEntry{Term: 1, Type: wal.WALEntryPut, Key: []byte("key1"), Value: []byte("value1")})

	r2, err := NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create second Raft state: %v", err)
	}

	if r2.GetCurrentTerm() != 1 {
		t.Fatalf("Expected term 1 after reload, got %d", r2.GetCurrentTerm())
	}

	if r2.GetLastLogIndex() != 1 {
		t.Fatalf("Expected 1 log entry after reload, got %d", r2.GetLastLogIndex())
	}
}

func TestElectionManager_StateTransitions(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "raft_state.json")

	cluster := &ClusterConfig{
		NodeID:          "node1",
		Address:         "localhost:8080",
		ReplicationPort: "8081",
		Peers:           []Node{},
	}

	r, err := NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create Raft state: %v", err)
	}

	em := NewElectionManager(r)

	if r.GetRole() != RoleFollower {
		t.Fatalf("Expected initial role Follower, got %s", r.GetRole())
	}

	em.Start()
	defer em.Stop()

	time.Sleep(500 * time.Millisecond)
}
