package vikdb

import (
	"path/filepath"
	"testing"
	"time"
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

	raft, err := NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create Raft state: %v", err)
	}

	// Test initial state
	if raft.GetCurrentTerm() != 0 {
		t.Fatalf("Expected term 0, got %d", raft.GetCurrentTerm())
	}

	if raft.GetRole() != RoleFollower {
		t.Fatalf("Expected role Follower, got %s", raft.GetRole())
	}

	// Test term increment
	if err := raft.IncrementTerm(); err != nil {
		t.Fatalf("Failed to increment term: %v", err)
	}

	if raft.GetCurrentTerm() != 1 {
		t.Fatalf("Expected term 1, got %d", raft.GetCurrentTerm())
	}

	// Test role change
	if err := raft.SetRole(RoleLeader); err != nil {
		t.Fatalf("Failed to set role: %v", err)
	}

	if raft.GetRole() != RoleLeader {
		t.Fatalf("Expected role Leader, got %s", raft.GetRole())
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

	raft, err := NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create Raft state: %v", err)
	}

	// Append log entry
	entry := LogEntry{
		Term:  1,
		Type:  WALEntryPut,
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}

	raft.AppendLogEntry(entry)

	if raft.GetLastLogIndex() != 1 {
		t.Fatalf("Expected last log index 1, got %d", raft.GetLastLogIndex())
	}

	// Get log entry
	retrieved, err := raft.GetLogEntry(1)
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

	// Create and modify state
	raft1, err := NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create Raft state: %v", err)
	}

	raft1.IncrementTerm()
	raft1.SetVotedFor("node2")
	raft1.AppendLogEntry(LogEntry{Term: 1, Type: WALEntryPut, Key: []byte("key1"), Value: []byte("value1")})

	// Create new instance and load state
	raft2, err := NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create second Raft state: %v", err)
	}

	if raft2.GetCurrentTerm() != 1 {
		t.Fatalf("Expected term 1 after reload, got %d", raft2.GetCurrentTerm())
	}

	if raft2.GetLastLogIndex() != 1 {
		t.Fatalf("Expected 1 log entry after reload, got %d", raft2.GetLastLogIndex())
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

	raft, err := NewRaftState("node1", cluster, statePath)
	if err != nil {
		t.Fatalf("Failed to create Raft state: %v", err)
	}

	em := NewElectionManager(raft)

	// Test initial state
	if raft.GetRole() != RoleFollower {
		t.Fatalf("Expected initial role Follower, got %s", raft.GetRole())
	}

	// Start election manager
	em.Start()
	defer em.Stop()

	// Wait a bit for election timeout
	time.Sleep(500 * time.Millisecond)

	// Note: In a real test with peers, we'd verify election behavior
	// For now, just verify the manager runs without errors
}
