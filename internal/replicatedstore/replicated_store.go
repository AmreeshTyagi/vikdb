package replicatedstore

import (
	"errors"
	"sync"
	"time"

	"vikdb/internal/kv"
	"vikdb/internal/raft"
	"vikdb/internal/store"
	"vikdb/internal/wal"
)

// ReplicatedStore wraps Store with Raft replication
type ReplicatedStore struct {
	store              *store.Store
	raft               *raft.RaftState
	replicationManager *raft.ReplicationManager
	electionManager    *raft.ElectionManager
	mu                 sync.RWMutex
}

// NewReplicatedStore creates a new replicated store
func NewReplicatedStore(s *store.Store, r *raft.RaftState) *ReplicatedStore {
	rs := &ReplicatedStore{
		store:              s,
		raft:               r,
		replicationManager: raft.NewReplicationManager(r, s),
		electionManager:    raft.NewElectionManager(r),
	}
	rs.electionManager.Start()
	go rs.applyCommittedEntriesLoop()
	return rs
}

// Put inserts or updates a key-value pair with replication
func (rs *ReplicatedStore) Put(key, value []byte) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if !rs.raft.IsLeader() {
		return errors.New("not the leader, cannot accept writes")
	}
	walEntry := wal.WALEntry{
		Type:  wal.WALEntryPut,
		Key:   key,
		Value: value,
	}
	term := rs.raft.GetCurrentTerm()
	logEntry := rs.raft.ConvertWALEntryToLogEntry(walEntry, term)
	if err := rs.replicationManager.ReplicateEntry(logEntry); err != nil {
		return err
	}
	if err := rs.store.AppendToWAL(walEntry); err != nil {
		return err
	}
	if rs.store.ShouldFlush() {
		if err := rs.store.FlushMemTable(); err != nil {
			return err
		}
	}
	return nil
}

// Read retrieves a value by key
func (rs *ReplicatedStore) Read(key []byte) ([]byte, bool) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.store.Read(key)
}

// Delete removes a key with replication
func (rs *ReplicatedStore) Delete(key []byte) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if !rs.raft.IsLeader() {
		return errors.New("not the leader, cannot accept writes")
	}
	walEntry := wal.WALEntry{
		Type:  wal.WALEntryDelete,
		Key:   key,
		Value: nil,
	}
	term := rs.raft.GetCurrentTerm()
	logEntry := rs.raft.ConvertWALEntryToLogEntry(walEntry, term)
	if err := rs.replicationManager.ReplicateEntry(logEntry); err != nil {
		return err
	}
	if err := rs.store.AppendToWAL(walEntry); err != nil {
		return err
	}
	return nil
}

// ReadKeyRange returns all key-value pairs in the specified range
func (rs *ReplicatedStore) ReadKeyRange(startKey, endKey []byte) []kv.KeyValue {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.store.ReadKeyRange(startKey, endKey)
}

// BatchPut performs a batch put operation with replication
func (rs *ReplicatedStore) BatchPut(keys, values [][]byte) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if !rs.raft.IsLeader() {
		return errors.New("not the leader, cannot accept writes")
	}
	if len(keys) != len(values) {
		return errors.New("keys and values must have the same length")
	}
	term := rs.raft.GetCurrentTerm()
	for i := range keys {
		walEntry := wal.WALEntry{
			Type:  wal.WALEntryPut,
			Key:   keys[i],
			Value: values[i],
		}
		logEntry := rs.raft.ConvertWALEntryToLogEntry(walEntry, term)
		if err := rs.replicationManager.ReplicateEntry(logEntry); err != nil {
			return err
		}
		if err := rs.store.AppendToWAL(walEntry); err != nil {
			return err
		}
	}
	if rs.store.ShouldFlush() {
		if err := rs.store.FlushMemTable(); err != nil {
			return err
		}
	}
	return nil
}

// GetLeaderAddress returns the API address of the current leader
func (rs *ReplicatedStore) GetLeaderAddress() string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	if rs.raft.IsLeader() {
		return rs.raft.GetNodeAddress()
	}
	return rs.raft.GetLeaderAPIAddress()
}

// GetRole returns the current node role
func (rs *ReplicatedStore) GetRole() raft.NodeRole {
	return rs.raft.GetRole()
}

// IsLeader returns true if this node is the leader
func (rs *ReplicatedStore) IsLeader() bool {
	return rs.raft.IsLeader()
}

// Close closes the replicated store
func (rs *ReplicatedStore) Close() error {
	rs.electionManager.Stop()
	return rs.store.Close()
}

// GetMemTableSize returns the current size of the MemTable
func (rs *ReplicatedStore) GetMemTableSize() int64 {
	return rs.store.GetMemTableSize()
}

// GetWALSize returns the current size of the WAL file
func (rs *ReplicatedStore) GetWALSize() (int64, error) {
	return rs.store.GetWALSize()
}

// GetSSTableCount returns the number of SSTables
func (rs *ReplicatedStore) GetSSTableCount() int {
	return rs.store.GetSSTableCount()
}

// GetCurrentTerm returns the current Raft term
func (rs *ReplicatedStore) GetCurrentTerm() int64 {
	return rs.raft.GetCurrentTerm()
}

// GetNodeID returns this node's ID
func (rs *ReplicatedStore) GetNodeID() string {
	return rs.raft.GetNodeID()
}

func (rs *ReplicatedStore) applyCommittedEntriesLoop() {
	for {
		lastApplied := rs.raft.GetLastApplied()
		commitIndex := rs.raft.GetCommitIndex()
		for i := lastApplied + 1; i <= commitIndex; i++ {
			entry, err := rs.raft.GetLogEntry(i)
			if err != nil {
				continue
			}
			if err := rs.replicationManager.ApplyLogEntry(entry); err != nil {
				continue
			}
			rs.raft.SetLastApplied(i)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
