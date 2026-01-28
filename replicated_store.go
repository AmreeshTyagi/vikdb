package vikdb

import (
	"errors"
	"sync"
	"time"
)

// ReplicatedStore wraps Store with Raft replication
type ReplicatedStore struct {
	store              *Store
	raft               *RaftState
	replicationManager *ReplicationManager
	electionManager    *ElectionManager
	mu                 sync.RWMutex
}

// NewReplicatedStore creates a new replicated store
func NewReplicatedStore(store *Store, raft *RaftState) *ReplicatedStore {
	rs := &ReplicatedStore{
		store:              store,
		raft:               raft,
		replicationManager: NewReplicationManager(raft, store),
		electionManager:    NewElectionManager(raft),
	}

	// Start election manager
	rs.electionManager.Start()

	// Start applying committed entries in background
	go rs.applyCommittedEntriesLoop()

	return rs
}

// Put inserts or updates a key-value pair with replication
func (rs *ReplicatedStore) Put(key, value []byte) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	// Only leader can accept writes
	if !rs.raft.IsLeader() {
		return errors.New("not the leader, cannot accept writes")
	}

	// Create WAL entry
	walEntry := WALEntry{
		Type:  WALEntryPut,
		Key:   key,
		Value: value,
	}

	// Convert to log entry
	term := rs.raft.GetCurrentTerm()
	logEntry := rs.raft.ConvertWALEntryToLogEntry(walEntry, term)

	// Replicate to followers
	if err := rs.replicationManager.ReplicateEntry(logEntry); err != nil {
		return err
	}

	// Write to local WAL (for durability)
	if rs.store.wal != nil {
		if err := rs.store.wal.Append(walEntry); err != nil {
			return err
		}
	}

	// Entry is already applied to MemTable by replication manager
	// Check if flush is needed
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

	// Only leader can accept writes
	if !rs.raft.IsLeader() {
		return errors.New("not the leader, cannot accept writes")
	}

	// Create WAL entry
	walEntry := WALEntry{
		Type:  WALEntryDelete,
		Key:   key,
		Value: nil,
	}

	// Convert to log entry
	term := rs.raft.GetCurrentTerm()
	logEntry := rs.raft.ConvertWALEntryToLogEntry(walEntry, term)

	// Replicate to followers
	if err := rs.replicationManager.ReplicateEntry(logEntry); err != nil {
		return err
	}

	// Write to local WAL
	if rs.store.wal != nil {
		if err := rs.store.wal.Append(walEntry); err != nil {
			return err
		}
	}

	// Entry is already applied to MemTable by replication manager
	return nil
}

// ReadKeyRange returns all key-value pairs in the specified range
func (rs *ReplicatedStore) ReadKeyRange(startKey, endKey []byte) []KeyValue {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.store.ReadKeyRange(startKey, endKey)
}

// BatchPut performs a batch put operation with replication
func (rs *ReplicatedStore) BatchPut(keys, values [][]byte) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	// Only leader can accept writes
	if !rs.raft.IsLeader() {
		return errors.New("not the leader, cannot accept writes")
	}

	if len(keys) != len(values) {
		return errors.New("keys and values must have the same length")
	}

	// Replicate each entry
	term := rs.raft.GetCurrentTerm()
	for i := range keys {
		walEntry := WALEntry{
			Type:  WALEntryPut,
			Key:   keys[i],
			Value: values[i],
		}

		logEntry := rs.raft.ConvertWALEntryToLogEntry(walEntry, term)

		if err := rs.replicationManager.ReplicateEntry(logEntry); err != nil {
			return err
		}

		// Write to local WAL
		if rs.store.wal != nil {
			if err := rs.store.wal.Append(walEntry); err != nil {
				return err
			}
		}
	}

	// Check if flush is needed
	if rs.store.ShouldFlush() {
		if err := rs.store.FlushMemTable(); err != nil {
			return err
		}
	}

	return nil
}

// GetLeaderAddress returns the API address of the current leader (for client redirects)
func (rs *ReplicatedStore) GetLeaderAddress() string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if rs.raft.IsLeader() {
		return rs.raft.cluster.Address
	}

	// Use leader's API address from last AppendEntries (avoids redirect loop)
	if addr := rs.raft.GetLeaderAPIAddress(); addr != "" {
		return addr
	}

	return ""
}

// GetRole returns the current node role
func (rs *ReplicatedStore) GetRole() NodeRole {
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

// applyCommittedEntriesLoop continuously applies committed entries
func (rs *ReplicatedStore) applyCommittedEntriesLoop() {
	for {
		rs.raft.mu.Lock()
		lastApplied := rs.raft.lastApplied
		commitIndex := rs.raft.commitIndex
		rs.raft.mu.Unlock()

		// Apply entries between lastApplied and commitIndex
		for i := lastApplied + 1; i <= commitIndex; i++ {
			entry, err := rs.raft.GetLogEntry(i)
			if err != nil {
				continue
			}

			// Apply to store
			if err := rs.replicationManager.ApplyLogEntry(entry); err != nil {
				continue
			}

			rs.raft.mu.Lock()
			rs.raft.lastApplied = i
			rs.raft.mu.Unlock()
		}

		// Sleep briefly before checking again
		time.Sleep(10 * time.Millisecond)
	}
}
