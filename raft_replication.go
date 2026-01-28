package vikdb

import (
	"fmt"
	"sync"
)

// ReplicationManager manages log replication from leader to followers
type ReplicationManager struct {
	raft  *RaftState
	store *Store
	mu    sync.Mutex
}

// NewReplicationManager creates a new replication manager
func NewReplicationManager(raft *RaftState, store *Store) *ReplicationManager {
	return &ReplicationManager{
		raft:  raft,
		store: store,
	}
}

// ReplicateEntry replicates a log entry to all followers and waits for majority
func (rm *ReplicationManager) ReplicateEntry(entry LogEntry) error {
	if !rm.raft.IsLeader() {
		return fmt.Errorf("only leader can replicate entries")
	}

	// Append to local log first
	rm.raft.AppendLogEntry(entry)

	// Replicate to all followers
	term := rm.raft.GetCurrentTerm()
	lastLogIndex := rm.raft.GetLastLogIndex()
	commitIndex := rm.raft.GetCommitIndex()

	successCount := 1 // Count self
	totalResponses := 1
	majority := rm.raft.GetMajorityCount()

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, peer := range rm.raft.cluster.Peers {
		wg.Add(1)
		go func(peerAddress string) {
			defer wg.Done()

			nextIndex := rm.raft.GetNextIndex(peerAddress)
			if nextIndex == 0 {
				nextIndex = lastLogIndex
				rm.raft.UpdateNextIndex(peerAddress, nextIndex)
			}

			prevLogIndex := nextIndex - 1
			prevLogTerm := int64(0)
			if prevLogIndex > 0 {
				prevEntry, err := rm.raft.GetLogEntry(prevLogIndex)
				if err == nil {
					prevLogTerm = prevEntry.Term
				}
			}

			// Send all missing entries (nextIndex..lastLogIndex) so lagging followers catch up
			entriesToSend := rm.raft.GetLogEntries(nextIndex, lastLogIndex)

			args := &AppendEntriesArgs{
				Term:             term,
				LeaderID:         rm.raft.nodeID,
				LeaderAPIAddress: rm.raft.cluster.Address,
				PrevLogIndex:     prevLogIndex,
				PrevLogTerm:      prevLogTerm,
				Entries:          entriesToSend,
				LeaderCommit:     commitIndex,
			}

			reply, err := CallAppendEntries(peerAddress, args)
			if err != nil {
				// Network error (e.g. timeout) - peer may be slow or unreachable
				return
			}

			mu.Lock()
			defer mu.Unlock()

			// Update term if we see a higher term
			if reply.Term > rm.raft.GetCurrentTerm() {
				rm.raft.mu.Lock()
				rm.raft.currentTerm = reply.Term
				rm.raft.votedFor = ""
				rm.raft.role = RoleFollower
				rm.raft.saveState()
				rm.raft.mu.Unlock()
				return
			}

			totalResponses++
			if reply.Success {
				successCount++
				rm.raft.UpdateNextIndex(peerAddress, lastLogIndex+1)
				rm.raft.UpdateMatchIndex(peerAddress, lastLogIndex)
			} else {
				// Decrement nextIndex so next replication will retry from earlier
				if reply.NextIndex > 0 {
					rm.raft.UpdateNextIndex(peerAddress, reply.NextIndex)
				} else {
					rm.raft.UpdateNextIndex(peerAddress, nextIndex-1)
				}
			}
		}(peer.Address)
	}

	wg.Wait()

	// Check if majority has replicated
	mu.Lock()
	defer mu.Unlock()

	if successCount >= majority {
		// Update commit index
		rm.raft.UpdateCommitIndex(lastLogIndex)
		// Apply to state machine
		rm.applyCommittedEntries()
		return nil
	}

	return fmt.Errorf("failed to replicate to majority: %d/%d", successCount, majority)
}

// applyCommittedEntries applies committed log entries to the state machine (Store)
func (rm *ReplicationManager) applyCommittedEntries() {
	lastApplied := rm.raft.lastApplied
	commitIndex := rm.raft.GetCommitIndex()

	// Apply all entries between lastApplied and commitIndex
	for i := lastApplied + 1; i <= commitIndex; i++ {
		entry, err := rm.raft.GetLogEntry(i)
		if err != nil {
			continue
		}

		// Convert LogEntry to WALEntry and apply
		walEntry := WALEntry{
			Type:  entry.Type,
			Key:   entry.Key,
			Value: entry.Value,
		}

		// Apply to store (but don't write to WAL again, it's already in the log)
		switch walEntry.Type {
		case WALEntryPut:
			rm.store.memTable.Put(walEntry.Key, walEntry.Value)
		case WALEntryDelete:
			rm.store.memTable.Delete(walEntry.Key)
		}

		rm.raft.lastApplied = i
	}
}

// ApplyLogEntry applies a log entry received from leader to the local store
func (rm *ReplicationManager) ApplyLogEntry(entry LogEntry) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Convert LogEntry to WALEntry
	walEntry := WALEntry{
		Type:  entry.Type,
		Key:   entry.Key,
		Value: entry.Value,
	}

	// Apply to store (but don't write to WAL - it's already in the Raft log)
	switch walEntry.Type {
	case WALEntryPut:
		return rm.store.memTable.Put(walEntry.Key, walEntry.Value)
	case WALEntryDelete:
		return rm.store.memTable.Delete(walEntry.Key)
	}

	return nil
}
