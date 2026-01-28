package raft

import (
	"fmt"
	"sync"

	"vikdb/internal/store"
	"vikdb/internal/wal"
)

// ReplicationManager manages log replication from leader to followers
type ReplicationManager struct {
	raft  *RaftState
	store *store.Store
	mu    sync.Mutex
}

// NewReplicationManager creates a new replication manager
func NewReplicationManager(raft *RaftState, s *store.Store) *ReplicationManager {
	return &ReplicationManager{raft: raft, store: s}
}

// ReplicateEntry replicates a log entry to all followers and waits for majority
func (rm *ReplicationManager) ReplicateEntry(entry LogEntry) error {
	if !rm.raft.IsLeader() {
		return fmt.Errorf("only leader can replicate entries")
	}
	rm.raft.AppendLogEntry(entry)

	term := rm.raft.GetCurrentTerm()
	lastLogIndex := rm.raft.GetLastLogIndex()
	commitIndex := rm.raft.GetCommitIndex()
	successCount := 1
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
				return
			}
			mu.Lock()
			defer mu.Unlock()
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
				if reply.NextIndex > 0 {
					rm.raft.UpdateNextIndex(peerAddress, reply.NextIndex)
				} else {
					rm.raft.UpdateNextIndex(peerAddress, nextIndex-1)
				}
			}
		}(peer.Address)
	}

	wg.Wait()
	mu.Lock()
	defer mu.Unlock()

	if successCount >= majority {
		rm.raft.UpdateCommitIndex(lastLogIndex)
		rm.applyCommittedEntries()
		return nil
	}
	return fmt.Errorf("failed to replicate to majority: %d/%d", successCount, majority)
}

func (rm *ReplicationManager) applyCommittedEntries() {
	lastApplied := rm.raft.lastApplied
	commitIndex := rm.raft.GetCommitIndex()
	for i := lastApplied + 1; i <= commitIndex; i++ {
		entry, err := rm.raft.GetLogEntry(i)
		if err != nil {
			continue
		}
		walEntry := wal.WALEntry{
			Type:  entry.Type,
			Key:   entry.Key,
			Value: entry.Value,
		}
		_ = rm.store.ApplyWALEntry(walEntry)
		rm.raft.lastApplied = i
	}
}

// ApplyLogEntry applies a log entry received from leader to the local store
func (rm *ReplicationManager) ApplyLogEntry(entry LogEntry) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	walEntry := wal.WALEntry{
		Type:  entry.Type,
		Key:   entry.Key,
		Value: entry.Value,
	}
	switch walEntry.Type {
	case wal.WALEntryPut:
		return rm.store.ApplyWALEntry(walEntry)
	case wal.WALEntryDelete:
		return rm.store.ApplyWALEntry(walEntry)
	}
	return nil
}
