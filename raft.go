package vikdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// RaftState represents the Raft consensus state
type RaftState struct {
	mu sync.RWMutex

	// Persistent state (must be updated on stable storage before responding to RPCs)
	currentTerm int64
	votedFor    string
	log         []LogEntry // Log entries; first index is 1

	// Volatile state on all servers
	commitIndex int64 // Index of highest log entry known to be committed
	lastApplied int64 // Index of highest log entry applied to state machine

	// Volatile state on leaders (reinitialized after election)
	nextIndex  map[string]int64 // For each server, index of next log entry to send
	matchIndex map[string]int64 // For each server, index of highest log entry known to be replicated

	// Node information
	nodeID        string
	role          NodeRole
	cluster       *ClusterConfig
	lastHeartbeat time.Time // Last time we received a heartbeat (for followers)

	// Persistence
	statePath string // Path to persisted state file
}

// RaftPersistentState represents the state that must be persisted
type RaftPersistentState struct {
	CurrentTerm int64
	VotedFor    string
	Log         []LogEntry
}

// NewRaftState creates a new Raft state
func NewRaftState(nodeID string, cluster *ClusterConfig, statePath string) (*RaftState, error) {
	raft := &RaftState{
		nodeID:        nodeID,
		role:          RoleFollower,
		cluster:       cluster,
		currentTerm:   0,
		votedFor:      "",
		log:           make([]LogEntry, 0),
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make(map[string]int64),
		matchIndex:    make(map[string]int64),
		lastHeartbeat: time.Now(),
		statePath:     statePath,
	}

	// Load persisted state if it exists
	if err := raft.loadState(); err != nil {
		return nil, fmt.Errorf("failed to load Raft state: %v", err)
	}

	// Recover lastApplied from log (should match committed entries)
	// This will be updated as we apply entries
	raft.lastApplied = 0

	return raft, nil
}

// loadState loads Raft state from disk
func (r *RaftState) loadState() error {
	if r.statePath == "" {
		return nil // No persistence configured
	}

	file, err := os.Open(r.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // First time, no state to load
		}
		return err
	}
	defer file.Close()

	var state RaftPersistentState
	if err := json.NewDecoder(file).Decode(&state); err != nil {
		return err
	}

	r.currentTerm = state.CurrentTerm
	r.votedFor = state.VotedFor
	r.log = state.Log

	return nil
}

// saveState persists Raft state to disk
func (r *RaftState) saveState() error {
	if r.statePath == "" {
		return nil // No persistence configured
	}

	// Create directory if needed
	if err := os.MkdirAll(getDir(r.statePath), 0755); err != nil {
		return err
	}

	file, err := os.Create(r.statePath)
	if err != nil {
		return err
	}
	defer file.Close()

	state := RaftPersistentState{
		CurrentTerm: r.currentTerm,
		VotedFor:    r.votedFor,
		Log:         r.log,
	}

	return json.NewEncoder(file).Encode(state)
}

// getDir returns the directory part of a file path
func getDir(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}

// GetCurrentTerm returns the current term
func (r *RaftState) GetCurrentTerm() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentTerm
}

// GetRole returns the current role
func (r *RaftState) GetRole() NodeRole {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.role
}

// GetCommitIndex returns the commit index
func (r *RaftState) GetCommitIndex() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.commitIndex
}

// GetLastLogIndex returns the index of the last log entry
func (r *RaftState) GetLastLogIndex() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return int64(len(r.log))
}

// GetLastLogTerm returns the term of the last log entry
func (r *RaftState) GetLastLogTerm() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.log) == 0 {
		return 0
	}
	return r.log[len(r.log)-1].Term
}

// SetRole changes the node's role
func (r *RaftState) SetRole(role NodeRole) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role == role {
		return nil // No change
	}

	r.role = role

	// Reinitialize leader state if becoming leader
	if role == RoleLeader {
		// Initialize nextIndex and matchIndex for each peer
		lastLogIndex := int64(len(r.log))
		for _, peer := range r.cluster.Peers {
			r.nextIndex[peer.Address] = lastLogIndex + 1
			r.matchIndex[peer.Address] = 0
		}
	}

	return r.saveState()
}

// IncrementTerm increments the current term and resets votedFor
func (r *RaftState) IncrementTerm() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm++
	r.votedFor = ""
	return r.saveState()
}

// SetVotedFor sets the candidate we voted for in the current term
func (r *RaftState) SetVotedFor(candidateID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.votedFor = candidateID
	return r.saveState()
}

// AppendLogEntry appends a new log entry
func (r *RaftState) AppendLogEntry(entry LogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry.Index = int64(len(r.log)) + 1
	r.log = append(r.log, entry)
	r.saveState() // Persist log
}

// GetLogEntry returns a log entry at the given index (1-based)
func (r *RaftState) GetLogEntry(index int64) (LogEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if index < 1 || index > int64(len(r.log)) {
		return LogEntry{}, errors.New("log index out of range")
	}

	return r.log[index-1], nil
}

// GetLogEntries returns log entries from startIndex to endIndex (1-based, inclusive)
func (r *RaftState) GetLogEntries(startIndex, endIndex int64) []LogEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if startIndex < 1 {
		startIndex = 1
	}
	if endIndex > int64(len(r.log)) {
		endIndex = int64(len(r.log))
	}
	if startIndex > endIndex {
		return []LogEntry{}
	}

	// Convert to 0-based indices
	start := startIndex - 1
	end := endIndex

	return r.log[start:end]
}

// TruncateLog truncates the log from the given index onwards
func (r *RaftState) TruncateLog(fromIndex int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if fromIndex < 1 {
		return
	}

	if fromIndex > int64(len(r.log)) {
		return
	}

	r.log = r.log[:fromIndex-1]
	r.saveState()
}

// UpdateCommitIndex updates the commit index
func (r *RaftState) UpdateCommitIndex(newCommitIndex int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if newCommitIndex > r.commitIndex {
		r.commitIndex = newCommitIndex
	}
}

// GetUncommittedEntries returns entries that are not yet committed
func (r *RaftState) GetUncommittedEntries() []LogEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.commitIndex >= int64(len(r.log)) {
		return []LogEntry{}
	}

	return r.log[r.commitIndex:]
}

// UpdateNextIndex updates the next index for a peer
func (r *RaftState) UpdateNextIndex(peerAddress string, nextIndex int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextIndex[peerAddress] = nextIndex
}

// UpdateMatchIndex updates the match index for a peer
func (r *RaftState) UpdateMatchIndex(peerAddress string, matchIndex int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.matchIndex[peerAddress] = matchIndex
}

// GetNextIndex returns the next index for a peer
func (r *RaftState) GetNextIndex(peerAddress string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nextIndex[peerAddress]
}

// GetMatchIndex returns the match index for a peer
func (r *RaftState) GetMatchIndex(peerAddress string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.matchIndex[peerAddress]
}

// IsLeader returns true if this node is the leader
func (r *RaftState) IsLeader() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.role == RoleLeader
}

// IsFollower returns true if this node is a follower
func (r *RaftState) IsFollower() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.role == RoleFollower
}

// IsCandidate returns true if this node is a candidate
func (r *RaftState) IsCandidate() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.role == RoleCandidate
}

// UpdateLastHeartbeat updates the last heartbeat time
func (r *RaftState) UpdateLastHeartbeat() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastHeartbeat = time.Now()
}

// GetLastHeartbeat returns the last heartbeat time
func (r *RaftState) GetLastHeartbeat() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastHeartbeat
}

// GetMajorityCount returns the number of nodes needed for majority
func (r *RaftState) GetMajorityCount() int {
	if r.cluster == nil {
		return 1
	}
	totalNodes := len(r.cluster.Peers) + 1 // Peers + self
	return (totalNodes / 2) + 1
}

// ConvertWALEntryToLogEntry converts a WALEntry to a LogEntry with term and index
func (r *RaftState) ConvertWALEntryToLogEntry(walEntry WALEntry, term int64) LogEntry {
	r.mu.RLock()
	nextIndex := int64(len(r.log)) + 1
	r.mu.RUnlock()

	return LogEntry{
		Term:  term,
		Index: nextIndex,
		Type:  walEntry.Type,
		Key:   walEntry.Key,
		Value: walEntry.Value,
	}
}
