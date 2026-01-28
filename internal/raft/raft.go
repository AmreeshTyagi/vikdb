package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"vikdb/internal/wal"
)

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Term  int64
	Index int64
	Type  wal.WALEntryType
	Key   []byte
	Value []byte
}

// RaftState represents the Raft consensus state
type RaftState struct {
	mu sync.RWMutex

	currentTerm int64
	votedFor    string
	log         []LogEntry

	commitIndex int64
	lastApplied int64

	nextIndex  map[string]int64
	matchIndex map[string]int64

	nodeID          string
	role            NodeRole
	cluster         *ClusterConfig
	lastHeartbeat   time.Time
	leaderAPIAddress string
	statePath       string
}

// RaftPersistentState represents the state that must be persisted
type RaftPersistentState struct {
	CurrentTerm int64
	VotedFor    string
	Log         []LogEntry
}

// NewRaftState creates a new Raft state
func NewRaftState(nodeID string, cluster *ClusterConfig, statePath string) (*RaftState, error) {
	r := &RaftState{
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

	if err := r.loadState(); err != nil {
		return nil, fmt.Errorf("failed to load Raft state: %v", err)
	}
	r.lastApplied = 0
	return r, nil
}

func (r *RaftState) loadState() error {
	if r.statePath == "" {
		return nil
	}
	file, err := os.Open(r.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
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

func (r *RaftState) saveState() error {
	if r.statePath == "" {
		return nil
	}
	if err := os.MkdirAll(getDir(r.statePath), 0755); err != nil {
		return err
	}
	file, err := os.Create(r.statePath)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewEncoder(file).Encode(RaftPersistentState{
		CurrentTerm: r.currentTerm,
		VotedFor:    r.votedFor,
		Log:         r.log,
	})
}

func getDir(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}

func (r *RaftState) GetCurrentTerm() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentTerm
}

func (r *RaftState) GetRole() NodeRole {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.role
}

func (r *RaftState) GetLeaderAPIAddress() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaderAPIAddress
}

// GetNodeAddress returns this node's API address (from cluster config)
func (r *RaftState) GetNodeAddress() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.cluster != nil {
		return r.cluster.Address
	}
	return ""
}

// GetNodeID returns this node's ID
func (r *RaftState) GetNodeID() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodeID
}

func (r *RaftState) GetCommitIndex() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.commitIndex
}

// GetLastApplied returns the index of the last applied log entry
func (r *RaftState) GetLastApplied() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastApplied
}

// SetLastApplied sets the last applied index (used when applying committed entries)
func (r *RaftState) SetLastApplied(i int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastApplied = i
}

func (r *RaftState) GetLastLogIndex() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return int64(len(r.log))
}

func (r *RaftState) GetLastLogTerm() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.log) == 0 {
		return 0
	}
	return r.log[len(r.log)-1].Term
}

func (r *RaftState) SetRole(role NodeRole) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role == role {
		return nil
	}
	r.role = role
	if role == RoleLeader {
		lastLogIndex := int64(len(r.log))
		for _, peer := range r.cluster.Peers {
			r.nextIndex[peer.Address] = lastLogIndex + 1
			r.matchIndex[peer.Address] = 0
		}
	}
	return r.saveState()
}

func (r *RaftState) IncrementTerm() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.currentTerm++
	r.votedFor = ""
	return r.saveState()
}

func (r *RaftState) SetVotedFor(candidateID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.votedFor = candidateID
	return r.saveState()
}

func (r *RaftState) AppendLogEntry(entry LogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry.Index = int64(len(r.log)) + 1
	r.log = append(r.log, entry)
	r.saveState()
}

func (r *RaftState) GetLogEntry(index int64) (LogEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if index < 1 || index > int64(len(r.log)) {
		return LogEntry{}, errors.New("log index out of range")
	}
	return r.log[index-1], nil
}

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
	return r.log[startIndex-1 : endIndex]
}

func (r *RaftState) TruncateLog(fromIndex int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if fromIndex < 1 || fromIndex > int64(len(r.log)) {
		return
	}
	r.log = r.log[:fromIndex-1]
	r.saveState()
}

func (r *RaftState) UpdateCommitIndex(newCommitIndex int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if newCommitIndex > r.commitIndex {
		r.commitIndex = newCommitIndex
	}
}

func (r *RaftState) GetUncommittedEntries() []LogEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.commitIndex >= int64(len(r.log)) {
		return []LogEntry{}
	}
	return r.log[r.commitIndex:]
}

func (r *RaftState) UpdateNextIndex(peerAddress string, nextIndex int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextIndex[peerAddress] = nextIndex
}

func (r *RaftState) UpdateMatchIndex(peerAddress string, matchIndex int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.matchIndex[peerAddress] = matchIndex
}

func (r *RaftState) GetNextIndex(peerAddress string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nextIndex[peerAddress]
}

func (r *RaftState) GetMatchIndex(peerAddress string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.matchIndex[peerAddress]
}

func (r *RaftState) IsLeader() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.role == RoleLeader
}

func (r *RaftState) IsFollower() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.role == RoleFollower
}

func (r *RaftState) IsCandidate() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.role == RoleCandidate
}

func (r *RaftState) UpdateLastHeartbeat() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastHeartbeat = time.Now()
}

func (r *RaftState) GetLastHeartbeat() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastHeartbeat
}

func (r *RaftState) GetMajorityCount() int {
	if r.cluster == nil {
		return 1
	}
	totalNodes := len(r.cluster.Peers) + 1
	return (totalNodes / 2) + 1
}

// ConvertWALEntryToLogEntry converts a WALEntry to a LogEntry with term and index
func (r *RaftState) ConvertWALEntryToLogEntry(walEntry wal.WALEntry, term int64) LogEntry {
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
