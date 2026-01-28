package vikdb

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// RPCTimeout is the HTTP client timeout for Raft RPCs (RequestVote, AppendEntries).
// Increased from 100ms so slow or busy followers can respond and catch up.
const RPCTimeout = 2 * time.Second

// RaftRPC represents RPC methods for Raft consensus
type RaftRPC struct {
	raft               *RaftState
	replicationManager *ReplicationManager
}

// RequestVoteArgs represents arguments for RequestVote RPC
type RequestVoteArgs struct {
	Term         int64 // Candidate's term
	CandidateID  string
	LastLogIndex int64
	LastLogTerm  int64
}

// RequestVoteReply represents reply for RequestVote RPC
type RequestVoteReply struct {
	Term        int64
	VoteGranted bool
}

// AppendEntriesArgs represents arguments for AppendEntries RPC
type AppendEntriesArgs struct {
	Term             int64      // Leader's term
	LeaderID         string
	LeaderAPIAddress string    // Leader's API address (host:port) for client redirects
	PrevLogIndex     int64      // Index of log entry immediately preceding new ones
	PrevLogTerm      int64      // Term of prevLogIndex entry
	Entries          []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit     int64      // Leader's commitIndex
}

// AppendEntriesReply represents reply for AppendEntries RPC
type AppendEntriesReply struct {
	Term    int64
	Success bool
	// For optimization: next index to try if failure
	NextIndex int64
}

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Term    int64
	Index   int64
	Type    WALEntryType
	Key     []byte
	Value   []byte
}

// NewRaftRPC creates a new RaftRPC instance
func NewRaftRPC(raft *RaftState, replicationManager *ReplicationManager) *RaftRPC {
	return &RaftRPC{
		raft:               raft,
		replicationManager: replicationManager,
	}
}

// RequestVote handles RequestVote RPC
func (r *RaftRPC) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	r.raft.mu.Lock()
	defer r.raft.mu.Unlock()

	reply.Term = r.raft.currentTerm

	// Reply false if term < currentTerm
	if args.Term < r.raft.currentTerm {
		reply.VoteGranted = false
		return nil
	}

	// If RPC request contains term > currentTerm, set currentTerm = term, convert to follower
	if args.Term > r.raft.currentTerm {
		r.raft.currentTerm = args.Term
		r.raft.votedFor = ""
		r.raft.role = RoleFollower
		reply.Term = r.raft.currentTerm
	}

	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
	if (r.raft.votedFor == "" || r.raft.votedFor == args.CandidateID) &&
		r.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		r.raft.votedFor = args.CandidateID
		reply.VoteGranted = true
		// Reset election timeout
		r.raft.lastHeartbeat = time.Now()
		return nil
	}

	reply.VoteGranted = false
	return nil
}

// AppendEntries handles AppendEntries RPC (used for both log replication and heartbeats)
func (r *RaftRPC) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.raft.mu.Lock()
	defer r.raft.mu.Unlock()

	reply.Term = r.raft.currentTerm

	// Reply false if term < currentTerm
	if args.Term < r.raft.currentTerm {
		reply.Success = false
		return nil
	}

	// If RPC request contains term > currentTerm, set currentTerm = term, convert to follower
	if args.Term > r.raft.currentTerm {
		r.raft.currentTerm = args.Term
		r.raft.votedFor = ""
		r.raft.role = RoleFollower
		reply.Term = r.raft.currentTerm
	}

	// Track leader's API address for redirects (avoid redirect loop when we're follower)
	if args.LeaderAPIAddress != "" {
		r.raft.leaderAPIAddress = args.LeaderAPIAddress
	}

	// Reset election timeout on receiving valid AppendEntries
	r.raft.lastHeartbeat = time.Now()

	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > int64(len(r.raft.log)) {
			reply.Success = false
			reply.NextIndex = int64(len(r.raft.log)) + 1
			return nil
		}

		if args.PrevLogIndex > 0 && (args.PrevLogIndex-1) < int64(len(r.raft.log)) {
			prevEntry := r.raft.log[args.PrevLogIndex-1]
			if prevEntry.Term != args.PrevLogTerm {
				reply.Success = false
				reply.NextIndex = args.PrevLogIndex
				return nil
			}
		}
	}

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow
	if len(args.Entries) > 0 {
		conflictIndex := -1
		for i, newEntry := range args.Entries {
			entryIndex := args.PrevLogIndex + int64(i) + 1
			if entryIndex <= int64(len(r.raft.log)) {
				existingEntry := r.raft.log[entryIndex-1]
				if existingEntry.Term != newEntry.Term {
					conflictIndex = int(entryIndex - 1)
					break
				}
			}
		}

		if conflictIndex >= 0 {
			// Truncate log from conflict point
			r.raft.log = r.raft.log[:conflictIndex]
		}

		// Append any new entries not already in the log
		for i, entry := range args.Entries {
			entryIndex := args.PrevLogIndex + int64(i) + 1
			if entryIndex > int64(len(r.raft.log)) {
				entry.Index = entryIndex
				r.raft.log = append(r.raft.log, entry)
			}
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > r.raft.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + int64(len(args.Entries))
		if args.LeaderCommit < lastNewEntryIndex {
			r.raft.commitIndex = args.LeaderCommit
		} else {
			r.raft.commitIndex = lastNewEntryIndex
		}
	}

	// Collect entries to apply while holding raft.mu. We must not call ApplyLogEntry
	// while holding raft.mu, or we deadlock with applyCommittedEntriesLoop (which
	// holds rm.mu in ApplyLogEntry then takes raft.mu to update lastApplied).
	var entriesToApply []LogEntry
	if r.replicationManager != nil && len(args.Entries) > 0 {
		for i := r.raft.lastApplied + 1; i <= r.raft.commitIndex; i++ {
			entry, err := r.raft.GetLogEntry(i)
			if err != nil {
				continue
			}
			entriesToApply = append(entriesToApply, entry)
		}
	}

	r.raft.mu.Unlock()

	// Apply without holding raft.mu to avoid deadlock with applyCommittedEntriesLoop
	for _, entry := range entriesToApply {
		_ = r.replicationManager.ApplyLogEntry(entry)
	}

	r.raft.mu.Lock()
	for _, entry := range entriesToApply {
		r.raft.lastApplied = entry.Index
	}
	// defer Unlock from function start will run on return

	reply.Success = true
	return nil
}

// isLogUpToDate checks if candidate's log is at least as up-to-date as receiver's log
func (r *RaftRPC) isLogUpToDate(lastLogTerm, lastLogIndex int64) bool {
	lastIndex := int64(len(r.raft.log))
	lastTerm := int64(0)
	if lastIndex > 0 {
		lastTerm = r.raft.log[lastIndex-1].Term
	}

	// Log is up-to-date if:
	// 1. Last term is greater, OR
	// 2. Last term is equal and last index is greater or equal
	return lastLogTerm > lastTerm || (lastLogTerm == lastTerm && lastLogIndex >= lastIndex)
}

// StartRaftRPCServer starts the RPC server for Raft communication
func StartRaftRPCServer(raft *RaftState, replicationManager *ReplicationManager, address string) error {
	rpc := NewRaftRPC(raft, replicationManager)

	// HTTP handlers for RPC calls
	http.HandleFunc("/raft/request-vote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var args RequestVoteArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var reply RequestVoteReply
		if err := rpc.RequestVote(&args, &reply); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reply)
	})

	http.HandleFunc("/raft/append-entries", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var args AppendEntriesArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var reply AppendEntriesReply
		if err := rpc.AppendEntries(&args, &reply); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reply)
	})

	return http.ListenAndServe(address, nil)
}

// CallRequestVote sends RequestVote RPC to a peer
func CallRequestVote(peerAddress string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	client := &http.Client{
		Timeout: RPCTimeout,
	}

	url := fmt.Sprintf("http://%s/raft/request-vote", peerAddress)
	body, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("RPC failed: %s", string(bodyBytes))
	}

	var reply RequestVoteReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return nil, err
	}

	return &reply, nil
}

// CallAppendEntries sends AppendEntries RPC to a peer
func CallAppendEntries(peerAddress string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	client := &http.Client{
		Timeout: RPCTimeout,
	}

	url := fmt.Sprintf("http://%s/raft/append-entries", peerAddress)
	body, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("RPC failed: %s", string(bodyBytes))
	}

	var reply AppendEntriesReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return nil, err
	}

	return &reply, nil
}
