package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// RPCTimeout is the HTTP client timeout for Raft RPCs
const RPCTimeout = 2 * time.Second

// RaftRPC represents RPC methods for Raft consensus
type RaftRPC struct {
	raft               *RaftState
	replicationManager *ReplicationManager
}

// RequestVoteArgs represents arguments for RequestVote RPC
type RequestVoteArgs struct {
	Term         int64
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
	Term             int64
	LeaderID         string
	LeaderAPIAddress string
	PrevLogIndex     int64
	PrevLogTerm      int64
	Entries          []LogEntry
	LeaderCommit     int64
}

// AppendEntriesReply represents reply for AppendEntries RPC
type AppendEntriesReply struct {
	Term    int64
	Success bool
	NextIndex int64
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
	if args.Term < r.raft.currentTerm {
		reply.VoteGranted = false
		return nil
	}
	if args.Term > r.raft.currentTerm {
		r.raft.currentTerm = args.Term
		r.raft.votedFor = ""
		r.raft.role = RoleFollower
		reply.Term = r.raft.currentTerm
	}
	if (r.raft.votedFor == "" || r.raft.votedFor == args.CandidateID) &&
		r.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		r.raft.votedFor = args.CandidateID
		reply.VoteGranted = true
		r.raft.lastHeartbeat = time.Now()
		return nil
	}
	reply.VoteGranted = false
	return nil
}

// AppendEntries handles AppendEntries RPC
func (r *RaftRPC) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.raft.mu.Lock()
	defer r.raft.mu.Unlock()
	reply.Term = r.raft.currentTerm
	if args.Term < r.raft.currentTerm {
		reply.Success = false
		return nil
	}
	if args.Term > r.raft.currentTerm {
		r.raft.currentTerm = args.Term
		r.raft.votedFor = ""
		r.raft.role = RoleFollower
		reply.Term = r.raft.currentTerm
	}
	if args.LeaderAPIAddress != "" {
		r.raft.leaderAPIAddress = args.LeaderAPIAddress
	}
	r.raft.lastHeartbeat = time.Now()

	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > int64(len(r.raft.log)) {
			reply.Success = false
			reply.NextIndex = int64(len(r.raft.log)) + 1
			return nil
		}
		if (args.PrevLogIndex-1) < int64(len(r.raft.log)) {
			prevEntry := r.raft.log[args.PrevLogIndex-1]
			if prevEntry.Term != args.PrevLogTerm {
				reply.Success = false
				reply.NextIndex = args.PrevLogIndex
				return nil
			}
		}
	}

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
			r.raft.log = r.raft.log[:conflictIndex]
		}
		for i, entry := range args.Entries {
			entryIndex := args.PrevLogIndex + int64(i) + 1
			if entryIndex > int64(len(r.raft.log)) {
				entry.Index = entryIndex
				r.raft.log = append(r.raft.log, entry)
			}
		}
	}

	if args.LeaderCommit > r.raft.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + int64(len(args.Entries))
		if args.LeaderCommit < lastNewEntryIndex {
			r.raft.commitIndex = args.LeaderCommit
		} else {
			r.raft.commitIndex = lastNewEntryIndex
		}
	}

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
	for _, entry := range entriesToApply {
		_ = r.replicationManager.ApplyLogEntry(entry)
	}
	r.raft.mu.Lock()
	for _, entry := range entriesToApply {
		r.raft.lastApplied = entry.Index
	}
	reply.Success = true
	return nil
}

func (r *RaftRPC) isLogUpToDate(lastLogTerm, lastLogIndex int64) bool {
	lastIndex := int64(len(r.raft.log))
	lastTerm := int64(0)
	if lastIndex > 0 {
		lastTerm = r.raft.log[lastIndex-1].Term
	}
	return lastLogTerm > lastTerm || (lastLogTerm == lastTerm && lastLogIndex >= lastIndex)
}

// StartRaftRPCServer starts the RPC server for Raft communication
func StartRaftRPCServer(raft *RaftState, replicationManager *ReplicationManager, address string) error {
	rpc := NewRaftRPC(raft, replicationManager)
	http.HandleFunc("/raft/request-vote", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var args RequestVoteArgs
		if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
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
	http.HandleFunc("/raft/append-entries", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var args AppendEntriesArgs
		if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
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
	client := &http.Client{Timeout: RPCTimeout}
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
	client := &http.Client{Timeout: RPCTimeout}
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
