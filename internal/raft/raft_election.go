package raft

import (
	"math/rand"
	"sync"
	"time"
)

const (
	ElectionTimeoutMin = 150
	ElectionTimeoutMax = 300
	HeartbeatInterval  = 50
)

// ElectionManager manages leader election
type ElectionManager struct {
	raft           *RaftState
	electionTimer  *time.Timer
	heartbeatTimer *time.Ticker
	stopChan       chan struct{}
	wg             sync.WaitGroup
	mu             sync.Mutex
}

// NewElectionManager creates a new election manager
func NewElectionManager(raft *RaftState) *ElectionManager {
	return &ElectionManager{
		raft:     raft,
		stopChan: make(chan struct{}),
	}
}

// Start starts the election manager
func (em *ElectionManager) Start() {
	em.wg.Add(1)
	go em.run()
}

// Stop stops the election manager
func (em *ElectionManager) Stop() {
	close(em.stopChan)
	em.wg.Wait()
}

func (em *ElectionManager) run() {
	defer em.wg.Done()
	for {
		select {
		case <-em.stopChan:
			return
		default:
			role := em.raft.GetRole()
			switch role {
			case RoleFollower, RoleCandidate:
				em.handleElectionTimeout()
			case RoleLeader:
				em.handleHeartbeat()
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (em *ElectionManager) handleElectionTimeout() {
	em.mu.Lock()
	defer em.mu.Unlock()
	lastHeartbeat := em.raft.GetLastHeartbeat()
	timeout := em.getRandomElectionTimeout()
	elapsed := time.Since(lastHeartbeat)
	if elapsed >= timeout {
		em.startElection()
	}
}

func (em *ElectionManager) startElection() {
	em.raft.IncrementTerm()
	em.raft.SetRole(RoleCandidate)
	em.raft.SetVotedFor(em.raft.nodeID)
	lastLogIndex := em.raft.GetLastLogIndex()
	lastLogTerm := em.raft.GetLastLogTerm()
	term := em.raft.GetCurrentTerm()
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  em.raft.nodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	votes := 1
	totalVotes := 1
	majority := em.raft.GetMajorityCount()
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, peer := range em.raft.cluster.Peers {
		wg.Add(1)
		go func(peerAddress string) {
			defer wg.Done()
			reply, err := CallRequestVote(peerAddress, args)
			if err != nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if reply.Term > em.raft.GetCurrentTerm() {
				em.raft.mu.Lock()
				em.raft.currentTerm = reply.Term
				em.raft.votedFor = ""
				em.raft.role = RoleFollower
				em.raft.saveState()
				em.raft.mu.Unlock()
				return
			}
			totalVotes++
			if reply.VoteGranted {
				votes++
			}
		}(peer.Address)
	}
	wg.Wait()
	mu.Lock()
	if votes >= majority && em.raft.GetRole() == RoleCandidate {
		em.raft.SetRole(RoleLeader)
		em.raft.UpdateLastHeartbeat()
	}
	mu.Unlock()
}

func (em *ElectionManager) handleHeartbeat() {
	em.mu.Lock()
	defer em.mu.Unlock()
	if em.heartbeatTimer == nil {
		em.heartbeatTimer = time.NewTicker(HeartbeatInterval * time.Millisecond)
		go em.sendHeartbeats()
	}
}

func (em *ElectionManager) sendHeartbeats() {
	for {
		select {
		case <-em.stopChan:
			if em.heartbeatTimer != nil {
				em.heartbeatTimer.Stop()
			}
			return
		case <-em.heartbeatTimer.C:
			if em.raft.IsLeader() {
				em.sendHeartbeatToAll()
			} else {
				em.heartbeatTimer.Stop()
				em.heartbeatTimer = nil
				return
			}
		}
	}
}

func (em *ElectionManager) sendHeartbeatToAll() {
	term := em.raft.GetCurrentTerm()
	lastLogIndex := em.raft.GetLastLogIndex()
	commitIndex := em.raft.GetCommitIndex()
	for _, peer := range em.raft.cluster.Peers {
		go func(peerAddress string) {
			nextIndex := em.raft.GetNextIndex(peerAddress)
			if nextIndex == 0 {
				nextIndex = lastLogIndex + 1
				em.raft.UpdateNextIndex(peerAddress, nextIndex)
			}
			prevLogIndex := nextIndex - 1
			prevLogTerm := int64(0)
			if prevLogIndex > 0 {
				entry, err := em.raft.GetLogEntry(prevLogIndex)
				if err == nil {
					prevLogTerm = entry.Term
				}
			}
			args := &AppendEntriesArgs{
				Term:             term,
				LeaderID:         em.raft.nodeID,
				LeaderAPIAddress: em.raft.cluster.Address,
				PrevLogIndex:     prevLogIndex,
				PrevLogTerm:      prevLogTerm,
				Entries:          []LogEntry{},
				LeaderCommit:     commitIndex,
			}
			reply, err := CallAppendEntries(peerAddress, args)
			if err != nil {
				return
			}
			if reply.Term > em.raft.GetCurrentTerm() {
				em.raft.mu.Lock()
				em.raft.currentTerm = reply.Term
				em.raft.votedFor = ""
				em.raft.role = RoleFollower
				em.raft.saveState()
				em.raft.mu.Unlock()
				return
			}
			if reply.Success {
				em.raft.UpdateNextIndex(peerAddress, nextIndex)
				em.raft.UpdateMatchIndex(peerAddress, prevLogIndex)
			} else {
				if reply.NextIndex > 0 {
					em.raft.UpdateNextIndex(peerAddress, reply.NextIndex)
				} else {
					em.raft.UpdateNextIndex(peerAddress, nextIndex-1)
				}
			}
		}(peer.Address)
	}
}

func (em *ElectionManager) getRandomElectionTimeout() time.Duration {
	timeout := ElectionTimeoutMin + rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)
	return time.Duration(timeout) * time.Millisecond
}

// ResetElectionTimeout resets the election timeout
func (em *ElectionManager) ResetElectionTimeout() {
	em.raft.UpdateLastHeartbeat()
}
