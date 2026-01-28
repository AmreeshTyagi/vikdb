package vikdb

import (
	"math/rand"
	"sync"
	"time"
)

const (
	// ElectionTimeoutMin is the minimum election timeout in milliseconds
	ElectionTimeoutMin = 150
	// ElectionTimeoutMax is the maximum election timeout in milliseconds
	ElectionTimeoutMax = 300
	// HeartbeatInterval is the interval between heartbeats in milliseconds
	HeartbeatInterval = 50
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

// run runs the main election loop
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

		time.Sleep(10 * time.Millisecond) // Small delay to prevent busy loop
	}
}

// handleElectionTimeout handles election timeout for followers and candidates
func (em *ElectionManager) handleElectionTimeout() {
	em.mu.Lock()
	defer em.mu.Unlock()

	// Check if election timeout has passed
	lastHeartbeat := em.raft.GetLastHeartbeat()
	timeout := em.getRandomElectionTimeout()
	elapsed := time.Since(lastHeartbeat)

	if elapsed >= timeout {
		// Start election
		em.startElection()
	}
}

// startElection starts a new election
func (em *ElectionManager) startElection() {
	// Convert to candidate
	em.raft.IncrementTerm()
	em.raft.SetRole(RoleCandidate)
	em.raft.SetVotedFor(em.raft.nodeID) // Vote for self

	// Prepare RequestVote arguments
	lastLogIndex := em.raft.GetLastLogIndex()
	lastLogTerm := em.raft.GetLastLogTerm()
	term := em.raft.GetCurrentTerm()

	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  em.raft.nodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// Send RequestVote to all peers
	votes := 1 // Vote for self
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
				// Network error, don't count vote
				return
			}

			mu.Lock()
			defer mu.Unlock()

			// Update term if we see a higher term
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

	// Check if we won the election
	mu.Lock()
	if votes >= majority && em.raft.GetRole() == RoleCandidate {
		// Become leader
		em.raft.SetRole(RoleLeader)
		em.raft.UpdateLastHeartbeat() // Reset heartbeat timer
	}
	mu.Unlock()
}

// handleHeartbeat handles periodic heartbeats for leaders
func (em *ElectionManager) handleHeartbeat() {
	em.mu.Lock()
	defer em.mu.Unlock()

	// Send heartbeats periodically
	if em.heartbeatTimer == nil {
		em.heartbeatTimer = time.NewTicker(HeartbeatInterval * time.Millisecond)
		go em.sendHeartbeats()
	}
}

// sendHeartbeats sends heartbeats to all followers
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
				// No longer leader, stop heartbeats
				em.heartbeatTimer.Stop()
				em.heartbeatTimer = nil
				return
			}
		}
	}
}

// sendHeartbeatToAll sends heartbeat (empty AppendEntries) to all followers
func (em *ElectionManager) sendHeartbeatToAll() {
	term := em.raft.GetCurrentTerm()
	lastLogIndex := em.raft.GetLastLogIndex()
	commitIndex := em.raft.GetCommitIndex()

	for _, peer := range em.raft.cluster.Peers {
		go func(peerAddress string) {
			nextIndex := em.raft.GetNextIndex(peerAddress)
			if nextIndex == 0 {
				// Initialize nextIndex
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
				Entries:          []LogEntry{}, // Empty for heartbeat
				LeaderCommit:     commitIndex,
			}

			reply, err := CallAppendEntries(peerAddress, args)
			if err != nil {
				// Network error, will retry on next heartbeat
				return
			}

			// Update term if we see a higher term
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
				// Update nextIndex and matchIndex
				em.raft.UpdateNextIndex(peerAddress, nextIndex)
				em.raft.UpdateMatchIndex(peerAddress, prevLogIndex)
			} else {
				// Decrement nextIndex and retry
				if reply.NextIndex > 0 {
					em.raft.UpdateNextIndex(peerAddress, reply.NextIndex)
				} else {
					em.raft.UpdateNextIndex(peerAddress, nextIndex-1)
				}
			}
		}(peer.Address)
	}
}

// getRandomElectionTimeout returns a random election timeout
func (em *ElectionManager) getRandomElectionTimeout() time.Duration {
	timeout := ElectionTimeoutMin + rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)
	return time.Duration(timeout) * time.Millisecond
}

// ResetElectionTimeout resets the election timeout (called when receiving heartbeat)
func (em *ElectionManager) ResetElectionTimeout() {
	em.raft.UpdateLastHeartbeat()
}
