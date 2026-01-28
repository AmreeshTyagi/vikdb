// Package vikdb provides a key-value store with LSM-style storage,
// WAL, Raft replication, and HTTP API. It re-exports the public API
// from internal packages for use by cmd and other importers.
package vikdb

import (
	"vikdb/internal/raft"
	"vikdb/internal/replicatedstore"
	"vikdb/internal/server"
	"vikdb/internal/store"
)

// Store type and constructors
type Store = store.Store

var (
	NewStore               = store.NewStore
	NewStoreWithCompaction = store.NewStoreWithCompaction
)

// Raft types and functions
type (
	ClusterConfig = raft.ClusterConfig
	Node          = raft.Node
	NodeRole      = raft.NodeRole
	RaftState     = raft.RaftState
)

const (
	RoleFollower  = raft.RoleFollower
	RoleCandidate = raft.RoleCandidate
	RoleLeader    = raft.RoleLeader
)

var (
	ParseClusterConfig    = raft.ParseClusterConfig
	NewRaftState          = raft.NewRaftState
	NewReplicationManager = raft.NewReplicationManager
	StartRaftRPCServer    = raft.StartRaftRPCServer
)

// ReplicatedStore type and constructor
type ReplicatedStore = replicatedstore.ReplicatedStore

var NewReplicatedStore = replicatedstore.NewReplicatedStore

// Server type and constructors
type Server = server.Server

var (
	NewServer           = server.NewServer
	NewReplicatedServer = server.NewReplicatedServer
)
