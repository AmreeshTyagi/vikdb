package raft

import (
	"fmt"
	"strings"
)

// NodeRole represents the role of a node in the Raft cluster
type NodeRole string

const (
	RoleFollower  NodeRole = "follower"
	RoleCandidate NodeRole = "candidate"
	RoleLeader    NodeRole = "leader"
)

// Node represents a node in the cluster
type Node struct {
	ID      string
	Address string // host:port format
	Role    NodeRole
}

// ClusterConfig represents the cluster configuration
type ClusterConfig struct {
	NodeID          string
	Address         string // This node's address
	ReplicationPort string // Port for replication RPCs
	Peers           []Node // Other nodes in the cluster
}

// ParseClusterConfig parses a comma-separated list of peer addresses
func ParseClusterConfig(nodeID, address, replicationPort, clusterStr string) (*ClusterConfig, error) {
	if clusterStr == "" {
		return nil, fmt.Errorf("cluster configuration is required")
	}

	peers := make([]Node, 0)
	addresses := strings.Split(clusterStr, ",")

	for i, addr := range addresses {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		peerID := fmt.Sprintf("node-%d", i+1)
		peers = append(peers, Node{
			ID:      peerID,
			Address: addr,
			Role:    RoleFollower,
		})
	}

	return &ClusterConfig{
		NodeID:          nodeID,
		Address:         address,
		ReplicationPort: replicationPort,
		Peers:           peers,
	}, nil
}

// GetPeerAddresses returns a list of all peer addresses
func (c *ClusterConfig) GetPeerAddresses() []string {
	addresses := make([]string, 0, len(c.Peers))
	for _, peer := range c.Peers {
		addresses = append(addresses, peer.Address)
	}
	return addresses
}

// IsInCluster returns true if the given address is in the cluster
func (c *ClusterConfig) IsInCluster(address string) bool {
	for _, peer := range c.Peers {
		if peer.Address == address {
			return true
		}
	}
	return false
}

// GetReplicationAddress returns the full replication address (host:replicationPort)
func (c *ClusterConfig) GetReplicationAddress() string {
	host := c.Address
	if idx := strings.LastIndex(c.Address, ":"); idx != -1 {
		host = c.Address[:idx]
	}
	return fmt.Sprintf("%s:%s", host, c.ReplicationPort)
}
