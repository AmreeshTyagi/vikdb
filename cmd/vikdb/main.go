package main

import (
	"flag"
	"fmt"
	"log"
	"vikdb"
)

func main() {
	addr := flag.String("addr", ":8080", "Server address (e.g., :8080)")
	memTableMaxSize := flag.Int64("memtable-max-size", 100*1024*1024, "Maximum MemTable size in bytes (default: 100MB)")
	walPath := flag.String("wal-path", "./data/wal", "Path to WAL file")
	sstableDir := flag.String("sstable-dir", "./data/sstables", "Directory for SSTable files")
	
	// Replication flags
	nodeID := flag.String("node-id", "", "Unique node identifier (required for replication)")
	cluster := flag.String("cluster", "", "Comma-separated list of peer addresses (e.g., \"node1:8080,node2:8080,node3:8080\")")
	replicationPort := flag.String("replication-port", "8081", "Port for replication RPCs (separate from HTTP API)")
	
	flag.Parse()

	// Parse cluster configuration if provided
	var clusterConfig *vikdb.ClusterConfig
	var replicatedStore *vikdb.ReplicatedStore
	var server *vikdb.Server

	if *nodeID != "" && *cluster != "" {
		var err error
		clusterConfig, err = vikdb.ParseClusterConfig(*nodeID, *addr, *replicationPort, *cluster)
		if err != nil {
			log.Fatalf("Failed to parse cluster config: %v", err)
		}
		fmt.Printf("Cluster mode enabled\n")
		fmt.Printf("Node ID: %s\n", clusterConfig.NodeID)
		fmt.Printf("Replication address: %s\n", clusterConfig.GetReplicationAddress())
		fmt.Printf("Peers: %v\n", clusterConfig.GetPeerAddresses())

		// Create Store
		store, err := vikdb.NewStore(*memTableMaxSize, *walPath, *sstableDir)
		if err != nil {
			log.Fatalf("Failed to create store: %v", err)
		}
		defer store.Close()

		// Create Raft state
		statePath := *walPath + ".raft"
		raft, err := vikdb.NewRaftState(clusterConfig.NodeID, clusterConfig, statePath)
		if err != nil {
			log.Fatalf("Failed to create Raft state: %v", err)
		}

		// Create ReplicatedStore
		replicatedStore = vikdb.NewReplicatedStore(store, raft)
		defer replicatedStore.Close()

		// Start Raft RPC server
		replicationManager := vikdb.NewReplicationManager(raft, store)
		go func() {
			if err := vikdb.StartRaftRPCServer(raft, replicationManager, clusterConfig.GetReplicationAddress()); err != nil {
				log.Printf("Raft RPC server error: %v", err)
			}
		}()

		// Create replicated server
		server = vikdb.NewReplicatedServer(replicatedStore)
	} else {
		// Single node mode
		store, err := vikdb.NewStore(*memTableMaxSize, *walPath, *sstableDir)
		if err != nil {
			log.Fatalf("Failed to create store: %v", err)
		}
		defer store.Close()

		server = vikdb.NewServer(store)
	}
	
	fmt.Printf("Starting VikDB server on %s\n", *addr)
	fmt.Printf("MemTable max size: %d bytes\n", *memTableMaxSize)
	fmt.Printf("WAL path: %s\n", *walPath)
	fmt.Printf("SSTable directory: %s\n", *sstableDir)
	
	if err := server.Start(*addr); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
