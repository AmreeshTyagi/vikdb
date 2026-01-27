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
	flag.Parse()

	// Create Store
	store, err := vikdb.NewStore(*memTableMaxSize, *walPath, *sstableDir)
	if err != nil {
		log.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create and start server
	server := vikdb.NewServer(store)
	
	fmt.Printf("Starting VikDB server on %s\n", *addr)
	fmt.Printf("MemTable max size: %d bytes\n", *memTableMaxSize)
	fmt.Printf("WAL path: %s\n", *walPath)
	fmt.Printf("SSTable directory: %s\n", *sstableDir)
	
	if err := server.Start(*addr); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
