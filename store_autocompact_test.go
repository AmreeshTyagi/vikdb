package vikdb

import (
	"path/filepath"
	"testing"
)

func TestStore_AutoCompactAfterFlush(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	// Create store with small max size and maxSSTables = 3
	store, err := NewStoreWithCompaction(50, walPath, sstableDir, 3)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create 3 SSTables by flushing multiple times
	// This should trigger automatic compaction
	// Put data that exceeds max size (50 bytes) to trigger flush
	// key (4 bytes) + value (50 bytes) = 54 bytes > 50, triggers flush
	store.Put([]byte("key1"), make([]byte, 50))
	// First flush - creates SSTable 1
	if store.GetSSTableCount() != 1 {
		t.Fatalf("Expected 1 SSTable after first flush, got %d", store.GetSSTableCount())
	}

	store.Put([]byte("key2"), make([]byte, 50))
	// Second flush - creates SSTable 2
	if store.GetSSTableCount() != 2 {
		t.Fatalf("Expected 2 SSTables after second flush, got %d", store.GetSSTableCount())
	}

	store.Put([]byte("key3"), make([]byte, 50))
	// Third flush - creates SSTable 3, should trigger compaction (3 >= 3)
	// After compaction, should have 1 merged SSTable
	if store.GetSSTableCount() != 1 {
		t.Fatalf("Expected 1 SSTable after compaction (was 3), got %d", store.GetSSTableCount())
	}

	// Verify all keys are still accessible after compaction
	_, exists := store.Read([]byte("key1"))
	if !exists {
		t.Fatal("key1 should exist after compaction")
	}

	_, exists = store.Read([]byte("key2"))
	if !exists {
		t.Fatal("key2 should exist after compaction")
	}

	_, exists = store.Read([]byte("key3"))
	if !exists {
		t.Fatal("key3 should exist after compaction")
	}
}

func TestStore_NoCompactWhenBelowThreshold(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	// Create store with maxSSTables = 5
	store, err := NewStoreWithCompaction(50, walPath, sstableDir, 5)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create 4 SSTables (below threshold of 5)
	for i := 0; i < 4; i++ {
		store.Put([]byte{byte('a' + i)}, make([]byte, 50))
	}

	// Should have 4 SSTables, no compaction
	if store.GetSSTableCount() != 4 {
		t.Fatalf("Expected 4 SSTables (below threshold), got %d", store.GetSSTableCount())
	}
}

func TestStore_CompactWithDuplicates(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	// Create store with maxSSTables = 2
	store, err := NewStoreWithCompaction(50, walPath, sstableDir, 2)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Put key1 with value1, flush
	store.Put([]byte("key1"), make([]byte, 50))
	// First flush - creates SSTable 1

	// Put key2 to trigger second flush (should trigger compaction when 2 SSTables exist)
	store.Put([]byte("key2"), make([]byte, 50))

	// After compaction, should have 1 SSTable
	if store.GetSSTableCount() != 1 {
		t.Fatalf("Expected 1 SSTable after compaction, got %d", store.GetSSTableCount())
	}

	// Verify both keys exist after compaction
	_, exists := store.Read([]byte("key1"))
	if !exists {
		t.Fatal("key1 should exist after compaction")
	}

	_, exists = store.Read([]byte("key2"))
	if !exists {
		t.Fatal("key2 should exist after compaction")
	}
}
