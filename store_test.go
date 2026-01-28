package vikdb

import (
	"path/filepath"
	"testing"
)

func TestStore_PutAndGet(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	store, err := NewStore(1024*1024, walPath, filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Put a key-value pair
	if err := store.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Get it back
	value, exists := store.Read([]byte("key1"))
	if !exists {
		t.Fatal("Key should exist")
	}
	if string(value) != "value1" {
		t.Fatalf("Expected value1, got %s", value)
	}
}

func TestStore_CrashRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	// Create store and add some data
	store1, err := NewStore(1024*1024, walPath, filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	store1.Put([]byte("key1"), []byte("value1"))
	store1.Put([]byte("key2"), []byte("value2"))
	store1.Delete([]byte("key1"))
	store1.Put([]byte("key3"), []byte("value3"))
	store1.Close()

	// Simulate crash and recovery - create new store with same WAL
	store2, err := NewStore(1024*1024, walPath, filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to recover store: %v", err)
	}
	defer store2.Close()

	// Verify key1 was deleted
	_, exists := store2.Read([]byte("key1"))
	if exists {
		t.Fatal("key1 should not exist after delete and recovery")
	}

	// Verify key2 exists
	value, exists := store2.Read([]byte("key2"))
	if !exists {
		t.Fatal("key2 should exist after recovery")
	}
	if string(value) != "value2" {
		t.Fatalf("Expected value2, got %s", value)
	}

	// Verify key3 exists
	value, exists = store2.Read([]byte("key3"))
	if !exists {
		t.Fatal("key3 should exist after recovery")
	}
	if string(value) != "value3" {
		t.Fatalf("Expected value3, got %s", value)
	}
}

func TestStore_BatchPut(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	store, err := NewStore(1024*1024, walPath, filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}

	if err := store.BatchPut(keys, values); err != nil {
		t.Fatalf("Failed to batch put: %v", err)
	}

	// Verify all keys exist
	for i, key := range keys {
		value, exists := store.Read(key)
		if !exists {
			t.Fatalf("Key %s should exist", key)
		}
		if string(value) != string(values[i]) {
			t.Fatalf("Expected %s, got %s", values[i], value)
		}
	}
}

func TestStore_FlushMemTable(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	store, err := NewStore(1024*1024, walPath, filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Add some data
	store.Put([]byte("key1"), []byte("value1"))
	store.Put([]byte("key2"), []byte("value2"))

	// Verify WAL has content
	walSize, err := store.GetWALSize()
	if err != nil {
		t.Fatalf("Failed to get WAL size: %v", err)
	}
	if walSize == 0 {
		t.Fatal("WAL should have content")
	}

	// Flush MemTable (simulating SSTable write)
	if err := store.FlushMemTable(); err != nil {
		t.Fatalf("Failed to flush MemTable: %v", err)
	}

	// Verify MemTable is empty
	if store.GetMemTableSize() != 0 {
		t.Fatal("MemTable should be empty after flush")
	}

	// Verify WAL is truncated
	walSize, err = store.GetWALSize()
	if err != nil {
		t.Fatalf("Failed to get WAL size after flush: %v", err)
	}
	if walSize != 0 {
		t.Fatalf("WAL should be empty after flush, got size %d", walSize)
	}

	// Add new data after flush
	store.Put([]byte("key3"), []byte("value3"))

	// Verify new data is in WAL
	walSize, err = store.GetWALSize()
	if err != nil {
		t.Fatalf("Failed to get WAL size: %v", err)
	}
	if walSize == 0 {
		t.Fatal("WAL should have new content after flush")
	}
}

func TestStore_WithoutWAL(t *testing.T) {
	// Create store without WAL
	store, err := NewStore(1024*1024, "", "")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Operations should still work
	if err := store.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	value, exists := store.Read([]byte("key1"))
	if !exists {
		t.Fatal("Key should exist")
	}
	if string(value) != "value1" {
		t.Fatalf("Expected value1, got %s", value)
	}

	// WAL size should be 0
	walSize, err := store.GetWALSize()
	if err != nil {
		t.Fatalf("Failed to get WAL size: %v", err)
	}
	if walSize != 0 {
		t.Fatalf("WAL size should be 0, got %d", walSize)
	}
}

func TestStore_GetRange(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	store, err := NewStore(1024*1024, walPath, filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Add keys in non-sorted order
	store.Put([]byte("key3"), []byte("value3"))
	store.Put([]byte("key1"), []byte("value1"))
	store.Put([]byte("key5"), []byte("value5"))
	store.Put([]byte("key2"), []byte("value2"))
	store.Put([]byte("key4"), []byte("value4"))

	// Test range query
	results := store.ReadKeyRange([]byte("key2"), []byte("key4"))
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	if string(results[0].Key) != "key2" {
		t.Fatalf("Expected key2, got %s", results[0].Key)
	}
	if string(results[1].Key) != "key3" {
		t.Fatalf("Expected key3, got %s", results[1].Key)
	}
}
