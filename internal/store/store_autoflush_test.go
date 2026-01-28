package store

import (
	"path/filepath"
	"testing"
)

func TestStore_AutoFlushOnPut(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	s, err := NewStore(100, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	s.Put([]byte("key1"), make([]byte, 50))
	s.Put([]byte("key2"), make([]byte, 60))

	if s.GetMemTableSize() > 0 {
		t.Fatalf("MemTable should be empty after auto-flush, got size %d", s.GetMemTableSize())
	}

	if s.GetSSTableCount() == 0 {
		t.Fatal("SSTable should be created after auto-flush")
	}

	value, exists := s.Read([]byte("key1"))
	if !exists {
		t.Fatal("key1 should exist in SSTable after flush")
	}
	if len(value) != 50 {
		t.Fatalf("Expected value length 50, got %d", len(value))
	}
}

func TestStore_AutoFlushOnBatchPut(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	s, err := NewStore(100, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	keys := [][]byte{[]byte("key1"), []byte("key2")}
	values := [][]byte{make([]byte, 50), make([]byte, 60)}

	if err := s.BatchPut(keys, values); err != nil {
		t.Fatalf("Failed to batch put: %v", err)
	}

	if s.GetMemTableSize() > 0 {
		t.Fatalf("MemTable should be empty after auto-flush, got size %d", s.GetMemTableSize())
	}

	if s.GetSSTableCount() == 0 {
		t.Fatal("SSTable should be created after auto-flush")
	}
}

func TestStore_NoFlushWhenNotFull(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	s, err := NewStore(1024*1024, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	s.Put([]byte("key1"), []byte("value1"))

	if s.GetMemTableSize() == 0 {
		t.Fatal("MemTable should still have data when not full")
	}

	if s.GetSSTableCount() != 0 {
		t.Fatal("No SSTable should be created when MemTable is not full")
	}
}
