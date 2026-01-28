package store

import (
	"path/filepath"
	"testing"
)

func TestStore_AutoCompactAfterFlush(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	s, err := NewStoreWithCompaction(50, walPath, sstableDir, 3)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	s.Put([]byte("key1"), make([]byte, 50))
	if s.GetSSTableCount() != 1 {
		t.Fatalf("Expected 1 SSTable after first flush, got %d", s.GetSSTableCount())
	}

	s.Put([]byte("key2"), make([]byte, 50))
	if s.GetSSTableCount() != 2 {
		t.Fatalf("Expected 2 SSTables after second flush, got %d", s.GetSSTableCount())
	}

	s.Put([]byte("key3"), make([]byte, 50))
	if s.GetSSTableCount() != 1 {
		t.Fatalf("Expected 1 SSTable after compaction (was 3), got %d", s.GetSSTableCount())
	}

	_, exists := s.Read([]byte("key1"))
	if !exists {
		t.Fatal("key1 should exist after compaction")
	}

	_, exists = s.Read([]byte("key2"))
	if !exists {
		t.Fatal("key2 should exist after compaction")
	}

	_, exists = s.Read([]byte("key3"))
	if !exists {
		t.Fatal("key3 should exist after compaction")
	}
}

func TestStore_NoCompactWhenBelowThreshold(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	s, err := NewStoreWithCompaction(50, walPath, sstableDir, 5)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	for i := 0; i < 4; i++ {
		s.Put([]byte{byte('a' + i)}, make([]byte, 50))
	}

	if s.GetSSTableCount() != 4 {
		t.Fatalf("Expected 4 SSTables (below threshold), got %d", s.GetSSTableCount())
	}
}

func TestStore_CompactWithDuplicates(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	s, err := NewStoreWithCompaction(50, walPath, sstableDir, 2)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	s.Put([]byte("key1"), make([]byte, 50))
	s.Put([]byte("key2"), make([]byte, 50))

	if s.GetSSTableCount() != 1 {
		t.Fatalf("Expected 1 SSTable after compaction, got %d", s.GetSSTableCount())
	}

	_, exists := s.Read([]byte("key1"))
	if !exists {
		t.Fatal("key1 should exist after compaction")
	}

	_, exists = s.Read([]byte("key2"))
	if !exists {
		t.Fatal("key2 should exist after compaction")
	}
}
