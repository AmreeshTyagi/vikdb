package store

import (
	"path/filepath"
	"testing"

	"vikdb/internal/compactor"
	"vikdb/internal/kv"
	"vikdb/internal/sstable"
)

func TestStore_FlushToSSTable(t *testing.T) {
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

	if err := s.FlushMemTable(); err != nil {
		t.Fatalf("Failed to flush MemTable: %v", err)
	}

	if s.GetSSTableCount() != 1 {
		t.Fatalf("Expected 1 SSTable, got %d", s.GetSSTableCount())
	}

	if s.GetMemTableSize() != 0 {
		t.Fatal("MemTable should be empty after flush")
	}

	value, exists := s.Read([]byte("key1"))
	if !exists {
		t.Fatal("key1 should exist in SSTable")
	}
	if len(value) != 50 {
		t.Fatalf("Expected value length 50, got %d", len(value))
	}
}

func TestStore_GetFromSSTable(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	s, err := NewStore(100, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	s.Put([]byte("key1"), []byte("value1"))
	s.Put([]byte("key2"), []byte("value2"))
	s.FlushMemTable()

	s.Put([]byte("key3"), []byte("value3"))

	value, exists := s.Read([]byte("key3"))
	if !exists || string(value) != "value3" {
		t.Fatal("key3 should be in MemTable")
	}

	value, exists = s.Read([]byte("key1"))
	if !exists || string(value) != "value1" {
		t.Fatal("key1 should be in SSTable")
	}
}

func TestStore_GetRangeWithSSTables(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	s, err := NewStore(100, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	s.Put([]byte("key1"), []byte("value1"))
	s.Put([]byte("key3"), []byte("value3"))
	s.FlushMemTable()

	s.Put([]byte("key2"), []byte("value2"))
	s.Put([]byte("key4"), []byte("value4"))
	s.FlushMemTable()

	results := s.ReadKeyRange([]byte("key1"), []byte("key4"))
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}
}

func TestCompactor_Compact(t *testing.T) {
	tmpDir := t.TempDir()
	sstableDir := filepath.Join(tmpDir, "sstables")

	entries1 := []kv.KeyValue{
		{Key: []byte("key1"), Value: []byte("value1_old")},
		{Key: []byte("key2"), Value: []byte("value2")},
	}
	path1, _ := sstable.WriteSSTable(entries1, sstableDir, 1)
	sst1, _ := sstable.OpenSSTable(path1)

	entries2 := []kv.KeyValue{
		{Key: []byte("key1"), Value: []byte("value1_new")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}
	path2, _ := sstable.WriteSSTable(entries2, sstableDir, 2)
	sst2, _ := sstable.OpenSSTable(path2)

	comp := compactor.NewCompactor(sstableDir, 2)
	mergedPath, err := comp.Compact([]*sstable.SSTable{sst1, sst2}, 3)
	if err != nil {
		t.Fatalf("Failed to compact: %v", err)
	}

	merged, err := sstable.OpenSSTable(mergedPath)
	if err != nil {
		t.Fatalf("Failed to open merged SSTable: %v", err)
	}
	defer merged.Close()

	value, exists := merged.Get([]byte("key1"))
	if !exists || string(value) != "value1_new" {
		t.Fatalf("Expected value1_new, got %s", value)
	}

	value, exists = merged.Get([]byte("key2"))
	if !exists || string(value) != "value2" {
		t.Fatal("key2 should exist")
	}

	value, exists = merged.Get([]byte("key3"))
	if !exists || string(value) != "value3" {
		t.Fatal("key3 should exist")
	}
}

func TestCompactStore(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	s, err := NewStore(50, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	s.Put([]byte("key1"), []byte("value1"))
	s.FlushMemTable()

	s.Put([]byte("key2"), []byte("value2"))
	s.FlushMemTable()

	s.Put([]byte("key3"), []byte("value3"))
	s.FlushMemTable()

	if s.GetSSTableCount() != 3 {
		t.Fatalf("Expected 3 SSTables, got %d", s.GetSSTableCount())
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("Failed to compact: %v", err)
	}

	if s.GetSSTableCount() != 1 {
		t.Fatalf("Expected 1 SSTable after compaction, got %d", s.GetSSTableCount())
	}

	value, exists := s.Read([]byte("key1"))
	if !exists || string(value) != "value1" {
		t.Fatal("key1 should exist after compaction")
	}

	value, exists = s.Read([]byte("key2"))
	if !exists || string(value) != "value2" {
		t.Fatal("key2 should exist after compaction")
	}

	value, exists = s.Read([]byte("key3"))
	if !exists || string(value) != "value3" {
		t.Fatal("key3 should exist after compaction")
	}
}
