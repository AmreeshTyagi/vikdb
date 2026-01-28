package store

import (
	"path/filepath"
	"testing"
)

func TestStore_PutAndGet(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	s, err := NewStore(1024*1024, walPath, filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	if err := s.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	value, exists := s.Read([]byte("key1"))
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

	s1, err := NewStore(1024*1024, walPath, filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	s1.Put([]byte("key1"), []byte("value1"))
	s1.Put([]byte("key2"), []byte("value2"))
	s1.Delete([]byte("key1"))
	s1.Put([]byte("key3"), []byte("value3"))
	s1.Close()

	s2, err := NewStore(1024*1024, walPath, filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to recover store: %v", err)
	}
	defer s2.Close()

	_, exists := s2.Read([]byte("key1"))
	if exists {
		t.Fatal("key1 should not exist after delete and recovery")
	}

	value, exists := s2.Read([]byte("key2"))
	if !exists {
		t.Fatal("key2 should exist after recovery")
	}
	if string(value) != "value2" {
		t.Fatalf("Expected value2, got %s", value)
	}

	value, exists = s2.Read([]byte("key3"))
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

	s, err := NewStore(1024*1024, walPath, filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}

	if err := s.BatchPut(keys, values); err != nil {
		t.Fatalf("Failed to batch put: %v", err)
	}

	for i, key := range keys {
		value, exists := s.Read(key)
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

	s, err := NewStore(1024*1024, walPath, filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	s.Put([]byte("key1"), []byte("value1"))
	s.Put([]byte("key2"), []byte("value2"))

	walSize, err := s.GetWALSize()
	if err != nil {
		t.Fatalf("Failed to get WAL size: %v", err)
	}
	if walSize == 0 {
		t.Fatal("WAL should have content")
	}

	if err := s.FlushMemTable(); err != nil {
		t.Fatalf("Failed to flush MemTable: %v", err)
	}

	if s.GetMemTableSize() != 0 {
		t.Fatal("MemTable should be empty after flush")
	}

	walSize, err = s.GetWALSize()
	if err != nil {
		t.Fatalf("Failed to get WAL size after flush: %v", err)
	}
	if walSize != 0 {
		t.Fatalf("WAL should be empty after flush, got size %d", walSize)
	}

	s.Put([]byte("key3"), []byte("value3"))

	walSize, err = s.GetWALSize()
	if err != nil {
		t.Fatalf("Failed to get WAL size: %v", err)
	}
	if walSize == 0 {
		t.Fatal("WAL should have new content after flush")
	}
}

func TestStore_WithoutWAL(t *testing.T) {
	s, err := NewStore(1024*1024, "", "")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	if err := s.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	value, exists := s.Read([]byte("key1"))
	if !exists {
		t.Fatal("Key should exist")
	}
	if string(value) != "value1" {
		t.Fatalf("Expected value1, got %s", value)
	}

	walSize, err := s.GetWALSize()
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

	s, err := NewStore(1024*1024, walPath, filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	s.Put([]byte("key3"), []byte("value3"))
	s.Put([]byte("key1"), []byte("value1"))
	s.Put([]byte("key5"), []byte("value5"))
	s.Put([]byte("key2"), []byte("value2"))
	s.Put([]byte("key4"), []byte("value4"))

	results := s.ReadKeyRange([]byte("key2"), []byte("key4"))
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
