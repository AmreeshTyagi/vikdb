package sstable

import (
	"path/filepath"
	"testing"

	"vikdb/internal/kv"
)

func TestWriteSSTable(t *testing.T) {
	tmpDir := t.TempDir()

	entries := []kv.KeyValue{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}

	filePath, err := WriteSSTable(entries, tmpDir, 1)
	if err != nil {
		t.Fatalf("Failed to write SSTable: %v", err)
	}

	if filePath == "" {
		t.Fatal("File path should not be empty")
	}

	expectedPath := filepath.Join(tmpDir, "sstable-1.sst")
	if filePath != expectedPath {
		t.Fatalf("Expected path %s, got %s", expectedPath, filePath)
	}
}

func TestOpenSSTable(t *testing.T) {
	tmpDir := t.TempDir()

	entries := []kv.KeyValue{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}

	filePath, err := WriteSSTable(entries, tmpDir, 1)
	if err != nil {
		t.Fatalf("Failed to write SSTable: %v", err)
	}

	sst, err := OpenSSTable(filePath)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer sst.Close()

	value, exists := sst.Get([]byte("key1"))
	if !exists {
		t.Fatal("key1 should exist")
	}
	if string(value) != "value1" {
		t.Fatalf("Expected value1, got %s", value)
	}

	value, exists = sst.Get([]byte("key2"))
	if !exists {
		t.Fatal("key2 should exist")
	}
	if string(value) != "value2" {
		t.Fatalf("Expected value2, got %s", value)
	}

	_, exists = sst.Get([]byte("nonexistent"))
	if exists {
		t.Fatal("nonexistent key should not exist")
	}
}

func TestSSTable_GetRange(t *testing.T) {
	tmpDir := t.TempDir()

	entries := []kv.KeyValue{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
		{Key: []byte("key4"), Value: []byte("value4")},
		{Key: []byte("key5"), Value: []byte("value5")},
	}

	filePath, err := WriteSSTable(entries, tmpDir, 1)
	if err != nil {
		t.Fatalf("Failed to write SSTable: %v", err)
	}

	sst, err := OpenSSTable(filePath)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer sst.Close()

	results := sst.GetRange([]byte("key2"), []byte("key4"))
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	if string(results[0].Key) != "key2" {
		t.Fatalf("Expected key2, got %s", results[0].Key)
	}
	if string(results[1].Key) != "key3" {
		t.Fatalf("Expected key3, got %s", results[1].Key)
	}

	results = sst.GetRange([]byte("key3"), nil)
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}
}

func TestSSTable_EmptyEntries(t *testing.T) {
	tmpDir := t.TempDir()

	_, err := WriteSSTable([]kv.KeyValue{}, tmpDir, 1)
	if err == nil {
		t.Fatal("Should fail to write empty SSTable")
	}
}

func TestSSTable_UnsortedEntries(t *testing.T) {
	tmpDir := t.TempDir()

	entries := []kv.KeyValue{
		{Key: []byte("key3"), Value: []byte("value3")},
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
	}

	filePath, err := WriteSSTable(entries, tmpDir, 1)
	if err != nil {
		t.Fatalf("Failed to write SSTable: %v", err)
	}

	sst, err := OpenSSTable(filePath)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer sst.Close()

	results := sst.GetRange(nil, nil)
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	if string(results[0].Key) != "key1" {
		t.Fatalf("Expected key1, got %s", results[0].Key)
	}
	if string(results[1].Key) != "key2" {
		t.Fatalf("Expected key2, got %s", results[1].Key)
	}
	if string(results[2].Key) != "key3" {
		t.Fatalf("Expected key3, got %s", results[2].Key)
	}
}

func TestSSTable_LargeValues(t *testing.T) {
	tmpDir := t.TempDir()

	largeValue := make([]byte, 100*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	entries := []kv.KeyValue{
		{Key: []byte("large_key"), Value: largeValue},
		{Key: []byte("small_key"), Value: []byte("small_value")},
	}

	filePath, err := WriteSSTable(entries, tmpDir, 1)
	if err != nil {
		t.Fatalf("Failed to write SSTable: %v", err)
	}

	sst, err := OpenSSTable(filePath)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer sst.Close()

	value, exists := sst.Get([]byte("large_key"))
	if !exists {
		t.Fatal("large_key should exist")
	}
	if len(value) != len(largeValue) {
		t.Fatalf("Expected value length %d, got %d", len(largeValue), len(value))
	}

	value, exists = sst.Get([]byte("small_key"))
	if !exists {
		t.Fatal("small_key should exist")
	}
	if string(value) != "small_value" {
		t.Fatalf("Expected small_value, got %s", value)
	}
}
