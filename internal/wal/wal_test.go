package wal

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
)

func TestWAL_AppendAndReplay(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	entries := []WALEntry{
		{Type: WALEntryPut, Key: []byte("key1"), Value: []byte("value1")},
		{Type: WALEntryPut, Key: []byte("key2"), Value: []byte("value2")},
		{Type: WALEntryDelete, Key: []byte("key1"), Value: nil},
		{Type: WALEntryPut, Key: []byte("key3"), Value: []byte("value3")},
	}

	for _, entry := range entries {
		if err := wal.Append(entry); err != nil {
			t.Fatalf("Failed to append entry: %v", err)
		}
	}

	wal.Close()

	wal2, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	replayed := make([]WALEntry, 0)
	if err := wal2.Replay(func(entry WALEntry) error {
		replayed = append(replayed, entry)
		return nil
	}); err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	if len(replayed) != len(entries) {
		t.Fatalf("Expected %d entries, got %d", len(entries), len(replayed))
	}

	for i, expected := range entries {
		if replayed[i].Type != expected.Type {
			t.Fatalf("Entry %d: expected type %d, got %d", i, expected.Type, replayed[i].Type)
		}
		if !bytes.Equal(replayed[i].Key, expected.Key) {
			t.Fatalf("Entry %d: expected key %s, got %s", i, expected.Key, replayed[i].Key)
		}
		if !bytes.Equal(replayed[i].Value, expected.Value) {
			t.Fatalf("Entry %d: expected value %s, got %s", i, expected.Value, replayed[i].Value)
		}
	}
}

func TestWAL_Truncate(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	entries := []WALEntry{
		{Type: WALEntryPut, Key: []byte("key1"), Value: []byte("value1")},
		{Type: WALEntryPut, Key: []byte("key2"), Value: []byte("value2")},
	}

	for _, entry := range entries {
		if err := wal.Append(entry); err != nil {
			t.Fatalf("Failed to append entry: %v", err)
		}
	}

	size, err := wal.Size()
	if err != nil {
		t.Fatalf("Failed to get WAL size: %v", err)
	}
	if size == 0 {
		t.Fatal("WAL file should have content")
	}

	if err := wal.Truncate(); err != nil {
		t.Fatalf("Failed to truncate WAL: %v", err)
	}

	size, err = wal.Size()
	if err != nil {
		t.Fatalf("Failed to get WAL size after truncate: %v", err)
	}
	if size != 0 {
		t.Fatalf("WAL file should be empty after truncate, got size %d", size)
	}

	count := 0
	if err := wal.Replay(func(entry WALEntry) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("Failed to replay empty WAL: %v", err)
	}
	if count != 0 {
		t.Fatalf("Expected 0 entries after truncate, got %d", count)
	}
}

func TestWAL_AppendAfterTruncate(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	wal.Append(WALEntry{Type: WALEntryPut, Key: []byte("key1"), Value: []byte("value1")})
	wal.Truncate()

	newEntries := []WALEntry{
		{Type: WALEntryPut, Key: []byte("key2"), Value: []byte("value2")},
		{Type: WALEntryPut, Key: []byte("key3"), Value: []byte("value3")},
	}

	for _, entry := range newEntries {
		if err := wal.Append(entry); err != nil {
			t.Fatalf("Failed to append entry after truncate: %v", err)
		}
	}

	replayed := make([]WALEntry, 0)
	if err := wal.Replay(func(entry WALEntry) error {
		replayed = append(replayed, entry)
		return nil
	}); err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	if len(replayed) != len(newEntries) {
		t.Fatalf("Expected %d entries, got %d", len(newEntries), len(replayed))
	}
}

func TestWAL_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	count := 0
	if err := wal.Replay(func(entry WALEntry) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("Failed to replay empty WAL: %v", err)
	}
	if count != 0 {
		t.Fatalf("Expected 0 entries, got %d", count)
	}
}

func TestWAL_LargeValues(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	entry := WALEntry{
		Type:  WALEntryPut,
		Key:   []byte("large_key"),
		Value: largeValue,
	}

	if err := wal.Append(entry); err != nil {
		t.Fatalf("Failed to append large entry: %v", err)
	}

	var replayed WALEntry
	if err := wal.Replay(func(e WALEntry) error {
		replayed = e
		return nil
	}); err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	if !bytes.Equal(replayed.Key, entry.Key) {
		t.Fatal("Keys don't match")
	}
	if !bytes.Equal(replayed.Value, entry.Value) {
		t.Fatal("Large values don't match")
	}
}

func TestWAL_ConcurrentAppends(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			entry := WALEntry{
				Type:  WALEntryPut,
				Key:   []byte{byte('a' + id), byte('0')},
				Value: []byte{byte('v'), byte('0' + id)},
			}
			if err := wal.Append(entry); err != nil {
				t.Errorf("Failed to append: %v", err)
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	count := 0
	if err := wal.Replay(func(entry WALEntry) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}
	if count != 10 {
		t.Fatalf("Expected 10 entries, got %d", count)
	}
}

func TestWAL_FileOperations(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	if wal.FilePath() != walPath {
		t.Fatalf("Expected file path %s, got %s", walPath, wal.FilePath())
	}

	if err := wal.Append(WALEntry{Type: WALEntryPut, Key: []byte("key1"), Value: []byte("value1")}); err != nil {
		t.Fatalf("Failed to append: %v", err)
	}

	size, err := wal.Size()
	if err != nil {
		t.Fatalf("Failed to get size: %v", err)
	}
	if size == 0 {
		t.Fatal("Size should be greater than 0")
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	if err := wal.Append(WALEntry{Type: WALEntryPut, Key: []byte("key2"), Value: []byte("value2")}); err == nil {
		t.Fatal("Append after close should fail")
	}

	if _, err := wal.Size(); err == nil {
		t.Fatal("Size after close should fail")
	}
}

func TestWAL_ReplayWithError(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	wal.Append(WALEntry{Type: WALEntryPut, Key: []byte("key1"), Value: []byte("value1")})
	wal.Append(WALEntry{Type: WALEntryPut, Key: []byte("key2"), Value: []byte("value2")})

	testError := errors.New("test error")
	err = wal.Replay(func(entry WALEntry) error {
		return testError
	})

	if err != testError {
		t.Fatalf("Expected test error, got %v", err)
	}
}
