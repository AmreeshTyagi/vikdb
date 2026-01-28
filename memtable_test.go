package vikdb

import (
	"bytes"
	"testing"
)

func TestMemTable_PutAndGet(t *testing.T) {
	mt := NewMemTable(1024 * 1024) // 1MB max size

	// Test Put
	err := mt.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get
	value, exists := mt.Get([]byte("key1"))
	if !exists {
		t.Fatal("Key should exist")
	}
	if !bytes.Equal(value, []byte("value1")) {
		t.Fatalf("Expected value1, got %s", value)
	}

	// Test non-existent key
	_, exists = mt.Get([]byte("nonexistent"))
	if exists {
		t.Fatal("Key should not exist")
	}
}

func TestMemTable_Update(t *testing.T) {
	mt := NewMemTable(1024 * 1024)

	// Put initial value
	mt.Put([]byte("key1"), []byte("value1"))

	// Update value
	mt.Put([]byte("key1"), []byte("value2"))

	// Verify update
	value, exists := mt.Get([]byte("key1"))
	if !exists {
		t.Fatal("Key should exist")
	}
	if !bytes.Equal(value, []byte("value2")) {
		t.Fatalf("Expected value2, got %s", value)
	}
}

func TestMemTable_Delete(t *testing.T) {
	mt := NewMemTable(1024 * 1024)

	// Put a key
	mt.Put([]byte("key1"), []byte("value1"))

	// Verify it exists
	_, exists := mt.Get([]byte("key1"))
	if !exists {
		t.Fatal("Key should exist before delete")
	}

	// Delete it
	err := mt.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's gone
	_, exists = mt.Get([]byte("key1"))
	if exists {
		t.Fatal("Key should not exist after delete")
	}

	// Delete non-existent key (should not error)
	err = mt.Delete([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Delete of non-existent key should not error: %v", err)
	}
}

func TestMemTable_GetRange(t *testing.T) {
	mt := NewMemTable(1024 * 1024)

	// Insert keys in non-sorted order
	keys := []string{"key3", "key1", "key5", "key2", "key4"}
	for i, key := range keys {
		mt.Put([]byte(key), []byte{byte('a' + i)})
	}

	// Test range query: key2 to key4 (inclusive start, exclusive end)
	results := mt.GetRange([]byte("key2"), []byte("key4"))
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	// Verify results are sorted
	if !bytes.Equal(results[0].Key, []byte("key2")) {
		t.Fatalf("Expected key2, got %s", results[0].Key)
	}
	if !bytes.Equal(results[1].Key, []byte("key3")) {
		t.Fatalf("Expected key3, got %s", results[1].Key)
	}

	// Test range with nil endKey (should return all keys >= startKey)
	results = mt.GetRange([]byte("key3"), nil)
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}
	expectedKeys := []string{"key3", "key4", "key5"}
	for i, expected := range expectedKeys {
		if !bytes.Equal(results[i].Key, []byte(expected)) {
			t.Fatalf("Expected %s, got %s", expected, results[i].Key)
		}
	}

	// Test empty range (use a key that's greater than all existing keys in byte comparison)
	results = mt.GetRange([]byte("keyz"), nil)
	if len(results) != 0 {
		t.Fatalf("Expected 0 results, got %d", len(results))
	}
}

func TestMemTable_Size(t *testing.T) {
	mt := NewMemTable(1024 * 1024)

	// Initially empty
	if mt.Size() != 0 {
		t.Fatalf("Expected size 0, got %d", mt.Size())
	}

	// Add a key-value pair
	mt.Put([]byte("key1"), []byte("value1"))
	expectedSize := int64(len("key1") + len("value1"))
	if mt.Size() != expectedSize {
		t.Fatalf("Expected size %d, got %d", expectedSize, mt.Size())
	}

	// Update value (should update size correctly)
	mt.Put([]byte("key1"), []byte("value123"))
	expectedSize = int64(len("key1") + len("value123"))
	if mt.Size() != expectedSize {
		t.Fatalf("Expected size %d after update, got %d", expectedSize, mt.Size())
	}

	// Add another key
	mt.Put([]byte("key2"), []byte("value2"))
	expectedSize = int64(len("key1") + len("value123") + len("key2") + len("value2"))
	if mt.Size() != expectedSize {
		t.Fatalf("Expected size %d, got %d", expectedSize, mt.Size())
	}

	// Delete a key
	mt.Delete([]byte("key1"))
	expectedSize = int64(len("key2") + len("value2"))
	if mt.Size() != expectedSize {
		t.Fatalf("Expected size %d after delete, got %d", expectedSize, mt.Size())
	}
}

func TestMemTable_ShouldFlush(t *testing.T) {
	maxSize := int64(100)
	mt := NewMemTable(maxSize)

	// Should not flush initially
	if mt.ShouldFlush() {
		t.Fatal("Should not flush when empty")
	}

	// Add data up to max size
	mt.Put([]byte("key1"), make([]byte, 50))
	if mt.ShouldFlush() {
		t.Fatal("Should not flush when below max size")
	}

	// Add more data to exceed max size
	mt.Put([]byte("key2"), make([]byte, 60))
	if !mt.ShouldFlush() {
		t.Fatal("Should flush when exceeding max size")
	}
}

func TestMemTable_GetAllEntries(t *testing.T) {
	mt := NewMemTable(1024 * 1024)

	// Insert keys in non-sorted order
	keys := []string{"key3", "key1", "key5", "key2", "key4"}
	for i, key := range keys {
		mt.Put([]byte(key), []byte{byte('a' + i)})
	}

	// Get all entries
	entries := mt.GetAllEntries()

	// Verify all entries are present
	if len(entries) != 5 {
		t.Fatalf("Expected 5 entries, got %d", len(entries))
	}

	// Verify entries are sorted
	expectedOrder := []string{"key1", "key2", "key3", "key4", "key5"}
	for i, expected := range expectedOrder {
		if !bytes.Equal(entries[i].Key, []byte(expected)) {
			t.Fatalf("Expected %s at index %d, got %s", expected, i, entries[i].Key)
		}
	}

	// Verify entries are copies (modifying shouldn't affect MemTable)
	// key1 was inserted with i=1, so its value is 'a'+1 = 'b'
	originalKey := make([]byte, len(entries[0].Key))
	copy(originalKey, entries[0].Key)
	entries[0].Key[0] = 'X'
	value, exists := mt.Get(originalKey)
	if !exists {
		t.Fatal("Key should still exist after modifying returned entries")
	}
	// key1 has value 'b' (was inserted with index 1: 'a'+1)
	if value[0] != 'b' {
		t.Fatalf("Modifying returned entries should not affect MemTable. Expected value 'b', got %c", value[0])
	}
}

func TestMemTable_Clear(t *testing.T) {
	mt := NewMemTable(1024 * 1024)

	// Add some data
	mt.Put([]byte("key1"), []byte("value1"))
	mt.Put([]byte("key2"), []byte("value2"))

	if mt.Size() == 0 {
		t.Fatal("Size should not be 0 before clear")
	}
	if mt.IsEmpty() {
		t.Fatal("Should not be empty before clear")
	}

	// Clear
	mt.Clear()

	if mt.Size() != 0 {
		t.Fatalf("Size should be 0 after clear, got %d", mt.Size())
	}
	if !mt.IsEmpty() {
		t.Fatal("Should be empty after clear")
	}

	// Verify keys are gone
	_, exists := mt.Get([]byte("key1"))
	if exists {
		t.Fatal("Key should not exist after clear")
	}
}

func TestMemTable_ConcurrentAccess(t *testing.T) {
	mt := NewMemTable(1024 * 1024)

	// Test concurrent writes
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := []byte{byte('a' + id), byte('0' + j%10)}
				value := []byte{byte('v'), byte('0' + j)}
				mt.Put(key, value)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify we can still read
	value, exists := mt.Get([]byte{'a', '0'})
	if !exists {
		t.Fatal("Key should exist after concurrent writes")
	}
	if len(value) != 2 {
		t.Fatalf("Expected value length 2, got %d", len(value))
	}
}
