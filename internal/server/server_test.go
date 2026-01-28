package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"vikdb/internal/store"
)

func TestServer_Put(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := store.NewStore(1024*1024, filepath.Join(tmpDir, "test.wal"), filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	svc := NewServer(s)
	handler := svc.Handler()

	key := "test-key"
	value := "test-value"
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))
	reqBody := PutRequest{Value: encodedValue}
	jsonBody, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPut, "/kv/"+base64.URLEncoding.EncodeToString([]byte(key)), bytes.NewReader(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}
	var resp PutResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if !resp.Success {
		t.Fatal("Response should be successful")
	}
	storedValue, exists := s.Read([]byte(key))
	if !exists {
		t.Fatal("Key should exist after PUT")
	}
	if string(storedValue) != value {
		t.Fatalf("Expected value %s, got %s", value, string(storedValue))
	}
}

func TestServer_Get(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := store.NewStore(1024*1024, filepath.Join(tmpDir, "test.wal"), filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	key := "test-key"
	value := "test-value"
	s.Put([]byte(key), []byte(value))
	svc := NewServer(s)
	handler := svc.Handler()

	req := httptest.NewRequest(http.MethodGet, "/kv/"+base64.URLEncoding.EncodeToString([]byte(key)), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}
	var resp GetResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if !resp.Success {
		t.Fatal("Response should be successful")
	}
	decodedValue, err := base64.StdEncoding.DecodeString(resp.Value)
	if err != nil {
		t.Fatalf("Failed to decode value: %v", err)
	}
	if string(decodedValue) != value {
		t.Fatalf("Expected value %s, got %s", value, string(decodedValue))
	}
}

func TestServer_GetNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := store.NewStore(1024*1024, filepath.Join(tmpDir, "test.wal"), filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	svc := NewServer(s)
	handler := svc.Handler()
	req := httptest.NewRequest(http.MethodGet, "/kv/nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("Expected status 404, got %d", w.Code)
	}
	var resp GetResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if resp.Success {
		t.Fatal("Response should not be successful")
	}
}

func TestServer_Delete(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := store.NewStore(1024*1024, filepath.Join(tmpDir, "test.wal"), filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	s.Put([]byte("test-key"), []byte("test-value"))
	svc := NewServer(s)
	handler := svc.Handler()

	req := httptest.NewRequest(http.MethodDelete, "/kv/"+base64.URLEncoding.EncodeToString([]byte("test-key")), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}
	var resp DeleteResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if !resp.Success {
		t.Fatal("Response should be successful")
	}
	_, exists := s.Read([]byte("test-key"))
	if exists {
		t.Fatal("Key should not exist after DELETE")
	}
}

func TestServer_GetRange(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := store.NewStore(1024*1024, filepath.Join(tmpDir, "test.wal"), filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	s.Put([]byte("key1"), []byte("value1"))
	s.Put([]byte("key2"), []byte("value2"))
	s.Put([]byte("key3"), []byte("value3"))
	s.Put([]byte("key4"), []byte("value4"))
	svc := NewServer(s)
	handler := svc.Handler()

	startKey := base64.URLEncoding.EncodeToString([]byte("key2"))
	endKey := base64.URLEncoding.EncodeToString([]byte("key4"))
	req := httptest.NewRequest(http.MethodGet, "/kv/range?start="+startKey+"&end="+endKey, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}
	var resp RangeResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if !resp.Success {
		t.Fatal("Response should be successful")
	}
	if len(resp.Items) != 2 {
		t.Fatalf("Expected 2 items, got %d", len(resp.Items))
	}
}

func TestServer_GetRangePOST(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := store.NewStore(1024*1024, filepath.Join(tmpDir, "test.wal"), filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	s.Put([]byte("key1"), []byte("value1"))
	s.Put([]byte("key2"), []byte("value2"))
	s.Put([]byte("key3"), []byte("value3"))
	svc := NewServer(s)
	handler := svc.Handler()

	reqBody := RangeRequest{
		StartKey: base64.StdEncoding.EncodeToString([]byte("key1")),
		EndKey:   base64.StdEncoding.EncodeToString([]byte("key3")),
	}
	jsonBody, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/kv/range", bytes.NewReader(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}
	var resp RangeResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if !resp.Success {
		t.Fatal("Response should be successful")
	}
	if len(resp.Items) != 2 {
		t.Fatalf("Expected 2 items, got %d", len(resp.Items))
	}
}

func TestServer_BatchPut(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := store.NewStore(1024*1024, filepath.Join(tmpDir, "test.wal"), filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	svc := NewServer(s)
	handler := svc.Handler()

	keys := []string{
		base64.StdEncoding.EncodeToString([]byte("key1")),
		base64.StdEncoding.EncodeToString([]byte("key2")),
	}
	values := []string{
		base64.StdEncoding.EncodeToString([]byte("value1")),
		base64.StdEncoding.EncodeToString([]byte("value2")),
	}
	reqBody := BatchPutRequest{Keys: keys, Values: values}
	jsonBody, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/kv/batch", bytes.NewReader(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}
	var resp BatchPutResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if !resp.Success {
		t.Fatal("Response should be successful")
	}
	if resp.Count != 2 {
		t.Fatalf("Expected count 2, got %d", resp.Count)
	}
	value1, exists := s.Read([]byte("key1"))
	if !exists || string(value1) != "value1" {
		t.Fatal("key1 should exist with value1")
	}
	value2, exists := s.Read([]byte("key2"))
	if !exists || string(value2) != "value2" {
		t.Fatal("key2 should exist with value2")
	}
}

func TestServer_BatchPutInvalidLength(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := store.NewStore(1024*1024, filepath.Join(tmpDir, "test.wal"), filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	svc := NewServer(s)
	handler := svc.Handler()

	reqBody := BatchPutRequest{
		Keys:   []string{base64.StdEncoding.EncodeToString([]byte("key1"))},
		Values: []string{base64.StdEncoding.EncodeToString([]byte("value1")), base64.StdEncoding.EncodeToString([]byte("value2"))},
	}
	jsonBody, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/kv/batch", bytes.NewReader(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("Expected status 400, got %d", w.Code)
	}
}

func TestServer_Health(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := store.NewStore(1024*1024, filepath.Join(tmpDir, "test.wal"), filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	svc := NewServer(s)
	handler := svc.Handler()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}
	var resp HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if resp.Status != "healthy" {
		t.Fatalf("Expected status 'healthy', got '%s'", resp.Status)
	}
}

func TestServer_InvalidMethod(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := store.NewStore(1024*1024, filepath.Join(tmpDir, "test.wal"), filepath.Join(tmpDir, "sstables"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	svc := NewServer(s)
	handler := svc.Handler()
	req := httptest.NewRequest(http.MethodPatch, "/kv/test", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("Expected status 405, got %d", w.Code)
	}
}
