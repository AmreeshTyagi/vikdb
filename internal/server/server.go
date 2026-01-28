package server

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"vikdb/internal/kv"
	"vikdb/internal/store"
	"vikdb/internal/replicatedstore"
)

// storeInterface is the interface satisfied by both Store and ReplicatedStore
type storeInterface interface {
	Put(key, value []byte) error
	Read(key []byte) ([]byte, bool)
	Delete(key []byte) error
	ReadKeyRange(startKey, endKey []byte) []kv.KeyValue
	BatchPut(keys, values [][]byte) error
}

// Server represents the HTTP API server for the KV store
type Server struct {
	store           *store.Store
	replicatedStore *replicatedstore.ReplicatedStore
	useReplication  bool
}

// NewServer creates a new HTTP server instance
func NewServer(s *store.Store) *Server {
	return &Server{store: s, useReplication: false}
}

// NewReplicatedServer creates a new HTTP server instance with replication
func NewReplicatedServer(rs *replicatedstore.ReplicatedStore) *Server {
	return &Server{replicatedStore: rs, useReplication: true}
}

func (s *Server) getStore() storeInterface {
	if s.useReplication {
		return s.replicatedStore
	}
	return s.store
}

func (s *Server) isLeader() bool {
	if !s.useReplication {
		return true
	}
	return s.replicatedStore.IsLeader()
}

func (s *Server) getLeaderAddress() string {
	if !s.useReplication {
		return ""
	}
	return s.replicatedStore.GetLeaderAddress()
}

func redirectURL(addr, path string) string {
	if addr != "" && addr[0] == ':' {
		addr = "localhost" + addr
	}
	return "http://" + addr + path
}

// PutRequest represents a PUT request body
type PutRequest struct {
	Value string `json:"value"`
}

// PutResponse represents a PUT response
type PutResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// GetResponse represents a GET response
type GetResponse struct {
	Success bool   `json:"success"`
	Value   string `json:"value,omitempty"`
	Error   string `json:"error,omitempty"`
}

// RangeRequest represents a range query request (for POST)
type RangeRequest struct {
	StartKey string `json:"start_key"`
	EndKey   string `json:"end_key"`
}

// RangeResponse represents a range query response
type RangeResponse struct {
	Success bool           `json:"success"`
	Items   []KeyValueJSON `json:"items"`
	Error   string         `json:"error,omitempty"`
}

// KeyValueJSON represents a key-value pair in JSON format
type KeyValueJSON struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// BatchPutRequest represents a batch put request
type BatchPutRequest struct {
	Keys   []string `json:"keys"`
	Values []string `json:"values"`
}

// BatchPutResponse represents a batch put response
type BatchPutResponse struct {
	Success bool   `json:"success"`
	Count   int    `json:"count,omitempty"`
	Error   string `json:"error,omitempty"`
}

// DeleteResponse represents a DELETE response
type DeleteResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}

// HealthResponse represents a health check response
type HealthResponse struct {
	Status       string `json:"status"`
	MemTableSize int64  `json:"memtable_size"`
	WALSize      int64  `json:"wal_size"`
	SSTableCount int    `json:"sstable_count"`
	Role         string `json:"role,omitempty"`
}

// Handler returns an http.Handler for testing without starting a real server
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/kv/range", s.handleRange)
	mux.HandleFunc("/kv/batch", s.handleBatchPut)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/cluster/status", s.handleClusterStatus)
	return mux
}

// Start starts the HTTP server on the specified address
func (s *Server) Start(addr string) error {
	http.HandleFunc("/kv/", s.handleKV)
	http.HandleFunc("/kv/range", s.handleRange)
	http.HandleFunc("/kv/batch", s.handleBatchPut)
	http.HandleFunc("/health", s.handleHealth)
	http.HandleFunc("/cluster/status", s.handleClusterStatus)
	return http.ListenAndServe(addr, nil)
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	keyPath := strings.TrimPrefix(r.URL.Path, "/kv/")
	if keyPath == "" {
		s.writeError(w, http.StatusBadRequest, "key is required")
		return
	}
	key, err := base64.URLEncoding.DecodeString(keyPath)
	if err != nil {
		key = []byte(keyPath)
	}
	switch r.Method {
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, fmt.Sprintf("method %s not allowed", r.Method))
	}
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key []byte) {
	if !s.isLeader() {
		leaderAddr := s.getLeaderAddress()
		if leaderAddr != "" {
			w.Header().Set("Location", redirectURL(leaderAddr, r.URL.Path))
			s.writeError(w, http.StatusTemporaryRedirect, "not the leader, redirecting to leader")
			return
		}
		s.writeError(w, http.StatusServiceUnavailable, "not the leader, leader address unknown")
		return
	}
	var req PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}
	value, err := base64.StdEncoding.DecodeString(req.Value)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid base64 value: %v", err))
		return
	}
	st := s.getStore()
	if err := st.Put(key, value); err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to put: %v", err))
		return
	}
	s.writeJSON(w, http.StatusOK, PutResponse{Success: true, Message: "key stored successfully"})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key []byte) {
	st := s.getStore()
	value, exists := st.Read(key)
	if !exists {
		s.writeJSON(w, http.StatusNotFound, GetResponse{Success: false, Error: "key not found"})
		return
	}
	s.writeJSON(w, http.StatusOK, GetResponse{
		Success: true,
		Value:   base64.StdEncoding.EncodeToString(value),
	})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key []byte) {
	if !s.isLeader() {
		leaderAddr := s.getLeaderAddress()
		if leaderAddr != "" {
			w.Header().Set("Location", redirectURL(leaderAddr, r.URL.Path))
			s.writeError(w, http.StatusTemporaryRedirect, "not the leader, redirecting to leader")
			return
		}
		s.writeError(w, http.StatusServiceUnavailable, "not the leader, leader address unknown")
		return
	}
	st := s.getStore()
	if err := st.Delete(key); err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to delete: %v", err))
		return
	}
	s.writeJSON(w, http.StatusOK, DeleteResponse{Success: true, Message: "key deleted successfully"})
}

func (s *Server) handleRange(w http.ResponseWriter, r *http.Request) {
	var startKey, endKey []byte
	var err error

	if r.Method == http.MethodPost {
		var req RangeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
			return
		}
		if req.StartKey == "" {
			s.writeError(w, http.StatusBadRequest, "start_key is required")
			return
		}
		startKey, err = base64.StdEncoding.DecodeString(req.StartKey)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid base64 start_key: %v", err))
			return
		}
		if req.EndKey != "" {
			endKey, err = base64.StdEncoding.DecodeString(req.EndKey)
			if err != nil {
				s.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid base64 end_key: %v", err))
				return
			}
		}
	} else if r.Method == http.MethodGet {
		startParam := r.URL.Query().Get("start")
		endParam := r.URL.Query().Get("end")
		if startParam == "" {
			s.writeError(w, http.StatusBadRequest, "start parameter is required")
			return
		}
		startKey, err = base64.URLEncoding.DecodeString(startParam)
		if err != nil {
			startKey = []byte(startParam)
		}
		if endParam != "" {
			endKey, _ = base64.URLEncoding.DecodeString(endParam)
			if err != nil {
				endKey = []byte(endParam)
			}
		}
	} else {
		s.writeError(w, http.StatusMethodNotAllowed, fmt.Sprintf("method %s not allowed", r.Method))
		return
	}

	st := s.getStore()
	items := st.ReadKeyRange(startKey, endKey)
	jsonItems := make([]KeyValueJSON, len(items))
	for i, item := range items {
		jsonItems[i] = KeyValueJSON{
			Key:   base64.StdEncoding.EncodeToString(item.Key),
			Value: base64.StdEncoding.EncodeToString(item.Value),
		}
	}
	s.writeJSON(w, http.StatusOK, RangeResponse{Success: true, Items: jsonItems})
}

func (s *Server) handleBatchPut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, fmt.Sprintf("method %s not allowed", r.Method))
		return
	}
	if !s.isLeader() {
		leaderAddr := s.getLeaderAddress()
		if leaderAddr != "" {
			w.Header().Set("Location", redirectURL(leaderAddr, "/kv/batch"))
			s.writeError(w, http.StatusTemporaryRedirect, "not the leader, redirecting to leader")
			return
		}
		s.writeError(w, http.StatusServiceUnavailable, "not the leader, leader address unknown")
		return
	}
	var req BatchPutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}
	if len(req.Keys) != len(req.Values) {
		s.writeError(w, http.StatusBadRequest, "keys and values arrays must have the same length")
		return
	}
	keys := make([][]byte, len(req.Keys))
	values := make([][]byte, len(req.Values))
	for i, keyStr := range req.Keys {
		key, err := base64.StdEncoding.DecodeString(keyStr)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid base64 key at index %d: %v", i, err))
			return
		}
		keys[i] = key
	}
	for i, valueStr := range req.Values {
		value, err := base64.StdEncoding.DecodeString(valueStr)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid base64 value at index %d: %v", i, err))
			return
		}
		values[i] = value
	}
	st := s.getStore()
	if err := st.BatchPut(keys, values); err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to batch put: %v", err))
		return
	}
	s.writeJSON(w, http.StatusOK, BatchPutResponse{Success: true, Count: len(keys)})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	var memTableSize int64
	var walSize int64
	var sstableCount int
	var role string
	if s.useReplication {
		memTableSize = s.replicatedStore.GetMemTableSize()
		walSize, _ = s.replicatedStore.GetWALSize()
		sstableCount = s.replicatedStore.GetSSTableCount()
		role = string(s.replicatedStore.GetRole())
	} else {
		memTableSize = s.store.GetMemTableSize()
		walSize, _ = s.store.GetWALSize()
		sstableCount = s.store.GetSSTableCount()
		role = "single-node"
	}
	s.writeJSON(w, http.StatusOK, HealthResponse{
		Status:       "healthy",
		MemTableSize: memTableSize,
		WALSize:      walSize,
		SSTableCount: sstableCount,
		Role:         role,
	})
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if !s.useReplication {
		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"replication": false,
			"mode":        "single-node",
		})
		return
	}
	role := s.replicatedStore.GetRole()
	isLeader := s.replicatedStore.IsLeader()
	term := s.replicatedStore.GetCurrentTerm()
	nodeID := s.replicatedStore.GetNodeID()
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"replication": true,
		"role":        string(role),
		"is_leader":   isLeader,
		"term":        term,
		"node_id":     nodeID,
	})
}

func (s *Server) writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, `{"success":false,"error":"failed to encode response"}`)
	}
}

func (s *Server) writeError(w http.ResponseWriter, statusCode int, message string) {
	s.writeJSON(w, statusCode, ErrorResponse{Success: false, Error: message})
}
