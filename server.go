package vikdb

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Server represents the HTTP API server for the KV store
type Server struct {
	store *Store
}

// NewServer creates a new HTTP server instance
func NewServer(store *Store) *Server {
	return &Server{
		store: store,
	}
}

// PutRequest represents a PUT request body
type PutRequest struct {
	Value string `json:"value"` // Base64 encoded value
}

// PutResponse represents a PUT response
type PutResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// GetResponse represents a GET response
type GetResponse struct {
	Success bool   `json:"success"`
	Value   string `json:"value,omitempty"` // Base64 encoded value
	Error   string `json:"error,omitempty"`
}

// RangeRequest represents a range query request (for POST)
type RangeRequest struct {
	StartKey string `json:"start_key"` // Base64 encoded
	EndKey   string `json:"end_key"`   // Base64 encoded, optional
}

// RangeResponse represents a range query response
type RangeResponse struct {
	Success bool           `json:"success"`
	Items   []KeyValueJSON `json:"items"`
	Error   string         `json:"error,omitempty"`
}

// KeyValueJSON represents a key-value pair in JSON format
type KeyValueJSON struct {
	Key   string `json:"key"`   // Base64 encoded
	Value string `json:"value"`  // Base64 encoded
}

// BatchPutRequest represents a batch put request
type BatchPutRequest struct {
	Keys   []string `json:"keys"`   // Base64 encoded keys
	Values []string `json:"values"`  // Base64 encoded values
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
	Status        string `json:"status"`
	MemTableSize  int64  `json:"memtable_size"`
	WALSize       int64  `json:"wal_size"`
	SSTableCount  int    `json:"sstable_count"`
}

// Start starts the HTTP server on the specified address
func (s *Server) Start(addr string) error {
	http.HandleFunc("/kv/", s.handleKV)
	http.HandleFunc("/kv/range", s.handleRange)
	http.HandleFunc("/kv/batch", s.handleBatchPut)
	http.HandleFunc("/health", s.handleHealth)
	return http.ListenAndServe(addr, nil)
}

// handleKV handles PUT, GET, and DELETE operations on /kv/:key
func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	// Extract key from URL path
	keyPath := strings.TrimPrefix(r.URL.Path, "/kv/")
	if keyPath == "" {
		s.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	// Decode key from URL (assuming it's base64 URL encoded)
	key, err := base64.URLEncoding.DecodeString(keyPath)
	if err != nil {
		// Try treating it as plain text if base64 decoding fails
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

// handlePut handles PUT /kv/:key
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key []byte) {
	var req PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	// Decode value from base64
	value, err := base64.StdEncoding.DecodeString(req.Value)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid base64 value: %v", err))
		return
	}

	// Put into Store
	if err := s.store.Put(key, value); err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to put: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, PutResponse{
		Success: true,
		Message: "key stored successfully",
	})
}

// handleGet handles GET /kv/:key
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key []byte) {
	value, exists := s.store.Get(key)
	if !exists {
		s.writeJSON(w, http.StatusNotFound, GetResponse{
			Success: false,
			Error:   "key not found",
		})
		return
	}

	// Encode value to base64
	encodedValue := base64.StdEncoding.EncodeToString(value)
	s.writeJSON(w, http.StatusOK, GetResponse{
		Success: true,
		Value:   encodedValue,
	})
}

// handleDelete handles DELETE /kv/:key
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key []byte) {
	if err := s.store.Delete(key); err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to delete: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, DeleteResponse{
		Success: true,
		Message: "key deleted successfully",
	})
}

// handleRange handles GET /kv/range?start=...&end=... or POST /kv/range
func (s *Server) handleRange(w http.ResponseWriter, r *http.Request) {
	var startKey, endKey []byte
	var err error

	if r.Method == http.MethodPost {
		// Handle POST with JSON body
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
		// Handle GET with query parameters
		startParam := r.URL.Query().Get("start")
		endParam := r.URL.Query().Get("end")

		if startParam == "" {
			s.writeError(w, http.StatusBadRequest, "start parameter is required")
			return
		}

		startKey, err = base64.URLEncoding.DecodeString(startParam)
		if err != nil {
			// Try as plain text
			startKey = []byte(startParam)
		}

		if endParam != "" {
			endKey, err = base64.URLEncoding.DecodeString(endParam)
			if err != nil {
				// Try as plain text
				endKey = []byte(endParam)
			}
		}
	} else {
		s.writeError(w, http.StatusMethodNotAllowed, fmt.Sprintf("method %s not allowed", r.Method))
		return
	}

	// Query range
	items := s.store.GetRange(startKey, endKey)

	// Convert to JSON format
	jsonItems := make([]KeyValueJSON, len(items))
	for i, item := range items {
		jsonItems[i] = KeyValueJSON{
			Key:   base64.StdEncoding.EncodeToString(item.Key),
			Value: base64.StdEncoding.EncodeToString(item.Value),
		}
	}

	s.writeJSON(w, http.StatusOK, RangeResponse{
		Success: true,
		Items:   jsonItems,
	})
}

// handleBatchPut handles POST /kv/batch
func (s *Server) handleBatchPut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, fmt.Sprintf("method %s not allowed", r.Method))
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

	// Decode and put all key-value pairs
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

	// Put all pairs
	if err := s.store.BatchPut(keys, values); err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to batch put: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, BatchPutResponse{
		Success: true,
		Count:   len(keys),
	})
}

// handleHealth handles GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	walSize, _ := s.store.GetWALSize()
	
	s.writeJSON(w, http.StatusOK, HealthResponse{
		Status:       "healthy",
		MemTableSize: s.store.GetMemTableSize(),
		WALSize:       walSize,
		SSTableCount: s.store.GetSSTableCount(),
	})
}

// writeJSON writes a JSON response
func (s *Server) writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		// Fallback error response
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, `{"success":false,"error":"failed to encode response"}`)
	}
}

// writeError writes an error response
func (s *Server) writeError(w http.ResponseWriter, statusCode int, message string) {
	s.writeJSON(w, statusCode, ErrorResponse{
		Success: false,
		Error:   message,
	})
}
