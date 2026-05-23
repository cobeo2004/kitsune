package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cobeo2004/kitsune/internal/events"
	"github.com/cobeo2004/kitsune/internal/metadata"
)

const (
	defaultMaxDocumentBytes int64 = 1024 * 1024
	maxSearchWindow               = 10_000
	metadataWatchRetryDelay       = 50 * time.Millisecond
)

var indexNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

// ServerConfig configures the KSCoordinator REST server.
type ServerConfig struct {
	MaxDocumentBytes int64
	Routes           StaticRoutes
	StaticConfig     StaticConfig
	MetadataManager  metadata.KSMetadataManager
	EventBus         events.Bus
}

// Server is the REST-first KSCoordinator HTTP surface.
type Server struct {
	mux              *http.ServeMux
	mu               sync.RWMutex
	maxDocumentBytes int64
	routes           StaticRoutes
	routeClients     StaticRoutes
	staticConfigured bool
	metadataManager  metadata.KSMetadataManager
	eventBus         events.Bus
	indexes          map[string]IndexInfo
}

// NewServer creates a KSCoordinator REST server.
func NewServer(cfg ServerConfig) *Server {
	if cfg.MaxDocumentBytes <= 0 {
		cfg.MaxDocumentBytes = defaultMaxDocumentBytes
	}
	if err := ValidateStaticConfig(cfg.StaticConfig); err != nil {
		panic(fmt.Sprintf("invalid static config: %v", err))
	}
	if err := validateRoutesAgainstStaticConfig(cfg.Routes, cfg.StaticConfig); err != nil {
		panic(fmt.Sprintf("invalid static routes: %v", err))
	}

	s := &Server{
		mux:              http.NewServeMux(),
		maxDocumentBytes: cfg.MaxDocumentBytes,
		routes:           cfg.Routes,
		routeClients:     cfg.Routes,
		staticConfigured: len(cfg.StaticConfig.Indexes) > 0,
		metadataManager:  cfg.MetadataManager,
		eventBus:         cfg.EventBus,
		indexes:          make(map[string]IndexInfo),
	}
	for _, idx := range cfg.StaticConfig.Indexes {
		s.indexes[idx.Name] = IndexInfo{
			Name:              idx.Name,
			ShardCount:        idx.ShardCount,
			ReplicationFactor: idx.ReplicationFactor,
			MappingVersion:    idx.MappingVersion,
			Mapping:           cloneMapping(idx.Mapping),
		}
	}
	if cfg.MetadataManager != nil {
		if _, err := s.loadMetadataSnapshot(context.Background()); err != nil {
			panic(fmt.Sprintf("load metadata snapshot: %v", err))
		}
	}
	s.mux.HandleFunc("/v1/indexes", s.handleIndexCollection)
	s.mux.HandleFunc("/v1/indexes/", s.handleIndexes)
	s.mux.HandleFunc("/v1/cluster/status", s.handleClusterStatus)
	return s
}

// StartMetadataWatch starts a metadata snapshot and watch loop for coordinator routing.
func (s *Server) StartMetadataWatch(ctx context.Context) error {
	if s.metadataManager == nil {
		return errors.New("metadata manager is required")
	}
	revision, err := s.loadMetadataSnapshot(ctx)
	if err != nil {
		return err
	}
	watch, err := s.metadataManager.Watch(ctx, revision)
	if err != nil {
		return fmt.Errorf("watch metadata: %w", err)
	}

	go s.runMetadataWatch(ctx, watch)
	return nil
}

func (s *Server) runMetadataWatch(ctx context.Context, watch <-chan metadata.WatchEvent) {
	for {
		if !s.consumeMetadataWatch(ctx, watch) {
			return
		}

		revision, err := s.loadMetadataSnapshot(ctx)
		if err != nil {
			if !sleepMetadataWatchRetry(ctx) {
				return
			}
			continue
		}
		watch, err = s.metadataManager.Watch(ctx, revision)
		if err != nil {
			if !sleepMetadataWatchRetry(ctx) {
				return
			}
			continue
		}
	}
}

func (s *Server) consumeMetadataWatch(ctx context.Context, watch <-chan metadata.WatchEvent) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case event, ok := <-watch:
			if !ok {
				return true
			}
			if event.Err != nil || event.ReloadRequired {
				return true
			}
			s.applyMetadataEvent(event)
		}
	}
}

func (s *Server) loadMetadataSnapshot(ctx context.Context) (int64, error) {
	snapshot, err := s.metadataManager.LoadSnapshot(ctx)
	if err != nil {
		return 0, fmt.Errorf("load metadata snapshot: %w", err)
	}
	s.applyMetadataSnapshot(snapshot)
	return snapshot.Revision, nil
}

func (s *Server) applyMetadataSnapshot(snapshot metadata.Snapshot) {
	indexes := make(map[string]IndexInfo, len(snapshot.Indexes))
	for _, index := range snapshot.Indexes {
		indexes[index.Name] = IndexInfo{
			Name:              index.Name,
			ShardCount:        index.ShardCount,
			ReplicationFactor: index.ReplicationFactor,
			MappingVersion:    index.MappingVersion,
			Mapping:           cloneMapping(index.Mapping),
		}
	}

	routes := routesFromMetadata(snapshot.ShardReplicas, s.routeClients)

	s.mu.Lock()
	s.indexes = indexes
	s.routes = routes
	s.staticConfigured = true
	s.mu.Unlock()
}

func (s *Server) applyMetadataEvent(event metadata.WatchEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch event.Kind {
	case metadata.EventKindIndex:
		if event.Index == nil {
			return
		}
		if event.Deleted {
			delete(s.indexes, event.Index.Name)
			delete(s.routes, event.Index.Name)
			return
		}
		s.indexes[event.Index.Name] = IndexInfo{
			Name:              event.Index.Name,
			ShardCount:        event.Index.ShardCount,
			ReplicationFactor: event.Index.ReplicationFactor,
			MappingVersion:    event.Index.MappingVersion,
			Mapping:           cloneMapping(event.Index.Mapping),
		}
	case metadata.EventKindShardReplica:
		if event.ShardReplica == nil {
			return
		}
		if event.Deleted {
			s.routes = removeRoute(s.routes, *event.ShardReplica)
			return
		}
		if route, ok := routeForReplica(s.routeClients, *event.ShardReplica); ok {
			s.routes = upsertRoute(s.routes, event.ShardReplica.IndexName, route)
		}
	}
}

// ServeHTTP serves KSCoordinator REST requests.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.Contains(r.URL.Path, "//") {
		http.NotFound(w, r)
		return
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleIndexCollection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}

	var req CreateIndexRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid index payload", http.StatusBadRequest)
		return
	}
	info, err := validateCreateIndex(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	existing, exists := s.indexes[info.Name]
	if exists && !sameIndexDefinition(existing, info) {
		s.mu.Unlock()
		http.Error(w, "index definition is immutable", http.StatusConflict)
		return
	}
	if exists {
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, existing)
		return
	}
	if s.staticConfigured {
		s.mu.Unlock()
		http.Error(w, fmt.Sprintf("index %q is not configured", info.Name), http.StatusNotFound)
		return
	}
	s.indexes[info.Name] = info
	s.mu.Unlock()

	writeJSON(w, http.StatusCreated, info)
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}

	s.mu.RLock()
	indexCount := len(s.indexes)
	routeIndexes := len(s.routes)
	s.mu.RUnlock()

	writeJSON(w, http.StatusOK, ClusterStatus{
		State:        "ready",
		IndexCount:   indexCount,
		RouteIndexes: routeIndexes,
	})
}

func (s *Server) handleIndexes(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/search") {
		s.handleSearch(w, r)
		return
	}

	if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/documents/") {
		index, documentID, ok := documentRefFromPath(r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		s.handleUpsertDocument(w, r, index, documentID)
		return
	}

	http.NotFound(w, r)
}

func (s *Server) handleUpsertDocument(w http.ResponseWriter, r *http.Request, index, documentID string) {
	info, ok := s.index(index)
	if !ok {
		http.Error(w, fmt.Sprintf("index %q not found", index), http.StatusNotFound)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, s.maxDocumentBytes)

	var payload map[string]any
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "document payload exceeds configured limit", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "invalid document payload", http.StatusBadRequest)
		return
	}
	if payload == nil {
		http.Error(w, "invalid document payload", http.StatusBadRequest)
		return
	}
	if s.eventBus != nil {
		fields := documentFields(payload)
		evt := newDocumentEvent(info, documentID, fields)
		if err := events.Validate(evt); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.eventBus.Publish(r.Context(), evt); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				http.Error(w, err.Error(), http.StatusRequestTimeout)
				return
			}
			http.Error(w, "publish document event: "+err.Error(), http.StatusBadGateway)
			return
		}
	}

	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) hasIndex(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.indexes[name]
	return ok
}

func (s *Server) index(name string) (IndexInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.indexes[name]
	return info, ok
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	index, ok := indexFromPath(r.URL.Path, "/search")
	if !ok {
		http.NotFound(w, r)
		return
	}

	_, routes, ok := s.indexRouteSnapshot(index)
	if !ok {
		http.Error(w, fmt.Sprintf("index %q not found", index), http.StatusNotFound)
		return
	}

	query := r.URL.Query().Get("q")
	limit, err := intQueryParam(r, "limit", 10)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	offset, err := intQueryParam(r, "offset", 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	shardLimit := limit + offset
	if shardLimit < limit || shardLimit > maxSearchWindow {
		http.Error(w, fmt.Sprintf("search window must be <= %d", maxSearchWindow), http.StatusBadRequest)
		return
	}

	if len(routes) == 0 {
		http.Error(w, fmt.Sprintf("index %q has no healthy replica for every shard", index), http.StatusServiceUnavailable)
		return
	}

	results := make([]ShardSearchResult, 0, len(routes))
	for _, route := range routes {
		result, err := route.Client.Search(r.Context(), query, shardLimit, 0)
		if err != nil {
			http.Error(w, "search shard route: "+err.Error(), http.StatusBadGateway)
			return
		}
		results = append(results, result)
	}

	writeJSON(w, http.StatusOK, MergeResults(results, limit, offset))
}

func documentRefFromPath(path string) (string, string, bool) {
	trimmed, ok := indexPathRemainder(path)
	if !ok {
		return "", "", false
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) != 3 || parts[1] != "documents" || parts[0] == "" || parts[2] == "" {
		return "", "", false
	}
	return parts[0], parts[2], true
}

func documentFields(payload map[string]any) map[string]any {
	fields, ok := payload["fields"].(map[string]any)
	if ok {
		return fields
	}
	return payload
}

func newDocumentEvent(index IndexInfo, documentID string, fields map[string]any) events.DocumentEvent {
	shardID := shardForDocument(documentID, index.ShardCount)
	now := time.Now().UTC()
	return events.DocumentEvent{
		ID:              fmt.Sprintf("%s/%d/%s/%d", index.Name, shardID, documentID, now.UnixNano()),
		Operation:       events.OperationUpsert,
		IndexName:       index.Name,
		ShardID:         shardID,
		DocumentID:      documentID,
		DocumentVersion: now.UnixNano(),
		Fields:          cloneMapping(fields),
		MappingVersion:  index.MappingVersion,
		Sequence:        now.UnixNano(),
		Timestamp:       now,
	}
}

func shardForDocument(documentID string, shardCount int) int {
	if shardCount <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(documentID))
	return int(h.Sum32() % uint32(shardCount))
}

func sameIndexDefinition(a, b IndexInfo) bool {
	return a.Name == b.Name &&
		a.ShardCount == b.ShardCount &&
		a.ReplicationFactor == b.ReplicationFactor &&
		a.MappingVersion == b.MappingVersion &&
		reflect.DeepEqual(a.Mapping, b.Mapping)
}

func validateCreateIndex(req CreateIndexRequest) (IndexInfo, error) {
	if !indexNamePattern.MatchString(req.Name) {
		return IndexInfo{}, errors.New("index name is invalid")
	}
	if req.ShardCount <= 0 {
		return IndexInfo{}, errors.New("shard count must be positive")
	}
	if req.ReplicationFactor <= 0 {
		return IndexInfo{}, errors.New("replication factor must be positive")
	}
	if req.MappingVersion < 0 {
		return IndexInfo{}, errors.New("mapping version must be non-negative")
	}
	if req.Mapping == nil {
		return IndexInfo{}, errors.New("mapping is required")
	}
	return IndexInfo{
		Name:              req.Name,
		ShardCount:        req.ShardCount,
		ReplicationFactor: req.ReplicationFactor,
		MappingVersion:    req.MappingVersion,
		Mapping:           cloneMapping(req.Mapping),
	}, nil
}

func cloneMapping(mapping map[string]any) map[string]any {
	if mapping == nil {
		return nil
	}
	out := make(map[string]any, len(mapping))
	for key, value := range mapping {
		out[key] = value
	}
	return out
}

func indexPathRemainder(path string) (string, bool) {
	trimmed := strings.TrimPrefix(path, "/v1/indexes/")
	if trimmed == path {
		return "", false
	}
	return trimmed, true
}

func indexFromPath(path, suffix string) (string, bool) {
	trimmed, ok := indexPathRemainder(path)
	if !ok {
		return "", false
	}

	if !strings.HasSuffix(trimmed, suffix) {
		return "", false
	}
	index := strings.TrimSuffix(trimmed, suffix)
	if index == "" || strings.Contains(index, "/") {
		return "", false
	}

	return index, true
}

func intQueryParam(r *http.Request, name string, defaultValue int) (int, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return defaultValue, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	if value < 0 {
		return 0, fmt.Errorf("%s must be non-negative", name)
	}
	if name == "limit" && value == 0 {
		return 0, errors.New("limit must be positive")
	}

	return value, nil
}

func (s *Server) indexRouteSnapshot(index string) (IndexInfo, []Route, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info, ok := s.indexes[index]
	if !ok {
		return IndexInfo{}, nil, false
	}
	routes, ok := s.routes.selectedRoutes(index, info.ShardCount)
	if !ok {
		return info, nil, true
	}
	return info, routes, true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(value); err != nil {
		http.Error(w, "encode response: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := io.Copy(w, &buf); err != nil {
		return
	}
}

func sleepMetadataWatchRetry(ctx context.Context) bool {
	timer := time.NewTimer(metadataWatchRetryDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
