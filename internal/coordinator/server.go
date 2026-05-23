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
	"github.com/cobeo2004/kitsune/internal/member"
	"github.com/cobeo2004/kitsune/internal/metadata"
	clusterstatus "github.com/cobeo2004/kitsune/internal/status"
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
	MemberCache      *member.Cache
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
	tabletStatuses   map[assignmentKey]metadata.TabletStatusRecord
	memberCache      *member.Cache
	assignments      []clusterstatus.AssignmentView
	tablets          []clusterstatus.TabletView
	checkpoints      []clusterstatus.CheckpointView
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
		routes:           cloneStaticRoutes(cfg.Routes),
		routeClients:     cloneStaticRoutes(cfg.Routes),
		staticConfigured: len(cfg.StaticConfig.Indexes) > 0,
		metadataManager:  cfg.MetadataManager,
		memberCache:      cfg.MemberCache,
		eventBus:         cfg.EventBus,
		indexes:          make(map[string]IndexInfo),
		tabletStatuses:   make(map[assignmentKey]metadata.TabletStatusRecord),
		assignments:      assignmentsFromStaticConfig(cfg.StaticConfig),
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

	tabletStatuses := statesFromMetadata(snapshot.TabletStatuses)
	routes := routesFromMetadata(snapshot.ShardReplicas, snapshot.TabletStatuses, s.routeClients)
	assignments := assignmentsFromMetadata(snapshot.ShardReplicas)
	tablets := tabletsFromMetadata(snapshot.TabletStatuses)
	checkpoints := checkpointsFromMetadata(snapshot.Checkpoints)

	s.mu.Lock()
	s.indexes = indexes
	s.routes = routes
	s.tabletStatuses = tabletStatuses
	s.assignments = assignments
	s.tablets = tablets
	s.checkpoints = checkpoints
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
			removeTabletStatusesForIndex(s.tabletStatuses, event.Index.Name)
			s.assignments = removeAssignmentsForIndex(s.assignments, event.Index.Name)
			s.tablets = removeTabletsForIndex(s.tablets, event.Index.Name)
			s.checkpoints = removeCheckpointsForIndex(s.checkpoints, event.Index.Name)
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
			s.assignments = removeAssignment(s.assignments, *event.ShardReplica)
			return
		}
		s.assignments = upsertAssignment(s.assignments, assignmentFromMetadata(*event.ShardReplica))
		if route, ok := routeForReplica(s.routeClients, *event.ShardReplica); ok {
			key := routeAssignmentKey(event.ShardReplica.IndexName, event.ShardReplica.ShardID, event.ShardReplica.ReplicaID)
			if status, ok := s.tabletStatuses[key]; ok && status.NodeID == route.NodeID {
				route.State = ReplicaState(status.State)
			} else {
				delete(s.tabletStatuses, key)
				s.tablets = removeTabletByAssignment(s.tablets, *event.ShardReplica)
				route.State = ReplicaUnknown
			}
			s.routes = upsertRoute(s.routes, event.ShardReplica.IndexName, route)
		}
	case metadata.EventKindTabletStatus:
		if event.TabletStatus == nil {
			return
		}
		status := *event.TabletStatus
		key := routeAssignmentKey(status.IndexName, status.ShardID, status.ReplicaID)
		if event.Deleted {
			delete(s.tabletStatuses, key)
			s.tablets = removeTablet(s.tablets, status)
			status.State = string(ReplicaUnknown)
		} else {
			s.tabletStatuses[key] = status
			s.tablets = upsertTablet(s.tablets, tabletFromMetadata(status))
		}
		s.routes = updateRouteState(s.routes, status)
	case metadata.EventKindCheckpoint:
		if event.Checkpoint == nil {
			return
		}
		if event.Deleted {
			s.checkpoints = removeCheckpoint(s.checkpoints, *event.Checkpoint)
			return
		}
		s.checkpoints = upsertCheckpoint(s.checkpoints, checkpointFromMetadata(*event.Checkpoint))
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
	assignments := append([]clusterstatus.AssignmentView(nil), s.assignments...)
	tablets := append([]clusterstatus.TabletView(nil), s.tablets...)
	checkpoints := append([]clusterstatus.CheckpointView(nil), s.checkpoints...)
	var nodes []clusterstatus.NodeHealthView
	if s.memberCache != nil {
		for _, view := range s.memberCache.List() {
			nodes = append(nodes, clusterstatus.NodeHealthView{
				NodeID:      view.NodeID,
				GRPCAddress: view.GRPCAddress,
				Health:      string(view.Health),
			})
		}
	}
	s.mu.RUnlock()
	cluster := clusterstatus.BuildClusterStatus(clusterstatus.Input{
		Assignments: assignments,
		Nodes:       nodes,
		Tablets:     tablets,
		Checkpoints: checkpoints,
	})

	writeJSON(w, http.StatusOK, ClusterStatus{
		State:        "ready",
		IndexCount:   indexCount,
		RouteIndexes: routeIndexes,
		Assignments:  cluster.Assignments,
		Nodes:        cluster.Nodes,
		Tablets:      cluster.Tablets,
		Checkpoints:  cluster.Checkpoints,
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
	if s.eventBus == nil {
		http.Error(w, "document event bus is not configured", http.StatusServiceUnavailable)
		return
	}

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

	_, routeGroups, ok, routeErr := s.indexSearchRouteSnapshot(index)
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

	if routeErr != nil {
		http.Error(w, routeErr.Error(), http.StatusServiceUnavailable)
		return
	}

	results := make([]ShardSearchResult, 0, len(routeGroups))
	for _, routes := range routeGroups {
		result, err := searchShardWithFallback(r.Context(), index, routes, query, shardLimit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
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
		SchemaVersion:   events.CurrentSchemaVersion,
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
	routes, err := s.routes.selectedRoutes(index, info.ShardCount)
	if err != nil {
		return info, nil, true
	}
	return info, routes, true
}

func (s *Server) indexSearchRouteSnapshot(index string) (IndexInfo, [][]Route, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info, ok := s.indexes[index]
	if !ok {
		return IndexInfo{}, nil, false, nil
	}
	routes, err := s.routes.readyRoutesByShard(index, info.ShardCount)
	if err != nil {
		return info, nil, true, err
	}
	return info, routes, true, nil
}

func searchShardWithFallback(ctx context.Context, index string, routes []Route, query string, limit int) (ShardSearchResult, error) {
	var lastErr error
	for _, route := range routes {
		result, err := route.Client.Search(ctx, query, limit, 0)
		if err == nil {
			return result, nil
		}
		lastErr = err
	}
	if len(routes) == 0 {
		return ShardSearchResult{}, fmt.Errorf("no healthy replica available for %s shard 0", index)
	}
	first := routes[0]
	return ShardSearchResult{}, fmt.Errorf("search %s shard %d: all ready replicas failed: %w", index, first.ShardID, lastErr)
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

func assignmentsFromStaticConfig(cfg StaticConfig) []clusterstatus.AssignmentView {
	assignments := make([]clusterstatus.AssignmentView, 0, len(cfg.Assignments))
	for _, assignment := range cfg.Assignments {
		assignments = append(assignments, clusterstatus.AssignmentView{
			IndexName: assignment.IndexName,
			ShardID:   assignment.ShardID,
			ReplicaID: assignment.ReplicaID,
			NodeID:    assignment.NodeID,
		})
	}
	return assignments
}

func assignmentsFromMetadata(replicas []metadata.ShardReplicaRecord) []clusterstatus.AssignmentView {
	assignments := make([]clusterstatus.AssignmentView, 0, len(replicas))
	for _, replica := range replicas {
		assignments = append(assignments, assignmentFromMetadata(replica))
	}
	return assignments
}

func assignmentFromMetadata(replica metadata.ShardReplicaRecord) clusterstatus.AssignmentView {
	return clusterstatus.AssignmentView{
		IndexName: replica.IndexName,
		ShardID:   replica.ShardID,
		ReplicaID: replica.ReplicaID,
		NodeID:    replica.NodeID,
	}
}

func tabletsFromMetadata(statuses []metadata.TabletStatusRecord) []clusterstatus.TabletView {
	tablets := make([]clusterstatus.TabletView, 0, len(statuses))
	for _, status := range statuses {
		tablets = append(tablets, tabletFromMetadata(status))
	}
	return tablets
}

func tabletFromMetadata(status metadata.TabletStatusRecord) clusterstatus.TabletView {
	return clusterstatus.TabletView{
		IndexName:      status.IndexName,
		ShardID:        status.ShardID,
		ReplicaID:      status.ReplicaID,
		NodeID:         status.NodeID,
		State:          status.State,
		LastCheckpoint: status.LastCheckpoint,
	}
}

func checkpointsFromMetadata(checkpoints []metadata.CheckpointRecord) []clusterstatus.CheckpointView {
	out := make([]clusterstatus.CheckpointView, 0, len(checkpoints))
	for _, checkpoint := range checkpoints {
		out = append(out, checkpointFromMetadata(checkpoint))
	}
	return out
}

func checkpointFromMetadata(checkpoint metadata.CheckpointRecord) clusterstatus.CheckpointView {
	return clusterstatus.CheckpointView{
		IndexName: checkpoint.IndexName,
		ShardID:   checkpoint.ShardID,
		ReplicaID: checkpoint.ReplicaID,
		Sequence:  checkpoint.Sequence,
		EventID:   checkpoint.EventID,
	}
}

func upsertAssignment(assignments []clusterstatus.AssignmentView, next clusterstatus.AssignmentView) []clusterstatus.AssignmentView {
	for i, current := range assignments {
		if current.IndexName == next.IndexName && current.ShardID == next.ShardID && current.ReplicaID == next.ReplicaID {
			assignments[i] = next
			return assignments
		}
	}
	return append(assignments, next)
}

func removeAssignment(assignments []clusterstatus.AssignmentView, replica metadata.ShardReplicaRecord) []clusterstatus.AssignmentView {
	out := assignments[:0]
	for _, current := range assignments {
		if current.IndexName == replica.IndexName && current.ShardID == replica.ShardID && current.ReplicaID == replica.ReplicaID {
			continue
		}
		out = append(out, current)
	}
	return out
}

func removeAssignmentsForIndex(assignments []clusterstatus.AssignmentView, indexName string) []clusterstatus.AssignmentView {
	out := assignments[:0]
	for _, current := range assignments {
		if current.IndexName == indexName {
			continue
		}
		out = append(out, current)
	}
	return out
}

func upsertTablet(tablets []clusterstatus.TabletView, next clusterstatus.TabletView) []clusterstatus.TabletView {
	for i, current := range tablets {
		if current.IndexName == next.IndexName && current.ShardID == next.ShardID && current.ReplicaID == next.ReplicaID {
			tablets[i] = next
			return tablets
		}
	}
	return append(tablets, next)
}

func removeTablet(tablets []clusterstatus.TabletView, status metadata.TabletStatusRecord) []clusterstatus.TabletView {
	out := tablets[:0]
	for _, current := range tablets {
		if current.IndexName == status.IndexName && current.ShardID == status.ShardID && current.ReplicaID == status.ReplicaID {
			continue
		}
		out = append(out, current)
	}
	return out
}

func removeTabletByAssignment(tablets []clusterstatus.TabletView, replica metadata.ShardReplicaRecord) []clusterstatus.TabletView {
	out := tablets[:0]
	for _, current := range tablets {
		if current.IndexName == replica.IndexName && current.ShardID == replica.ShardID && current.ReplicaID == replica.ReplicaID {
			continue
		}
		out = append(out, current)
	}
	return out
}

func removeTabletsForIndex(tablets []clusterstatus.TabletView, indexName string) []clusterstatus.TabletView {
	out := tablets[:0]
	for _, current := range tablets {
		if current.IndexName == indexName {
			continue
		}
		out = append(out, current)
	}
	return out
}

func removeTabletStatusesForIndex(statuses map[assignmentKey]metadata.TabletStatusRecord, indexName string) {
	for key := range statuses {
		if key.indexName == indexName {
			delete(statuses, key)
		}
	}
}

func upsertCheckpoint(checkpoints []clusterstatus.CheckpointView, next clusterstatus.CheckpointView) []clusterstatus.CheckpointView {
	for i, current := range checkpoints {
		if current.IndexName == next.IndexName && current.ShardID == next.ShardID && current.ReplicaID == next.ReplicaID {
			checkpoints[i] = next
			return checkpoints
		}
	}
	return append(checkpoints, next)
}

func removeCheckpoint(checkpoints []clusterstatus.CheckpointView, checkpoint metadata.CheckpointRecord) []clusterstatus.CheckpointView {
	out := checkpoints[:0]
	for _, current := range checkpoints {
		if current.IndexName == checkpoint.IndexName && current.ShardID == checkpoint.ShardID && current.ReplicaID == checkpoint.ReplicaID {
			continue
		}
		out = append(out, current)
	}
	return out
}

func removeCheckpointsForIndex(checkpoints []clusterstatus.CheckpointView, indexName string) []clusterstatus.CheckpointView {
	out := checkpoints[:0]
	for _, current := range checkpoints {
		if current.IndexName == indexName {
			continue
		}
		out = append(out, current)
	}
	return out
}
