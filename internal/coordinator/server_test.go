package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	searchnodev1 "github.com/cobeo2004/kitsune/api/searchnode/v1"
	"github.com/cobeo2004/kitsune/internal/events"
	"github.com/cobeo2004/kitsune/internal/member"
	"github.com/cobeo2004/kitsune/internal/metadata"
	"github.com/cobeo2004/kitsune/internal/searchnode"
	"github.com/cobeo2004/kitsune/internal/tablet"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMergeOrdersHitsByScoreAndPaginates(t *testing.T) {
	t.Parallel()

	got := MergeResults([]ShardSearchResult{
		{Hits: []SearchHit{{DocumentID: "a", Score: 1.0}, {DocumentID: "b", Score: 3.0}}},
		{Hits: []SearchHit{{DocumentID: "c", Score: 2.0}}},
	}, 2, 0)

	if len(got.Hits) != 2 {
		t.Fatalf("len(hits) = %d, want 2", len(got.Hits))
	}
	if got.Hits[0].DocumentID != "b" || got.Hits[1].DocumentID != "c" {
		t.Fatalf("order = %#v, want b,c", got.Hits)
	}
}

func TestUpsertRejectsDocumentAboveDefaultLimit(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{})
	createBooksIndex(t, srv, 1, 1)
	body := strings.NewReader(`{"documentId":"big","fields":{"body":"` + strings.Repeat("x", 1024*1024) + `"}}`)
	req := httptest.NewRequest(http.MethodPut, "/v1/indexes/books/documents/big", body)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestUpsertRejectsDocumentAboveConfiguredLimit(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{MaxDocumentBytes: 64})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodPut, "/v1/indexes/books/documents/doc-1", strings.NewReader(`{"fields":{"body":"`+strings.Repeat("x", 128)+`"}}`))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestUpsertRejectsMalformedJSON(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodPut, "/v1/indexes/books/documents/doc-1", strings.NewReader(`{"fields":`))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestUpsertRejectsMalformedDocumentPath(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{})
	req := httptest.NewRequest(http.MethodPut, "/v1/indexes/books/documents/doc-1/extra", strings.NewReader(`{"fields":{}}`))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestUpsertRejectsMissingIndex(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{})
	req := httptest.NewRequest(http.MethodPut, "/v1/indexes/books/documents/doc-1", strings.NewReader(`{"fields":{}}`))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestUpsertAcceptsExistingIndex(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{EventBus: events.NewMemoryBus()})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodPut, "/v1/indexes/books/documents/doc-1", strings.NewReader(`{"fields":{"title":"Bleve"}}`))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
}

func TestUpsertRequiresEventBus(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodPut, "/v1/indexes/books/documents/doc-1", strings.NewReader(`{"fields":{"title":"Bleve"}}`))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
}

func TestUpsertPublishesDocumentEvent(t *testing.T) {
	t.Parallel()

	bus := events.NewMemoryBus()
	srv := NewServer(ServerConfig{EventBus: bus})
	createBooksIndex(t, srv, 2, 1)
	req := httptest.NewRequest(http.MethodPut, "/v1/indexes/books/documents/doc-1", strings.NewReader(`{"fields":{"title":"Bleve"}}`))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	got := bus.Events()
	if len(got) != 1 {
		t.Fatalf("events len = %d, want 1", len(got))
	}
	evt := got[0]
	if evt.IndexName != "books" || evt.DocumentID != "doc-1" {
		t.Fatalf("event identity = %#v", evt)
	}
	if evt.SchemaVersion != events.CurrentSchemaVersion {
		t.Fatalf("schema version = %d, want %d", evt.SchemaVersion, events.CurrentSchemaVersion)
	}
	if evt.MappingVersion != 0 {
		t.Fatalf("mapping version = %d, want 0", evt.MappingVersion)
	}
	if evt.Fields["title"] != "Bleve" {
		t.Fatalf("fields = %#v", evt.Fields)
	}
}

func TestUpsertFailsWhenEventPublishFails(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{EventBus: failingEventBus{}})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodPut, "/v1/indexes/books/documents/doc-1", strings.NewReader(`{"fields":{"title":"Bleve"}}`))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadGateway, rec.Body.String())
	}
}

func TestUpsertRejectsInvalidDocumentEventBeforePublish(t *testing.T) {
	t.Parallel()

	bus := events.NewMemoryBus()
	srv := NewServer(ServerConfig{EventBus: bus})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodPut, "/v1/indexes/books/documents/doc-1", strings.NewReader(`{"fields":{}}`))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := bus.Events(); len(got) != 0 {
		t.Fatalf("events len = %d, want 0", len(got))
	}
}

func TestCreateIndexValidatesRequiredFields(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{})
	req := httptest.NewRequest(http.MethodPost, "/v1/indexes", strings.NewReader(`{"name":"","shardCount":0,"replicationFactor":0}`))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestCreateIndexRejectsMappingChange(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodPost, "/v1/indexes", strings.NewReader(`{"name":"books","shardCount":1,"replicationFactor":1,"mapping":{"defaultAnalyzer":"keyword"}}`))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
	}
}

func TestCreateIndexRejectsTopologyChange(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodPost, "/v1/indexes", strings.NewReader(`{"name":"books","shardCount":2,"replicationFactor":1,"mapping":{"defaultAnalyzer":"standard"}}`))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
	}
}

func TestCreateIndexMustMatchStaticConfig(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{StaticConfig: StaticConfig{
		Indexes: []IndexConfig{
			{
				Name:              "books",
				ShardCount:        1,
				ReplicationFactor: 1,
				MappingVersion:    1,
				Mapping:           map[string]any{"defaultAnalyzer": "standard"},
			},
		},
		Assignments: []ShardAssignment{
			{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"},
		},
	}})
	req := httptest.NewRequest(http.MethodPost, "/v1/indexes", strings.NewReader(`{"name":"books","shardCount":1,"replicationFactor":1,"mappingVersion":1,"mapping":{"defaultAnalyzer":"keyword"}}`))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
	}
}

func TestCreateIndexRejectsUnknownIndexWhenStaticConfigExists(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{StaticConfig: StaticConfig{
		Indexes: []IndexConfig{
			{Name: "books", ShardCount: 1, ReplicationFactor: 1, Mapping: map[string]any{"defaultAnalyzer": "standard"}},
		},
		Assignments: []ShardAssignment{
			{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"},
		},
	}})
	req := httptest.NewRequest(http.MethodPost, "/v1/indexes", strings.NewReader(`{"name":"movies","shardCount":1,"replicationFactor":1,"mapping":{"defaultAnalyzer":"standard"}}`))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestClusterStatusReportsReady(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{Routes: StaticRoutes{
		"books": {{ShardID: 0, Client: &fakeShardClient{}}},
	}})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodGet, "/v1/cluster/status", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body ClusterStatus
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if body.State != "ready" {
		t.Fatalf("state = %q, want ready", body.State)
	}
	if body.IndexCount != 1 {
		t.Fatalf("index count = %d, want 1", body.IndexCount)
	}
	if body.RouteIndexes != 1 {
		t.Fatalf("route indexes = %d, want 1", body.RouteIndexes)
	}
}

func TestClusterStatusIncludesAuthoritativeMetadataAndAdvisoryHealth(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	manager := metadata.NewMemoryManager()
	if err := manager.PutIndex(ctx, metadata.IndexRecord{Name: "books", ShardCount: 1, ReplicationFactor: 1, MappingVersion: 1, Mapping: map[string]any{"defaultAnalyzer": "standard"}}, 0); err != nil {
		t.Fatalf("put index: %v", err)
	}
	if err := manager.PutShardReplica(ctx, metadata.ShardReplicaRecord{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"}, 0); err != nil {
		t.Fatalf("put shard replica: %v", err)
	}
	if err := manager.PutTabletStatus(ctx, metadata.TabletStatusRecord{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", State: string(ReplicaReady), LastCheckpoint: 42}, 0); err != nil {
		t.Fatalf("put tablet status: %v", err)
	}
	if err := manager.PutCheckpoint(ctx, metadata.CheckpointRecord{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", Sequence: 42, EventID: "event-42"}, 0); err != nil {
		t.Fatalf("put checkpoint: %v", err)
	}
	cache := member.NewCache()
	cache.Update(member.NodeView{NodeID: "node-a", GRPCAddress: "127.0.0.1:9001", Health: member.HealthDead})
	srv := NewServer(ServerConfig{MetadataManager: manager, MemberCache: cache})
	req := httptest.NewRequest(http.MethodGet, "/v1/cluster/status", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body ClusterStatus
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if len(body.Assignments) != 1 || body.Assignments[0].NodeID != "node-a" {
		t.Fatalf("assignments = %#v", body.Assignments)
	}
	if len(body.Nodes) != 1 || body.Nodes[0].Health != string(member.HealthDead) {
		t.Fatalf("nodes = %#v", body.Nodes)
	}
	if len(body.Tablets) != 1 || body.Tablets[0].State != string(ReplicaReady) || body.Tablets[0].LastCheckpoint != 42 {
		t.Fatalf("tablets = %#v", body.Tablets)
	}
	if len(body.Checkpoints) != 1 || body.Checkpoints[0].Sequence != 42 {
		t.Fatalf("checkpoints = %#v", body.Checkpoints)
	}
}

func TestClusterStatusReplacesMovedReplicaOwnership(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{})
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindShardReplica,
		ShardReplica: &metadata.ShardReplicaRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-a",
		},
	})
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindShardReplica,
		ShardReplica: &metadata.ShardReplicaRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-b",
		},
	})
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindTabletStatus,
		TabletStatus: &metadata.TabletStatusRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-a",
			State:     string(ReplicaReady),
		},
	})
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindTabletStatus,
		TabletStatus: &metadata.TabletStatusRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-b",
			State:     string(ReplicaReady),
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/cluster/status", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body ClusterStatus
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if len(body.Assignments) != 1 || body.Assignments[0].NodeID != "node-b" {
		t.Fatalf("assignments = %#v, want only moved node-b assignment", body.Assignments)
	}
	if len(body.Tablets) != 1 || body.Tablets[0].NodeID != "node-b" {
		t.Fatalf("tablets = %#v, want only moved node-b tablet", body.Tablets)
	}
}

func TestClusterStatusDropsDeletedIndexViews(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{})
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindShardReplica,
		ShardReplica: &metadata.ShardReplicaRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-a",
		},
	})
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindTabletStatus,
		TabletStatus: &metadata.TabletStatusRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-a",
			State:     string(ReplicaReady),
		},
	})
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindCheckpoint,
		Checkpoint: &metadata.CheckpointRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			Sequence:  42,
		},
	})
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind:    metadata.EventKindIndex,
		Deleted: true,
		Index:   &metadata.IndexRecord{Name: "books"},
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/cluster/status", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body ClusterStatus
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if len(body.Assignments) != 0 || len(body.Tablets) != 0 || len(body.Checkpoints) != 0 {
		t.Fatalf("status = %#v, want no deleted index views", body)
	}
}

func TestMetadataReplicaMoveReplacesStaleRoute(t *testing.T) {
	t.Parallel()

	oldClient := &fakeShardClient{err: errors.New("old node should not be routed")}
	newClient := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "doc-1", Score: 2}}}}
	srv := NewServer(ServerConfig{
		Routes: StaticRoutes{
			"books": {
				{ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", State: ReplicaReady, Client: oldClient},
				{ShardID: 0, ReplicaID: "replica-a", NodeID: "node-b", State: ReplicaReady, Client: newClient},
			},
		},
	})
	createBooksIndex(t, srv, 1, 1)
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindShardReplica,
		ShardReplica: &metadata.ShardReplicaRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-a",
		},
	})
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindShardReplica,
		ShardReplica: &metadata.ShardReplicaRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-b",
		},
	})
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindTabletStatus,
		TabletStatus: &metadata.TabletStatusRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-b",
			State:     string(ReplicaReady),
		},
	})

	_, routes, ok := srv.indexRouteSnapshot("books")
	if !ok {
		t.Fatal("route snapshot was not available")
	}
	if len(routes) != 1 || routes[0].NodeID != "node-b" {
		t.Fatalf("routes = %#v, want only node-b route", routes)
	}
}

func TestReplicaMoveBackDoesNotReuseStaleReadyState(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{
		Routes: StaticRoutes{
			"books": {
				{ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", Client: &fakeShardClient{}},
				{ShardID: 0, ReplicaID: "replica-a", NodeID: "node-b", Client: &fakeShardClient{}},
			},
		},
	})
	createBooksIndex(t, srv, 1, 1)
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindShardReplica,
		ShardReplica: &metadata.ShardReplicaRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-a",
		},
	})
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindTabletStatus,
		TabletStatus: &metadata.TabletStatusRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-a",
			State:     string(ReplicaReady),
		},
	})
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindShardReplica,
		ShardReplica: &metadata.ShardReplicaRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-b",
		},
	})
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindTabletStatus,
		TabletStatus: &metadata.TabletStatusRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-b",
			State:     string(ReplicaReady),
		},
	})
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindShardReplica,
		ShardReplica: &metadata.ShardReplicaRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-a",
		},
	})

	_, routes, ok := srv.indexRouteSnapshot("books")
	if !ok {
		t.Fatal("route snapshot was not available")
	}
	if len(routes) != 0 {
		t.Fatalf("routes = %#v, want no ready route until node-a reports after move-back", routes)
	}
}

func TestClusterStatusDropsStaleTabletAfterReplicaMove(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{
		Routes: StaticRoutes{
			"books": {
				{ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", Client: &fakeShardClient{}},
				{ShardID: 0, ReplicaID: "replica-a", NodeID: "node-b", Client: &fakeShardClient{}},
			},
		},
	})
	createBooksIndex(t, srv, 1, 1)
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindShardReplica,
		ShardReplica: &metadata.ShardReplicaRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-a",
		},
	})
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindTabletStatus,
		TabletStatus: &metadata.TabletStatusRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-a",
			State:     string(ReplicaReady),
		},
	})
	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindShardReplica,
		ShardReplica: &metadata.ShardReplicaRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-b",
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/cluster/status", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body ClusterStatus
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if len(body.Assignments) != 1 || body.Assignments[0].NodeID != "node-b" {
		t.Fatalf("assignments = %#v, want moved node-b assignment", body.Assignments)
	}
	if len(body.Tablets) != 0 {
		t.Fatalf("tablets = %#v, want no tablet view until moved node reports status", body.Tablets)
	}
}

func TestSearchRoutesToConfiguredShardClient(t *testing.T) {
	t.Parallel()

	client := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "doc-1", Score: 2}}}}
	srv := NewServer(ServerConfig{Routes: StaticRoutes{
		"books": {{ShardID: 0, Client: client}},
	}})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=10", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if client.query != "bleve" {
		t.Fatalf("query = %q, want bleve", client.query)
	}
}

func TestSearchRoutesFromMetadataSnapshot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	manager := metadata.NewMemoryManager()
	if err := manager.PutIndex(ctx, metadata.IndexRecord{Name: "books", ShardCount: 1, ReplicationFactor: 1, MappingVersion: 1, Mapping: map[string]any{"defaultAnalyzer": "standard"}}, 0); err != nil {
		t.Fatalf("put index: %v", err)
	}
	if err := manager.PutShardReplica(ctx, metadata.ShardReplicaRecord{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"}, 0); err != nil {
		t.Fatalf("put shard replica: %v", err)
	}
	if err := manager.PutTabletStatus(ctx, metadata.TabletStatusRecord{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", State: string(ReplicaReady)}, 0); err != nil {
		t.Fatalf("put tablet status: %v", err)
	}

	client := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "doc-1", Score: 2}}}}
	srv := NewServer(ServerConfig{
		MetadataManager: manager,
		Routes: StaticRoutes{
			"books": {{ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", Client: client}},
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=10", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if client.calls != 1 {
		t.Fatalf("client calls = %d, want 1", client.calls)
	}
}

func TestSearchRoutesFromMetadataSnapshotSkipsFailedTabletStatus(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	manager := metadata.NewMemoryManager()
	if err := manager.PutIndex(ctx, metadata.IndexRecord{Name: "books", ShardCount: 1, ReplicationFactor: 2, MappingVersion: 1, Mapping: map[string]any{"defaultAnalyzer": "standard"}}, 0); err != nil {
		t.Fatalf("put index: %v", err)
	}
	if err := manager.PutShardReplica(ctx, metadata.ShardReplicaRecord{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"}, 0); err != nil {
		t.Fatalf("put shard replica a: %v", err)
	}
	if err := manager.PutShardReplica(ctx, metadata.ShardReplicaRecord{IndexName: "books", ShardID: 0, ReplicaID: "replica-b", NodeID: "node-b"}, 0); err != nil {
		t.Fatalf("put shard replica b: %v", err)
	}
	if err := manager.PutTabletStatus(ctx, metadata.TabletStatusRecord{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", State: string(ReplicaFailed)}, 0); err != nil {
		t.Fatalf("put tablet status a: %v", err)
	}
	if err := manager.PutTabletStatus(ctx, metadata.TabletStatusRecord{IndexName: "books", ShardID: 0, ReplicaID: "replica-b", NodeID: "node-b", State: string(ReplicaReady)}, 0); err != nil {
		t.Fatalf("put tablet status b: %v", err)
	}

	failedClient := &fakeShardClient{err: errors.New("failed replica was queried")}
	readyClient := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "doc-1", Score: 2}}}}
	srv := NewServer(ServerConfig{
		MetadataManager: manager,
		Routes: StaticRoutes{
			"books": {
				{ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", Client: failedClient},
				{ShardID: 0, ReplicaID: "replica-b", NodeID: "node-b", Client: readyClient},
			},
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=10", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if failedClient.calls != 0 {
		t.Fatalf("failed client calls = %d, want 0", failedClient.calls)
	}
	if readyClient.calls != 1 {
		t.Fatalf("ready client calls = %d, want 1", readyClient.calls)
	}
}

func TestMetadataWatchRefreshesRouteCache(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager := metadata.NewMemoryManager()
	client := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "doc-1", Score: 2}}}}
	srv := NewServer(ServerConfig{
		MetadataManager: manager,
		Routes: StaticRoutes{
			"books": {{ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", Client: client}},
		},
	})
	if err := srv.StartMetadataWatch(ctx); err != nil {
		t.Fatalf("start metadata watch: %v", err)
	}
	if err := manager.PutIndex(ctx, metadata.IndexRecord{Name: "books", ShardCount: 1, ReplicationFactor: 1, MappingVersion: 1, Mapping: map[string]any{"defaultAnalyzer": "standard"}}, 0); err != nil {
		t.Fatalf("put index: %v", err)
	}
	if err := manager.PutShardReplica(ctx, metadata.ShardReplicaRecord{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"}, 0); err != nil {
		t.Fatalf("put shard replica: %v", err)
	}
	if err := manager.PutTabletStatus(ctx, metadata.TabletStatusRecord{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", State: string(ReplicaReady)}, 0); err != nil {
		t.Fatalf("put tablet status: %v", err)
	}

	eventually(t, func() bool {
		req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=10", nil)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		return rec.Code == http.StatusOK
	})
}

func TestMetadataWatchReloadsAfterClosedWatch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager := &reloadMetadataManager{
		closeFirstWatch: true,
		snapshots: []metadata.Snapshot{
			{Revision: 1},
			{Revision: 2},
			{
				Revision: 3,
				Indexes: []metadata.IndexRecord{
					{Name: "books", ShardCount: 1, ReplicationFactor: 1, MappingVersion: 1, Mapping: map[string]any{"defaultAnalyzer": "standard"}},
				},
				ShardReplicas: []metadata.ShardReplicaRecord{
					{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"},
				},
				TabletStatuses: []metadata.TabletStatusRecord{
					{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", State: string(ReplicaReady)},
				},
			},
		},
	}
	client := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "doc-1", Score: 2}}}}
	srv := NewServer(ServerConfig{
		MetadataManager: manager,
		Routes: StaticRoutes{
			"books": {{ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", Client: client}},
		},
	})
	if err := srv.StartMetadataWatch(ctx); err != nil {
		t.Fatalf("start metadata watch: %v", err)
	}

	eventually(t, func() bool {
		req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=10", nil)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		return rec.Code == http.StatusOK
	})
}

func TestMetadataWatchRetriesAfterLoadFailure(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager := &reloadMetadataManager{
		failFirstLoad: true,
		snapshots: []metadata.Snapshot{
			{
				Revision: 1,
				Indexes: []metadata.IndexRecord{
					{Name: "books", ShardCount: 1, ReplicationFactor: 1, MappingVersion: 1, Mapping: map[string]any{"defaultAnalyzer": "standard"}},
				},
				ShardReplicas: []metadata.ShardReplicaRecord{
					{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"},
				},
				TabletStatuses: []metadata.TabletStatusRecord{
					{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", State: string(ReplicaReady)},
				},
			},
		},
	}
	client := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "doc-1", Score: 2}}}}
	srv := NewServer(ServerConfig{
		Routes: StaticRoutes{
			"books": {{ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", Client: client}},
		},
	})
	srv.metadataManager = manager
	srv.routeClients = srv.routes

	go srv.runMetadataWatch(ctx, closedWatch())

	eventually(t, func() bool {
		req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=10", nil)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		return rec.Code == http.StatusOK
	})
}

func TestApplyMetadataEventUpdatesRouteReplicaState(t *testing.T) {
	t.Parallel()

	failedClient := &fakeShardClient{}
	readyClient := &fakeShardClient{}
	srv := NewServer(ServerConfig{
		EventBus: events.NewMemoryBus(),
		Routes: StaticRoutes{
			"books": {
				{ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", State: ReplicaReady, Client: failedClient},
				{ShardID: 0, ReplicaID: "replica-b", NodeID: "node-b", State: ReplicaReady, Client: readyClient},
			},
		},
	})
	createBooksIndex(t, srv, 1, 2)

	srv.applyMetadataEvent(metadata.WatchEvent{
		Kind: metadata.EventKindTabletStatus,
		TabletStatus: &metadata.TabletStatusRecord{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-a",
			State:     string(ReplicaFailed),
		},
	})

	_, routes, ok := srv.indexRouteSnapshot("books")
	if !ok {
		t.Fatal("route snapshot was not available")
	}
	if routes[0].ReplicaID != "replica-b" {
		t.Fatalf("replica = %q, want replica-b", routes[0].ReplicaID)
	}
}

func TestSearchUsesConsistentRouteSnapshot(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{Routes: StaticRoutes{
		"books": {{ShardID: 0, Client: &fakeShardClient{}}},
	}})
	createBooksIndex(t, srv, 1, 1)

	info, routes, ok := srv.indexRouteSnapshot("books")
	if !ok {
		t.Fatal("index route snapshot not found")
	}
	if info.ShardCount != 1 || len(routes) != 1 {
		t.Fatalf("snapshot info=%#v routes=%#v, want one shard route", info, routes)
	}
}

func TestSearchRoutesThroughSearchNodeGRPCClient(t *testing.T) {
	t.Parallel()

	node := searchnode.New(searchnode.NodeConfig{NodeID: "node-a"})
	node.RegisterTablet("books", 0, "replica-a", &fakeTablet{
		status: tablet.Status{
			Identity: tablet.Identity{
				IndexName: "books",
				ShardID:   0,
				ReplicaID: "replica-a",
				NodeID:    "node-a",
			},
			State: tablet.StateReady,
		},
		result: tablet.SearchResult{
			Total: 1,
			Hits:  []tablet.SearchHit{{DocumentID: "doc-1", Score: 2.5}},
		},
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	searchnodev1.RegisterSearchNodeServiceServer(grpcServer, searchnode.NewServer(node))
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("serve grpc: %v", err)
		}
	}()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.Dial(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close grpc conn: %v", err)
		}
	})

	srv := NewServer(ServerConfig{Routes: StaticRoutes{
		"books": {{ShardID: 0, Client: NewSearchNodeShardClient(searchnodev1.NewSearchNodeServiceClient(conn), &searchnodev1.TabletRef{
			IndexName: "books",
			ShardId:   0,
			ReplicaId: "replica-a",
		})}},
	}})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=10", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body SearchResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode search response: %v", err)
	}
	if body.Total != 1 {
		t.Fatalf("total = %d, want 1", body.Total)
	}
	if len(body.Hits) != 1 || body.Hits[0].DocumentID != "doc-1" {
		t.Fatalf("hits = %#v, want doc-1", body.Hits)
	}
}

func TestSearchRejectsMissingIndex(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{Routes: StaticRoutes{
		"books": {{ShardID: 0, Client: &fakeShardClient{}}},
	}})
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestSearchQueriesOneReplicaPerShard(t *testing.T) {
	t.Parallel()

	firstReplica := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "doc-1", Score: 2}}}}
	secondReplica := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "duplicate", Score: 3}}}}
	srv := NewServer(ServerConfig{Routes: StaticRoutes{
		"books": {
			{ShardID: 0, Client: firstReplica},
			{ShardID: 0, Client: secondReplica},
		},
	}})
	createBooksIndex(t, srv, 1, 2)
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=10", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if firstReplica.calls != 1 {
		t.Fatalf("first replica calls = %d, want 1", firstReplica.calls)
	}
	if secondReplica.calls != 0 {
		t.Fatalf("second replica calls = %d, want 0", secondReplica.calls)
	}
}

func TestSearchSkipsFailedReplicaWhenReadyReplicaExists(t *testing.T) {
	t.Parallel()

	failedReplica := &fakeShardClient{err: errors.New("failed replica was queried")}
	readyReplica := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "doc-1", Score: 2}}}}
	srv := NewServer(ServerConfig{
		EventBus: events.NewMemoryBus(),
		Routes: StaticRoutes{
			"books": {
				{ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", State: ReplicaFailed, Client: failedReplica},
				{ShardID: 0, ReplicaID: "replica-b", NodeID: "node-b", State: ReplicaReady, Client: readyReplica},
			},
		},
	})
	createBooksIndex(t, srv, 1, 2)
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=10", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if failedReplica.calls != 0 {
		t.Fatalf("failed replica calls = %d, want 0", failedReplica.calls)
	}
	if readyReplica.calls != 1 {
		t.Fatalf("ready replica calls = %d, want 1", readyReplica.calls)
	}
}

func TestSearchFallsBackWhenReadyReplicaCallFails(t *testing.T) {
	t.Parallel()

	failingReplica := &fakeShardClient{err: errors.New("replica unavailable")}
	readyReplica := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "doc-1", Score: 2}}}}
	srv := NewServer(ServerConfig{
		EventBus: events.NewMemoryBus(),
		Routes: StaticRoutes{
			"books": {
				{ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", State: ReplicaReady, Client: failingReplica},
				{ShardID: 0, ReplicaID: "replica-b", NodeID: "node-b", State: ReplicaReady, Client: readyReplica},
			},
		},
	})
	createBooksIndex(t, srv, 1, 2)
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=10", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if failingReplica.calls != 1 {
		t.Fatalf("failing replica calls = %d, want 1", failingReplica.calls)
	}
	if readyReplica.calls != 1 {
		t.Fatalf("ready replica calls = %d, want 1", readyReplica.calls)
	}
}

func TestSearchRejectsMissingShardReplica(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{Routes: StaticRoutes{
		"books": {{ShardID: 0, Client: &fakeShardClient{}}},
	}})
	createBooksIndex(t, srv, 2, 1)
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=10", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestSearchReportsShardWhenNoHealthyReplicaAvailable(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{
		EventBus: events.NewMemoryBus(),
		Routes: StaticRoutes{
			"books": {{ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", State: ReplicaFailed, Client: &fakeShardClient{}}},
		},
	})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=10", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if !strings.Contains(rec.Body.String(), "books shard 0") {
		t.Fatalf("body = %q, want shard context", rec.Body.String())
	}
}

func TestSearchRequestsShardEnoughHitsForGlobalPagination(t *testing.T) {
	t.Parallel()

	client := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "doc-2", Score: 1}}}}
	srv := NewServer(ServerConfig{Routes: StaticRoutes{
		"books": {{ShardID: 0, Client: client}},
	}})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=10&offset=5", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if client.limit != 15 {
		t.Fatalf("shard limit = %d, want 15", client.limit)
	}
	if client.offset != 0 {
		t.Fatalf("shard offset = %d, want 0", client.offset)
	}
}

func TestSearchRejectsInvalidPagination(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{Routes: StaticRoutes{
		"books": {{ShardID: 0, Client: &fakeShardClient{}}},
	}})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=abc", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestSearchRejectsZeroLimit(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{Routes: StaticRoutes{
		"books": {{ShardID: 0, Client: &fakeShardClient{}}},
	}})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=0", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestSearchRejectsWindowAboveLimit(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{Routes: StaticRoutes{
		"books": {{ShardID: 0, Client: &fakeShardClient{}}},
	}})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=10000&offset=1", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestSearchRejectsOverflowingWindow(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{Routes: StaticRoutes{
		"books": {{ShardID: 0, Client: &fakeShardClient{}}},
	}})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=bleve&limit=10&offset="+strconv.Itoa(math.MaxInt), nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestSearchRejectsMalformedSearchPath(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{Routes: StaticRoutes{
		"books": {{ShardID: 0, Client: &fakeShardClient{}}},
	}})
	createBooksIndex(t, srv, 1, 1)
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books//search?q=bleve", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestSearchDoesNotQueryOtherIndexRoutes(t *testing.T) {
	t.Parallel()

	booksClient := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "book", Score: 1}}}}
	moviesClient := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "movie", Score: 1}}}}
	srv := NewServer(ServerConfig{Routes: StaticRoutes{
		"books":  {{ShardID: 0, Client: booksClient}},
		"movies": {{ShardID: 0, Client: moviesClient}},
	}})
	createBooksIndex(t, srv, 1, 1)
	createIndex(t, srv, `{"name":"movies","shardCount":1,"replicationFactor":1,"mapping":{"defaultAnalyzer":"keyword"}}`)
	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=test", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if booksClient.calls != 1 {
		t.Fatalf("books client calls = %d, want 1", booksClient.calls)
	}
	if moviesClient.calls != 0 {
		t.Fatalf("movies client calls = %d, want 0", moviesClient.calls)
	}
}

func createIndex(t *testing.T, srv *Server, body string) {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/v1/indexes", strings.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create index status = %d, want %d; body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func createBooksIndex(t *testing.T, srv *Server, shardCount, replicationFactor int) {
	t.Helper()

	createIndex(t, srv, `{"name":"books","shardCount":`+strconv.Itoa(shardCount)+`,"replicationFactor":`+strconv.Itoa(replicationFactor)+`,"mapping":{"defaultAnalyzer":"standard"}}`)
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not satisfied before timeout")
}

type fakeTablet struct {
	status tablet.Status
	result tablet.SearchResult
}

func (f *fakeTablet) Status() tablet.Status {
	return f.status
}

func (f *fakeTablet) Search(context.Context, tablet.SearchRequest) (tablet.SearchResult, error) {
	return f.result, nil
}

type fakeShardClient struct {
	result ShardSearchResult
	query  string
	limit  int
	offset int
	calls  int
	err    error
}

func (c *fakeShardClient) Search(ctx context.Context, query string, limit, offset int) (ShardSearchResult, error) {
	c.calls++
	c.query = query
	c.limit = limit
	c.offset = offset
	if c.err != nil {
		return ShardSearchResult{}, c.err
	}
	return c.result, nil
}

type reloadMetadataManager struct {
	metadata.KSMetadataManager

	mu              sync.Mutex
	failFirstLoad   bool
	closeFirstWatch bool
	snapshots       []metadata.Snapshot
	watches         int
}

func (m *reloadMetadataManager) LoadSnapshot(ctx context.Context) (metadata.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return metadata.Snapshot{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.failFirstLoad {
		m.failFirstLoad = false
		return metadata.Snapshot{}, errors.New("temporary metadata load failure")
	}
	if len(m.snapshots) == 0 {
		return metadata.Snapshot{}, nil
	}
	snapshot := m.snapshots[0]
	m.snapshots = m.snapshots[1:]
	return snapshot, nil
}

func (m *reloadMetadataManager) Watch(ctx context.Context, afterRevision int64) (<-chan metadata.WatchEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.watches++
	call := m.watches
	m.mu.Unlock()

	events := make(chan metadata.WatchEvent)
	if call == 1 && m.closeFirstWatch {
		close(events)
		return events, nil
	}
	go func() {
		defer close(events)
		<-ctx.Done()
	}()
	return events, nil
}

func closedWatch() <-chan metadata.WatchEvent {
	events := make(chan metadata.WatchEvent)
	close(events)
	return events
}

type failingEventBus struct{}

func (failingEventBus) Publish(context.Context, events.DocumentEvent) error {
	return errors.New("publish failed")
}
