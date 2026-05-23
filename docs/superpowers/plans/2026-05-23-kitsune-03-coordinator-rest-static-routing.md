# Kitsune 03 Coordinator REST Static Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `KSCoordinator` with REST-first public APIs and static routing to internal search-node clients.

**Architecture:** Add `internal/coordinator` for route cache, request validation, result merging, and HTTP handlers. Use an internal `ShardClient` interface so coordinator behavior is tested without running gRPC in most tests.

**Tech Stack:** Go 1.26.3, standard `net/http`, `httptest`, `encoding/json`, internal search-node gRPC client adapter.

---

Design spec: [../specs/2026-05-23-kitsune-03-coordinator-rest-static-routing-design.md](../specs/2026-05-23-kitsune-03-coordinator-rest-static-routing-design.md)  
Roadmap spec: [../../roadmaps/03-coordinator-rest-static-routing.md](../../roadmaps/03-coordinator-rest-static-routing.md)

## File Structure

- Create: `internal/coordinator/types.go` for index, shard, route, request, and response types.
- Create: `internal/coordinator/router.go` for static route table.
- Create: `internal/coordinator/server.go` for REST handlers.
- Create: `internal/coordinator/merge.go` for result merging and pagination.
- Create: `internal/coordinator/server_test.go` for handler tests.

### Task 1: Static Routing and Result Merge

**Files:**
- Create: `internal/coordinator/types.go`
- Create: `internal/coordinator/router.go`
- Create: `internal/coordinator/merge.go`
- Test: `internal/coordinator/server_test.go`

- [ ] **Step 1: Write the failing test**

```go
package coordinator

import "testing"

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/coordinator -run TestMergeOrdersHitsByScoreAndPaginates -count=1`

Expected: FAIL with undefined `MergeResults`.

- [ ] **Step 3: Write minimal implementation**

```go
package coordinator

type SearchHit struct {
	DocumentID string  `json:"documentId"`
	Score      float64 `json:"score"`
}

type ShardSearchResult struct {
	Total uint64
	Hits  []SearchHit
}

type SearchResponse struct {
	Total uint64      `json:"total"`
	Hits  []SearchHit `json:"hits"`
}
```

```go
package coordinator

import "sort"

func MergeResults(results []ShardSearchResult, limit, offset int) SearchResponse {
	if limit <= 0 {
		limit = 10
	}
	var total uint64
	var hits []SearchHit
	for _, result := range results {
		total += result.Total
		hits = append(hits, result.Hits...)
	}
	sort.SliceStable(hits, func(i, j int) bool {
		return hits[i].Score > hits[j].Score
	})
	if offset > len(hits) {
		return SearchResponse{Total: total, Hits: nil}
	}
	end := offset + limit
	if end > len(hits) {
		end = len(hits)
	}
	return SearchResponse{Total: total, Hits: hits[offset:end]}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/coordinator -run TestMergeOrdersHitsByScoreAndPaginates -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/coordinator
git commit -m "Merge coordinator shard search results

Constraint: Coordinator owns distributed result merging.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/coordinator -run TestMergeOrdersHitsByScoreAndPaginates -count=1"
```

### Task 2: REST Validation and Payload Limit

**Files:**
- Modify: `internal/coordinator/types.go`
- Create: `internal/coordinator/server.go`
- Test: `internal/coordinator/server_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestUpsertRejectsDocumentAboveDefaultLimit(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{MaxDocumentBytes: 1024 * 1024})
	body := strings.NewReader(`{"documentId":"big","fields":{"body":"` + strings.Repeat("x", 1024*1024) + `"}}`)
	req := httptest.NewRequest(http.MethodPut, "/v1/indexes/books/documents/big", body)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/coordinator -run TestUpsertRejectsDocumentAboveDefaultLimit -count=1`

Expected: FAIL with undefined `NewServer`.

- [ ] **Step 3: Write minimal implementation**

```go
type ServerConfig struct {
	MaxDocumentBytes int64
}

type Server struct {
	mux              *http.ServeMux
	maxDocumentBytes int64
}

func NewServer(cfg ServerConfig) *Server {
	if cfg.MaxDocumentBytes <= 0 {
		cfg.MaxDocumentBytes = 1024 * 1024
	}
	s := &Server{mux: http.NewServeMux(), maxDocumentBytes: cfg.MaxDocumentBytes}
	s.mux.HandleFunc("/v1/indexes/", s.handleIndexes)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleIndexes(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/documents/") {
		r.Body = http.MaxBytesReader(w, r.Body, s.maxDocumentBytes)
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "document payload exceeds configured limit", http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}
	http.NotFound(w, r)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/coordinator -run TestUpsertRejectsDocumentAboveDefaultLimit -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/coordinator
git commit -m "Validate coordinator document payload size

Constraint: MVP default document size is 1 MiB and configurable by knobs.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/coordinator -run TestUpsertRejectsDocumentAboveDefaultLimit -count=1"
```

### Task 3: Search Handler with Fake Shard Client

**Files:**
- Modify: `internal/coordinator/server.go`
- Modify: `internal/coordinator/router.go`
- Test: `internal/coordinator/server_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestSearchRoutesToConfiguredShardClient(t *testing.T) {
	t.Parallel()

	client := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "doc-1", Score: 2}}}}
	srv := NewServer(ServerConfig{Routes: StaticRoutes{
		"books": {{ShardID: 0, Client: client}},
	}})
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/coordinator -run TestSearchRoutesToConfiguredShardClient -count=1`

Expected: FAIL with undefined `StaticRoutes`.

- [ ] **Step 3: Write minimal implementation**

```go
type ShardClient interface {
	Search(ctx context.Context, query string, limit, offset int) (ShardSearchResult, error)
}

type Route struct {
	ShardID int
	Client  ShardClient
}

type StaticRoutes map[string][]Route
```

Add `Routes StaticRoutes` to `ServerConfig`, store it on `Server`, and implement `/v1/indexes/{index}/search` by calling each configured client and encoding `MergeResults`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/coordinator -run TestSearchRoutesToConfiguredShardClient -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/coordinator
git commit -m "Route coordinator searches through static shard clients

Constraint: Static routing precedes etcd metadata.
Confidence: medium
Scope-risk: moderate
Tested: go test ./internal/coordinator -run TestSearchRoutesToConfiguredShardClient -count=1"
```
