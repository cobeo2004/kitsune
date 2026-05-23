# Kitsune 04 Multiple Indexes Static Shards Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure multiple logical indexes work from the start with static shard assignment and isolated storage/routing.

**Architecture:** Promote index identity into config validation, tablet path construction, coordinator route lookup, and request validation. Keep static assignment as the source of truth for this milestone.

**Tech Stack:** Go 1.26.3, existing `internal/tablet`, existing `internal/coordinator`, table-driven tests.

---

Design spec: [../specs/2026-05-23-kitsune-04-multiple-indexes-static-shards-design.md](../specs/2026-05-23-kitsune-04-multiple-indexes-static-shards-design.md)  
Roadmap spec: [../../roadmaps/04-multiple-indexes-static-shards.md](../../roadmaps/04-multiple-indexes-static-shards.md)

## File Structure

- Modify: `internal/tablet/tablet.go` for index-aware paths if not already complete.
- Modify: `internal/coordinator/router.go` for multi-index route validation.
- Create: `internal/coordinator/config.go` for static assignment structures.
- Create: `internal/coordinator/config_test.go` for assignment validation.

### Task 1: Storage Isolation by Index

**Files:**
- Modify: `internal/tablet/tablet_test.go`
- Modify: `internal/tablet/tablet.go`

- [ ] **Step 1: Write the failing test**

```go
func TestSameDocumentIDIsIsolatedAcrossIndexes(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	books, err := Open(context.Background(), Config{RootDir: root, Identity: Identity{IndexName: "books", ShardID: 0, ReplicaID: "r1", NodeID: "n1", MappingVersion: 1}, Mapping: DefaultMapping()})
	if err != nil {
		t.Fatalf("open books: %v", err)
	}
	defer books.Close()
	movies, err := Open(context.Background(), Config{RootDir: root, Identity: Identity{IndexName: "movies", ShardID: 0, ReplicaID: "r1", NodeID: "n1", MappingVersion: 1}, Mapping: DefaultMapping()})
	if err != nil {
		t.Fatalf("open movies: %v", err)
	}
	defer movies.Close()

	books.Upsert(context.Background(), UpsertRequest{DocumentID: "same", Fields: map[string]any{"title": "Go Search"}})
	movies.Upsert(context.Background(), UpsertRequest{DocumentID: "same", Fields: map[string]any{"title": "Action Film"}})

	got, err := books.Search(context.Background(), SearchRequest{Query: "Film", Limit: 10})
	if err != nil {
		t.Fatalf("search books: %v", err)
	}
	if got.Total != 0 {
		t.Fatalf("books total = %d, want 0", got.Total)
	}
}
```

- [ ] **Step 2: Run test to verify it fails or proves existing behavior**

Run: `go test ./internal/tablet -run TestSameDocumentIDIsIsolatedAcrossIndexes -count=1`

Expected: FAIL if paths collide; PASS if Task 01 already used index-aware paths. If it passes, keep the test as regression coverage.

- [ ] **Step 3: Write minimal implementation when needed**

Ensure tablet paths include index name:

```go
indexDir := filepath.Join(cfg.RootDir, cfg.Identity.IndexName, fmt.Sprintf("shard-%d", cfg.Identity.ShardID), cfg.Identity.ReplicaID)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/tablet -run TestSameDocumentIDIsIsolatedAcrossIndexes -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/tablet
git commit -m "Isolate tablet storage by logical index

Constraint: Kitsune supports multiple logical indexes from the start.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/tablet -run TestSameDocumentIDIsIsolatedAcrossIndexes -count=1"
```

### Task 2: Static Assignment Validation

**Files:**
- Create: `internal/coordinator/config.go`
- Test: `internal/coordinator/config_test.go`

- [ ] **Step 1: Write the failing test**

```go
package coordinator

import "testing"

func TestValidateStaticConfigRejectsMissingShardAssignment(t *testing.T) {
	t.Parallel()

	cfg := StaticConfig{
		Indexes: []IndexConfig{{Name: "books", ShardCount: 2, ReplicationFactor: 1}},
		Assignments: []ShardAssignment{{IndexName: "books", ShardID: 0, ReplicaID: "r1", NodeID: "node-a"}},
	}

	err := ValidateStaticConfig(cfg)
	if err == nil {
		t.Fatal("expected missing shard 1 assignment to fail")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/coordinator -run TestValidateStaticConfigRejectsMissingShardAssignment -count=1`

Expected: FAIL with undefined `StaticConfig`.

- [ ] **Step 3: Write minimal implementation**

```go
type IndexConfig struct {
	Name              string
	ShardCount        int
	ReplicationFactor int
}

type ShardAssignment struct {
	IndexName string
	ShardID   int
	ReplicaID string
	NodeID    string
}

type StaticConfig struct {
	Indexes     []IndexConfig
	Assignments []ShardAssignment
}

func ValidateStaticConfig(cfg StaticConfig) error {
	for _, idx := range cfg.Indexes {
		if idx.Name == "" {
			return fmt.Errorf("index name is required")
		}
		if idx.ShardCount <= 0 {
			return fmt.Errorf("index %q shard count must be positive", idx.Name)
		}
		if idx.ReplicationFactor <= 0 {
			return fmt.Errorf("index %q replication factor must be positive", idx.Name)
		}
		for shardID := 0; shardID < idx.ShardCount; shardID++ {
			count := 0
			for _, assignment := range cfg.Assignments {
				if assignment.IndexName == idx.Name && assignment.ShardID == shardID {
					count++
				}
			}
			if count < idx.ReplicationFactor {
				return fmt.Errorf("index %q shard %d has %d replicas, want %d", idx.Name, shardID, count, idx.ReplicationFactor)
			}
		}
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/coordinator -run TestValidateStaticConfigRejectsMissingShardAssignment -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/coordinator
git commit -m "Validate static shard assignments

Constraint: Static shard config is the first assignment mechanism.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/coordinator -run TestValidateStaticConfigRejectsMissingShardAssignment -count=1"
```

### Task 3: Route Isolation by Index

**Files:**
- Modify: `internal/coordinator/router.go`
- Test: `internal/coordinator/server_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestSearchDoesNotQueryOtherIndexRoutes(t *testing.T) {
	t.Parallel()
	booksClient := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "book", Score: 1}}}}
	moviesClient := &fakeShardClient{result: ShardSearchResult{Total: 1, Hits: []SearchHit{{DocumentID: "movie", Score: 1}}}}
	srv := NewServer(ServerConfig{Routes: StaticRoutes{
		"books":  {{ShardID: 0, Client: booksClient}},
		"movies": {{ShardID: 0, Client: moviesClient}},
	}})

	req := httptest.NewRequest(http.MethodGet, "/v1/indexes/books/search?q=test", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if moviesClient.calls != 0 {
		t.Fatalf("movies client calls = %d, want 0", moviesClient.calls)
	}
}
```

- [ ] **Step 2: Run test to verify it fails or proves existing behavior**

Run: `go test ./internal/coordinator -run TestSearchDoesNotQueryOtherIndexRoutes -count=1`

Expected: FAIL if router fans out globally; PASS if routing is already index-scoped.

- [ ] **Step 3: Write minimal implementation**

Ensure search route lookup is `routes[indexName]`, not iteration across all indexes.

```go
routes, ok := s.routes[indexName]
if !ok {
	http.Error(w, "index not found", http.StatusNotFound)
	return
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/coordinator -run TestSearchDoesNotQueryOtherIndexRoutes -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/coordinator
git commit -m "Keep coordinator routing isolated by index

Constraint: Multi-index requests must not cross query boundaries.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/coordinator -run TestSearchDoesNotQueryOtherIndexRoutes -count=1"
```
