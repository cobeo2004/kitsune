# Kitsune 01 Bleve Tablet Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `KSTablet`, a local Bleve-backed shard replica that can open, index, search, delete, and report status.

**Architecture:** Add a focused `internal/tablet` package. `Tablet` is the only type that opens Bleve index files. It stores mapping metadata beside the index and exposes a small synchronous API.

**Tech Stack:** Go 1.26.3, `github.com/blevesearch/bleve/v2`, standard `testing`, `context`, filesystem temp dirs.

---

Design spec: [../specs/2026-05-23-kitsune-01-bleve-tablet-core-design.md](../specs/2026-05-23-kitsune-01-bleve-tablet-core-design.md)  
Roadmap spec: [../../roadmaps/01-bleve-tablet-core.md](../../roadmaps/01-bleve-tablet-core.md)

## File Structure

- Create: `internal/tablet/tablet.go` for public tablet API and lifecycle.
- Create: `internal/tablet/types.go` for identity, mapping, requests, results, and status types.
- Create: `internal/tablet/metadata.go` for mapping metadata file read/write.
- Create: `internal/tablet/tablet_test.go` for behavior tests.
- Modify: `go.mod` to add Bleve dependency through `go get github.com/blevesearch/bleve/v2`.

### Task 1: Tablet Creation and Status

**Files:**
- Create: `internal/tablet/types.go`
- Create: `internal/tablet/tablet.go`
- Test: `internal/tablet/tablet_test.go`

- [ ] **Step 1: Write the failing test**

```go
package tablet

import (
	"context"
	"testing"
)

func TestOpenCreatesReadyTablet(t *testing.T) {
	t.Parallel()

	tb, err := Open(context.Background(), Config{
		RootDir: t.TempDir(),
		Identity: Identity{
			IndexName:      "books",
			ShardID:        0,
			ReplicaID:      "replica-a",
			NodeID:         "node-a",
			MappingVersion: 1,
		},
		Mapping: DefaultMapping(),
	})
	if err != nil {
		t.Fatalf("open tablet: %v", err)
	}
	defer tb.Close()

	status := tb.Status()
	if status.State != StateReady {
		t.Fatalf("state = %s, want %s", status.State, StateReady)
	}
	if status.Identity.IndexName != "books" {
		t.Fatalf("index name = %q, want books", status.Identity.IndexName)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/tablet -run TestOpenCreatesReadyTablet -count=1`

Expected: FAIL with `package github.com/cobeo2004/kitsune/internal/tablet is not in std` or undefined `Open`.

- [ ] **Step 3: Add Bleve dependency**

Run: `go get github.com/blevesearch/bleve/v2`

Expected: `go.mod` and `go.sum` update with Bleve packages.

- [ ] **Step 4: Write minimal implementation**

```go
package tablet

type State string

const (
	StateOpening State = "opening"
	StateReady   State = "ready"
	StateFailed  State = "failed"
	StateClosed  State = "closed"
)

type Identity struct {
	IndexName      string
	ShardID        int
	ReplicaID      string
	NodeID         string
	MappingVersion int
}

type Status struct {
	Identity Identity
	State    State
}
```

```go
package tablet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	bleve "github.com/blevesearch/bleve/v2"
)

type Config struct {
	RootDir  string
	Identity Identity
	Mapping  *bleve.IndexMappingImpl
}

type Tablet struct {
	mu       sync.RWMutex
	id       Identity
	state    State
	index    bleve.Index
	indexDir string
}

func DefaultMapping() *bleve.IndexMappingImpl {
	return bleve.NewIndexMapping()
}

func Open(ctx context.Context, cfg Config) (*Tablet, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.RootDir == "" {
		return nil, fmt.Errorf("tablet root dir is required")
	}
	if cfg.Mapping == nil {
		cfg.Mapping = DefaultMapping()
	}
	indexDir := filepath.Join(cfg.RootDir, cfg.Identity.IndexName, fmt.Sprintf("shard-%d", cfg.Identity.ShardID), cfg.Identity.ReplicaID)
	if err := os.MkdirAll(indexDir, 0o755); err != nil {
		return nil, fmt.Errorf("create tablet directory: %w", err)
	}
	idx, err := bleve.New(filepath.Join(indexDir, "index.bleve"), cfg.Mapping)
	if err != nil {
		return nil, fmt.Errorf("create bleve index: %w", err)
	}
	return &Tablet{id: cfg.Identity, state: StateReady, index: idx, indexDir: indexDir}, nil
}

func (t *Tablet) Status() Status {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return Status{Identity: t.id, State: t.state}
}

func (t *Tablet) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == StateClosed {
		return nil
	}
	t.state = StateClosed
	return t.index.Close()
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/tablet -run TestOpenCreatesReadyTablet -count=1`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/tablet
git commit -m "Create local tablet lifecycle

Constraint: Bleve owns local full-text indexing.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/tablet -run TestOpenCreatesReadyTablet -count=1"
```

### Task 2: Upsert and Search

**Files:**
- Modify: `internal/tablet/types.go`
- Modify: `internal/tablet/tablet.go`
- Test: `internal/tablet/tablet_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestUpsertMakesDocumentSearchable(t *testing.T) {
	t.Parallel()

	tb, err := Open(context.Background(), Config{RootDir: t.TempDir(), Identity: Identity{IndexName: "books", ShardID: 0, ReplicaID: "r1", NodeID: "n1", MappingVersion: 1}, Mapping: DefaultMapping()})
	if err != nil {
		t.Fatalf("open tablet: %v", err)
	}
	defer tb.Close()

	err = tb.Upsert(context.Background(), UpsertRequest{
		DocumentID: "doc-1",
		Fields: map[string]any{
			"title": "Distributed systems with Go",
			"body":  "Bleve powers local full text search",
		},
	})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	result, err := tb.Search(context.Background(), SearchRequest{Query: "Bleve", Limit: 10})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(result.Hits) != 1 || result.Hits[0].DocumentID != "doc-1" {
		t.Fatalf("hits = %#v, want doc-1", result.Hits)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/tablet -run TestUpsertMakesDocumentSearchable -count=1`

Expected: FAIL with undefined `UpsertRequest` or `Tablet.Upsert`.

- [ ] **Step 3: Write minimal implementation**

```go
type UpsertRequest struct {
	DocumentID string
	Fields     map[string]any
}

type SearchRequest struct {
	Query  string
	Limit  int
	Offset int
}

type SearchHit struct {
	DocumentID string
	Score      float64
	Fields     map[string]any
}

type SearchResult struct {
	Total uint64
	Hits  []SearchHit
}
```

```go
func (t *Tablet) Upsert(ctx context.Context, req UpsertRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if req.DocumentID == "" {
		return fmt.Errorf("document id is required")
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.state != StateReady {
		return fmt.Errorf("tablet is not ready: %s", t.state)
	}
	if err := t.index.Index(req.DocumentID, req.Fields); err != nil {
		return fmt.Errorf("index document %q: %w", req.DocumentID, err)
	}
	return nil
}

func (t *Tablet) Search(ctx context.Context, req SearchRequest) (SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	if req.Limit <= 0 {
		req.Limit = 10
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.state != StateReady {
		return SearchResult{}, fmt.Errorf("tablet is not ready: %s", t.state)
	}
	query := bleve.NewQueryStringQuery(req.Query)
	search := bleve.NewSearchRequestOptions(query, req.Limit, req.Offset, false)
	search.Fields = []string{"*"}
	res, err := t.index.SearchInContext(ctx, search)
	if err != nil {
		return SearchResult{}, fmt.Errorf("search tablet: %w", err)
	}
	hits := make([]SearchHit, 0, len(res.Hits))
	for _, hit := range res.Hits {
		hits = append(hits, SearchHit{DocumentID: hit.ID, Score: hit.Score, Fields: hit.Fields})
	}
	return SearchResult{Total: res.Total, Hits: hits}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/tablet -run TestUpsertMakesDocumentSearchable -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/tablet
git commit -m "Route tablet upserts into Bleve search

Constraint: KSTablet remains the only local Bleve owner.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/tablet -run TestUpsertMakesDocumentSearchable -count=1"
```

### Task 3: Delete, Reopen, and Mapping Guard

**Files:**
- Create: `internal/tablet/metadata.go`
- Modify: `internal/tablet/tablet.go`
- Test: `internal/tablet/tablet_test.go`

- [ ] **Step 1: Write failing tests**

```go
func TestDeleteRemovesDocumentFromSearch(t *testing.T) {
	t.Parallel()
	tb, err := Open(context.Background(), Config{RootDir: t.TempDir(), Identity: Identity{IndexName: "books", ShardID: 0, ReplicaID: "r1", NodeID: "n1", MappingVersion: 1}, Mapping: DefaultMapping()})
	if err != nil {
		t.Fatalf("open tablet: %v", err)
	}
	defer tb.Close()
	if err := tb.Upsert(context.Background(), UpsertRequest{DocumentID: "doc-1", Fields: map[string]any{"title": "delete me"}}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := tb.Delete(context.Background(), "doc-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, err := tb.Search(context.Background(), SearchRequest{Query: "delete", Limit: 10})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if got.Total != 0 {
		t.Fatalf("total = %d, want 0", got.Total)
	}
}

func TestOpenRejectsMappingVersionChange(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	id := Identity{IndexName: "books", ShardID: 0, ReplicaID: "r1", NodeID: "n1", MappingVersion: 1}
	tb, err := Open(context.Background(), Config{RootDir: root, Identity: id, Mapping: DefaultMapping()})
	if err != nil {
		t.Fatalf("open tablet: %v", err)
	}
	if err := tb.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	id.MappingVersion = 2
	_, err = Open(context.Background(), Config{RootDir: root, Identity: id, Mapping: DefaultMapping()})
	if err == nil {
		t.Fatal("expected mapping version change to fail")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/tablet -run 'Test(DeleteRemovesDocumentFromSearch|OpenRejectsMappingVersionChange)' -count=1`

Expected: FAIL with undefined `Delete` and missing mapping-version guard.

- [ ] **Step 3: Write minimal implementation**

```go
package tablet

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type storedMetadata struct {
	MappingVersion int `json:"mappingVersion"`
}

func writeMetadata(dir string, id Identity) error {
	data, err := json.MarshalIndent(storedMetadata{MappingVersion: id.MappingVersion}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal tablet metadata: %w", err)
	}
	return os.WriteFile(filepath.Join(dir, "tablet.json"), data, 0o644)
}

func readMetadata(dir string) (storedMetadata, bool, error) {
	data, err := os.ReadFile(filepath.Join(dir, "tablet.json"))
	if os.IsNotExist(err) {
		return storedMetadata{}, false, nil
	}
	if err != nil {
		return storedMetadata{}, false, fmt.Errorf("read tablet metadata: %w", err)
	}
	var meta storedMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return storedMetadata{}, false, fmt.Errorf("decode tablet metadata: %w", err)
	}
	return meta, true, nil
}
```

```go
func (t *Tablet) Delete(ctx context.Context, documentID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if documentID == "" {
		return fmt.Errorf("document id is required")
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.state != StateReady {
		return fmt.Errorf("tablet is not ready: %s", t.state)
	}
	if err := t.index.Delete(documentID); err != nil {
		return fmt.Errorf("delete document %q: %w", documentID, err)
	}
	return nil
}
```

Update `Open` to read/write metadata and use `bleve.Open` when the index path exists.

```go
meta, exists, err := readMetadata(indexDir)
if err != nil {
	return nil, err
}
if exists && meta.MappingVersion != cfg.Identity.MappingVersion {
	return nil, fmt.Errorf("mapping version mismatch: stored %d requested %d", meta.MappingVersion, cfg.Identity.MappingVersion)
}
indexPath := filepath.Join(indexDir, "index.bleve")
var idx bleve.Index
if _, err := os.Stat(indexPath); err == nil {
	idx, err = bleve.Open(indexPath)
} else if os.IsNotExist(err) {
	idx, err = bleve.New(indexPath, cfg.Mapping)
	if err == nil {
		err = writeMetadata(indexDir, cfg.Identity)
	}
} else {
	err = fmt.Errorf("stat bleve index: %w", err)
}
if err != nil {
	return nil, err
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/tablet -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/tablet
git commit -m "Preserve tablet mapping and delete semantics

Constraint: Bleve mappings are immutable after index creation for MVP.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/tablet -count=1"
```
