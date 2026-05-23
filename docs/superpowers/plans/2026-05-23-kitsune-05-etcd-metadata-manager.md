# Kitsune 05 etcd Metadata Manager Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move authoritative index, shard, replica, and checkpoint metadata behind `KSMetadataManager`, backed by etcd first.

**Architecture:** Define an `internal/metadata` interface and in-memory tests first. Add an etcd implementation with key encoding, transactions, and watch support. Coordinator route cache consumes the interface rather than static config directly.

**Tech Stack:** Go 1.26.3, `go.etcd.io/etcd/client/v3`, standard `encoding/json`, context-aware watches.

---

Design spec: [../specs/2026-05-23-kitsune-05-etcd-metadata-manager-design.md](../specs/2026-05-23-kitsune-05-etcd-metadata-manager-design.md)  
Roadmap spec: [../../roadmaps/05-etcd-metadata-manager.md](../../roadmaps/05-etcd-metadata-manager.md)

## File Structure

- Create: `internal/metadata/types.go` for metadata records and interfaces.
- Create: `internal/metadata/memory.go` for test fake.
- Create: `internal/metadata/etcd.go` for etcd implementation.
- Create: `internal/metadata/keys.go` for key construction.
- Create: `internal/metadata/metadata_test.go` for interface behavior tests.
- Modify: `internal/coordinator/router.go` to load routes from metadata in a later task.

### Task 1: Metadata Interface and In-Memory Behavior

**Files:**
- Create: `internal/metadata/types.go`
- Create: `internal/metadata/memory.go`
- Test: `internal/metadata/metadata_test.go`

- [ ] **Step 1: Write the failing test**

```go
package metadata

import (
	"context"
	"testing"
)

func TestManagerStoresAndLoadsIndex(t *testing.T) {
	t.Parallel()

	m := NewMemoryManager()
	err := m.PutIndex(context.Background(), IndexRecord{Name: "books", ShardCount: 3, ReplicationFactor: 2, MappingVersion: 1}, 0)
	if err != nil {
		t.Fatalf("put index: %v", err)
	}
	got, err := m.GetIndex(context.Background(), "books")
	if err != nil {
		t.Fatalf("get index: %v", err)
	}
	if got.ShardCount != 3 || got.ReplicationFactor != 2 {
		t.Fatalf("got = %#v", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/metadata -run TestManagerStoresAndLoadsIndex -count=1`

Expected: FAIL with undefined package or `NewMemoryManager`.

- [ ] **Step 3: Write minimal implementation**

```go
package metadata

import "context"

type IndexRecord struct {
	Name              string
	ShardCount        int
	ReplicationFactor int
	MappingVersion    int
	Revision          int64
}

type Manager interface {
	PutIndex(ctx context.Context, index IndexRecord, expectedRevision int64) error
	GetIndex(ctx context.Context, name string) (IndexRecord, error)
}
```

```go
package metadata

import (
	"context"
	"fmt"
	"sync"
)

type MemoryManager struct {
	mu      sync.RWMutex
	indexes map[string]IndexRecord
}

func NewMemoryManager() *MemoryManager {
	return &MemoryManager{indexes: make(map[string]IndexRecord)}
}

func (m *MemoryManager) PutIndex(ctx context.Context, index IndexRecord, expectedRevision int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current, exists := m.indexes[index.Name]
	if exists && expectedRevision != current.Revision {
		return fmt.Errorf("stale index revision: got %d want %d", expectedRevision, current.Revision)
	}
	index.Revision = current.Revision + 1
	m.indexes[index.Name] = index
	return nil
}

func (m *MemoryManager) GetIndex(ctx context.Context, name string) (IndexRecord, error) {
	if err := ctx.Err(); err != nil {
		return IndexRecord{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	index, ok := m.indexes[name]
	if !ok {
		return IndexRecord{}, fmt.Errorf("index %q not found", name)
	}
	return index, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/metadata -run TestManagerStoresAndLoadsIndex -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/metadata
git commit -m "Define Kitsune metadata manager boundary

Constraint: etcd is first backend but callers depend on a Kitsune interface.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/metadata -run TestManagerStoresAndLoadsIndex -count=1"
```

### Task 2: Stale Revision Protection

**Files:**
- Modify: `internal/metadata/memory.go`
- Test: `internal/metadata/metadata_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestManagerRejectsStaleIndexUpdate(t *testing.T) {
	t.Parallel()

	m := NewMemoryManager()
	if err := m.PutIndex(context.Background(), IndexRecord{Name: "books", ShardCount: 1, ReplicationFactor: 1}, 0); err != nil {
		t.Fatalf("put index: %v", err)
	}
	err := m.PutIndex(context.Background(), IndexRecord{Name: "books", ShardCount: 2, ReplicationFactor: 1}, 0)
	if err == nil {
		t.Fatal("expected stale update to fail")
	}
}
```

- [ ] **Step 2: Run test to verify it fails or proves existing behavior**

Run: `go test ./internal/metadata -run TestManagerRejectsStaleIndexUpdate -count=1`

Expected: FAIL if stale revisions are accepted; PASS if Task 1 already includes the guard.

- [ ] **Step 3: Write minimal implementation**

Keep the guard:

```go
if exists && expectedRevision != current.Revision {
	return fmt.Errorf("stale index revision: got %d want %d", expectedRevision, current.Revision)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/metadata -run TestManagerRejectsStaleIndexUpdate -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/metadata
git commit -m "Reject stale metadata updates

Constraint: Metadata writes must be compare-and-swap safe.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/metadata -run TestManagerRejectsStaleIndexUpdate -count=1"
```

### Task 3: etcd Key Encoding and Transaction Write

**Files:**
- Create: `internal/metadata/keys.go`
- Create: `internal/metadata/etcd.go`
- Test: `internal/metadata/metadata_test.go`

- [ ] **Step 1: Write the failing key test**

```go
func TestIndexKeyIsNamespaced(t *testing.T) {
	t.Parallel()

	got := indexKey("books")
	want := "/kitsune/indexes/books"
	if got != want {
		t.Fatalf("key = %q, want %q", got, want)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/metadata -run TestIndexKeyIsNamespaced -count=1`

Expected: FAIL with undefined `indexKey`.

- [ ] **Step 3: Write minimal implementation**

```go
package metadata

func indexKey(name string) string {
	return "/kitsune/indexes/" + name
}
```

- [ ] **Step 4: Add etcd dependency and implementation skeleton**

Run: `go get go.etcd.io/etcd/client/v3`

Implementation skeleton:

```go
type EtcdManager struct {
	client *clientv3.Client
}

func NewEtcdManager(client *clientv3.Client) *EtcdManager {
	return &EtcdManager{client: client}
}
```

Use `client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", expectedRevision)).Then(clientv3.OpPut(key, string(data))).Commit()` for guarded writes.

- [ ] **Step 5: Run package tests**

Run: `go test ./internal/metadata -count=1`

Expected: PASS for non-etcd unit tests.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/metadata
git commit -m "Add etcd metadata key and transaction foundation

Constraint: etcd stores authoritative metadata.
Confidence: medium
Scope-risk: moderate
Tested: go test ./internal/metadata -count=1"
```
