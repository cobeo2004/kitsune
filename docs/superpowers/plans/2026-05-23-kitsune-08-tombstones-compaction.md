# Kitsune 08 Tombstones Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make deletes durable through tombstone events and define explicit compaction that preserves final document state.

**Architecture:** Extend event replay with sequence-aware document state. Tombstones remove documents from Bleve and prevent stale upserts from resurrecting deleted documents. Compaction runs only through an explicit safety check.

**Tech Stack:** Go 1.26.3, existing `internal/events`, existing `internal/replay`, existing `internal/tablet`.

---

Design spec: [../specs/2026-05-23-kitsune-08-tombstones-compaction-design.md](../specs/2026-05-23-kitsune-08-tombstones-compaction-design.md)  
Roadmap spec: [../../roadmaps/08-tombstones-compaction.md](../../roadmaps/08-tombstones-compaction.md)

## File Structure

- Modify: `internal/events/types.go` for tombstone metadata.
- Modify: `internal/replay/applier.go` for sequence-aware apply rules.
- Create: `internal/replay/state.go` for document sequence tracking.
- Create: `internal/compaction/compactor.go` for explicit compaction.
- Create: `internal/compaction/compactor_test.go` for compaction safety.

### Task 1: Tombstone Hides Document

**Files:**
- Modify: `internal/replay/applier.go`
- Test: `internal/replay/applier_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestDeleteEventRemovesDocument(t *testing.T) {
	t.Parallel()

	tb := &fakeTablet{}
	applier := NewApplier(tb)
	err := applier.Apply(context.Background(), events.DocumentEvent{
		ID: "evt-2", Operation: events.OperationDelete, IndexName: "books", DocumentID: "doc-1", Sequence: 2,
	})
	if err != nil {
		t.Fatalf("apply delete: %v", err)
	}
	if tb.deletes != 1 {
		t.Fatalf("deletes = %d, want 1", tb.deletes)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/replay -run TestDeleteEventRemovesDocument -count=1`

Expected: FAIL if delete events are not handled.

- [ ] **Step 3: Write minimal implementation**

Ensure applier handles `events.OperationDelete`:

```go
case events.OperationDelete:
	return a.tablet.Delete(ctx, evt.DocumentID)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/replay -run TestDeleteEventRemovesDocument -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/replay internal/events
git commit -m "Apply tombstone delete events

Constraint: Deletes are durable tombstones while local search removes documents.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/replay -run TestDeleteEventRemovesDocument -count=1"
```

### Task 2: Prevent Stale Resurrection

**Files:**
- Create: `internal/replay/state.go`
- Modify: `internal/replay/applier.go`
- Test: `internal/replay/applier_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestOlderUpsertDoesNotResurrectNewerTombstone(t *testing.T) {
	t.Parallel()

	tb := &fakeTablet{}
	applier := NewApplier(tb)
	applier.Apply(context.Background(), events.DocumentEvent{ID: "evt-2", Operation: events.OperationDelete, IndexName: "books", DocumentID: "doc-1", Sequence: 2})
	applier.Apply(context.Background(), events.DocumentEvent{ID: "evt-1", Operation: events.OperationUpsert, IndexName: "books", DocumentID: "doc-1", Sequence: 1, Fields: map[string]any{"title": "stale"}})

	if tb.upserts != 0 {
		t.Fatalf("upserts = %d, want 0", tb.upserts)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/replay -run TestOlderUpsertDoesNotResurrectNewerTombstone -count=1`

Expected: FAIL because stale upsert is applied.

- [ ] **Step 3: Write minimal implementation**

```go
type documentState struct {
	lastSequence int64
}

type stateTracker struct {
	seen map[string]documentState
}

func newStateTracker() *stateTracker {
	return &stateTracker{seen: make(map[string]documentState)}
}

func (s *stateTracker) accept(documentID string, sequence int64) bool {
	current := s.seen[documentID]
	if sequence <= current.lastSequence {
		return false
	}
	s.seen[documentID] = documentState{lastSequence: sequence}
	return true
}
```

Add a tracker to `Applier` and return nil for stale events before mutating the tablet.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/replay -run TestOlderUpsertDoesNotResurrectNewerTombstone -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/replay
git commit -m "Prevent stale events from resurrecting tombstones

Constraint: Replay order must preserve final document semantics.
Confidence: medium
Scope-risk: moderate
Tested: go test ./internal/replay -run TestOlderUpsertDoesNotResurrectNewerTombstone -count=1"
```

### Task 3: Explicit Compaction Safety

**Files:**
- Create: `internal/compaction/compactor.go`
- Test: `internal/compaction/compactor_test.go`

- [ ] **Step 1: Write the failing test**

```go
package compaction

import "testing"

func TestCompactionRejectsLaggingReplica(t *testing.T) {
	t.Parallel()

	err := CanCompact(SafetyInput{MinimumRequiredSequence: 10, ReplicaCheckpoints: map[string]int64{"r1": 10, "r2": 7}})
	if err == nil {
		t.Fatal("expected lagging replica to block compaction")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/compaction -run TestCompactionRejectsLaggingReplica -count=1`

Expected: FAIL with undefined `CanCompact`.

- [ ] **Step 3: Write minimal implementation**

```go
package compaction

import "fmt"

type SafetyInput struct {
	MinimumRequiredSequence int64
	ReplicaCheckpoints     map[string]int64
}

func CanCompact(input SafetyInput) error {
	for replicaID, checkpoint := range input.ReplicaCheckpoints {
		if checkpoint < input.MinimumRequiredSequence {
			return fmt.Errorf("replica %s checkpoint %d is behind compaction sequence %d", replicaID, checkpoint, input.MinimumRequiredSequence)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/compaction -run TestCompactionRejectsLaggingReplica -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/compaction
git commit -m "Gate compaction on replica checkpoints

Constraint: Compaction must not remove recovery data needed by lagging replicas.
Confidence: medium
Scope-risk: moderate
Tested: go test ./internal/compaction -run TestCompactionRejectsLaggingReplica -count=1"
```
