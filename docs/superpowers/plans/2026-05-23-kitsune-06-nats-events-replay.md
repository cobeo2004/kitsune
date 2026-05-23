# Kitsune 06 NATS Events Replay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move document writes to durable NATS JetStream events and replay them into tablets from checkpoints.

**Architecture:** Define an event envelope under `internal/events`, an event bus interface with a NATS implementation, and a tablet applier that validates and applies events. Coordinator write success means event publish success; search remains eventually consistent.

**Tech Stack:** Go 1.26.3, `github.com/nats-io/nats.go/jetstream`, JSON event envelopes, existing `internal/tablet`, existing `internal/metadata`.

---

Design spec: [../specs/2026-05-23-kitsune-06-nats-events-replay-design.md](../specs/2026-05-23-kitsune-06-nats-events-replay-design.md)  
Roadmap spec: [../../roadmaps/06-nats-events-replay.md](../../roadmaps/06-nats-events-replay.md)

## File Structure

- Create: `internal/events/types.go` for event envelope and operation types.
- Create: `internal/events/validate.go` for event validation.
- Create: `internal/events/bus.go` for event bus interfaces.
- Create: `internal/events/memory.go` for tests.
- Create: `internal/events/nats.go` for JetStream implementation.
- Create: `internal/replay/applier.go` for applying events to tablets.
- Create: `internal/replay/applier_test.go` for replay behavior.

### Task 1: Event Envelope Validation

**Files:**
- Create: `internal/events/types.go`
- Create: `internal/events/validate.go`
- Test: `internal/events/events_test.go`

- [ ] **Step 1: Write the failing test**

```go
package events

import "testing"

func TestValidateRejectsMissingIndex(t *testing.T) {
	t.Parallel()

	err := Validate(DocumentEvent{
		ID:         "evt-1",
		Operation:  OperationUpsert,
		DocumentID: "doc-1",
		ShardID:    0,
		Fields:     map[string]any{"title": "Bleve"},
	})
	if err == nil {
		t.Fatal("expected missing index to fail")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/events -run TestValidateRejectsMissingIndex -count=1`

Expected: FAIL with undefined package or `DocumentEvent`.

- [ ] **Step 3: Write minimal implementation**

```go
package events

type Operation string

const (
	OperationUpsert Operation = "upsert"
	OperationDelete Operation = "delete"
)

type DocumentEvent struct {
	ID             string         `json:"id"`
	Operation      Operation      `json:"operation"`
	IndexName      string         `json:"indexName"`
	ShardID        int            `json:"shardId"`
	DocumentID     string         `json:"documentId"`
	Fields         map[string]any `json:"fields,omitempty"`
	MappingVersion int            `json:"mappingVersion"`
	Sequence       int64          `json:"sequence"`
}
```

```go
package events

import "fmt"

func Validate(evt DocumentEvent) error {
	if evt.ID == "" {
		return fmt.Errorf("event id is required")
	}
	if evt.IndexName == "" {
		return fmt.Errorf("index name is required")
	}
	if evt.DocumentID == "" {
		return fmt.Errorf("document id is required")
	}
	switch evt.Operation {
	case OperationUpsert:
		if len(evt.Fields) == 0 {
			return fmt.Errorf("upsert fields are required")
		}
	case OperationDelete:
	default:
		return fmt.Errorf("unsupported operation %q", evt.Operation)
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/events -run TestValidateRejectsMissingIndex -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/events
git commit -m "Define document event validation

Constraint: Search nodes validate direct NATS events for MVP.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/events -run TestValidateRejectsMissingIndex -count=1"
```

### Task 2: Replay Applier

**Files:**
- Create: `internal/replay/applier.go`
- Test: `internal/replay/applier_test.go`

- [ ] **Step 1: Write the failing test**

```go
package replay

import (
	"context"
	"testing"

	"github.com/cobeo2004/kitsune/internal/events"
)

func TestApplierAppliesUpsertEvent(t *testing.T) {
	t.Parallel()

	tb := &fakeTablet{}
	applier := NewApplier(tb)
	err := applier.Apply(context.Background(), events.DocumentEvent{
		ID: "evt-1", Operation: events.OperationUpsert, IndexName: "books", DocumentID: "doc-1", Fields: map[string]any{"title": "Bleve"},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if tb.upserts != 1 {
		t.Fatalf("upserts = %d, want 1", tb.upserts)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/replay -run TestApplierAppliesUpsertEvent -count=1`

Expected: FAIL with undefined `NewApplier`.

- [ ] **Step 3: Write minimal implementation**

```go
package replay

import (
	"context"

	"github.com/cobeo2004/kitsune/internal/events"
	"github.com/cobeo2004/kitsune/internal/tablet"
)

type Tablet interface {
	Upsert(ctx context.Context, req tablet.UpsertRequest) error
	Delete(ctx context.Context, documentID string) error
}

type Applier struct {
	tablet Tablet
}

func NewApplier(tb Tablet) *Applier {
	return &Applier{tablet: tb}
}

func (a *Applier) Apply(ctx context.Context, evt events.DocumentEvent) error {
	if err := events.Validate(evt); err != nil {
		return err
	}
	switch evt.Operation {
	case events.OperationUpsert:
		return a.tablet.Upsert(ctx, tablet.UpsertRequest{DocumentID: evt.DocumentID, Fields: evt.Fields})
	case events.OperationDelete:
		return a.tablet.Delete(ctx, evt.DocumentID)
	default:
		return events.Validate(evt)
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/replay -run TestApplierAppliesUpsertEvent -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/replay
git commit -m "Replay document events into tablets

Constraint: Writes become eventually consistent through durable events.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/replay -run TestApplierAppliesUpsertEvent -count=1"
```

### Task 3: Event Bus Interface and NATS Skeleton

**Files:**
- Create: `internal/events/bus.go`
- Create: `internal/events/memory.go`
- Create: `internal/events/nats.go`
- Test: `internal/events/bus_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestMemoryBusPublishesAndFetchesEvent(t *testing.T) {
	t.Parallel()

	bus := NewMemoryBus()
	evt := DocumentEvent{ID: "evt-1", Operation: OperationUpsert, IndexName: "books", DocumentID: "doc-1", Fields: map[string]any{"title": "Bleve"}}
	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("publish: %v", err)
	}
	got := bus.Events()
	if len(got) != 1 || got[0].ID != "evt-1" {
		t.Fatalf("events = %#v", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/events -run TestMemoryBusPublishesAndFetchesEvent -count=1`

Expected: FAIL with undefined `NewMemoryBus`.

- [ ] **Step 3: Write minimal implementation**

```go
type Bus interface {
	Publish(ctx context.Context, evt DocumentEvent) error
}

type MemoryBus struct {
	mu     sync.Mutex
	events []DocumentEvent
}

func NewMemoryBus() *MemoryBus {
	return &MemoryBus{}
}

func (b *MemoryBus) Publish(ctx context.Context, evt DocumentEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := Validate(evt); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, evt)
	return nil
}

func (b *MemoryBus) Events() []DocumentEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]DocumentEvent(nil), b.events...)
}
```

- [ ] **Step 4: Add NATS dependency and skeleton**

Run: `go get github.com/nats-io/nats.go`

Create `NATSBus` with constructor accepting a JetStream context or NATS connection. Keep integration tests separate from unit tests.

- [ ] **Step 5: Run tests**

Run: `go test ./internal/events ./internal/replay -count=1`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/events internal/replay
git commit -m "Introduce event bus boundary for document replay

Constraint: JetStream is the durable event bus, tested through a local boundary first.
Confidence: medium
Scope-risk: moderate
Tested: go test ./internal/events ./internal/replay -count=1"
```
