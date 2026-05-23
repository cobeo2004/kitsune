package replay

import (
	"context"
	"testing"

	"github.com/cobeo2004/kitsune/internal/events"
	"github.com/cobeo2004/kitsune/internal/tablet"
)

func TestApplierAppliesUpsertEvent(t *testing.T) {
	t.Parallel()

	tb := &fakeTablet{}
	applier := NewApplier(tb)

	err := applier.Apply(context.Background(), events.DocumentEvent{
		ID:         "evt-1",
		Operation:  events.OperationUpsert,
		IndexName:  "books",
		ShardID:    0,
		DocumentID: "doc-1",
		Fields:     map[string]any{"title": "Bleve"},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if tb.upserts != 1 {
		t.Fatalf("upserts = %d, want 1", tb.upserts)
	}
	if tb.lastUpsert.DocumentID != "doc-1" {
		t.Fatalf("document ID = %q, want doc-1", tb.lastUpsert.DocumentID)
	}
}

func TestApplierAppliesDeleteEvent(t *testing.T) {
	t.Parallel()

	tb := &fakeTablet{}
	applier := NewApplier(tb)

	err := applier.Apply(context.Background(), events.DocumentEvent{
		ID:         "evt-1",
		Operation:  events.OperationDelete,
		IndexName:  "books",
		ShardID:    0,
		DocumentID: "doc-1",
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if tb.deletes != 1 {
		t.Fatalf("deletes = %d, want 1", tb.deletes)
	}
	if tb.lastDelete != "doc-1" {
		t.Fatalf("deleted document ID = %q, want doc-1", tb.lastDelete)
	}
}

func TestApplierRejectsInvalidEvent(t *testing.T) {
	t.Parallel()

	tb := &fakeTablet{}
	applier := NewApplier(tb)

	err := applier.Apply(context.Background(), events.DocumentEvent{
		ID:         "evt-1",
		Operation:  events.OperationUpsert,
		IndexName:  "books",
		ShardID:    0,
		DocumentID: "doc-1",
	})
	if err == nil {
		t.Fatal("expected invalid event to fail")
	}
	if tb.upserts != 0 {
		t.Fatalf("upserts = %d, want 0", tb.upserts)
	}
}

type fakeTablet struct {
	upserts    int
	deletes    int
	lastUpsert tablet.UpsertRequest
	lastDelete string
}

func (t *fakeTablet) Upsert(_ context.Context, req tablet.UpsertRequest) error {
	t.upserts++
	t.lastUpsert = req
	return nil
}

func (t *fakeTablet) Delete(_ context.Context, documentID string) error {
	t.deletes++
	t.lastDelete = documentID
	return nil
}
