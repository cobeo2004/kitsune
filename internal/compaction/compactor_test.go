package compaction

import (
	"testing"

	"github.com/cobeo2004/kitsune/internal/events"
)

func TestCompactionRejectsLaggingReplica(t *testing.T) {
	t.Parallel()

	err := CanCompact(SafetyInput{
		MinimumRequiredSequence: 10,
		ReplicaCheckpoints:      map[string]int64{"r1": 10, "r2": 7},
	})
	if err == nil {
		t.Fatal("expected lagging replica to block compaction")
	}
}

func TestCompactionRejectsMissingReplicaCheckpoints(t *testing.T) {
	t.Parallel()

	err := CanCompact(SafetyInput{MinimumRequiredSequence: 10})
	if err == nil {
		t.Fatal("expected missing checkpoint evidence to block compaction")
	}
}

func TestCompactEventsPreservesFinalDocumentState(t *testing.T) {
	t.Parallel()

	got, err := CompactEvents(SafetyInput{
		MinimumRequiredSequence: 3,
		ReplicaCheckpoints:      map[string]int64{"r1": 3, "r2": 3},
	}, []events.DocumentEvent{
		{ID: "evt-1", SchemaVersion: events.CurrentSchemaVersion, Operation: events.OperationUpsert, IndexName: "books", ShardID: 0, DocumentID: "doc-1", Sequence: 1, Fields: map[string]any{"title": "old"}},
		{ID: "evt-2", SchemaVersion: events.CurrentSchemaVersion, Operation: events.OperationDelete, IndexName: "books", ShardID: 0, DocumentID: "doc-1", Sequence: 2},
		{ID: "evt-3", SchemaVersion: events.CurrentSchemaVersion, Operation: events.OperationUpsert, IndexName: "books", ShardID: 0, DocumentID: "doc-2", Sequence: 3, Fields: map[string]any{"title": "kept"}},
	})
	if err != nil {
		t.Fatalf("compact events: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("events len = %d, want 2", len(got))
	}
	if got[0].ID != "evt-2" {
		t.Fatalf("first event ID = %q, want evt-2 tombstone", got[0].ID)
	}
	if got[1].ID != "evt-3" {
		t.Fatalf("second event ID = %q, want evt-3", got[1].ID)
	}
}
