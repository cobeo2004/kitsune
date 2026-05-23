package replay

import (
	"context"
	"errors"
	"testing"

	"github.com/cobeo2004/kitsune/internal/events"
	"github.com/cobeo2004/kitsune/internal/tablet"
)

func TestApplierAppliesUpsertEvent(t *testing.T) {
	t.Parallel()

	tb := &fakeTablet{}
	applier := NewApplier(tb)

	err := applier.Apply(context.Background(), events.DocumentEvent{
		ID:            "evt-1",
		SchemaVersion: events.CurrentSchemaVersion,
		Operation:     events.OperationUpsert,
		IndexName:     "books",
		ShardID:       0,
		DocumentID:    "doc-1",
		Fields:        map[string]any{"title": "Bleve"},
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
		ID:            "evt-1",
		SchemaVersion: events.CurrentSchemaVersion,
		Operation:     events.OperationDelete,
		IndexName:     "books",
		ShardID:       0,
		DocumentID:    "doc-1",
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
		ID:            "evt-1",
		SchemaVersion: events.CurrentSchemaVersion,
		Operation:     events.OperationUpsert,
		IndexName:     "books",
		ShardID:       0,
		DocumentID:    "doc-1",
	})
	if err == nil {
		t.Fatal("expected invalid event to fail")
	}
	if tb.upserts != 0 {
		t.Fatalf("upserts = %d, want 0", tb.upserts)
	}
}

func TestShardApplierRejectsWrongShard(t *testing.T) {
	t.Parallel()

	tb := &fakeTablet{}
	applier := NewShardApplier(Identity{IndexName: "books", ShardID: 1, ReplicaID: "replica-a"}, tb, nil)

	err := applier.Apply(context.Background(), events.DocumentEvent{
		ID:            "evt-1",
		SchemaVersion: events.CurrentSchemaVersion,
		Operation:     events.OperationUpsert,
		IndexName:     "books",
		ShardID:       0,
		DocumentID:    "doc-1",
		Sequence:      10,
		Fields:        map[string]any{"title": "Bleve"},
	})
	if err == nil {
		t.Fatal("expected wrong shard to fail")
	}
	if tb.upserts != 0 {
		t.Fatalf("upserts = %d, want 0", tb.upserts)
	}
}

func TestShardApplierRejectsWrongMappingVersion(t *testing.T) {
	t.Parallel()

	tb := &fakeTablet{}
	applier := NewShardApplier(Identity{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", MappingVersion: 2}, tb, nil)

	err := applier.Apply(context.Background(), events.DocumentEvent{
		ID:             "evt-1",
		SchemaVersion:  events.CurrentSchemaVersion,
		Operation:      events.OperationUpsert,
		IndexName:      "books",
		ShardID:        0,
		DocumentID:     "doc-1",
		MappingVersion: 1,
		Sequence:       10,
		Fields:         map[string]any{"title": "Bleve"},
	})
	if err == nil {
		t.Fatal("expected wrong mapping version to fail")
	}
	if tb.upserts != 0 {
		t.Fatalf("upserts = %d, want 0", tb.upserts)
	}
}

func TestShardApplierSkipsCheckpointedEvent(t *testing.T) {
	t.Parallel()

	tb := &fakeTablet{}
	store := &fakeCheckpointStore{checkpoint: Checkpoint{Sequence: 10, EventID: "evt-10", Revision: 3}}
	applier := NewShardApplier(Identity{IndexName: "books", ShardID: 0, ReplicaID: "replica-a"}, tb, store)

	err := applier.Apply(context.Background(), events.DocumentEvent{
		ID:            "evt-9",
		SchemaVersion: events.CurrentSchemaVersion,
		Operation:     events.OperationUpsert,
		IndexName:     "books",
		ShardID:       0,
		DocumentID:    "doc-1",
		Sequence:      9,
		Fields:        map[string]any{"title": "Bleve"},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if tb.upserts != 0 {
		t.Fatalf("upserts = %d, want 0", tb.upserts)
	}
	if store.puts != 0 {
		t.Fatalf("checkpoint puts = %d, want 0", store.puts)
	}
}

func TestShardApplierPersistsCheckpointAfterApply(t *testing.T) {
	t.Parallel()

	tb := &fakeTablet{}
	store := &fakeCheckpointStore{checkpoint: Checkpoint{Sequence: 10, EventID: "evt-10", Revision: 3}}
	applier := NewShardApplier(Identity{IndexName: "books", ShardID: 0, ReplicaID: "replica-a"}, tb, store)

	err := applier.Apply(context.Background(), events.DocumentEvent{
		ID:            "evt-11",
		SchemaVersion: events.CurrentSchemaVersion,
		Operation:     events.OperationUpsert,
		IndexName:     "books",
		ShardID:       0,
		DocumentID:    "doc-1",
		Sequence:      11,
		Fields:        map[string]any{"title": "Bleve"},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if tb.upserts != 1 {
		t.Fatalf("upserts = %d, want 1", tb.upserts)
	}
	if store.puts != 1 {
		t.Fatalf("checkpoint puts = %d, want 1", store.puts)
	}
	if store.lastCheckpoint.Sequence != 11 || store.lastCheckpoint.EventID != "evt-11" {
		t.Fatalf("checkpoint = %#v", store.lastCheckpoint)
	}
	if store.expectedRevision != 3 {
		t.Fatalf("expected revision = %d, want 3", store.expectedRevision)
	}
}

func TestShardApplierAcksMessageAfterApply(t *testing.T) {
	t.Parallel()

	tb := &fakeTablet{}
	applier := NewShardApplier(Identity{IndexName: "books", ShardID: 0, ReplicaID: "replica-a"}, tb, nil)
	msg := &fakeMessage{event: events.DocumentEvent{
		ID:            "evt-1",
		SchemaVersion: events.CurrentSchemaVersion,
		Operation:     events.OperationUpsert,
		IndexName:     "books",
		ShardID:       0,
		DocumentID:    "doc-1",
		Sequence:      1,
		Fields:        map[string]any{"title": "Bleve"},
	}}

	if err := applier.ApplyMessage(context.Background(), msg); err != nil {
		t.Fatalf("apply message: %v", err)
	}
	if tb.upserts != 1 {
		t.Fatalf("upserts = %d, want 1", tb.upserts)
	}
	if msg.acks != 1 {
		t.Fatalf("acks = %d, want 1", msg.acks)
	}
}

func TestShardApplierUsesMessageSequenceForCheckpoint(t *testing.T) {
	t.Parallel()

	tb := &fakeTablet{}
	store := &fakeCheckpointStore{}
	applier := NewShardApplier(Identity{IndexName: "books", ShardID: 0, ReplicaID: "replica-a"}, tb, store)
	msg := &fakeMessage{
		sequence: 42,
		event: events.DocumentEvent{
			ID:            "evt-42",
			SchemaVersion: events.CurrentSchemaVersion,
			Operation:     events.OperationUpsert,
			IndexName:     "books",
			ShardID:       0,
			DocumentID:    "doc-1",
			Sequence:      7,
			Fields:        map[string]any{"title": "Bleve"},
		},
	}

	if err := applier.ApplyMessage(context.Background(), msg); err != nil {
		t.Fatalf("apply message: %v", err)
	}
	if store.lastCheckpoint.Sequence != 42 {
		t.Fatalf("checkpoint sequence = %d, want 42", store.lastCheckpoint.Sequence)
	}
}

func TestShardApplierDoesNotAckFailedApply(t *testing.T) {
	t.Parallel()

	tb := &fakeTablet{upsertErr: errors.New("tablet down")}
	applier := NewShardApplier(Identity{IndexName: "books", ShardID: 0, ReplicaID: "replica-a"}, tb, nil)
	msg := &fakeMessage{event: events.DocumentEvent{
		ID:            "evt-1",
		SchemaVersion: events.CurrentSchemaVersion,
		Operation:     events.OperationUpsert,
		IndexName:     "books",
		ShardID:       0,
		DocumentID:    "doc-1",
		Sequence:      1,
		Fields:        map[string]any{"title": "Bleve"},
	}}

	err := applier.ApplyMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected apply message to fail")
	}
	if msg.acks != 0 {
		t.Fatalf("acks = %d, want 0", msg.acks)
	}
}

type fakeTablet struct {
	upserts    int
	deletes    int
	lastUpsert tablet.UpsertRequest
	lastDelete string
	upsertErr  error
}

func (t *fakeTablet) Upsert(_ context.Context, req tablet.UpsertRequest) error {
	if t.upsertErr != nil {
		return t.upsertErr
	}
	t.upserts++
	t.lastUpsert = req
	return nil
}

func (t *fakeTablet) Delete(_ context.Context, documentID string) error {
	t.deletes++
	t.lastDelete = documentID
	return nil
}

type fakeCheckpointStore struct {
	checkpoint       Checkpoint
	getErr           error
	puts             int
	lastIdentity     Identity
	lastCheckpoint   Checkpoint
	expectedRevision int64
}

func (s *fakeCheckpointStore) GetCheckpoint(context.Context, Identity) (Checkpoint, error) {
	if s.getErr != nil {
		return Checkpoint{}, s.getErr
	}
	return s.checkpoint, nil
}

func (s *fakeCheckpointStore) PutCheckpoint(_ context.Context, id Identity, checkpoint Checkpoint, expectedRevision int64) error {
	s.puts++
	s.lastIdentity = id
	s.lastCheckpoint = checkpoint
	s.expectedRevision = expectedRevision
	return nil
}

type fakeMessage struct {
	event    events.DocumentEvent
	sequence int64
	acks     int
}

func (m *fakeMessage) Event() events.DocumentEvent {
	return m.event
}

func (m *fakeMessage) Sequence() int64 {
	return m.sequence
}

func (m *fakeMessage) Ack(context.Context) error {
	m.acks++
	return nil
}
