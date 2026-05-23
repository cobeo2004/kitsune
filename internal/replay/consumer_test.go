package replay

import (
	"context"
	"testing"

	"github.com/cobeo2004/kitsune/internal/events"
)

func TestConsumerRunOnceAppliesFetchedMessages(t *testing.T) {
	t.Parallel()

	tb := &fakeTablet{}
	applier := NewShardApplier(Identity{IndexName: "books", ShardID: 0, ReplicaID: "replica-a"}, tb, nil)
	source := &fakeMessageSource{messages: []Message{
		&fakeMessage{sequence: 1, event: events.DocumentEvent{
			ID:              "evt-1",
			SchemaVersion:   events.CurrentSchemaVersion,
			Operation:       events.OperationUpsert,
			IndexName:       "books",
			ShardID:         0,
			DocumentID:      "doc-1",
			DocumentVersion: 1,
			Fields:          map[string]any{"title": "Bleve"},
		}},
	}}
	consumer := NewConsumer(source, applier, ConsumerConfig{BatchSize: 10})

	applied, err := consumer.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if applied != 1 {
		t.Fatalf("applied = %d, want 1", applied)
	}
	if tb.upserts != 1 {
		t.Fatalf("upserts = %d, want 1", tb.upserts)
	}
	if source.batchSize != 10 {
		t.Fatalf("batch size = %d, want 10", source.batchSize)
	}
}

type fakeMessageSource struct {
	messages  []Message
	batchSize int
	err       error
}

func (s *fakeMessageSource) Fetch(ctx context.Context, batchSize int) ([]Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.err != nil {
		return nil, s.err
	}
	s.batchSize = batchSize
	return append([]Message(nil), s.messages...), nil
}
