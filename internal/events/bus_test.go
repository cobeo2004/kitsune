package events

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestMemoryBusPublishesAndFetchesEvent(t *testing.T) {
	t.Parallel()

	bus := NewMemoryBus()
	evt := DocumentEvent{
		ID:            "evt-1",
		SchemaVersion: CurrentSchemaVersion,
		Operation:     OperationUpsert,
		IndexName:     "books",
		ShardID:       0,
		DocumentID:    "doc-1",
		Sequence:      1,
		Fields:        map[string]any{"title": "Bleve"},
	}
	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("publish: %v", err)
	}

	got := bus.Events()
	if len(got) != 1 {
		t.Fatalf("events len = %d, want 1", len(got))
	}
	if got[0].ID != "evt-1" {
		t.Fatalf("event ID = %q, want evt-1", got[0].ID)
	}
}

func TestNATSBusPublishesJSONEventToShardSubject(t *testing.T) {
	t.Parallel()

	pub := &fakePublisher{}
	bus := NewNATSBus(pub)
	evt := DocumentEvent{
		ID:            "evt-1",
		SchemaVersion: CurrentSchemaVersion,
		Operation:     OperationUpsert,
		IndexName:     "books",
		ShardID:       2,
		DocumentID:    "doc-1",
		Sequence:      1,
		Fields:        map[string]any{"title": "Bleve"},
	}

	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if pub.subject != "kitsune.index.Ym9va3M.shard.2.events" {
		t.Fatalf("subject = %q", pub.subject)
	}
	if len(pub.opts) != 1 {
		t.Fatalf("publish opts len = %d, want 1", len(pub.opts))
	}

	var got DocumentEvent
	if err := json.Unmarshal(pub.payload, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.ID != "evt-1" || got.DocumentID != "doc-1" {
		t.Fatalf("event payload = %#v", got)
	}
}

func TestSubjectEncodesIndexNameAsSingleToken(t *testing.T) {
	t.Parallel()

	subject := Subject(DocumentEvent{IndexName: "books.en", ShardID: 2})

	if subject == "kitsune.index.books.en.shard.2.events" {
		t.Fatal("subject used raw dotted index name")
	}
	if got, want := subject, "kitsune.index.Ym9va3MuZW4.shard.2.events"; got != want {
		t.Fatalf("subject = %q, want %q", got, want)
	}
}

func TestNATSBusReturnsPublishError(t *testing.T) {
	t.Parallel()

	pub := &fakePublisher{err: errors.New("nats down")}
	bus := NewNATSBus(pub)
	err := bus.Publish(context.Background(), DocumentEvent{
		ID:            "evt-1",
		SchemaVersion: CurrentSchemaVersion,
		Operation:     OperationUpsert,
		IndexName:     "books",
		ShardID:       0,
		DocumentID:    "doc-1",
		Sequence:      1,
		Fields:        map[string]any{"title": "Bleve"},
	})
	if err == nil {
		t.Fatal("expected publish error")
	}
}

func TestNATSBusRejectsMissingPublisher(t *testing.T) {
	t.Parallel()

	bus := NewNATSBus(nil)
	err := bus.Publish(context.Background(), DocumentEvent{
		ID:            "evt-1",
		SchemaVersion: CurrentSchemaVersion,
		Operation:     OperationUpsert,
		IndexName:     "books",
		ShardID:       0,
		DocumentID:    "doc-1",
		Sequence:      1,
		Fields:        map[string]any{"title": "Bleve"},
	})
	if err == nil {
		t.Fatal("expected missing publisher to fail")
	}
}

type fakePublisher struct {
	subject string
	payload []byte
	opts    []jetstream.PublishOpt
	err     error
}

func (p *fakePublisher) Publish(_ context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if p.err != nil {
		return nil, p.err
	}
	p.subject = subject
	p.payload = append([]byte(nil), payload...)
	p.opts = append([]jetstream.PublishOpt(nil), opts...)
	return &jetstream.PubAck{}, nil
}
