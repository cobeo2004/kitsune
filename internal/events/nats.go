package events

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

type jetStreamPublisher interface {
	Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// NATSBus publishes document events to NATS JetStream.
type NATSBus struct {
	publisher jetStreamPublisher
}

// NewNATSBus creates a JetStream-backed event bus.
func NewNATSBus(publisher jetStreamPublisher) *NATSBus {
	return &NATSBus{publisher: publisher}
}

// Publish validates and publishes evt to its shard subject.
func (b *NATSBus) Publish(ctx context.Context, evt DocumentEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := Validate(evt); err != nil {
		return err
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	if _, err := b.publisher.Publish(ctx, Subject(evt), payload, jetstream.WithMsgID(evt.ID)); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}
	return nil
}

// Subject returns the JetStream subject for evt.
func Subject(evt DocumentEvent) string {
	return fmt.Sprintf("kitsune.index.%s.shard.%d.events", evt.IndexName, evt.ShardID)
}
