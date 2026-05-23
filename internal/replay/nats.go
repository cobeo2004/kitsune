package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/cobeo2004/kitsune/internal/events"
	"github.com/nats-io/nats.go/jetstream"
)

// NATSMessageSourceConfig controls JetStream fetch behavior.
type NATSMessageSourceConfig struct {
	MaxWait time.Duration
}

// NATSMessageSource fetches replay messages from a JetStream consumer.
type NATSMessageSource struct {
	consumer jetstream.Consumer
	maxWait  time.Duration
}

// NewNATSMessageSource creates a JetStream-backed replay message source.
func NewNATSMessageSource(consumer jetstream.Consumer, cfg NATSMessageSourceConfig) *NATSMessageSource {
	return &NATSMessageSource{
		consumer: consumer,
		maxWait:  cfg.MaxWait,
	}
}

// Fetch fetches and decodes up to batchSize JetStream messages.
func (s *NATSMessageSource) Fetch(ctx context.Context, batchSize int) ([]Message, error) {
	if s == nil || s.consumer == nil {
		return nil, errors.New("jetstream consumer is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if batchSize <= 0 {
		batchSize = 1
	}

	opts := make([]jetstream.FetchOpt, 0, 1)
	if s.maxWait > 0 {
		opts = append(opts, jetstream.FetchMaxWait(s.maxWait))
	}
	batch, err := s.consumer.Fetch(batchSize, opts...)
	if err != nil {
		return nil, fmt.Errorf("fetch replay events: %w", err)
	}

	messages := make([]Message, 0, batchSize)
	for msg := range batch.Messages() {
		wrapped, err := wrapNATSMessage(msg)
		if err != nil {
			return nil, err
		}
		messages = append(messages, wrapped)
	}
	if err := batch.Error(); err != nil {
		return nil, fmt.Errorf("fetch replay events: %w", err)
	}
	return messages, nil
}

type natsMessage struct {
	msg      jetstream.Msg
	event    events.DocumentEvent
	sequence int64
}

func wrapNATSMessage(msg jetstream.Msg) (*natsMessage, error) {
	var evt events.DocumentEvent
	if err := json.Unmarshal(msg.Data(), &evt); err != nil {
		return nil, fmt.Errorf("decode replay event: %w", err)
	}
	meta, err := msg.Metadata()
	if err != nil {
		return nil, fmt.Errorf("read replay event metadata: %w", err)
	}
	if meta.Sequence.Stream > math.MaxInt64 {
		return nil, fmt.Errorf("stream sequence %d overflows int64", meta.Sequence.Stream)
	}
	return &natsMessage{
		msg:      msg,
		event:    evt,
		sequence: int64(meta.Sequence.Stream),
	}, nil
}

func (m *natsMessage) Event() events.DocumentEvent {
	return m.event
}

func (m *natsMessage) Sequence() int64 {
	return m.sequence
}

func (m *natsMessage) Ack(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.msg.Ack(); err != nil {
		return fmt.Errorf("ack replay event: %w", err)
	}
	return nil
}
