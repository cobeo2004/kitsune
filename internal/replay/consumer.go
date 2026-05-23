package replay

import (
	"context"
	"errors"
)

// MessageSource fetches replay messages for one consumer pass.
type MessageSource interface {
	Fetch(ctx context.Context, batchSize int) ([]Message, error)
}

// ConsumerConfig controls replay consumer batch behavior.
type ConsumerConfig struct {
	BatchSize int
}

// Consumer fetches event messages and applies them to a tablet replica.
type Consumer struct {
	source    MessageSource
	applier   *Applier
	batchSize int
}

// NewConsumer creates a replay consumer.
func NewConsumer(source MessageSource, applier *Applier, cfg ConsumerConfig) *Consumer {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}
	return &Consumer{
		source:    source,
		applier:   applier,
		batchSize: batchSize,
	}
}

// RunOnce fetches one batch and applies messages sequentially.
func (c *Consumer) RunOnce(ctx context.Context) (int, error) {
	if c.source == nil {
		return 0, errors.New("message source is required")
	}
	if c.applier == nil {
		return 0, errors.New("replay applier is required")
	}

	messages, err := c.source.Fetch(ctx, c.batchSize)
	if err != nil {
		return 0, err
	}
	applied := 0
	for _, msg := range messages {
		if err := c.applier.ApplyMessage(ctx, msg); err != nil {
			return applied, err
		}
		applied++
	}
	return applied, nil
}
