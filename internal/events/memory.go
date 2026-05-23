package events

import (
	"context"
	"sync"
)

// MemoryBus stores events in memory for tests and local development.
type MemoryBus struct {
	mu     sync.Mutex
	events []DocumentEvent
}

// NewMemoryBus creates an empty in-memory event bus.
func NewMemoryBus() *MemoryBus {
	return &MemoryBus{}
}

// Publish validates and stores evt.
func (b *MemoryBus) Publish(ctx context.Context, evt DocumentEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := Validate(evt); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.events = append(b.events, cloneEvent(evt))
	return nil
}

// Events returns a snapshot of published events.
func (b *MemoryBus) Events() []DocumentEvent {
	b.mu.Lock()
	defer b.mu.Unlock()

	events := make([]DocumentEvent, 0, len(b.events))
	for _, evt := range b.events {
		events = append(events, cloneEvent(evt))
	}
	return events
}

func cloneEvent(evt DocumentEvent) DocumentEvent {
	evt.Fields = cloneFields(evt.Fields)
	return evt
}

func cloneFields(fields map[string]any) map[string]any {
	if fields == nil {
		return nil
	}
	out := make(map[string]any, len(fields))
	for key, value := range fields {
		out[key] = value
	}
	return out
}
