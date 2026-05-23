package events

import "context"

// Bus publishes durable document events.
type Bus interface {
	Publish(ctx context.Context, evt DocumentEvent) error
}
