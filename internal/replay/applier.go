package replay

import (
	"context"

	"github.com/cobeo2004/kitsune/internal/events"
	"github.com/cobeo2004/kitsune/internal/tablet"
)

// Tablet is the KSTablet write surface used by replay.
type Tablet interface {
	Upsert(ctx context.Context, req tablet.UpsertRequest) error
	Delete(ctx context.Context, documentID string) error
}

// Applier validates and applies document events to a tablet replica.
type Applier struct {
	tablet Tablet
}

// NewApplier creates an event applier for tb.
func NewApplier(tb Tablet) *Applier {
	return &Applier{tablet: tb}
}

// Apply applies one document event.
func (a *Applier) Apply(ctx context.Context, evt events.DocumentEvent) error {
	if err := events.Validate(evt); err != nil {
		return err
	}

	switch evt.Operation {
	case events.OperationUpsert:
		return a.tablet.Upsert(ctx, tablet.UpsertRequest{
			DocumentID: evt.DocumentID,
			Fields:     evt.Fields,
		})
	case events.OperationDelete:
		return a.tablet.Delete(ctx, evt.DocumentID)
	default:
		return events.Validate(evt)
	}
}
