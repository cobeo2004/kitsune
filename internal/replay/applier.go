package replay

import (
	"context"
	"errors"
	"fmt"

	"github.com/cobeo2004/kitsune/internal/events"
	"github.com/cobeo2004/kitsune/internal/tablet"
)

// ErrNoCheckpoint reports that no prior checkpoint exists for a replica.
var ErrNoCheckpoint = errors.New("checkpoint not found")

// Identity identifies one shard replica replay target.
type Identity struct {
	IndexName      string
	ShardID        int
	ReplicaID      string
	MappingVersion int
}

// Checkpoint records the last safely applied event for one replica.
type Checkpoint struct {
	Sequence int64
	EventID  string
	Revision int64
}

// CheckpointStore persists replay checkpoints.
type CheckpointStore interface {
	GetCheckpoint(ctx context.Context, id Identity) (Checkpoint, error)
	PutCheckpoint(ctx context.Context, id Identity, checkpoint Checkpoint, expectedRevision int64) error
}

// Message is an event message that is acknowledged after successful apply.
type Message interface {
	Event() events.DocumentEvent
	Ack(ctx context.Context) error
}

// Tablet is the KSTablet write surface used by replay.
type Tablet interface {
	Upsert(ctx context.Context, req tablet.UpsertRequest) error
	Delete(ctx context.Context, documentID string) error
}

// Applier validates and applies document events to a tablet replica.
type Applier struct {
	identity        Identity
	tablet          Tablet
	checkpointStore CheckpointStore
}

// NewApplier creates an event applier for tb.
func NewApplier(tb Tablet) *Applier {
	return &Applier{tablet: tb}
}

// NewShardApplier creates an event applier bound to one shard replica.
func NewShardApplier(id Identity, tb Tablet, checkpoints CheckpointStore) *Applier {
	return &Applier{
		identity:        id,
		tablet:          tb,
		checkpointStore: checkpoints,
	}
}

// Apply applies one document event.
func (a *Applier) Apply(ctx context.Context, evt events.DocumentEvent) error {
	if err := events.Validate(evt); err != nil {
		return err
	}
	if err := a.validateTarget(evt); err != nil {
		return err
	}

	checkpoint, err := a.checkpoint(ctx)
	if err != nil {
		return err
	}
	if checkpoint.EventID != "" && evt.Sequence <= checkpoint.Sequence {
		return nil
	}

	switch evt.Operation {
	case events.OperationUpsert:
		err = a.tablet.Upsert(ctx, tablet.UpsertRequest{
			DocumentID: evt.DocumentID,
			Fields:     evt.Fields,
		})
	case events.OperationDelete:
		err = a.tablet.Delete(ctx, evt.DocumentID)
	default:
		err = events.Validate(evt)
	}
	if err != nil {
		return err
	}

	return a.putCheckpoint(ctx, evt, checkpoint.Revision)
}

// ApplyMessage applies a message and acknowledges it only after success.
func (a *Applier) ApplyMessage(ctx context.Context, msg Message) error {
	if err := a.Apply(ctx, msg.Event()); err != nil {
		return err
	}
	if err := msg.Ack(ctx); err != nil {
		return fmt.Errorf("ack event message: %w", err)
	}
	return nil
}

func (a *Applier) validateTarget(evt events.DocumentEvent) error {
	if a.identity.IndexName == "" {
		return nil
	}
	if evt.IndexName != a.identity.IndexName {
		return fmt.Errorf("event index %q does not match tablet index %q", evt.IndexName, a.identity.IndexName)
	}
	if evt.ShardID != a.identity.ShardID {
		return fmt.Errorf("event shard %d does not match tablet shard %d", evt.ShardID, a.identity.ShardID)
	}
	if evt.MappingVersion != a.identity.MappingVersion {
		return fmt.Errorf("event mapping version %d does not match tablet mapping version %d", evt.MappingVersion, a.identity.MappingVersion)
	}
	return nil
}

func (a *Applier) checkpoint(ctx context.Context) (Checkpoint, error) {
	if a.checkpointStore == nil {
		return Checkpoint{}, nil
	}

	checkpoint, err := a.checkpointStore.GetCheckpoint(ctx, a.identity)
	if err == nil {
		return checkpoint, nil
	}
	if errors.Is(err, ErrNoCheckpoint) {
		return Checkpoint{}, nil
	}
	return Checkpoint{}, fmt.Errorf("get replay checkpoint: %w", err)
}

func (a *Applier) putCheckpoint(ctx context.Context, evt events.DocumentEvent, expectedRevision int64) error {
	if a.checkpointStore == nil {
		return nil
	}

	checkpoint := Checkpoint{
		Sequence: evt.Sequence,
		EventID:  evt.ID,
	}
	if err := a.checkpointStore.PutCheckpoint(ctx, a.identity, checkpoint, expectedRevision); err != nil {
		return fmt.Errorf("put replay checkpoint: %w", err)
	}
	return nil
}
