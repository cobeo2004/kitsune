package replay

import (
	"context"
	"errors"
	"testing"

	"github.com/cobeo2004/kitsune/internal/metadata"
)

func TestMetadataCheckpointStorePersistsCheckpoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	manager := metadata.NewMemoryManager()
	store := NewMetadataCheckpointStore(manager)
	id := Identity{IndexName: "books", ShardID: 0, ReplicaID: "replica-a"}

	err := store.PutCheckpoint(ctx, id, Checkpoint{Sequence: 12, EventID: "evt-12"}, 0)
	if err != nil {
		t.Fatalf("put checkpoint: %v", err)
	}
	got, err := store.GetCheckpoint(ctx, id)
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if got.Sequence != 12 || got.EventID != "evt-12" || got.Revision == 0 {
		t.Fatalf("checkpoint = %#v", got)
	}
}

func TestMetadataCheckpointStoreReportsNoCheckpoint(t *testing.T) {
	t.Parallel()

	store := NewMetadataCheckpointStore(metadata.NewMemoryManager())
	_, err := store.GetCheckpoint(context.Background(), Identity{IndexName: "books", ShardID: 0, ReplicaID: "replica-a"})
	if !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("error = %v, want ErrNoCheckpoint", err)
	}
}
