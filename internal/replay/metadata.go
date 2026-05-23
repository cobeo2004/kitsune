package replay

import (
	"context"
	"errors"
	"fmt"

	"github.com/cobeo2004/kitsune/internal/metadata"
)

// MetadataCheckpointStore persists replay checkpoints through KSMetadataManager.
type MetadataCheckpointStore struct {
	manager metadata.KSMetadataManager
}

// NewMetadataCheckpointStore creates a metadata-backed checkpoint store.
func NewMetadataCheckpointStore(manager metadata.KSMetadataManager) *MetadataCheckpointStore {
	return &MetadataCheckpointStore{manager: manager}
}

// GetCheckpoint loads the last applied event checkpoint for id.
func (s *MetadataCheckpointStore) GetCheckpoint(ctx context.Context, id Identity) (Checkpoint, error) {
	checkpoint, err := s.manager.GetCheckpoint(ctx, id.IndexName, id.ShardID, id.ReplicaID)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return Checkpoint{}, fmt.Errorf("%w: %w", ErrNoCheckpoint, err)
		}
		return Checkpoint{}, err
	}
	return Checkpoint{
		Sequence: checkpoint.Sequence,
		EventID:  checkpoint.EventID,
		Revision: checkpoint.Revision,
	}, nil
}

// PutCheckpoint stores the last applied event checkpoint for id.
func (s *MetadataCheckpointStore) PutCheckpoint(ctx context.Context, id Identity, checkpoint Checkpoint, expectedRevision int64) error {
	return s.manager.PutCheckpoint(ctx, metadata.CheckpointRecord{
		IndexName: id.IndexName,
		ShardID:   id.ShardID,
		ReplicaID: id.ReplicaID,
		Sequence:  checkpoint.Sequence,
		EventID:   checkpoint.EventID,
	}, expectedRevision)
}
