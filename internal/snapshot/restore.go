package snapshot

import (
	"context"
	"errors"
	"fmt"
)

// RestoreState describes whether a restoring tablet may serve traffic.
type RestoreState string

const (
	// RestoreRestoring means the snapshot payload is being restored.
	RestoreRestoring RestoreState = "restoring"
	// RestoreReplaying means post-snapshot events are being replayed.
	RestoreReplaying RestoreState = "replaying"
	// RestoreReady means restore and replay completed successfully.
	RestoreReady RestoreState = "ready"
	// RestoreFailed means restore cannot complete.
	RestoreFailed RestoreState = "failed"
)

// Target receives a verified snapshot payload and restore state updates.
type Target interface {
	SetRestoreState(state RestoreState)
	RestoreSnapshot(ctx context.Context, manifest Manifest, data []byte) error
}

// Replayer replays document events after a snapshot checkpoint.
type Replayer interface {
	ReplayAfter(ctx context.Context, sequence int64) error
	ReplayAll(ctx context.Context) error
}

// Pointer is the metadata trust anchor for a durable snapshot.
type Pointer struct {
	IndexName         string
	ShardID           int
	ReplicaSourceNode string
	Generation        int64
	Checksum          string
	Checkpoint        int64
	URI               string
}

// RestoreInput configures one restore operation.
type RestoreInput struct {
	Store      Store
	Pointer    *Pointer
	Target     Target
	Replayer   Replayer
	IndexName  string
	ShardID    int
	Generation int64
}

// VerifySnapshot checks the manifest and payload checksum before restore.
func VerifySnapshot(manifest Manifest, data []byte) error {
	if err := manifest.Validate(); err != nil {
		return err
	}
	got := SHA256Hex(data)
	if got != manifest.ChecksumSHA256 {
		return fmt.Errorf("snapshot checksum mismatch: got %s want %s", got, manifest.ChecksumSHA256)
	}
	return nil
}

// Restore verifies a snapshot, restores it, replays later events, and only then marks the target ready.
func Restore(ctx context.Context, input RestoreInput) (Manifest, error) {
	if input.Target == nil {
		return Manifest{}, fmt.Errorf("restore target is required")
	}
	if input.Replayer == nil {
		return Manifest{}, fmt.Errorf("replayer is required")
	}

	input.Target.SetRestoreState(RestoreRestoring)
	if input.Pointer == nil {
		input.Target.SetRestoreState(RestoreReplaying)
		if replayErr := input.Replayer.ReplayAll(ctx); replayErr != nil {
			input.Target.SetRestoreState(RestoreFailed)
			return Manifest{}, fmt.Errorf("full replay without snapshot: %w", replayErr)
		}
		input.Target.SetRestoreState(RestoreReady)
		return Manifest{}, nil
	}
	if input.Store == nil {
		input.Target.SetRestoreState(RestoreFailed)
		return Manifest{}, fmt.Errorf("snapshot store is required")
	}

	manifest, data, err := input.Store.Get(ctx, input.Pointer.IndexName, input.Pointer.ShardID, input.Pointer.ReplicaSourceNode, input.Pointer.Generation)
	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) {
			input.Target.SetRestoreState(RestoreReplaying)
			if replayErr := input.Replayer.ReplayAll(ctx); replayErr != nil {
				input.Target.SetRestoreState(RestoreFailed)
				return Manifest{}, fmt.Errorf("full replay without snapshot: %w", replayErr)
			}
			input.Target.SetRestoreState(RestoreReady)
			return Manifest{}, nil
		}
		input.Target.SetRestoreState(RestoreFailed)
		return Manifest{}, err
	}
	if err := VerifySnapshot(manifest, data); err != nil {
		input.Target.SetRestoreState(RestoreFailed)
		return Manifest{}, err
	}
	if err := verifySnapshotIdentity(manifest, input.IndexName, input.ShardID, input.Generation); err != nil {
		input.Target.SetRestoreState(RestoreFailed)
		return Manifest{}, err
	}
	if err := verifySnapshotPointer(input.Pointer, manifest, data); err != nil {
		input.Target.SetRestoreState(RestoreFailed)
		return Manifest{}, err
	}
	rawData, err := Decompress(manifest, data)
	if err != nil {
		input.Target.SetRestoreState(RestoreFailed)
		return Manifest{}, err
	}
	if err := input.Target.RestoreSnapshot(ctx, manifest, rawData); err != nil {
		input.Target.SetRestoreState(RestoreFailed)
		return Manifest{}, fmt.Errorf("restore snapshot: %w", err)
	}

	input.Target.SetRestoreState(RestoreReplaying)
	if err := input.Replayer.ReplayAfter(ctx, manifest.LastEventSequence); err != nil {
		input.Target.SetRestoreState(RestoreFailed)
		return Manifest{}, fmt.Errorf("replay after snapshot: %w", err)
	}

	input.Target.SetRestoreState(RestoreReady)
	return manifest, nil
}

func verifySnapshotIdentity(manifest Manifest, indexName string, shardID int, generation int64) error {
	if manifest.IndexName != indexName {
		return fmt.Errorf("snapshot index %q does not match requested index %q", manifest.IndexName, indexName)
	}
	if manifest.ShardID != shardID {
		return fmt.Errorf("snapshot shard %d does not match requested shard %d", manifest.ShardID, shardID)
	}
	if manifest.SnapshotGeneration != generation {
		return fmt.Errorf("snapshot generation %d does not match requested generation %d", manifest.SnapshotGeneration, generation)
	}
	return nil
}

func verifySnapshotPointer(pointer *Pointer, manifest Manifest, data []byte) error {
	if pointer == nil {
		return nil
	}
	if pointer.IndexName != manifest.IndexName {
		return fmt.Errorf("snapshot pointer index %q does not match manifest index %q", pointer.IndexName, manifest.IndexName)
	}
	if pointer.ShardID != manifest.ShardID {
		return fmt.Errorf("snapshot pointer shard %d does not match manifest shard %d", pointer.ShardID, manifest.ShardID)
	}
	if pointer.ReplicaSourceNode != manifest.ReplicaSourceNode {
		return fmt.Errorf("snapshot pointer source node %q does not match manifest source node %q", pointer.ReplicaSourceNode, manifest.ReplicaSourceNode)
	}
	if pointer.Generation != manifest.SnapshotGeneration {
		return fmt.Errorf("snapshot pointer generation %d does not match manifest generation %d", pointer.Generation, manifest.SnapshotGeneration)
	}
	if pointer.Checkpoint != manifest.LastEventSequence {
		return fmt.Errorf("snapshot pointer checkpoint %d does not match manifest checkpoint %d", pointer.Checkpoint, manifest.LastEventSequence)
	}
	if pointer.Checksum == "" {
		return fmt.Errorf("snapshot pointer checksum is required")
	}
	got := SHA256Hex(data)
	if pointer.Checksum != got {
		return fmt.Errorf("snapshot pointer checksum mismatch: got %s want %s", got, pointer.Checksum)
	}
	if pointer.Checksum != manifest.ChecksumSHA256 {
		return fmt.Errorf("snapshot pointer checksum %s does not match manifest checksum %s", pointer.Checksum, manifest.ChecksumSHA256)
	}
	return nil
}
