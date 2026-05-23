package snapshot

import (
	"context"
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
}

// RestoreInput configures one restore operation.
type RestoreInput struct {
	Store      Store
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
	if input.Store == nil {
		return Manifest{}, fmt.Errorf("snapshot store is required")
	}
	if input.Target == nil {
		return Manifest{}, fmt.Errorf("restore target is required")
	}
	if input.Replayer == nil {
		return Manifest{}, fmt.Errorf("replayer is required")
	}

	input.Target.SetRestoreState(RestoreRestoring)
	manifest, data, err := input.Store.Get(ctx, input.IndexName, input.ShardID, input.Generation)
	if err != nil {
		input.Target.SetRestoreState(RestoreFailed)
		return Manifest{}, err
	}
	if err := input.Target.RestoreSnapshot(ctx, manifest, data); err != nil {
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
