package snapshot

import (
	"context"
	"errors"
)

// ErrSnapshotNotFound reports that a requested snapshot is not available.
var ErrSnapshotNotFound = errors.New("snapshot not found")

// ErrReplayHistoryUnavailable reports that retained events cannot rebuild a tablet.
var ErrReplayHistoryUnavailable = errors.New("replay history unavailable")

// Store persists and loads snapshot manifests plus their binary payload.
type Store interface {
	Put(ctx context.Context, manifest Manifest, data []byte) error
	Get(ctx context.Context, indexName string, shardID int, replicaSourceNode string, generation int64) (Manifest, []byte, error)
}
