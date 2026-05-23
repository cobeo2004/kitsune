package snapshot

import "context"

// Store persists and loads snapshot manifests plus their binary payload.
type Store interface {
	Put(ctx context.Context, manifest Manifest, data []byte) error
	Get(ctx context.Context, indexName string, shardID int, generation int64) (Manifest, []byte, error)
}
