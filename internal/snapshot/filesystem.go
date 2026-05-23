package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	manifestFile = "manifest.json"
	snapshotFile = "snapshot.bin"
)

// FilesystemStore stores snapshots under a local root for tests and development.
type FilesystemStore struct {
	root string
}

// NewFilesystemStore creates a filesystem-backed snapshot store.
func NewFilesystemStore(root string) *FilesystemStore {
	return &FilesystemStore{root: root}
}

// Put stores a snapshot payload and manifest.
func (s *FilesystemStore) Put(ctx context.Context, manifest Manifest, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := VerifySnapshot(manifest, data); err != nil {
		return err
	}

	dir := s.snapshotDir(manifest.IndexName, manifest.ShardID, manifest.ReplicaSourceNode, manifest.SnapshotGeneration)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, manifestFile), manifestData, 0o644); err != nil {
		return fmt.Errorf("write snapshot manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, snapshotFile), data, 0o644); err != nil {
		return fmt.Errorf("write snapshot data: %w", err)
	}
	return nil
}

// Get loads a snapshot payload and manifest.
func (s *FilesystemStore) Get(ctx context.Context, indexName string, shardID int, replicaSourceNode string, generation int64) (Manifest, []byte, error) {
	if err := ctx.Err(); err != nil {
		return Manifest{}, nil, err
	}

	dir := s.snapshotDir(indexName, shardID, replicaSourceNode, generation)
	manifestData, err := os.ReadFile(filepath.Join(dir, manifestFile))
	if err != nil {
		if os.IsNotExist(err) {
			return Manifest{}, nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, dir)
		}
		return Manifest{}, nil, fmt.Errorf("read snapshot manifest: %w", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return Manifest{}, nil, fmt.Errorf("decode snapshot manifest: %w", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, snapshotFile))
	if err != nil {
		if os.IsNotExist(err) {
			return Manifest{}, nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, dir)
		}
		return Manifest{}, nil, fmt.Errorf("read snapshot data: %w", err)
	}
	if err := VerifySnapshot(manifest, data); err != nil {
		return Manifest{}, nil, err
	}
	return manifest, data, nil
}

func (s *FilesystemStore) snapshotDir(indexName string, shardID int, replicaSourceNode string, generation int64) string {
	return filepath.Join(s.root, indexName, fmt.Sprintf("shard-%d", shardID), replicaSourceNode, fmt.Sprintf("generation-%06d", generation))
}
