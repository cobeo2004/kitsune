// Package metadata defines Kitsune's KSMetadataManager boundary.
package metadata

import (
	"context"
	"errors"
)

var (
	// ErrNotFound reports that metadata does not exist.
	ErrNotFound = errors.New("metadata not found")

	// ErrRevisionMismatch reports a failed compare-and-swap metadata write.
	ErrRevisionMismatch = errors.New("metadata revision mismatch")
)

// IndexRecord describes a logical Kitsune index.
type IndexRecord struct {
	SchemaVersion     int            `json:"schemaVersion"`
	Name              string         `json:"name"`
	ShardCount        int            `json:"shardCount"`
	ReplicationFactor int            `json:"replicationFactor"`
	MappingVersion    int            `json:"mappingVersion"`
	Mapping           map[string]any `json:"mapping,omitempty"`
	MappingRef        string         `json:"mappingRef,omitempty"`
	Revision          int64          `json:"-"`
}

// ShardReplicaRecord describes one assigned shard replica.
type ShardReplicaRecord struct {
	IndexName string `json:"indexName"`
	ShardID   int    `json:"shardId"`
	ReplicaID string `json:"replicaId"`
	NodeID    string `json:"nodeId"`
	Revision  int64  `json:"-"`
}

// TabletStatusRecord describes a tablet's reported readiness.
type TabletStatusRecord struct {
	IndexName      string `json:"indexName"`
	ShardID        int    `json:"shardId"`
	ReplicaID      string `json:"replicaId"`
	NodeID         string `json:"nodeId"`
	State          string `json:"state"`
	LastCheckpoint int64  `json:"lastCheckpoint"`
	Revision       int64  `json:"-"`
}

// CheckpointRecord stores the last safely applied document event for a replica.
type CheckpointRecord struct {
	IndexName string `json:"indexName"`
	ShardID   int    `json:"shardId"`
	ReplicaID string `json:"replicaId"`
	Sequence  int64  `json:"sequence"`
	EventID   string `json:"eventId"`
	Revision  int64  `json:"-"`
}

// SnapshotPointerRecord points to a durable tablet snapshot.
type SnapshotPointerRecord struct {
	IndexName  string `json:"indexName"`
	ShardID    int    `json:"shardId"`
	ReplicaID  string `json:"replicaId"`
	Generation int64  `json:"generation"`
	URI        string `json:"uri"`
	Checksum   string `json:"checksum"`
	Checkpoint int64  `json:"checkpoint"`
	Revision   int64  `json:"-"`
}

// Snapshot is a consistent metadata view plus the revision it was read at.
type Snapshot struct {
	Revision       int64
	Indexes        []IndexRecord
	ShardReplicas  []ShardReplicaRecord
	TabletStatuses []TabletStatusRecord
	Checkpoints    []CheckpointRecord
	Snapshots      []SnapshotPointerRecord
}

// IndexWatchEvent describes an index metadata watch event.
type IndexWatchEvent struct {
	Index    IndexRecord
	Revision int64
	Deleted  bool
	Err      error
}

// EventKind identifies a metadata watch event type.
type EventKind string

const (
	EventKindIndex          EventKind = "index"
	EventKindShardReplica   EventKind = "shard_replica"
	EventKindTabletStatus   EventKind = "tablet_status"
	EventKindCheckpoint     EventKind = "checkpoint"
	EventKindSnapshot       EventKind = "snapshot"
	EventKindReloadRequired EventKind = "reload_required"
)

// WatchEvent describes a metadata change or a reload-required signal.
type WatchEvent struct {
	Kind           EventKind
	Revision       int64
	Deleted        bool
	ReloadRequired bool
	Err            error

	Index          *IndexRecord
	ShardReplica   *ShardReplicaRecord
	TabletStatus   *TabletStatusRecord
	Checkpoint     *CheckpointRecord
	SnapshotRecord *SnapshotPointerRecord
}

// KSMetadataManager stores authoritative Kitsune metadata.
type KSMetadataManager interface {
	PutIndex(ctx context.Context, index IndexRecord, expectedRevision int64) error
	GetIndex(ctx context.Context, name string) (IndexRecord, error)
	PutShardReplica(ctx context.Context, replica ShardReplicaRecord, expectedRevision int64) error
	ListShardReplicas(ctx context.Context, indexName string) ([]ShardReplicaRecord, error)
	PutTabletStatus(ctx context.Context, status TabletStatusRecord, expectedRevision int64) error
	PutCheckpoint(ctx context.Context, checkpoint CheckpointRecord, expectedRevision int64) error
	GetCheckpoint(ctx context.Context, indexName string, shardID int, replicaID string) (CheckpointRecord, error)
	PutSnapshotPointer(ctx context.Context, snapshot SnapshotPointerRecord, expectedRevision int64) error
	GetSnapshotPointer(ctx context.Context, indexName string, shardID int, replicaID string) (SnapshotPointerRecord, error)
	LoadSnapshot(ctx context.Context) (Snapshot, error)
	Watch(ctx context.Context, afterRevision int64) (<-chan WatchEvent, error)
	WatchIndexes(ctx context.Context, afterRevision int64) (<-chan IndexWatchEvent, error)
}

// Manager is a short package-local spelling for KSMetadataManager.
type Manager = KSMetadataManager
