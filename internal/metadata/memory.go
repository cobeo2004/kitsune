package metadata

import (
	"context"
	"fmt"
	"sync"
)

// MemoryManager stores metadata in memory for tests and local wiring.
type MemoryManager struct {
	mu             sync.RWMutex
	indexes        map[string]IndexRecord
	shardReplicas  map[string]ShardReplicaRecord
	tabletStatuses map[string]TabletStatusRecord
	checkpoints    map[string]CheckpointRecord
	snapshots      map[string]SnapshotPointerRecord
	watchers       map[int]chan WatchEvent
	nextWatcherID  int
	revision       int64
}

// NewMemoryManager returns an empty in-memory metadata manager.
func NewMemoryManager() *MemoryManager {
	return &MemoryManager{
		indexes:        make(map[string]IndexRecord),
		shardReplicas:  make(map[string]ShardReplicaRecord),
		tabletStatuses: make(map[string]TabletStatusRecord),
		checkpoints:    make(map[string]CheckpointRecord),
		snapshots:      make(map[string]SnapshotPointerRecord),
		watchers:       make(map[int]chan WatchEvent),
	}
}

// PutIndex creates or updates an index when expectedRevision matches.
func (m *MemoryManager) PutIndex(ctx context.Context, index IndexRecord, expectedRevision int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	current, exists := m.indexes[index.Name]
	if err := checkExpectedRevision(exists, current.Revision, expectedRevision); err != nil {
		return err
	}

	m.revision++
	index.Revision = m.revision
	m.indexes[index.Name] = index
	m.publishLocked(WatchEvent{Kind: EventKindIndex, Revision: index.Revision, Index: cloneIndexPointer(index)})

	return nil
}

// GetIndex returns a stored index by name.
func (m *MemoryManager) GetIndex(ctx context.Context, name string) (IndexRecord, error) {
	if err := ctx.Err(); err != nil {
		return IndexRecord{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	index, ok := m.indexes[name]
	if !ok {
		return IndexRecord{}, fmt.Errorf("%w: index %q", ErrNotFound, name)
	}

	return cloneIndexRecord(index), nil
}

// PutShardReplica creates or updates one shard replica assignment.
func (m *MemoryManager) PutShardReplica(ctx context.Context, replica ShardReplicaRecord, expectedRevision int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	key := shardReplicaKey(replica.IndexName, replica.ShardID, replica.ReplicaID)
	m.mu.Lock()
	defer m.mu.Unlock()

	current, exists := m.shardReplicas[key]
	if err := checkExpectedRevision(exists, current.Revision, expectedRevision); err != nil {
		return err
	}

	m.revision++
	replica.Revision = m.revision
	m.shardReplicas[key] = replica
	m.publishLocked(WatchEvent{Kind: EventKindShardReplica, Revision: replica.Revision, ShardReplica: cloneShardReplicaPointer(replica)})

	return nil
}

// ListShardReplicas returns shard replica assignments for an index.
func (m *MemoryManager) ListShardReplicas(ctx context.Context, indexName string) ([]ShardReplicaRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	replicas := make([]ShardReplicaRecord, 0)
	for _, replica := range m.shardReplicas {
		if replica.IndexName == indexName {
			replicas = append(replicas, replica)
		}
	}

	return replicas, nil
}

// PutTabletStatus creates or updates a tablet readiness record.
func (m *MemoryManager) PutTabletStatus(ctx context.Context, status TabletStatusRecord, expectedRevision int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	key := tabletStatusKey(status.IndexName, status.ShardID, status.ReplicaID)
	m.mu.Lock()
	defer m.mu.Unlock()

	current, exists := m.tabletStatuses[key]
	if err := checkExpectedRevision(exists, current.Revision, expectedRevision); err != nil {
		return err
	}

	m.revision++
	status.Revision = m.revision
	m.tabletStatuses[key] = status
	m.publishLocked(WatchEvent{Kind: EventKindTabletStatus, Revision: status.Revision, TabletStatus: cloneTabletStatusPointer(status)})

	return nil
}

// PutCheckpoint creates or updates a tablet checkpoint record.
func (m *MemoryManager) PutCheckpoint(ctx context.Context, checkpoint CheckpointRecord, expectedRevision int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	key := checkpointKey(checkpoint.IndexName, checkpoint.ShardID, checkpoint.ReplicaID)
	m.mu.Lock()
	defer m.mu.Unlock()

	current, exists := m.checkpoints[key]
	if err := checkExpectedRevision(exists, current.Revision, expectedRevision); err != nil {
		return err
	}

	m.revision++
	checkpoint.Revision = m.revision
	m.checkpoints[key] = checkpoint
	m.publishLocked(WatchEvent{Kind: EventKindCheckpoint, Revision: checkpoint.Revision, Checkpoint: cloneCheckpointPointer(checkpoint)})

	return nil
}

// GetCheckpoint returns a tablet checkpoint record.
func (m *MemoryManager) GetCheckpoint(ctx context.Context, indexName string, shardID int, replicaID string) (CheckpointRecord, error) {
	if err := ctx.Err(); err != nil {
		return CheckpointRecord{}, err
	}

	key := checkpointKey(indexName, shardID, replicaID)
	m.mu.RLock()
	defer m.mu.RUnlock()

	checkpoint, ok := m.checkpoints[key]
	if !ok {
		return CheckpointRecord{}, fmt.Errorf("%w: checkpoint %q", ErrNotFound, key)
	}

	return checkpoint, nil
}

// PutSnapshotPointer creates or updates a durable snapshot pointer record.
func (m *MemoryManager) PutSnapshotPointer(ctx context.Context, snapshot SnapshotPointerRecord, expectedRevision int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	key := snapshotPointerKey(snapshot.IndexName, snapshot.ShardID, snapshot.ReplicaID)
	m.mu.Lock()
	defer m.mu.Unlock()

	current, exists := m.snapshots[key]
	if err := checkExpectedRevision(exists, current.Revision, expectedRevision); err != nil {
		return err
	}

	m.revision++
	snapshot.Revision = m.revision
	m.snapshots[key] = snapshot
	m.publishLocked(WatchEvent{Kind: EventKindSnapshot, Revision: snapshot.Revision, SnapshotRecord: cloneSnapshotPointer(snapshot)})

	return nil
}

// GetSnapshotPointer returns a durable snapshot pointer record.
func (m *MemoryManager) GetSnapshotPointer(ctx context.Context, indexName string, shardID int, replicaID string) (SnapshotPointerRecord, error) {
	if err := ctx.Err(); err != nil {
		return SnapshotPointerRecord{}, err
	}

	key := snapshotPointerKey(indexName, shardID, replicaID)
	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshot, ok := m.snapshots[key]
	if !ok {
		return SnapshotPointerRecord{}, fmt.Errorf("%w: snapshot pointer %q", ErrNotFound, key)
	}

	return snapshot, nil
}

// LoadSnapshot returns a consistent in-memory metadata snapshot.
func (m *MemoryManager) LoadSnapshot(ctx context.Context) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshot := Snapshot{
		Revision:       m.revision,
		Indexes:        make([]IndexRecord, 0, len(m.indexes)),
		ShardReplicas:  make([]ShardReplicaRecord, 0, len(m.shardReplicas)),
		TabletStatuses: make([]TabletStatusRecord, 0, len(m.tabletStatuses)),
		Checkpoints:    make([]CheckpointRecord, 0, len(m.checkpoints)),
		Snapshots:      make([]SnapshotPointerRecord, 0, len(m.snapshots)),
	}
	for _, index := range m.indexes {
		snapshot.Indexes = append(snapshot.Indexes, cloneIndexRecord(index))
	}
	for _, replica := range m.shardReplicas {
		snapshot.ShardReplicas = append(snapshot.ShardReplicas, replica)
	}
	for _, status := range m.tabletStatuses {
		snapshot.TabletStatuses = append(snapshot.TabletStatuses, status)
	}
	for _, checkpoint := range m.checkpoints {
		snapshot.Checkpoints = append(snapshot.Checkpoints, checkpoint)
	}
	for _, snapshotPointer := range m.snapshots {
		snapshot.Snapshots = append(snapshot.Snapshots, snapshotPointer)
	}

	return snapshot, nil
}

// Watch returns metadata changes after the current call.
func (m *MemoryManager) Watch(ctx context.Context, afterRevision int64) (<-chan WatchEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	events := make(chan WatchEvent, 16)
	m.mu.Lock()
	m.nextWatcherID++
	id := m.nextWatcherID
	m.watchers[id] = events
	m.mu.Unlock()

	go func() {
		<-ctx.Done()
		m.mu.Lock()
		delete(m.watchers, id)
		close(events)
		m.mu.Unlock()
	}()

	return events, nil
}

// WatchIndexes returns index-only metadata changes.
func (m *MemoryManager) WatchIndexes(ctx context.Context, afterRevision int64) (<-chan IndexWatchEvent, error) {
	watch, err := m.Watch(ctx, afterRevision)
	if err != nil {
		return nil, err
	}

	events := make(chan IndexWatchEvent, 16)
	go func() {
		defer close(events)
		for event := range watch {
			if event.Kind != EventKindIndex && event.Err == nil {
				continue
			}
			indexEvent := IndexWatchEvent{
				Revision: event.Revision,
				Deleted:  event.Deleted,
				Err:      event.Err,
			}
			if event.Index != nil {
				indexEvent.Index = *event.Index
			}
			if !sendIndexWatchEvent(ctx, events, indexEvent) {
				return
			}
		}
	}()

	return events, nil
}

func (m *MemoryManager) publishLocked(event WatchEvent) {
	for _, watcher := range m.watchers {
		watcher <- event
	}
}

func checkExpectedRevision(exists bool, currentRevision, expectedRevision int64) error {
	if !exists && expectedRevision != 0 {
		return fmt.Errorf("%w: got %d want new record", ErrRevisionMismatch, expectedRevision)
	}
	if exists && expectedRevision != currentRevision {
		return fmt.Errorf("%w: got %d want %d", ErrRevisionMismatch, expectedRevision, currentRevision)
	}
	return nil
}

func cloneIndexRecord(index IndexRecord) IndexRecord {
	index.Mapping = cloneMapping(index.Mapping)
	return index
}

func cloneIndexPointer(index IndexRecord) *IndexRecord {
	cloned := cloneIndexRecord(index)
	return &cloned
}

func cloneShardReplicaPointer(replica ShardReplicaRecord) *ShardReplicaRecord {
	cloned := replica
	return &cloned
}

func cloneTabletStatusPointer(status TabletStatusRecord) *TabletStatusRecord {
	cloned := status
	return &cloned
}

func cloneCheckpointPointer(checkpoint CheckpointRecord) *CheckpointRecord {
	cloned := checkpoint
	return &cloned
}

func cloneSnapshotPointer(snapshot SnapshotPointerRecord) *SnapshotPointerRecord {
	cloned := snapshot
	return &cloned
}

func cloneMapping(mapping map[string]any) map[string]any {
	if mapping == nil {
		return nil
	}
	cloned := make(map[string]any, len(mapping))
	for key, value := range mapping {
		cloned[key] = value
	}
	return cloned
}
