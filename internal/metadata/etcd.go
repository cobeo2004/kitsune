package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type revisionCompareTarget int

const (
	revisionCompareCreate revisionCompareTarget = iota + 1
	revisionCompareMod
)

// EtcdManager stores Kitsune metadata in etcd.
type EtcdManager struct {
	kv      clientv3.KV
	watcher clientv3.Watcher
}

// NewEtcdManager returns an etcd-backed metadata manager.
func NewEtcdManager(client *clientv3.Client) *EtcdManager {
	return &EtcdManager{
		kv:      clientv3.NewKV(client),
		watcher: clientv3.NewWatcher(client),
	}
}

// PutIndex creates or updates an index using etcd compare-and-swap semantics.
func (m *EtcdManager) PutIndex(ctx context.Context, index IndexRecord, expectedRevision int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return m.putJSON(ctx, indexKey(index.Name), index, expectedRevision, fmt.Sprintf("index %q", index.Name))
}

// GetIndex returns an index record from etcd.
func (m *EtcdManager) GetIndex(ctx context.Context, name string) (IndexRecord, error) {
	if err := ctx.Err(); err != nil {
		return IndexRecord{}, err
	}

	resp, err := m.kv.Get(ctx, indexKey(name))
	if err != nil {
		return IndexRecord{}, fmt.Errorf("get index metadata: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return IndexRecord{}, fmt.Errorf("%w: index %q", ErrNotFound, name)
	}

	index, err := unmarshalIndexRecord(resp.Kvs[0].Value, resp.Kvs[0].ModRevision)
	if err != nil {
		return IndexRecord{}, fmt.Errorf("unmarshal index metadata: %w", err)
	}

	return index, nil
}

// PutShardReplica creates or updates one shard replica assignment.
func (m *EtcdManager) PutShardReplica(ctx context.Context, replica ShardReplicaRecord, expectedRevision int64) error {
	return m.putJSON(ctx, shardReplicaKey(replica.IndexName, replica.ShardID, replica.ReplicaID), replica, expectedRevision, fmt.Sprintf("shard replica %s/%d/%s", replica.IndexName, replica.ShardID, replica.ReplicaID))
}

// ListShardReplicas returns shard replica assignments for an index.
func (m *EtcdManager) ListShardReplicas(ctx context.Context, indexName string) ([]ShardReplicaRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	resp, err := m.kv.Get(ctx, shardReplicaPrefix(indexName), clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list shard replicas: %w", err)
	}

	replicas := make([]ShardReplicaRecord, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var replica ShardReplicaRecord
		if err := json.Unmarshal(kv.Value, &replica); err != nil {
			return nil, fmt.Errorf("unmarshal shard replica metadata: %w", err)
		}
		replica.Revision = kv.ModRevision
		replicas = append(replicas, replica)
	}

	return replicas, nil
}

// PutTabletStatus creates or updates a tablet readiness record.
func (m *EtcdManager) PutTabletStatus(ctx context.Context, status TabletStatusRecord, expectedRevision int64) error {
	return m.putJSON(ctx, tabletStatusKey(status.IndexName, status.ShardID, status.ReplicaID), status, expectedRevision, fmt.Sprintf("tablet status %s/%d/%s", status.IndexName, status.ShardID, status.ReplicaID))
}

// PutCheckpoint creates or updates a tablet checkpoint record.
func (m *EtcdManager) PutCheckpoint(ctx context.Context, checkpoint CheckpointRecord, expectedRevision int64) error {
	return m.putJSON(ctx, checkpointKey(checkpoint.IndexName, checkpoint.ShardID, checkpoint.ReplicaID), checkpoint, expectedRevision, fmt.Sprintf("checkpoint %s/%d/%s", checkpoint.IndexName, checkpoint.ShardID, checkpoint.ReplicaID))
}

// GetCheckpoint returns a tablet checkpoint record from etcd.
func (m *EtcdManager) GetCheckpoint(ctx context.Context, indexName string, shardID int, replicaID string) (CheckpointRecord, error) {
	if err := ctx.Err(); err != nil {
		return CheckpointRecord{}, err
	}

	key := checkpointKey(indexName, shardID, replicaID)
	resp, err := m.kv.Get(ctx, key)
	if err != nil {
		return CheckpointRecord{}, fmt.Errorf("get checkpoint metadata: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return CheckpointRecord{}, fmt.Errorf("%w: checkpoint %q", ErrNotFound, key)
	}

	var checkpoint CheckpointRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &checkpoint); err != nil {
		return CheckpointRecord{}, fmt.Errorf("unmarshal checkpoint metadata: %w", err)
	}
	checkpoint.Revision = resp.Kvs[0].ModRevision

	return checkpoint, nil
}

// PutSnapshotPointer creates or updates a durable snapshot pointer record.
func (m *EtcdManager) PutSnapshotPointer(ctx context.Context, snapshot SnapshotPointerRecord, expectedRevision int64) error {
	return m.putJSON(ctx, snapshotPointerKey(snapshot.IndexName, snapshot.ShardID, snapshot.ReplicaID), snapshot, expectedRevision, fmt.Sprintf("snapshot pointer %s/%d/%s", snapshot.IndexName, snapshot.ShardID, snapshot.ReplicaID))
}

// GetSnapshotPointer returns a durable snapshot pointer record.
func (m *EtcdManager) GetSnapshotPointer(ctx context.Context, indexName string, shardID int, replicaID string) (SnapshotPointerRecord, error) {
	if err := ctx.Err(); err != nil {
		return SnapshotPointerRecord{}, err
	}

	key := snapshotPointerKey(indexName, shardID, replicaID)
	resp, err := m.kv.Get(ctx, key)
	if err != nil {
		return SnapshotPointerRecord{}, fmt.Errorf("get snapshot pointer metadata: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return SnapshotPointerRecord{}, fmt.Errorf("%w: snapshot pointer %q", ErrNotFound, key)
	}

	var snapshot SnapshotPointerRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &snapshot); err != nil {
		return SnapshotPointerRecord{}, fmt.Errorf("unmarshal snapshot pointer metadata: %w", err)
	}
	snapshot.Revision = resp.Kvs[0].ModRevision

	return snapshot, nil
}

// LoadSnapshot returns a full metadata snapshot and its etcd revision.
func (m *EtcdManager) LoadSnapshot(ctx context.Context) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}

	resp, err := m.kv.Get(ctx, rootPrefix, clientv3.WithPrefix())
	if err != nil {
		return Snapshot{}, fmt.Errorf("load metadata snapshot: %w", err)
	}

	snapshot := Snapshot{Revision: resp.Header.GetRevision()}
	for _, kv := range resp.Kvs {
		event, err := watchEventFromKeyValue(string(kv.Key), kv.Value, kv.ModRevision, mvccpb.PUT)
		if err != nil {
			return Snapshot{}, err
		}
		appendSnapshotEvent(&snapshot, event)
	}

	return snapshot, nil
}

func (m *EtcdManager) putJSON(ctx context.Context, key string, value any, expectedRevision int64, label string) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal %s metadata: %w", label, err)
	}
	txn, err := m.kv.Txn(ctx).
		If(revisionCompare(key, expectedRevision)).
		Then(clientv3.OpPut(key, string(data))).
		Commit()
	if err != nil {
		return fmt.Errorf("put %s metadata: %w", label, err)
	}
	if !txn.Succeeded {
		return fmt.Errorf("%w: %s expected revision %d", ErrRevisionMismatch, label, expectedRevision)
	}

	return nil
}

// WatchIndexes watches index metadata changes from etcd.
func (m *EtcdManager) WatchIndexes(ctx context.Context, afterRevision int64) (<-chan IndexWatchEvent, error) {
	watch, err := m.Watch(ctx, afterRevision)
	if err != nil {
		return nil, err
	}

	events := make(chan IndexWatchEvent)
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

// Watch watches all Kitsune metadata changes from etcd.
func (m *EtcdManager) Watch(ctx context.Context, afterRevision int64) (<-chan WatchEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	opts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithPrevKV()}
	if afterRevision > 0 {
		opts = append(opts, clientv3.WithRev(afterRevision+1))
	}

	events := make(chan WatchEvent)
	watch := m.watcher.Watch(ctx, rootPrefix, opts...)
	go forwardWatchEvents(ctx, watch, events)

	return events, nil
}

func marshalIndexRecord(index IndexRecord) ([]byte, error) {
	return json.Marshal(index)
}

func unmarshalIndexRecord(data []byte, revision int64) (IndexRecord, error) {
	var index IndexRecord
	if err := json.Unmarshal(data, &index); err != nil {
		return IndexRecord{}, err
	}
	index.Revision = revision

	return index, nil
}

func compareTargetForExpectedRevision(expectedRevision int64) revisionCompareTarget {
	if expectedRevision == 0 {
		return revisionCompareCreate
	}

	return revisionCompareMod
}

func revisionCompare(key string, expectedRevision int64) clientv3.Cmp {
	if compareTargetForExpectedRevision(expectedRevision) == revisionCompareCreate {
		return clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	}

	return clientv3.Compare(clientv3.ModRevision(key), "=", expectedRevision)
}

func forwardWatchEvents(ctx context.Context, watch clientv3.WatchChan, out chan<- WatchEvent) {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-watch:
			if !ok {
				return
			}
			if resp.CompactRevision > 0 {
				sendWatchEvent(ctx, out, WatchEvent{
					Kind:           EventKindReloadRequired,
					Revision:       resp.CompactRevision,
					ReloadRequired: true,
					Err:            fmt.Errorf("metadata watch compacted at revision %d", resp.CompactRevision),
				})
				return
			}
			if err := resp.Err(); err != nil {
				sendWatchEvent(ctx, out, WatchEvent{Revision: resp.Header.GetRevision(), Err: err})
				return
			}
			for _, event := range resp.Events {
				watchEvent, err := watchEventFromEtcd(event)
				if err != nil {
					watchEvent = WatchEvent{
						Revision: event.Kv.ModRevision,
						Err:      err,
					}
				}
				if !sendWatchEvent(ctx, out, watchEvent) {
					return
				}
			}
		}
	}
}

func watchEventFromEtcd(event *clientv3.Event) (WatchEvent, error) {
	kv := event.Kv
	if event.Type == mvccpb.DELETE && event.PrevKv != nil {
		kv = event.PrevKv
	}
	return watchEventFromKeyValue(string(kv.Key), kv.Value, event.Kv.ModRevision, event.Type)
}

func watchEventFromKeyValue(key string, data []byte, revision int64, eventType mvccpb.Event_EventType) (WatchEvent, error) {
	watchEvent := WatchEvent{
		Revision: revision,
		Deleted:  eventType == mvccpb.DELETE,
	}
	if strings.HasPrefix(key, indexPrefix) && strings.HasSuffix(key, "/config") {
		index, err := unmarshalIndexRecord(data, revision)
		if err != nil {
			return WatchEvent{}, fmt.Errorf("unmarshal watched index metadata: %w", err)
		}
		watchEvent.Kind = EventKindIndex
		watchEvent.Index = &index
		return watchEvent, nil
	}
	if strings.HasPrefix(key, indexPrefix) && strings.Contains(key, "/shards/") {
		var replica ShardReplicaRecord
		if err := json.Unmarshal(data, &replica); err != nil {
			return WatchEvent{}, fmt.Errorf("unmarshal watched shard replica metadata: %w", err)
		}
		replica.Revision = revision
		watchEvent.Kind = EventKindShardReplica
		watchEvent.ShardReplica = &replica
		return watchEvent, nil
	}
	if strings.HasPrefix(key, tabletPrefix) {
		var status TabletStatusRecord
		if err := json.Unmarshal(data, &status); err != nil {
			return WatchEvent{}, fmt.Errorf("unmarshal watched tablet status metadata: %w", err)
		}
		status.Revision = revision
		watchEvent.Kind = EventKindTabletStatus
		watchEvent.TabletStatus = &status
		return watchEvent, nil
	}
	if strings.HasPrefix(key, checkpointPrefix) {
		var checkpoint CheckpointRecord
		if err := json.Unmarshal(data, &checkpoint); err != nil {
			return WatchEvent{}, fmt.Errorf("unmarshal watched checkpoint metadata: %w", err)
		}
		checkpoint.Revision = revision
		watchEvent.Kind = EventKindCheckpoint
		watchEvent.Checkpoint = &checkpoint
		return watchEvent, nil
	}
	if strings.HasPrefix(key, snapshotPrefix) {
		var snapshot SnapshotPointerRecord
		if err := json.Unmarshal(data, &snapshot); err != nil {
			return WatchEvent{}, fmt.Errorf("unmarshal watched snapshot metadata: %w", err)
		}
		snapshot.Revision = revision
		watchEvent.Kind = EventKindSnapshot
		watchEvent.SnapshotRecord = &snapshot
		return watchEvent, nil
	}

	return WatchEvent{}, fmt.Errorf("unknown metadata key %q", key)
}

func appendSnapshotEvent(snapshot *Snapshot, event WatchEvent) {
	switch event.Kind {
	case EventKindIndex:
		if event.Index != nil {
			snapshot.Indexes = append(snapshot.Indexes, *event.Index)
		}
	case EventKindShardReplica:
		if event.ShardReplica != nil {
			snapshot.ShardReplicas = append(snapshot.ShardReplicas, *event.ShardReplica)
		}
	case EventKindTabletStatus:
		if event.TabletStatus != nil {
			snapshot.TabletStatuses = append(snapshot.TabletStatuses, *event.TabletStatus)
		}
	case EventKindCheckpoint:
		if event.Checkpoint != nil {
			snapshot.Checkpoints = append(snapshot.Checkpoints, *event.Checkpoint)
		}
	case EventKindSnapshot:
		if event.SnapshotRecord != nil {
			snapshot.Snapshots = append(snapshot.Snapshots, *event.SnapshotRecord)
		}
	}
}

func sendIndexWatchEvent(ctx context.Context, out chan<- IndexWatchEvent, event IndexWatchEvent) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- event:
		return true
	}
}

func sendWatchEvent(ctx context.Context, out chan<- WatchEvent, event WatchEvent) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- event:
		return true
	}
}
