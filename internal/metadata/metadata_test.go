package metadata

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestMemoryManagerStoresAndLoadsIndex(t *testing.T) {
	t.Parallel()

	m := NewMemoryManager()
	err := m.PutIndex(context.Background(), IndexRecord{
		Name:              "books",
		ShardCount:        3,
		ReplicationFactor: 2,
		MappingVersion:    1,
	}, 0)
	if err != nil {
		t.Fatalf("put index: %v", err)
	}

	got, err := m.GetIndex(context.Background(), "books")
	if err != nil {
		t.Fatalf("get index: %v", err)
	}

	if got.Name != "books" {
		t.Errorf("Name = %q, want %q", got.Name, "books")
	}
	if got.ShardCount != 3 {
		t.Errorf("ShardCount = %d, want %d", got.ShardCount, 3)
	}
	if got.ReplicationFactor != 2 {
		t.Errorf("ReplicationFactor = %d, want %d", got.ReplicationFactor, 2)
	}
	if got.MappingVersion != 1 {
		t.Errorf("MappingVersion = %d, want %d", got.MappingVersion, 1)
	}
	if got.Revision != 1 {
		t.Errorf("Revision = %d, want %d", got.Revision, 1)
	}
}

func TestMemoryManagerRejectsStaleIndexUpdate(t *testing.T) {
	t.Parallel()

	m := NewMemoryManager()
	err := m.PutIndex(context.Background(), IndexRecord{
		Name:              "books",
		ShardCount:        1,
		ReplicationFactor: 1,
	}, 0)
	if err != nil {
		t.Fatalf("put index: %v", err)
	}

	err = m.PutIndex(context.Background(), IndexRecord{
		Name:              "books",
		ShardCount:        2,
		ReplicationFactor: 1,
	}, 0)
	if !errors.Is(err, ErrRevisionMismatch) {
		t.Fatalf("error = %v, want %v", err, ErrRevisionMismatch)
	}
}

func TestMemoryManagerUpdatesWithCurrentRevision(t *testing.T) {
	t.Parallel()

	m := NewMemoryManager()
	if err := m.PutIndex(context.Background(), IndexRecord{Name: "books", ShardCount: 1, ReplicationFactor: 1}, 0); err != nil {
		t.Fatalf("create index: %v", err)
	}

	current, err := m.GetIndex(context.Background(), "books")
	if err != nil {
		t.Fatalf("get index: %v", err)
	}

	err = m.PutIndex(context.Background(), IndexRecord{
		Name:              "books",
		ShardCount:        4,
		ReplicationFactor: 2,
		MappingVersion:    3,
	}, current.Revision)
	if err != nil {
		t.Fatalf("update index: %v", err)
	}

	got, err := m.GetIndex(context.Background(), "books")
	if err != nil {
		t.Fatalf("get updated index: %v", err)
	}
	if got.ShardCount != 4 {
		t.Errorf("ShardCount = %d, want %d", got.ShardCount, 4)
	}
	if got.Revision != current.Revision+1 {
		t.Errorf("Revision = %d, want %d", got.Revision, current.Revision+1)
	}
}

func TestMemoryManagerReturnsNotFound(t *testing.T) {
	t.Parallel()

	m := NewMemoryManager()
	_, err := m.GetIndex(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v, want %v", err, ErrNotFound)
	}
}

func TestIndexKeyIsNamespaced(t *testing.T) {
	t.Parallel()

	got := indexKey("books")
	want := "/kitsune/indexes/books/config"
	if got != want {
		t.Fatalf("key = %q, want %q", got, want)
	}
}

func TestShardReplicaKeyIsHierarchical(t *testing.T) {
	t.Parallel()

	got := shardReplicaKey("books", 2, "replica-a")
	want := "/kitsune/indexes/books/shards/2/replicas/replica-a"
	if got != want {
		t.Fatalf("key = %q, want %q", got, want)
	}
}

func TestMemoryManagerStoresAssignmentsStatusAndCheckpoints(t *testing.T) {
	t.Parallel()

	m := NewMemoryManager()
	ctx := context.Background()

	if err := m.PutShardReplica(ctx, ShardReplicaRecord{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"}, 0); err != nil {
		t.Fatalf("put shard replica: %v", err)
	}
	replicas, err := m.ListShardReplicas(ctx, "books")
	if err != nil {
		t.Fatalf("list shard replicas: %v", err)
	}
	if len(replicas) != 1 || replicas[0].NodeID != "node-a" {
		t.Fatalf("replicas = %#v, want node-a", replicas)
	}

	if err := m.PutTabletStatus(ctx, TabletStatusRecord{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", State: "ready", LastCheckpoint: 11}, 0); err != nil {
		t.Fatalf("put tablet status: %v", err)
	}
	if err := m.PutCheckpoint(ctx, CheckpointRecord{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", Sequence: 12, EventID: "evt-12"}, 0); err != nil {
		t.Fatalf("put checkpoint: %v", err)
	}
	checkpoint, err := m.GetCheckpoint(ctx, "books", 0, "replica-a")
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if checkpoint.Sequence != 12 {
		t.Fatalf("checkpoint sequence = %d, want 12", checkpoint.Sequence)
	}

	if err := m.PutSnapshotPointer(ctx, SnapshotPointerRecord{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", Generation: 1, URI: "s3://kitsune/snapshots/books/0/replica-a", Checkpoint: 12}, 0); err != nil {
		t.Fatalf("put snapshot pointer: %v", err)
	}
	snapshot, err := m.GetSnapshotPointer(ctx, "books", 0, "replica-a")
	if err != nil {
		t.Fatalf("get snapshot pointer: %v", err)
	}
	if snapshot.URI == "" || snapshot.Checkpoint != 12 {
		t.Fatalf("snapshot pointer = %#v, want uri and checkpoint 12", snapshot)
	}
}

func TestMemoryManagerSnapshotIncludesRoutingMetadata(t *testing.T) {
	t.Parallel()

	m := NewMemoryManager()
	ctx := context.Background()
	if err := m.PutIndex(ctx, IndexRecord{Name: "books", ShardCount: 1, ReplicationFactor: 1, MappingVersion: 1, Mapping: map[string]any{"defaultAnalyzer": "standard"}}, 0); err != nil {
		t.Fatalf("put index: %v", err)
	}
	if err := m.PutShardReplica(ctx, ShardReplicaRecord{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"}, 0); err != nil {
		t.Fatalf("put shard replica: %v", err)
	}

	snapshot, err := m.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if snapshot.Revision != 2 {
		t.Fatalf("snapshot revision = %d, want 2", snapshot.Revision)
	}
	if len(snapshot.Indexes) != 1 || len(snapshot.ShardReplicas) != 1 {
		t.Fatalf("snapshot = %#v, want index and shard replica", snapshot)
	}
}

func TestMemoryManagerWatchEmitsIndexUpdates(t *testing.T) {
	t.Parallel()

	m := NewMemoryManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watch, err := m.Watch(ctx, 0)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	if err := m.PutIndex(context.Background(), IndexRecord{Name: "books", ShardCount: 1, ReplicationFactor: 1}, 0); err != nil {
		t.Fatalf("put index: %v", err)
	}

	select {
	case event := <-watch:
		if event.Kind != EventKindIndex || event.Index == nil || event.Index.Name != "books" {
			t.Fatalf("event = %#v, want books index event", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for metadata watch event")
	}
}

func TestIndexRecordJSONRoundTrip(t *testing.T) {
	t.Parallel()

	give := IndexRecord{
		Name:              "books",
		ShardCount:        3,
		ReplicationFactor: 2,
		MappingVersion:    7,
		Mapping:           map[string]any{"defaultAnalyzer": "standard"},
		Revision:          11,
	}

	data, err := marshalIndexRecord(give)
	if err != nil {
		t.Fatalf("marshal index record: %v", err)
	}
	got, err := unmarshalIndexRecord(data, give.Revision)
	if err != nil {
		t.Fatalf("unmarshal index record: %v", err)
	}

	if !reflect.DeepEqual(got, give) {
		t.Fatalf("record = %#v, want %#v", got, give)
	}
}

func TestWatchEventFromDeletedKeyCarriesIdentity(t *testing.T) {
	t.Parallel()

	data, err := marshalIndexRecord(IndexRecord{Name: "books", ShardCount: 1, ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("marshal index: %v", err)
	}

	event, err := watchEventFromKeyValue(indexKey("books"), data, 9, mvccpb.DELETE)
	if err != nil {
		t.Fatalf("watch event: %v", err)
	}
	if !event.Deleted || event.Kind != EventKindIndex || event.Index == nil || event.Index.Name != "books" {
		t.Fatalf("event = %#v, want deleted books index event", event)
	}
}

func TestEtcdRevisionCompareTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		expectedRevision int64
		wantTarget       revisionCompareTarget
	}{
		{
			name:             "new index uses create revision",
			expectedRevision: 0,
			wantTarget:       revisionCompareCreate,
		},
		{
			name:             "existing index uses mod revision",
			expectedRevision: 9,
			wantTarget:       revisionCompareMod,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := compareTargetForExpectedRevision(tt.expectedRevision)
			if got != tt.wantTarget {
				t.Fatalf("compare target = %v, want %v", got, tt.wantTarget)
			}
		})
	}
}

func TestEtcdManagerPutIndexWritesJSONThroughTxn(t *testing.T) {
	t.Parallel()

	txn := &fakeTxn{succeeded: true}
	m := &EtcdManager{kv: fakeKV{txn: txn}}

	err := m.PutIndex(context.Background(), IndexRecord{
		Name:              "books",
		ShardCount:        3,
		ReplicationFactor: 2,
		MappingVersion:    5,
		Revision:          99,
	}, 0)
	if err != nil {
		t.Fatalf("put index: %v", err)
	}
	if len(txn.thenOps) != 1 {
		t.Fatalf("then ops = %d, want %d", len(txn.thenOps), 1)
	}
	op := txn.thenOps[0]
	if string(op.KeyBytes()) != "/kitsune/indexes/books/config" {
		t.Fatalf("put key = %q, want %q", string(op.KeyBytes()), "/kitsune/indexes/books/config")
	}

	got, err := unmarshalIndexRecord(op.ValueBytes(), 7)
	if err != nil {
		t.Fatalf("unmarshal put value: %v", err)
	}
	if got.Revision != 7 {
		t.Errorf("Revision = %d, want etcd revision %d", got.Revision, 7)
	}
	if got.ShardCount != 3 || got.ReplicationFactor != 2 || got.MappingVersion != 5 {
		t.Fatalf("record = %#v", got)
	}
}

func TestEtcdManagerPutIndexReturnsRevisionMismatchWhenTxnCompareFails(t *testing.T) {
	t.Parallel()

	m := &EtcdManager{kv: fakeKV{txn: &fakeTxn{succeeded: false}}}

	err := m.PutIndex(context.Background(), IndexRecord{Name: "books"}, 12)
	if !errors.Is(err, ErrRevisionMismatch) {
		t.Fatalf("error = %v, want %v", err, ErrRevisionMismatch)
	}
}

type fakeKV struct {
	clientv3.KV
	txn clientv3.Txn
}

func (kv fakeKV) Txn(ctx context.Context) clientv3.Txn {
	return kv.txn
}

type fakeTxn struct {
	succeeded bool
	cmps      []clientv3.Cmp
	thenOps   []clientv3.Op
	elseOps   []clientv3.Op
}

func (t *fakeTxn) If(cmps ...clientv3.Cmp) clientv3.Txn {
	t.cmps = append(t.cmps, cmps...)
	return t
}

func (t *fakeTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	t.thenOps = append(t.thenOps, ops...)
	return t
}

func (t *fakeTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	t.elseOps = append(t.elseOps, ops...)
	return t
}

func (t *fakeTxn) Commit() (*clientv3.TxnResponse, error) {
	return &clientv3.TxnResponse{Succeeded: t.succeeded}, nil
}
