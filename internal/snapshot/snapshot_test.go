package snapshot

import (
	"context"
	"testing"
)

func TestManifestRequiresChecksum(t *testing.T) {
	t.Parallel()

	err := Manifest{
		IndexName:          "books",
		ShardID:            0,
		ReplicaSourceNode:  "node-a",
		SnapshotGeneration: 1,
		LastEventSequence:  10,
		CreatedUnix:        1,
	}.Validate()
	if err == nil {
		t.Fatal("expected missing checksum to fail")
	}
}

func TestFilesystemStoreRoundTrip(t *testing.T) {
	t.Parallel()

	store := NewFilesystemStore(t.TempDir())
	data := []byte("data")
	manifest := Manifest{
		IndexName:          "books",
		ShardID:            0,
		ReplicaSourceNode:  "node-a",
		SnapshotGeneration: 1,
		CreatedUnix:        1,
		ChecksumSHA256:     SHA256Hex(data),
	}

	if err := store.Put(context.Background(), manifest, data); err != nil {
		t.Fatalf("put: %v", err)
	}
	gotManifest, gotData, err := store.Get(context.Background(), "books", 0, 1)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if gotManifest.ChecksumSHA256 != manifest.ChecksumSHA256 || string(gotData) != "data" {
		t.Fatalf("round trip manifest=%#v data=%q", gotManifest, gotData)
	}
}

func TestRestoreRejectsChecksumMismatch(t *testing.T) {
	t.Parallel()

	err := VerifySnapshot(Manifest{
		IndexName:          "books",
		ShardID:            0,
		ReplicaSourceNode:  "node-a",
		SnapshotGeneration: 1,
		CreatedUnix:        1,
		ChecksumSHA256:     SHA256Hex([]byte("expected")),
	}, []byte("actual"))
	if err == nil {
		t.Fatal("expected checksum mismatch")
	}
}

func TestRestoreReplaysAfterSnapshotCheckpoint(t *testing.T) {
	t.Parallel()

	store := NewFilesystemStore(t.TempDir())
	data := []byte("snapshot")
	manifest := Manifest{
		IndexName:          "books",
		ShardID:            0,
		ReplicaSourceNode:  "node-a",
		SnapshotGeneration: 3,
		LastEventSequence:  42,
		CreatedUnix:        1,
		ChecksumSHA256:     SHA256Hex(data),
	}
	if err := store.Put(context.Background(), manifest, data); err != nil {
		t.Fatalf("put snapshot: %v", err)
	}
	target := &fakeRestoreTarget{}
	replayer := &fakeReplayer{}

	if _, err := Restore(context.Background(), RestoreInput{
		Store:      store,
		Target:     target,
		Replayer:   replayer,
		IndexName:  "books",
		ShardID:    0,
		Generation: 3,
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if replayer.after != 42 {
		t.Fatalf("replay after = %d, want 42", replayer.after)
	}
	if target.state != RestoreReady {
		t.Fatalf("state = %q, want ready", target.state)
	}
}

func TestRestoreKeepsTargetNotReadyUntilReplayFinishes(t *testing.T) {
	t.Parallel()

	store := NewFilesystemStore(t.TempDir())
	data := []byte("snapshot")
	manifest := Manifest{
		IndexName:          "books",
		ShardID:            0,
		ReplicaSourceNode:  "node-a",
		SnapshotGeneration: 3,
		LastEventSequence:  42,
		CreatedUnix:        1,
		ChecksumSHA256:     SHA256Hex(data),
	}
	if err := store.Put(context.Background(), manifest, data); err != nil {
		t.Fatalf("put snapshot: %v", err)
	}
	target := &fakeRestoreTarget{}
	replayer := &fakeReplayer{onReplay: func() {
		if target.state == RestoreReady {
			t.Fatal("target became ready before replay finished")
		}
	}}

	if _, err := Restore(context.Background(), RestoreInput{
		Store:      store,
		Target:     target,
		Replayer:   replayer,
		IndexName:  "books",
		ShardID:    0,
		Generation: 3,
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}
}

func TestNewMinIOStoreRequiresBucket(t *testing.T) {
	t.Parallel()

	_, err := NewMinIOStore(MinIOConfig{
		Endpoint:     "localhost:9000",
		AccessKeyID:  "minio",
		SecretAccess: "password",
	})
	if err == nil {
		t.Fatal("expected missing bucket to fail")
	}
}

func TestMinIOStoreObjectNames(t *testing.T) {
	t.Parallel()

	store := &MinIOStore{bucket: "kitsune"}
	manifestObject, dataObject := store.objectNames("books", 2, 7)

	if manifestObject != "snapshots/books/shard-2/generation-000007/manifest.json" {
		t.Fatalf("manifest object = %q", manifestObject)
	}
	if dataObject != "snapshots/books/shard-2/generation-000007/snapshot.bin" {
		t.Fatalf("data object = %q", dataObject)
	}
}

type fakeRestoreTarget struct {
	state RestoreState
}

func (t *fakeRestoreTarget) SetRestoreState(state RestoreState) {
	t.state = state
}

func (t *fakeRestoreTarget) RestoreSnapshot(_ context.Context, _ Manifest, _ []byte) error {
	return nil
}

type fakeReplayer struct {
	after    int64
	onReplay func()
}

func (r *fakeReplayer) ReplayAfter(_ context.Context, sequence int64) error {
	r.after = sequence
	if r.onReplay != nil {
		r.onReplay()
	}
	return nil
}
