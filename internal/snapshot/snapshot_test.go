package snapshot

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/minio/minio-go/v7"
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

func TestManifestRequiresLastEventID(t *testing.T) {
	t.Parallel()

	err := Manifest{
		IndexName:          "books",
		ShardID:            0,
		ReplicaSourceNode:  "node-a",
		SnapshotGeneration: 1,
		MappingVersion:     1,
		LastEventSequence:  10,
		CreatedUnix:        1,
		ChecksumSHA256:     SHA256Hex([]byte("data")),
	}.Validate()
	if err == nil {
		t.Fatal("expected missing last event ID to fail")
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
		MappingVersion:     1,
		LastEventID:        "event-1",
		CreatedUnix:        1,
		ChecksumSHA256:     SHA256Hex(data),
	}

	if err := store.Put(context.Background(), manifest, data); err != nil {
		t.Fatalf("put: %v", err)
	}
	gotManifest, gotData, err := store.Get(context.Background(), "books", 0, "node-a", 1)
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
		MappingVersion:     1,
		LastEventID:        "event-1",
		CreatedUnix:        1,
		ChecksumSHA256:     SHA256Hex([]byte("expected")),
	}, []byte("actual"))
	if err == nil {
		t.Fatal("expected checksum mismatch")
	}
}

func TestRestoreVerifiesSnapshotBeforeTargetRestore(t *testing.T) {
	t.Parallel()

	target := &fakeRestoreTarget{}
	replayer := &fakeReplayer{}
	data := []byte("actual")
	manifest := validManifest([]byte("expected"))
	_, err := Restore(context.Background(), RestoreInput{
		Store: fakeStore{
			manifest: manifest,
			data:     data,
		},
		Pointer:    pointerFor(manifest),
		Target:     target,
		Replayer:   replayer,
		IndexName:  "books",
		ShardID:    0,
		Generation: 3,
	})
	if err == nil {
		t.Fatal("expected checksum mismatch")
	}
	if target.restored {
		t.Fatal("target restored corrupted snapshot data")
	}
	if target.state != RestoreFailed {
		t.Fatalf("state = %q, want failed", target.state)
	}
	if replayer.after != 0 {
		t.Fatalf("replay after = %d, want no replay", replayer.after)
	}
}

func TestRestoreRejectsManifestIdentityMismatch(t *testing.T) {
	t.Parallel()

	data := []byte("snapshot")
	manifest := validManifest(data)
	manifest.IndexName = "movies"
	target := &fakeRestoreTarget{}
	_, err := Restore(context.Background(), RestoreInput{
		Store: fakeStore{
			manifest: manifest,
			data:     data,
		},
		Pointer:    pointerFor(validManifest(data)),
		Target:     target,
		Replayer:   &fakeReplayer{},
		IndexName:  "books",
		ShardID:    0,
		Generation: 3,
	})
	if err == nil {
		t.Fatal("expected manifest identity mismatch")
	}
	if target.restored {
		t.Fatal("target restored snapshot for the wrong identity")
	}
	if target.state != RestoreFailed {
		t.Fatalf("state = %q, want failed", target.state)
	}
}

func TestRestoreRejectsMetadataPointerChecksumMismatch(t *testing.T) {
	t.Parallel()

	data := []byte("snapshot")
	manifest := validManifest(data)
	target := &fakeRestoreTarget{}
	_, err := Restore(context.Background(), RestoreInput{
		Store: fakeStore{
			manifest: manifest,
			data:     data,
		},
		Pointer: &Pointer{
			IndexName:         "books",
			ShardID:           0,
			ReplicaSourceNode: "node-a",
			Generation:        3,
			Checksum:          SHA256Hex([]byte("metadata trust anchor")),
			Checkpoint:        42,
		},
		Target:     target,
		Replayer:   &fakeReplayer{},
		IndexName:  "books",
		ShardID:    0,
		Generation: 3,
	})
	if err == nil {
		t.Fatal("expected metadata pointer checksum mismatch")
	}
	if target.restored {
		t.Fatal("target restored snapshot that disagreed with metadata pointer")
	}
	if target.state != RestoreFailed {
		t.Fatalf("state = %q, want failed", target.state)
	}
}

func TestRestoreWithoutMetadataPointerUsesFullReplayOnly(t *testing.T) {
	t.Parallel()

	data := []byte("snapshot")
	target := &fakeRestoreTarget{}
	replayer := &fakeReplayer{}
	_, err := Restore(context.Background(), RestoreInput{
		Store: fakeStore{
			manifest: validManifest(data),
			data:     data,
		},
		Target:     target,
		Replayer:   replayer,
		IndexName:  "books",
		ShardID:    0,
		Generation: 3,
	})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if target.restored {
		t.Fatal("target restored snapshot without metadata trust anchor")
	}
	if !replayer.all {
		t.Fatal("expected full replay without metadata pointer")
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
		MappingVersion:     1,
		LastEventID:        "event-42",
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
		Pointer:    pointerFor(manifest),
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
		MappingVersion:     1,
		LastEventID:        "event-42",
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
		Pointer:    pointerFor(manifest),
		Target:     target,
		Replayer:   replayer,
		IndexName:  "books",
		ShardID:    0,
		Generation: 3,
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}
}

func TestRestoreFallsBackToFullReplayWhenSnapshotMissing(t *testing.T) {
	t.Parallel()

	target := &fakeRestoreTarget{}
	replayer := &fakeReplayer{}
	manifest, err := Restore(context.Background(), RestoreInput{
		Store:      fakeStore{err: ErrSnapshotNotFound},
		Target:     target,
		Replayer:   replayer,
		IndexName:  "books",
		ShardID:    0,
		Generation: 3,
	})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if manifest != (Manifest{}) {
		t.Fatalf("manifest = %#v, want empty manifest for full replay", manifest)
	}
	if target.restored {
		t.Fatal("target restored a missing snapshot")
	}
	if !replayer.all {
		t.Fatal("expected full replay")
	}
	if target.state != RestoreReady {
		t.Fatalf("state = %q, want ready", target.state)
	}
}

func TestRestoreFailsWhenSnapshotMissingAndFullReplayUnavailable(t *testing.T) {
	t.Parallel()

	target := &fakeRestoreTarget{}
	replayer := &fakeReplayer{err: ErrReplayHistoryUnavailable}
	_, err := Restore(context.Background(), RestoreInput{
		Store:      fakeStore{err: ErrSnapshotNotFound},
		Target:     target,
		Replayer:   replayer,
		IndexName:  "books",
		ShardID:    0,
		Generation: 3,
	})
	if err == nil {
		t.Fatal("expected restore failure")
	}
	if !errors.Is(err, ErrReplayHistoryUnavailable) {
		t.Fatalf("error = %v, want replay history unavailable", err)
	}
	if target.state != RestoreFailed {
		t.Fatalf("state = %q, want failed", target.state)
	}
}

func TestCreateManualSnapshotCompressesPayload(t *testing.T) {
	t.Parallel()

	store := &recordingStore{}
	rawData := []byte("manual tablet snapshot")
	manifest := Manifest{
		IndexName:          "books",
		ShardID:            0,
		ReplicaSourceNode:  "node-a",
		SnapshotGeneration: 9,
		MappingVersion:     1,
		LastEventID:        "event-9",
		LastEventSequence:  9,
		CreatedUnix:        1,
	}
	got, err := Create(context.Background(), CreateInput{
		Store:    store,
		Manifest: manifest,
		Data:     rawData,
	})
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	if got.Compression != CompressionGzip {
		t.Fatalf("compression = %q, want gzip", got.Compression)
	}
	if got.ChecksumSHA256 != SHA256Hex(store.data) {
		t.Fatalf("checksum = %q, want checksum of stored payload", got.ChecksumSHA256)
	}
	restored, err := Decompress(got, store.data)
	if err != nil {
		t.Fatalf("decompress snapshot: %v", err)
	}
	if !bytes.Equal(restored, rawData) {
		t.Fatalf("restored data = %q, want %q", restored, rawData)
	}
}

func TestRestoreDecompressesSnapshotBeforeTargetRestore(t *testing.T) {
	t.Parallel()

	rawData := []byte("manual tablet snapshot")
	compressed, err := CompressGzip(rawData)
	if err != nil {
		t.Fatalf("compress snapshot: %v", err)
	}
	target := &fakeRestoreTarget{}
	_, err = Restore(context.Background(), RestoreInput{
		Store:      fakeStore{manifest: validCompressedManifest(compressed), data: compressed},
		Pointer:    pointerFor(validCompressedManifest(compressed)),
		Target:     target,
		Replayer:   &fakeReplayer{},
		IndexName:  "books",
		ShardID:    0,
		Generation: 3,
	})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !bytes.Equal(target.data, rawData) {
		t.Fatalf("target data = %q, want %q", target.data, rawData)
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
	manifestObject, dataObject := store.objectNames("books", 2, "node-a", 7)

	if manifestObject != "snapshots/books/shard-2/node-a/generation-000007/manifest.json" {
		t.Fatalf("manifest object = %q", manifestObject)
	}
	if dataObject != "snapshots/books/shard-2/node-a/generation-000007/snapshot.bin" {
		t.Fatalf("data object = %q", dataObject)
	}
}

func TestMinIOStorePutStoresDataThenManifest(t *testing.T) {
	t.Parallel()

	client := &fakeObjectClient{}
	store := &MinIOStore{client: client, bucket: "kitsune"}
	data := []byte("snapshot")
	manifest := Manifest{
		IndexName:          "books",
		ShardID:            0,
		ReplicaSourceNode:  "node-a",
		SnapshotGeneration: 7,
		MappingVersion:     1,
		LastEventID:        "event-7",
		LastEventSequence:  7,
		CreatedUnix:        1,
		ChecksumSHA256:     SHA256Hex(data),
	}

	if err := store.Put(context.Background(), manifest, data); err != nil {
		t.Fatalf("put snapshot: %v", err)
	}
	if len(client.puts) != 2 {
		t.Fatalf("puts = %d, want 2", len(client.puts))
	}
	if client.puts[0].objectName != "snapshots/books/shard-0/node-a/generation-000007/snapshot.bin" {
		t.Fatalf("first object = %q, want data object first", client.puts[0].objectName)
	}
	if client.puts[1].objectName != "snapshots/books/shard-0/node-a/generation-000007/manifest.json" {
		t.Fatalf("second object = %q, want manifest object second", client.puts[1].objectName)
	}
}

func TestMinIOStoreGetMapsMissingObjectToSnapshotNotFound(t *testing.T) {
	t.Parallel()

	client := &fakeObjectClient{err: ErrSnapshotNotFound}
	store := &MinIOStore{client: client, bucket: "kitsune"}

	_, _, err := store.Get(context.Background(), "books", 0, "node-a", 7)
	if !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("error = %v, want snapshot not found", err)
	}
}

func TestMinIOStoreGetRoundTripThroughObjectClient(t *testing.T) {
	t.Parallel()

	data := []byte("snapshot")
	manifest := validManifest(data)
	client := &fakeObjectClient{objects: make(map[string][]byte)}
	store := &MinIOStore{client: client, bucket: "kitsune"}
	if err := store.Put(context.Background(), manifest, data); err != nil {
		t.Fatalf("put snapshot: %v", err)
	}

	gotManifest, gotData, err := store.Get(context.Background(), "books", 0, "node-a", 3)
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	if gotManifest.ChecksumSHA256 != manifest.ChecksumSHA256 || !bytes.Equal(gotData, data) {
		t.Fatalf("snapshot = %#v %q, want %#v %q", gotManifest, gotData, manifest, data)
	}
}

func TestManualRecoveryWorkflowCreatesRestoresAndReplays(t *testing.T) {
	t.Parallel()

	store := NewFilesystemStore(t.TempDir())
	rawData := []byte("manual tablet snapshot")
	manifest, err := Create(context.Background(), CreateInput{
		Store: store,
		Manifest: Manifest{
			IndexName:          "books",
			ShardID:            0,
			ReplicaSourceNode:  "node-a",
			SnapshotGeneration: 3,
			MappingVersion:     1,
			LastEventID:        "event-42",
			LastEventSequence:  42,
			CreatedUnix:        1,
		},
		Data: rawData,
	})
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	target := &fakeRestoreTarget{}
	replayer := &fakeReplayer{}

	if _, err := Restore(context.Background(), RestoreInput{
		Store:      store,
		Pointer:    pointerFor(manifest),
		Target:     target,
		Replayer:   replayer,
		IndexName:  "books",
		ShardID:    0,
		Generation: 3,
	}); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	if !bytes.Equal(target.data, rawData) {
		t.Fatalf("target data = %q, want %q", target.data, rawData)
	}
	if replayer.after != 42 {
		t.Fatalf("replay after = %d, want 42", replayer.after)
	}
	if target.state != RestoreReady {
		t.Fatalf("state = %q, want ready", target.state)
	}
}

type fakeRestoreTarget struct {
	state    RestoreState
	restored bool
	data     []byte
}

func (t *fakeRestoreTarget) SetRestoreState(state RestoreState) {
	t.state = state
}

func (t *fakeRestoreTarget) RestoreSnapshot(_ context.Context, _ Manifest, data []byte) error {
	t.restored = true
	t.data = append(t.data[:0], data...)
	return nil
}

type fakeReplayer struct {
	after    int64
	all      bool
	err      error
	onReplay func()
}

func (r *fakeReplayer) ReplayAfter(_ context.Context, sequence int64) error {
	r.after = sequence
	if r.onReplay != nil {
		r.onReplay()
	}
	return r.err
}

func (r *fakeReplayer) ReplayAll(_ context.Context) error {
	r.all = true
	return r.err
}

type fakeStore struct {
	manifest Manifest
	data     []byte
	err      error
}

func (s fakeStore) Put(context.Context, Manifest, []byte) error {
	return nil
}

func (s fakeStore) Get(context.Context, string, int, string, int64) (Manifest, []byte, error) {
	if s.err != nil {
		return Manifest{}, nil, s.err
	}
	return s.manifest, s.data, nil
}

type recordingStore struct {
	manifest Manifest
	data     []byte
}

func (s *recordingStore) Put(_ context.Context, manifest Manifest, data []byte) error {
	s.manifest = manifest
	s.data = append(s.data[:0], data...)
	return nil
}

func (s *recordingStore) Get(context.Context, string, int, string, int64) (Manifest, []byte, error) {
	return s.manifest, s.data, nil
}

type fakeObjectClient struct {
	puts    []fakePut
	objects map[string][]byte
	err     error
}

type fakePut struct {
	objectName string
	data       []byte
}

func (c *fakeObjectClient) PutObject(_ context.Context, _ string, objectName string, r io.Reader, _ int64, _ minio.PutObjectOptions) error {
	if c.err != nil {
		return c.err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	c.puts = append(c.puts, fakePut{objectName: objectName, data: data})
	if c.objects != nil {
		c.objects[objectName] = data
	}
	return nil
}

func (c *fakeObjectClient) GetObject(_ context.Context, _ string, objectName string) ([]byte, error) {
	if c.err != nil {
		return nil, c.err
	}
	data, ok := c.objects[objectName]
	if !ok {
		return nil, ErrSnapshotNotFound
	}
	return data, nil
}

func (c *fakeObjectClient) StatObject(_ context.Context, _ string, objectName string) error {
	if c.err != nil {
		return c.err
	}
	if _, ok := c.objects[objectName]; !ok {
		return ErrSnapshotNotFound
	}
	return nil
}

func validManifest(data []byte) Manifest {
	return Manifest{
		IndexName:          "books",
		ShardID:            0,
		ReplicaSourceNode:  "node-a",
		SnapshotGeneration: 3,
		MappingVersion:     1,
		LastEventID:        "event-42",
		LastEventSequence:  42,
		CreatedUnix:        1,
		ChecksumSHA256:     SHA256Hex(data),
	}
}

func validCompressedManifest(data []byte) Manifest {
	manifest := validManifest(data)
	manifest.Compression = CompressionGzip
	return manifest
}

func pointerFor(manifest Manifest) *Pointer {
	return &Pointer{
		IndexName:         manifest.IndexName,
		ShardID:           manifest.ShardID,
		ReplicaSourceNode: manifest.ReplicaSourceNode,
		Generation:        manifest.SnapshotGeneration,
		Checksum:          manifest.ChecksumSHA256,
		Checkpoint:        manifest.LastEventSequence,
	}
}
