# Kitsune 09 Snapshots Restore Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Support manual tablet snapshots to S3-compatible storage and restore replicas through snapshot plus event replay.

**Architecture:** Add `internal/snapshot` with a store interface, manifest, checksum utilities, filesystem fake, and S3-compatible object-store implementation. Restore is a state machine that keeps tablets not ready until snapshot verification and replay finish.

**Tech Stack:** Go 1.26.3, `github.com/aws/aws-sdk-go-v2/service/s3`, gzip or tar helpers from the standard library, SHA-256 checksums.

---

Design spec: [../specs/2026-05-23-kitsune-09-snapshots-restore-design.md](../specs/2026-05-23-kitsune-09-snapshots-restore-design.md)  
Roadmap spec: [../../roadmaps/09-snapshots-restore.md](../../roadmaps/09-snapshots-restore.md)

## File Structure

- Create: `internal/snapshot/manifest.go` for snapshot manifest.
- Create: `internal/snapshot/checksum.go` for SHA-256 helpers.
- Create: `internal/snapshot/store.go` for store interface.
- Create: `internal/snapshot/filesystem.go` for tests.
- Create: `internal/snapshot/s3.go` for S3-compatible store.
- Create: `internal/snapshot/restore.go` for restore flow.
- Create: `internal/snapshot/snapshot_test.go` for behavior tests.

### Task 1: Manifest and Checksum

**Files:**
- Create: `internal/snapshot/manifest.go`
- Create: `internal/snapshot/checksum.go`
- Test: `internal/snapshot/snapshot_test.go`

- [ ] **Step 1: Write the failing test**

```go
package snapshot

import "testing"

func TestManifestRequiresChecksum(t *testing.T) {
	t.Parallel()

	err := Manifest{IndexName: "books", ShardID: 0, SnapshotGeneration: 1, LastEventSequence: 10}.Validate()
	if err == nil {
		t.Fatal("expected missing checksum to fail")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/snapshot -run TestManifestRequiresChecksum -count=1`

Expected: FAIL with undefined `Manifest`.

- [ ] **Step 3: Write minimal implementation**

```go
package snapshot

import "fmt"

type Manifest struct {
	IndexName          string `json:"indexName"`
	ShardID            int    `json:"shardId"`
	ReplicaSourceNode  string `json:"replicaSourceNode"`
	SnapshotGeneration int64  `json:"snapshotGeneration"`
	MappingVersion     int    `json:"mappingVersion"`
	LastEventSequence   int64  `json:"lastEventSequence"`
	CreatedUnix        int64  `json:"createdUnix"`
	ChecksumSHA256     string `json:"checksumSha256"`
}

func (m Manifest) Validate() error {
	if m.IndexName == "" {
		return fmt.Errorf("index name is required")
	}
	if m.ChecksumSHA256 == "" {
		return fmt.Errorf("checksum is required")
	}
	return nil
}
```

```go
package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
)

func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/snapshot -run TestManifestRequiresChecksum -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/snapshot
git commit -m "Define snapshot manifest checksums

Constraint: Restores verify snapshot checksums before use.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/snapshot -run TestManifestRequiresChecksum -count=1"
```

### Task 2: Store Interface and Filesystem Fake

**Files:**
- Create: `internal/snapshot/store.go`
- Create: `internal/snapshot/filesystem.go`
- Test: `internal/snapshot/snapshot_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestFilesystemStoreRoundTrip(t *testing.T) {
	t.Parallel()

	store := NewFilesystemStore(t.TempDir())
	manifest := Manifest{IndexName: "books", ShardID: 0, SnapshotGeneration: 1, ChecksumSHA256: SHA256Hex([]byte("data"))}
	if err := store.Put(context.Background(), manifest, []byte("data")); err != nil {
		t.Fatalf("put: %v", err)
	}
	gotManifest, gotData, err := store.Get(context.Background(), "books", 0, 1)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if gotManifest.ChecksumSHA256 != manifest.ChecksumSHA256 || string(gotData) != "data" {
		t.Fatalf("round trip mismatch")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/snapshot -run TestFilesystemStoreRoundTrip -count=1`

Expected: FAIL with undefined `NewFilesystemStore`.

- [ ] **Step 3: Write minimal implementation**

```go
type Store interface {
	Put(ctx context.Context, manifest Manifest, data []byte) error
	Get(ctx context.Context, indexName string, shardID int, generation int64) (Manifest, []byte, error)
}
```

Implement filesystem store with two files per snapshot: `manifest.json` and `snapshot.bin`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/snapshot -run TestFilesystemStoreRoundTrip -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/snapshot
git commit -m "Add snapshot store contract

Constraint: Snapshot storage must support S3-compatible object storage through a narrow interface.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/snapshot -run TestFilesystemStoreRoundTrip -count=1"
```

### Task 3: Restore Checksum Guard

**Files:**
- Create: `internal/snapshot/restore.go`
- Test: `internal/snapshot/snapshot_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestRestoreRejectsChecksumMismatch(t *testing.T) {
	t.Parallel()

	err := VerifySnapshot(Manifest{ChecksumSHA256: SHA256Hex([]byte("expected"))}, []byte("actual"))
	if err == nil {
		t.Fatal("expected checksum mismatch")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/snapshot -run TestRestoreRejectsChecksumMismatch -count=1`

Expected: FAIL with undefined `VerifySnapshot`.

- [ ] **Step 3: Write minimal implementation**

```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/snapshot -run TestRestoreRejectsChecksumMismatch -count=1`

Expected: PASS.

- [ ] **Step 5: Add S3-compatible client dependency and skeleton**

Run: `go get github.com/aws/aws-sdk-go-v2/config github.com/aws/aws-sdk-go-v2/credentials github.com/aws/aws-sdk-go-v2/service/s3 github.com/aws/smithy-go`

Create `S3Store` with `PutObject`, `GetObject`, and `StatObject` usage behind the `Store` interface.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/snapshot
git commit -m "Guard snapshot restore with checksums

Constraint: Corrupt snapshots must not become ready tablets.
Confidence: medium
Scope-risk: moderate
Tested: go test ./internal/snapshot -count=1"
```
