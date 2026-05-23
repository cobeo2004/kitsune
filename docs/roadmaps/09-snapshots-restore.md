# 09 Snapshots Restore

Roadmap index: [index.md](index.md)  
Previous: [08 Tombstones Compaction](08-tombstones-compaction.md)  
Next: [10 Memberlist Health Cluster Status](10-memberlist-health-cluster-status.md)  
Traceability: [PRD Traceability](prd-traceability.md)

## Outcome

Support manual shard snapshots to S3-compatible storage and restore replicas through snapshot plus event replay.

## Scope

- `KSSnapshotStore` interface.
- MinIO/S3-compatible upload and download.
- Compressed shard snapshot artifacts.
- Snapshot manifest with index name, shard ID, replica source node, generation, mapping version, last applied event ID, creation time, and checksum.
- Checksum validation before restore.
- Manual snapshot trigger first.
- Event-count based snapshot trigger later.

## Out of Scope

- Time-based snapshot scheduling.
- Hot query path through S3/MinIO.
- Fully automatic recovery orchestration.

## Acceptance Criteria

- A tablet can create a manual compressed snapshot.
- Snapshot upload stores both data and manifest.
- Restore verifies checksum before using snapshot data.
- Restored tablet replays events after the snapshot checkpoint.
- Tablet becomes ready only after replay completes.
- If no snapshot and no retained event history are available, the tablet enters failed state with a clear error.

## TDD Plan Shape

- RED: snapshot manifest contains required fields.
- RED: restore rejects checksum mismatch.
- RED: restore replays events after checkpoint.
- RED: tablet is not ready during restore/replay.
- RED: missing recovery data marks tablet failed.

## OMX Usage

Use architect for recovery state design. Use verifier for failure and checksum coverage.

## Verification

- Snapshot manifest tests.
- Restore tests using local filesystem fake store.
- MinIO integration smoke test.
- Manual recovery workflow test.
