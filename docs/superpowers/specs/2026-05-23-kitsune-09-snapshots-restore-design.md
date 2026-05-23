# Kitsune 09 Snapshots Restore Design

Master spec: [Kitsune Distributed Search Roadmap Design](2026-05-23-kitsune-distributed-search-roadmap-design.md)  
Roadmap spec: [09 Snapshots Restore](../../roadmaps/09-snapshots-restore.md)  
Previous: [08 Tombstones Compaction](2026-05-23-kitsune-08-tombstones-compaction-design.md)  
Next: [10 memberlist Health Cluster Status](2026-05-23-kitsune-10-memberlist-health-cluster-status-design.md)

## Goal

Support manual shard snapshots to S3-compatible storage and restore replicas through snapshot plus event replay.

## Architecture

`KSSnapshotStore` abstracts object storage. MinIO/S3 is the first backing store. A snapshot contains compressed tablet data and a manifest with identity, generation, mapping version, checkpoint, creation time, and checksum. Restore verifies the snapshot, opens the tablet, replays events after the checkpoint, and only then marks the tablet ready.

## Components

- Snapshot store interface.
- MinIO/S3 implementation.
- Snapshot manifest.
- Compression/checksum utility.
- Manual snapshot trigger.
- Restore/replay state machine.

## Data Flow

An operator triggers a snapshot for a ready tablet. The tablet creates compressed snapshot data, writes a manifest, uploads both to object storage, and records metadata. Restore downloads the latest suitable snapshot, verifies checksum, restores data, replays events after the manifest checkpoint, and transitions to ready.

## Error Handling

Checksum mismatch aborts restore. Missing snapshot falls back to full event replay if retained history is sufficient. If neither snapshot nor event history can rebuild the tablet, the tablet enters failed state with a clear error.

## Testing

Implementation must use TDD for:

- Manifest required fields.
- Snapshot checksum creation and validation.
- Restore rejects checksum mismatch.
- Restore replays events after checkpoint.
- Tablet is not ready during restore/replay.
- Missing recovery data marks tablet failed.

## Plan Handoff

Future plan path: `docs/superpowers/plans/2026-05-23-kitsune-09-snapshots-restore.md`.

The plan should start with a filesystem fake store, then add MinIO integration coverage.
