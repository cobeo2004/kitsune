# 08 Tombstones Compaction

Roadmap index: [index.md](index.md)  
Previous: [07 Replication Replica Selection](07-replication-replica-selection.md)  
Next: [09 Snapshots Restore](09-snapshots-restore.md)  
Traceability: [PRD Traceability](prd-traceability.md)

## Outcome

Make delete semantics durable through tombstones and define a compaction path.

## Scope

- Delete events create durable tombstones.
- Local Bleve search state removes deleted documents.
- Replay respects tombstone ordering.
- Compaction removes obsolete document history only when safe.
- Compaction is explicit and testable, not automatic in the first pass.

## Out of Scope

- Full storage engine compaction beyond event/tombstone history.
- Time-based automatic compaction.
- Mapping migration.

## Acceptance Criteria

- Delete tombstones survive replay.
- A replayed older upsert cannot resurrect a newer tombstoned document.
- A newer upsert after a tombstone restores the document.
- Compaction keeps the latest semantic state for each document.
- Compaction does not remove events needed by lagging replicas unless snapshot/replay safety criteria are met.

## TDD Plan Shape

- RED: tombstone hides a document from search after replay.
- RED: event ordering prevents stale resurrection.
- RED: new upsert after tombstone is searchable.
- RED: compaction preserves final document state.

## OMX Usage

Use debugger or verifier if event ordering bugs appear. Compaction should be reviewed before implementation because it can destroy recovery data.

## Verification

- Event ordering tests.
- Replay tests with mixed upsert/delete history.
- Compaction safety tests.
