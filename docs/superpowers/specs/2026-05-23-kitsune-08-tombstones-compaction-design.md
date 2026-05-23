# Kitsune 08 Tombstones Compaction Design

Master spec: [Kitsune Distributed Search Roadmap Design](2026-05-23-kitsune-distributed-search-roadmap-design.md)  
Roadmap spec: [08 Tombstones Compaction](../../roadmaps/08-tombstones-compaction.md)  
Previous: [07 Replication Replica Selection](2026-05-23-kitsune-07-replication-replica-selection-design.md)  
Next: [09 Snapshots Restore](2026-05-23-kitsune-09-snapshots-restore-design.md)

## Goal

Make deletes durable through tombstone events and define a safe compaction model.

## Architecture

Delete events create tombstones in durable event history. The local tablet removes the document from Bleve so search results are correct. Replay applies events in order so tombstones prevent stale resurrection. Compaction preserves final semantic state and must not remove recovery history that lagging replicas still need.

## Components

- Tombstone event type.
- Document version/order comparator.
- Tablet delete applier.
- Replay state reducer.
- Explicit compaction command or service.
- Compaction safety checker.

## Data Flow

A delete request becomes a tombstone event. Consumers apply it by recording delete state and removing the document from local Bleve. During replay, event order determines whether a document is live or tombstoned. Compaction reduces historical events only after safety checks pass.

## Error Handling

Out-of-order or stale events are ignored or rejected according to the ordering contract. Compaction refuses to run when snapshots, checkpoints, or replica lag make event removal unsafe.

## Testing

Implementation must use TDD for:

- Tombstone hides a document from search.
- Older upsert does not resurrect a newer tombstone.
- Newer upsert after tombstone restores the document.
- Compaction preserves final state.
- Unsafe compaction is rejected.

## Plan Handoff

Future plan path: `docs/superpowers/plans/2026-05-23-kitsune-08-tombstones-compaction.md`.

The plan must include destructive-operation safeguards for compaction.
