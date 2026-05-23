# Kitsune 07 Replication Replica Selection Design

Master spec: [Kitsune Distributed Search Roadmap Design](2026-05-23-kitsune-distributed-search-roadmap-design.md)  
Roadmap spec: [07 Replication Replica Selection](../../roadmaps/07-replication-replica-selection.md)  
Previous: [06 NATS Events Replay](2026-05-23-kitsune-06-nats-events-replay-design.md)  
Next: [08 Tombstones Compaction](2026-05-23-kitsune-08-tombstones-compaction-design.md)

## Goal

Support multiple replicas per shard and select ready replicas for coordinator search.

## Architecture

Replica assignment is explicit and metadata-backed. Each replica maintains its own local Bleve index and consumes the same shard event stream. The coordinator selects a ready replica for each shard and avoids failed, restoring, replaying, or unhealthy replicas.

## Components

- Replica assignment validator.
- Replica state model.
- Replica selector.
- Coordinator shard fan-out integration.
- Search failure aggregation.

## Data Flow

Index metadata identifies shards and replicas. Search chooses one acceptable replica for every shard, sends internal gRPC search requests, and merges results. If a selected replica fails and another is available, retry behavior may be implemented only if specified by the plan.

## Error Handling

No healthy replica for a required shard returns a clear coordinator error. Same-node duplicate replicas are rejected when enough nodes exist. Replica states are explicit so unavailable and replaying states are not confused.

## Testing

Implementation must use TDD for:

- Replication factor validation.
- Ready replica selection.
- Skipping failed/restoring/replaying replicas.
- Clear no-healthy-replica error.
- Search success after one node is unavailable when another replica is ready.

## Plan Handoff

Future plan path: `docs/superpowers/plans/2026-05-23-kitsune-07-replication-replica-selection.md`.

The plan should include a selector test matrix before integration tests.
