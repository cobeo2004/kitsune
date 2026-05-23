# 07 Replication Replica Selection

Roadmap index: [index.md](index.md)  
Previous: [06 NATS Events Replay](06-nats-events-replay.md)  
Next: [08 Tombstones Compaction](08-tombstones-compaction.md)  
Traceability: [PRD Traceability](prd-traceability.md)

## Outcome

Support multiple replicas per shard and coordinator selection of ready, healthy replicas.

## Scope

- Replication factor greater than one.
- Multiple tablets consume the same shard event stream.
- Coordinator chooses from ready replicas.
- Coordinator avoids replicas marked failed, restoring, or replaying.
- Node failure handling initially avoids unhealthy replicas.

## Out of Scope

- Fully automatic failover with primary election.
- Strong global consistency.
- Automatic replica repair.
- Snapshot-based manual recovery, which belongs to milestone 09.

## Acceptance Criteria

- Index creation requires replication factor.
- Replica assignments avoid placing two replicas of the same shard on the same node when enough nodes exist.
- Search can use any ready replica.
- Search does not route to failed/restoring/replaying replicas.
- If one node is stopped and another ready replica exists, search still succeeds.
- If no healthy replica exists, the coordinator returns a clear error.

## TDD Plan Shape

- RED: replica selector prefers ready replicas.
- RED: selector skips failed/restoring/replaying replicas.
- RED: no healthy replica produces a clear error.
- RED: duplicate same-node replica assignment is rejected when avoidable.

## OMX Usage

Use architect review for replica state machine and selector rules. Use verifier for failure matrix coverage.

## Verification

- Selector unit tests.
- Coordinator route tests with multiple fake replicas.
- Local integration test that stops one search node.
