# 06 NATS Events Replay

Roadmap index: [index.md](index.md)  
Previous: [05 Etcd Metadata Manager](05-etcd-metadata-manager.md)  
Next: [07 Replication Replica Selection](07-replication-replica-selection.md)  
Traceability: [PRD Traceability](prd-traceability.md)

## Outcome

Move document writes onto durable NATS JetStream events and make tablets replay events from checkpoints.

## Scope

- Define document upsert/delete event envelope.
- Coordinator publishes write events to JetStream.
- Search nodes consume relevant events for hosted tablets.
- Search nodes validate direct NATS-published events.
- Tablets track last applied event/checkpoint.
- Writes are eventually consistent: success means durable event acceptance, not immediate search visibility.

## Out of Scope

- Snapshot restore.
- Compaction.
- Strong read-after-write semantics.
- Separate validation service.

## Acceptance Criteria

- Coordinator writes publish durable JetStream events.
- Direct NATS event publishing is accepted only when events validate.
- Invalid direct events are rejected or dead-lettered according to the implementation plan.
- Tablets replay from their last checkpoint.
- Search freshness is visible through tablet checkpoint/status.
- Event retention requirements are documented for replay safety.

## TDD Plan Shape

- RED: coordinator write publishes the expected event envelope.
- RED: tablet consumer applies upsert events idempotently.
- RED: tablet consumer applies delete tombstone events according to milestone 08 semantics once available.
- RED: invalid direct event does not mutate tablet state.
- RED: checkpoint prevents duplicate mutation on restart.

## OMX Usage

Use planner or architect for event schema review. Use verifier for replay/checkpoint edge cases.

## Verification

- Event envelope tests.
- Consumer replay tests with a fake event bus.
- JetStream integration smoke tests.
- Explicit eventually-consistent status tests.
