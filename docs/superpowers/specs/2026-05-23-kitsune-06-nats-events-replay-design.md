# Kitsune 06 NATS Events Replay Design

Master spec: [Kitsune Distributed Search Roadmap Design](2026-05-23-kitsune-distributed-search-roadmap-design.md)  
Roadmap spec: [06 NATS Events Replay](../../roadmaps/06-nats-events-replay.md)  
Previous: [05 etcd Metadata Manager](2026-05-23-kitsune-05-etcd-metadata-manager-design.md)  
Next: [07 Replication Replica Selection](2026-05-23-kitsune-07-replication-replica-selection-design.md)

## Goal

Move document writes onto durable NATS JetStream events and make tablets replay events from checkpoints.

## Architecture

The coordinator publishes validated document events to JetStream. Search nodes consume events for hosted tablets. Direct NATS publishing is allowed in MVP, but search nodes validate events before applying them. Write success means durable event acceptance; search is eventually consistent.

## Components

- Event envelope and schema version.
- Stream subject naming.
- Coordinator publisher.
- Search-node durable consumer.
- Tablet event applier.
- Checkpoint recorder.
- Invalid-event handling path.

## Data Flow

REST write requests are validated, converted into event envelopes, and published to JetStream. Search-node consumers fetch events, validate target index/shard, apply them to tablets, and record checkpoints. Search requests observe whatever each selected tablet has applied.

## Error Handling

Publish failures return write failure to the client. Invalid direct events do not mutate tablet state and are surfaced through logs/status or a dead-letter path defined by the implementation plan. Replay errors mark affected tablets not ready until recovered.

## Testing

Implementation must use TDD for:

- Event envelope creation.
- Durable publish on coordinator write.
- Idempotent upsert replay.
- Invalid event rejection.
- Checkpoint persistence preventing duplicate application.
- Eventual consistency status visibility.

## Plan Handoff

Future plan path: `docs/superpowers/plans/2026-05-23-kitsune-06-nats-events-replay.md`.

The plan should use a fake event bus first, then a JetStream smoke test.
