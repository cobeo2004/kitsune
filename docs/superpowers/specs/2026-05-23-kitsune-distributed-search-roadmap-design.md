# Kitsune Distributed Search Roadmap Design

Source roadmap: [docs/roadmaps/index.md](../../roadmaps/index.md)  
Traceability: [docs/roadmaps/prd-traceability.md](../../roadmaps/prd-traceability.md)  
Authoritative PRD: [docs/prd/prd-kitsune-distributed-search-engine.md](../../prd/prd-kitsune-distributed-search-engine.md)

## Goal

Deliver the full Kitsune distributed search engine PRD through small, runnable implementation milestones, each with its own implementation plan and Superpowers TDD red/green/refactor loop.

## Approved Decisions

- Split by implementation sequence, not by component taxonomy.
- Store roadmap artifacts under `docs/roadmaps/`.
- Use `docs/roadmaps/index.md` as the linked roadmap entrypoint.
- Use static shard assignment first; add automatic placement later.
- Expose REST first for public client APIs.
- Use internal gRPC for coordinator-to-search-node calls.
- Support multiple logical indexes from the start.
- Use tombstones with later compaction for deletes.
- Trigger snapshots manually first; add event-count based snapshots later.
- Let search nodes validate direct NATS events in MVP.
- Default document payload limit is 1 MiB, configurable through knobs.
- Use eventual consistency after durable write/event acceptance.
- Freeze Bleve mappings after index creation for MVP.
- Avoid unhealthy replicas first; add manual recovery workflows in the snapshot/restore milestone.

## Architecture

Kitsune is implemented as a layered distributed search system. The earliest milestones build the local search primitive, then wrap it in a search-node process, then add a REST coordinator that routes to search nodes through internal gRPC. Later milestones move static metadata into etcd, move writes onto NATS JetStream, add replica selection, tombstones, snapshots, gossip health, and a local Docker Compose cluster.

The design deliberately avoids implementing custom full-text search internals. Bleve owns local indexing, search query execution, tokenization, ranking, and storage format. Kitsune owns distributed concerns: shard identity, replica placement, metadata, event replay, snapshots, health, and coordinator-level merging.

## Milestone Specs

| Order | Superpowers spec | Roadmap spec |
| --- | --- | --- |
| 01 | [Bleve Tablet Core](2026-05-23-kitsune-01-bleve-tablet-core-design.md) | [01 Bleve Tablet Core](../../roadmaps/01-bleve-tablet-core.md) |
| 02 | [Search Node gRPC](2026-05-23-kitsune-02-search-node-grpc-design.md) | [02 Search Node gRPC](../../roadmaps/02-search-node-grpc.md) |
| 03 | [Coordinator REST Static Routing](2026-05-23-kitsune-03-coordinator-rest-static-routing-design.md) | [03 Coordinator REST Static Routing](../../roadmaps/03-coordinator-rest-static-routing.md) |
| 04 | [Multiple Indexes Static Shards](2026-05-23-kitsune-04-multiple-indexes-static-shards-design.md) | [04 Multiple Indexes Static Shards](../../roadmaps/04-multiple-indexes-static-shards.md) |
| 05 | [etcd Metadata Manager](2026-05-23-kitsune-05-etcd-metadata-manager-design.md) | [05 Etcd Metadata Manager](../../roadmaps/05-etcd-metadata-manager.md) |
| 06 | [NATS Events Replay](2026-05-23-kitsune-06-nats-events-replay-design.md) | [06 NATS Events Replay](../../roadmaps/06-nats-events-replay.md) |
| 07 | [Replication Replica Selection](2026-05-23-kitsune-07-replication-replica-selection-design.md) | [07 Replication Replica Selection](../../roadmaps/07-replication-replica-selection.md) |
| 08 | [Tombstones Compaction](2026-05-23-kitsune-08-tombstones-compaction-design.md) | [08 Tombstones Compaction](../../roadmaps/08-tombstones-compaction.md) |
| 09 | [Snapshots Restore](2026-05-23-kitsune-09-snapshots-restore-design.md) | [09 Snapshots Restore](../../roadmaps/09-snapshots-restore.md) |
| 10 | [memberlist Health Cluster Status](2026-05-23-kitsune-10-memberlist-health-cluster-status-design.md) | [10 Memberlist Health Cluster Status](../../roadmaps/10-memberlist-health-cluster-status.md) |
| 11 | [Docker Compose Local Ops](2026-05-23-kitsune-11-docker-compose-local-ops-design.md) | [11 Docker Compose Local Ops](../../roadmaps/11-docker-compose-local-ops.md) |

## Data Flow

Early data flow is synchronous: REST client to coordinator, coordinator to search node over internal gRPC, search node to local tablet, tablet to Bleve. Once NATS is introduced, writes become durable events: REST client to coordinator, coordinator to JetStream, search nodes consume events, tablets apply events to local Bleve indexes, and searches read the eventually consistent local tablet state.

Metadata starts as static configuration to keep early milestones runnable. It later moves to etcd through `KSMetadataManager`, with the coordinator maintaining an in-memory routing cache from watched metadata.

## Error Handling

Errors should be explicit at each boundary. REST errors return stable client-facing status and messages. Internal gRPC errors use appropriate status codes. Metadata conflicts are surfaced as stale-write or compare-and-swap failures. Recovery errors distinguish missing snapshot, checksum mismatch, insufficient event history, and replay failure.

## Testing Strategy

Every implementation plan derived from these specs must use Superpowers TDD:

- Write one failing behavior test.
- Run it and verify the expected red failure.
- Implement the smallest production change.
- Run it and verify green.
- Refactor only after green.
- Repeat for the next behavior.

Milestone implementation plans should be saved under `docs/superpowers/plans/` and should reference both this design suite and the linked roadmap spec.

## Completion Criteria

The design suite is complete when all milestone specs exist, link back to this master spec and the roadmap, and define architecture, components, data flow, error handling, TDD gates, and handoff expectations.
