# Kitsune Distributed Search Engine Roadmap

This roadmap decomposes the full Kitsune distributed search engine PRD into implementation-sequenced milestone specs. Each milestone should become its own smaller implementation plan before code changes begin.

Authoritative PRD: [PRD: Kitsune Distributed Search Engine](../prd/prd-kitsune-distributed-search-engine.md)

Traceability matrix: [PRD Traceability](prd-traceability.md)

## Planning Rules

- Cover the full PRD, but implement it through small runnable milestones.
- Use Superpowers TDD for each implementation plan: write the failing test, verify red, implement minimal code, verify green, refactor, then repeat.
- Use oh-my-codex workflows for larger milestones when they improve throughput or review quality.
- Keep public client APIs REST-first until a later public gRPC milestone is explicitly planned.
- Use internal gRPC for coordinator-to-search-node communication.
- Start with static shard assignment; add automatic placement later.
- Support multiple logical indexes from the start.
- Use eventual consistency for writes: coordinator success means durable event acceptance, while search freshness is reported by tablet checkpoints/status.
- Keep Bleve mappings immutable after index creation for MVP.
- Use tombstones for deletes and add compaction in a later milestone.
- Use manual snapshots first, then add event-count based snapshots later.
- Default document payload limit is 1 MiB, configurable through runtime/config knobs.

## Milestones

| Order | Milestone | Spec | Outcome |
| --- | --- | --- | --- |
| 01 | Bleve tablet core | [01 Bleve Tablet Core](01-bleve-tablet-core.md) | Local per-shard indexing/searching exists behind `KSTablet`. |
| 02 | Search node and internal gRPC | [02 Search Node gRPC](02-search-node-grpc.md) | A `KSSearchNode` hosts tablets and serves shard RPCs. |
| 03 | Coordinator REST and static routing | [03 Coordinator REST Static Routing](03-coordinator-rest-static-routing.md) | REST clients can create indexes, write documents, and search through a coordinator using static routing. |
| 04 | Multiple indexes and shard config | [04 Multiple Indexes Static Shards](04-multiple-indexes-static-shards.md) | Multiple logical indexes are supported with explicit static shard/replica assignment. |
| 05 | etcd metadata manager | [05 Etcd Metadata Manager](05-etcd-metadata-manager.md) | Authoritative index, shard, replica, and checkpoint metadata move into etcd. |
| 06 | NATS events and replay | [06 NATS Events Replay](06-nats-events-replay.md) | Writes flow through durable JetStream events and tablets replay from checkpoints. |
| 07 | Replication and replica selection | [07 Replication Replica Selection](07-replication-replica-selection.md) | Shards can have multiple replicas, and coordinator search avoids unavailable replicas. |
| 08 | Tombstones and compaction | [08 Tombstones Compaction](08-tombstones-compaction.md) | Delete tombstones are durable and compaction is defined/testable. |
| 09 | Snapshots and restore | [09 Snapshots Restore](09-snapshots-restore.md) | Tablets can snapshot to S3-compatible object storage and restore with event replay. |
| 10 | memberlist health and cluster status | [10 Memberlist Health Cluster Status](10-memberlist-health-cluster-status.md) | Gossip provides advisory health and cluster status reports node/tablet state. |
| 11 | Docker Compose local cluster and ops | [11 Docker Compose Local Ops](11-docker-compose-local-ops.md) | A local three-node cluster runs with etcd, NATS, S3-compatible object storage, coordinator, and search nodes. |

## Later Roadmap Items

- Automatic shard placement and rebalancing.
- Public gRPC client API.
- Mapping migration and reindex workflows.
- Optional read-after-write behavior.
- Time-based or event-count based snapshot scheduling beyond the first event-count trigger.
- Production-grade security hardening.

## Completion Definition

The roadmap is complete when every milestone has:

- A reviewed spec in this folder.
- A smaller implementation plan that references its spec.
- TDD acceptance tests that are observed red before implementation and green after implementation.
- Verification evidence covering the milestone's stated outcome.
- Traceability back to the PRD requirements in [PRD Traceability](prd-traceability.md).
