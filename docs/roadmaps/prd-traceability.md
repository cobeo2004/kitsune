# PRD Traceability

This file maps the Kitsune distributed search engine PRD to the implementation-sequenced roadmap.

Authoritative PRD: [PRD: Kitsune Distributed Search Engine](../prd/prd-kitsune-distributed-search-engine.md)

Roadmap index: [Kitsune Distributed Search Engine Roadmap](index.md)

## Decision Record

| PRD decision area | Roadmap decision | Covered by |
| --- | --- | --- |
| Scope | Cover the full PRD through smaller implementation-sequenced specs. | [index.md](index.md) |
| Shard assignment | Static config first; automatic placement later. | [03](03-coordinator-rest-static-routing.md), [04](04-multiple-indexes-static-shards.md), later roadmap |
| Public API | REST first; public gRPC deferred. | [03](03-coordinator-rest-static-routing.md), later roadmap |
| Logical indexes | Multiple logical indexes from the start. | [04](04-multiple-indexes-static-shards.md) |
| Delete semantics | Durable tombstones with compaction later. | [08](08-tombstones-compaction.md) |
| Snapshot trigger | Manual first; event-count based later. | [09](09-snapshots-restore.md), later roadmap |
| Direct NATS publishing | Search nodes validate direct events for MVP. | [06](06-nats-events-replay.md) |
| Document size | 1 MiB default JSON payload limit, configurable by knobs. | [03](03-coordinator-rest-static-routing.md), [06](06-nats-events-replay.md) |
| Consistency | Eventually consistent after durable event acceptance. | [06](06-nats-events-replay.md), [07](07-replication-replica-selection.md) |
| Bleve mappings | Immutable after index creation for MVP. | [01](01-bleve-tablet-core.md), [04](04-multiple-indexes-static-shards.md) |
| Failure handling | Avoid unhealthy replicas first; manual recovery in snapshot/restore milestone. | [07](07-replication-replica-selection.md), [09](09-snapshots-restore.md) |

## PRD Section Mapping

| PRD area | Roadmap milestone |
| --- | --- |
| Introduction, goals, and user stories | [index.md](index.md), all milestones |
| System roles | [01](01-bleve-tablet-core.md), [02](02-search-node-grpc.md), [03](03-coordinator-rest-static-routing.md), [05](05-etcd-metadata-manager.md), [06](06-nats-events-replay.md), [09](09-snapshots-restore.md), [10](10-memberlist-health-cluster-status.md) |
| `KSCoordinator` | [03](03-coordinator-rest-static-routing.md), [05](05-etcd-metadata-manager.md), [07](07-replication-replica-selection.md), [10](10-memberlist-health-cluster-status.md) |
| `KSSearchNode` | [02](02-search-node-grpc.md), [06](06-nats-events-replay.md), [07](07-replication-replica-selection.md), [10](10-memberlist-health-cluster-status.md) |
| `KSTablet` and Bleve local search | [01](01-bleve-tablet-core.md), [08](08-tombstones-compaction.md), [09](09-snapshots-restore.md) |
| Index and shard management | [03](03-coordinator-rest-static-routing.md), [04](04-multiple-indexes-static-shards.md), [05](05-etcd-metadata-manager.md), later automatic placement roadmap |
| Search API | [03](03-coordinator-rest-static-routing.md), [07](07-replication-replica-selection.md) |
| Document write API | [03](03-coordinator-rest-static-routing.md), [06](06-nats-events-replay.md), [08](08-tombstones-compaction.md) |
| NATS JetStream event bus | [06](06-nats-events-replay.md), [08](08-tombstones-compaction.md), [09](09-snapshots-restore.md) |
| Metadata manager | [05](05-etcd-metadata-manager.md) |
| Member manager | [10](10-memberlist-health-cluster-status.md) |
| Snapshot store | [09](09-snapshots-restore.md) |
| Replica recovery | [09](09-snapshots-restore.md) |
| Replication | [07](07-replication-replica-selection.md), [06](06-nats-events-replay.md), [09](09-snapshots-restore.md) |
| Observability and operations | [10](10-memberlist-health-cluster-status.md), [11](11-docker-compose-local-ops.md) |
| Recommended Go modules | Each milestone's implementation plan |
| Suggested project structure | First implementation plan, then revised as needed per milestone |
| Internal gRPC services | [02](02-search-node-grpc.md), [03](03-coordinator-rest-static-routing.md) |
| Metadata backend strategy | [05](05-etcd-metadata-manager.md) |
| Consistency model | [06](06-nats-events-replay.md), [07](07-replication-replica-selection.md) |
| S3-compatible object storage role | [09](09-snapshots-restore.md) |
| Success metrics | [11](11-docker-compose-local-ops.md), with earlier milestones proving subsets |

## Documentation Evidence Used

- Context7 documentation was checked for Bleve, NATS Go JetStream, gRPC Go, etcd, HashiCorp memberlist, S3-compatible object storage clients, and Docker Compose.
- Best-practice sources were checked for NATS stream retention and consumers, Elastic shard distribution and replicas, etcd API guarantees, gossip membership behavior, Docker Compose startup healthchecks, and S3-compatible object storage usage.

## Verification Rule

Before a milestone is considered complete, its implementation plan must identify:

- The failing tests to write first.
- The expected red failure.
- The smallest green implementation path.
- Refactor checks.
- Targeted verification commands.
- PRD rows from this traceability file that the milestone proves.
