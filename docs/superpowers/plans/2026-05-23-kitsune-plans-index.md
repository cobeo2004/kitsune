# Kitsune Implementation Plans Index

Design suite: [../specs/2026-05-23-kitsune-distributed-search-roadmap-design.md](../specs/2026-05-23-kitsune-distributed-search-roadmap-design.md)  
Roadmap: [../../roadmaps/index.md](../../roadmaps/index.md)

Implement these plans in order. Each plan is intentionally scoped to produce a runnable milestone and must be executed with Superpowers TDD.

| Order | Plan | Depends on |
| --- | --- | --- |
| 01 | [Bleve Tablet Core](2026-05-23-kitsune-01-bleve-tablet-core.md) | none |
| 02 | [Search Node gRPC](2026-05-23-kitsune-02-search-node-grpc.md) | 01 |
| 03 | [Coordinator REST Static Routing](2026-05-23-kitsune-03-coordinator-rest-static-routing.md) | 01, 02 |
| 04 | [Multiple Indexes Static Shards](2026-05-23-kitsune-04-multiple-indexes-static-shards.md) | 01, 02, 03 |
| 05 | [etcd Metadata Manager](2026-05-23-kitsune-05-etcd-metadata-manager.md) | 03, 04 |
| 06 | [NATS Events Replay](2026-05-23-kitsune-06-nats-events-replay.md) | 01, 04, 05 |
| 07 | [Replication Replica Selection](2026-05-23-kitsune-07-replication-replica-selection.md) | 04, 05, 06 |
| 08 | [Tombstones Compaction](2026-05-23-kitsune-08-tombstones-compaction.md) | 06, 07 |
| 09 | [Snapshots Restore](2026-05-23-kitsune-09-snapshots-restore.md) | 01, 06, 08 |
| 10 | [memberlist Health Cluster Status](2026-05-23-kitsune-10-memberlist-health-cluster-status.md) | 05, 07, 09 |
| 11 | [Docker Compose Local Ops](2026-05-23-kitsune-11-docker-compose-local-ops.md) | all previous milestones |

## Execution Rules

- Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` before executing any plan.
- Follow the red/green/refactor sequence in every task.
- Do not write production code before the matching failing test has been observed.
- Commit after each task using the Lore Commit Protocol in `AGENTS.md`.
- Prefer small packages with narrow ownership boundaries.
