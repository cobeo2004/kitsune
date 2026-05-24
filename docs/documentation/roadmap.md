---
title: Roadmap
description: Milestones, implementation status, and future work for Kitsune.
sidebar:
  label: Roadmap
  order: 6
---

# Roadmap

The implementation follows the PRD in `../prd/prd-kitsune-distributed-search-engine.md` and the milestone roadmap in `../roadmaps/index.md`.

## Milestone Sequence

| Order | Milestone | Outcome |
| --- | --- | --- |
| 01 | Bleve tablet core | Local per-shard indexing and search behind `KSTablet`. |
| 02 | Search node gRPC | `KSSearchNode` hosts tablets and serves shard RPCs. |
| 03 | Coordinator REST and static routing | REST clients create indexes, write documents, and search through a coordinator. |
| 04 | Multiple indexes and shard config | Multiple logical indexes with explicit static shard and replica assignment. |
| 05 | etcd metadata manager | Index, shard, replica, status, checkpoint, and snapshot metadata move into etcd. |
| 06 | NATS events and replay | Writes flow through JetStream and tablets replay from checkpoints. |
| 07 | Replication and replica selection | Shards have multiple replicas and search avoids unavailable replicas. |
| 08 | Tombstones and compaction | Deletes have durable tombstones and compaction has testable safety rules. |
| 09 | Snapshots and restore | Tablets snapshot to S3-compatible object storage and restore with replay. |
| 10 | memberlist health and cluster status | Gossip health is surfaced through cluster status. |
| 11 | Docker Compose local ops | A local cluster runs with coordinator, three search nodes, etcd, NATS, and S3-compatible storage. |

## Current State

The current tree has implemented the first local-distributed slices across tablets, search nodes, coordinator routing, metadata, events, replay, snapshots, compaction, memberlist status, and local Compose wiring.

The implementation is still a development system. Operator CLI commands for manual snapshot create/restore and full production hardening are not complete.

## Near-Term Work

- Finish operator commands for snapshot create, snapshot restore, and failover drills.
- Add stronger smoke coverage around snapshot restore and node stop/start behavior.
- Add status details for event lag and latest snapshot generation.
- Improve configuration documentation and validation examples.
- Add CI checks for Go tests, vet, docs build, and Markdown link health.

## Later Roadmap

- Automatic shard placement and rebalancing.
- Public gRPC client API.
- Mapping migration and reindex workflows.
- Optional read-after-write behavior.
- Time-based or event-count based snapshot scheduling beyond the first trigger.
- Prometheus metrics and structured production logging.
- Production-grade security hardening.
- Kubernetes deployment or operator.

## Completion Definition

Each milestone should stay linked to:

- a roadmap entry in `../roadmaps`
- a design spec in `../superpowers/specs`
- an execution plan in `../superpowers/plans`
- tests that were observed red before implementation and green after implementation
- verification evidence from Go checks, smoke checks, or docs build as appropriate
