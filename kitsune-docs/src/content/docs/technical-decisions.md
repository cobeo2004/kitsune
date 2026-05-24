---
title: Technical Decisions
description: Durable design decisions, constraints, and rejected alternatives for Kitsune.
sidebar:
  label: Technical Decisions
  order: 5
---

# Technical Decisions

This page records decisions that should remain stable unless a future roadmap explicitly changes them.

## Use Bleve for Local Search

Kitsune delegates tokenization, indexing, querying, and scoring to Bleve inside each tablet.

Constraint: the distributed system is the learning and product focus.
Rejected: building a custom full-text engine for MVP, because it would dominate implementation effort and delay sharding, replication, replay, and recovery.

## Keep Coordinator REST-First

The public API is REST-first for MVP. Internal coordinator-to-search-node communication uses gRPC.

Constraint: REST is easier to smoke test and document early.
Rejected: public gRPC first, because it increases client and docs overhead before the distributed path is stable.

## Treat Writes as Eventually Consistent

A coordinator write succeeds when the document event is accepted by the event bus. Search nodes apply events asynchronously.

Constraint: replicas consume their own shard streams and can lag independently.
Rejected: read-after-write by default, because it requires cross-replica freshness coordination that is outside the MVP.

## Use etcd as Metadata Authority

Shard ownership, tablet state, checkpoints, and snapshot pointers live behind `KSMetadataManager`, with etcd as the first distributed backend.

Constraint: routing and recovery require an authoritative control plane.
Rejected: using memberlist gossip for ownership, because gossip health is approximate and eventually convergent.

## Use NATS JetStream for Document Events

Writes are represented as durable document events. Replicas consume shard-specific subjects and acknowledge after successful local apply.

Constraint: replicas need replay after restart, snapshot restore, and consumer lag.
Rejected: direct coordinator-to-tablet writes only, because it couples write success to every replica and weakens recovery.

## Use S3-Compatible APIs for Snapshots

Snapshots use an S3-compatible object-store API through the AWS SDK for Go v2 S3 client.

Constraint: production and local development should share the same object-store contract.
Rejected: depending on a MinIO-specific SDK, because the project should target S3-compatible APIs rather than one server implementation.

## Keep Object Storage Out of the Hot Path

Search nodes serve from local Bleve indexes. Object storage is for backup, restore, and replica bootstrap.

Constraint: search latency should not depend on remote object storage.
Rejected: serving live search queries from object storage, because it would make the hot path slower and operationally fragile.

## Use Tombstones Before Compaction

Deletes are represented with tombstone evidence. Compaction is allowed only when snapshot and replay checkpoints prove the tombstone is no longer needed for recovery.

Constraint: restored replicas must not resurrect deleted documents.
Rejected: immediate hard delete without durable delete evidence, because replay and snapshot restore could lose delete intent.

## Keep Shard Counts Fixed for MVP

Indexes have fixed shard counts after creation.

Constraint: dynamic re-sharding requires placement, movement, and reindex workflows.
Rejected: automatic shard rebalancing in MVP, because static assignment is enough to validate routing and replication.

## Default Document Limit Is a Knob

The default coordinator document payload limit is 1 MiB and lives in runtime config.

Constraint: the API needs a predictable resource boundary.
Rejected: unlimited document payloads, because they create unbounded memory and event-bus pressure.

## Make Health Advisory

memberlist health helps status and routing preference, but metadata remains the source of ownership and readiness.

Constraint: gossip is useful for quick liveness hints but not strong control-plane truth.
Rejected: routing only from gossip, because shard movement and tablet readiness must be explicit.
