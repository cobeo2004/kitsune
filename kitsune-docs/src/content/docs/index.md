---
title: Kitsune Overview
description: Start here for the Kitsune distributed search engine architecture, usage, and roadmap.
sidebar:
  label: Overview
  order: 1
---

# Kitsune

![Kitsune logo](/assets/kitsune-logo.png)

Kitsune is a Go distributed search engine that uses Bleve as the local search engine inside each shard replica. The project keeps the full-text search core local and focused while adding distributed-system boundaries around it: routing, replication, metadata, durable event replay, snapshots, and cluster status.

![Kitsune distributed search architecture](/assets/distributed-search-architecture.png)

## What Kitsune Provides

- Logical indexes split into fixed shards.
- One or more `KSTablet` replicas per shard.
- REST coordinator APIs for index creation, document writes, search, and cluster status.
- Internal gRPC search-node APIs for shard search.
- etcd-backed metadata with an in-memory implementation for tests.
- NATS JetStream document events and replay.
- Tombstone-aware delete handling with compaction safety checks.
- Compressed snapshots stored through an S3-compatible API.
- HashiCorp memberlist health hints for search nodes.
- Docker Compose topology for a local three-node development cluster.

## Consistency Model

Kitsune is eventually consistent. A write through the coordinator succeeds after the document event is accepted by the event bus. Search nodes consume shard-specific events, apply them to local Bleve tablets, acknowledge only after a successful apply, and publish checkpoint evidence through metadata.

Search routes use metadata and tablet readiness. Memberlist health is advisory and does not replace metadata ownership.

## Documentation Map

- [Architecture](./architecture/) explains the process topology, request flows, and state boundaries.
- [Usage](./usage/) shows local development, API calls, smoke tests, and docs-site commands.
- [Components](./components/) maps the Go packages and runtime services.
- [Technical Decisions](./technical-decisions/) records the durable design decisions.
- [Roadmap](./roadmap/) links the implemented milestones to the remaining product direction.

## Source Artifacts

The Starlight docs are the publishable human-facing documentation. The planning source remains in the repository:

- PRD: `docs/prd/prd-kitsune-distributed-search-engine.md`
- Roadmaps: `docs/roadmaps`
- Specs: `docs/superpowers/specs`
- Plans: `docs/superpowers/plans`
- Local operations notes: `docs/operations/local-cluster.md`
