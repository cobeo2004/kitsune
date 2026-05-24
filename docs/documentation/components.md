---
title: Components
description: The runtime services and Go packages that make up Kitsune.
sidebar:
  label: Components
  order: 4
---

# Components

Kitsune is organized around narrow runtime roles and matching Go packages.

## Process Components

### KSCoordinator

Package: `internal/coordinator`

The coordinator is the public REST entrypoint. It owns index creation, document write validation, search fan-out, result merge, cluster status, and metadata-watch driven route-cache updates.

It does not own local Bleve indexes.

### KSSearchNode

Package: `internal/searchnode`

A search node hosts tablets and exposes internal gRPC search APIs. In the local deployment, each search node opens configured tablets, registers ready tablet status in metadata, starts JetStream consumers for its tablets, and optionally joins memberlist.

### KSTablet

Package: `internal/tablet`

A tablet is one local shard replica backed by one Bleve index directory. It validates identity, opens or creates the local Bleve index, applies upserts and deletes, exposes local search, and reports state.

### KSMetadataManager

Package: `internal/metadata`

The metadata manager is the authoritative control-plane interface. It stores index records, shard replica records, tablet statuses, checkpoints, and snapshot pointers. It supports both full snapshots and watch events.

Implementations:

- `MemoryManager` for tests and local in-process use.
- `EtcdManager` for the distributed metadata backend.

### KSEventBus

Package: `internal/events`

The event bus validates and publishes document events. The NATS implementation publishes JSON document events to shard-specific JetStream subjects.

### Replay Applier

Package: `internal/replay`

Replay validates event identity, applies events to tablets, stores checkpoints, and acknowledges messages only after the local apply succeeds.

### KSSnapshotStore

Package: `internal/snapshot`

Snapshots package tablet state with a manifest and checksum. The package includes restore validation, filesystem storage for tests, and an S3-compatible object store backed by the AWS SDK for Go v2 S3 client.

### Compaction

Package: `internal/compaction`

Compaction enforces tombstone safety around snapshot checkpoint floors. It prevents compaction from removing delete evidence before snapshots and replay guarantees make that safe.

### KSMemberManager

Package: `internal/member`

The member manager wraps memberlist health and node-view behavior. Its output is advisory and feeds status or route preference, not authoritative shard ownership.

### Cluster Status

Package: `internal/status`

Cluster status aggregates node views, index metadata, route metadata, tablet states, checkpoints, and snapshot pointers into an operator-facing response.

## API and Deployment Components

### Search Node API

Package: `api/searchnode/v1`

The internal gRPC API contains the search-node service contract used by the coordinator for shard-level search.

### Binary Entrypoint

File: `main.go`

The binary supports:

- `coordinator --config <path>`
- `search-node --config <path>`

It loads strict YAML config, wires external clients, starts HTTP or gRPC servers, and handles graceful shutdown.

### Local Deployment

Directory: `deploy/local`

The local deployment includes Docker Compose, coordinator config, and three search-node configs. It is the reference topology for smoke tests and development.

### Smoke Programs

Directory: `scripts/smoke`

- `localcluster` tests the REST path through the coordinator.
- `directnats` tests direct document-event publication.
