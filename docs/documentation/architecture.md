---
title: Architecture
description: How Kitsune routes search, accepts writes, replays events, stores metadata, and restores shard replicas.
sidebar:
  label: Architecture
  order: 2
---

# Architecture

Kitsune separates the hot search path from durability and recovery paths. Search uses local Bleve indexes hosted by search nodes. Durability uses NATS JetStream events, etcd metadata, and S3-compatible snapshots.

## Runtime Topology

```txt
Client
  -> KSCoordinator REST API
      -> metadata route cache
      -> internal gRPC fan-out
          -> KSSearchNode
              -> KSTablet
                  -> local Bleve index

Durability and control plane:
  - etcd stores index, shard, replica, tablet, checkpoint, and snapshot metadata.
  - NATS JetStream stores document events for replay.
  - S3-compatible object storage stores compressed shard snapshots.
  - memberlist provides advisory search-node health.
```

## Search Flow

1. A client calls `GET /v1/indexes/{index}/search?q=...`.
2. The coordinator loads the index definition and shard routes from its metadata-backed route cache.
3. For each shard, the coordinator selects ready replicas and tries one replica first.
4. If the selected replica fails, the coordinator falls back to another ready replica for that shard.
5. Search nodes execute local Bleve searches against the addressed tablet.
6. The coordinator merges shard results, applies `limit` and `offset`, and returns one response.

The coordinator does not open Bleve files. It routes and merges only.

## Write Flow

1. A client calls `PUT /v1/indexes/{index}/documents/{documentID}`.
2. The coordinator validates the index and document payload.
3. The coordinator computes `shardID = hash(documentID) % shardCount`.
4. The coordinator publishes a document event to `kitsune.index.<index>.shard.<shardID>.events`.
5. The coordinator returns an accepted response after publication succeeds.
6. Every replica that owns the shard consumes the event through its durable JetStream consumer.
7. The replay applier validates shard identity and mapping version before applying the event.
8. The tablet updates or deletes the local Bleve document.
9. The consumer acknowledges after successful apply and persists checkpoint evidence.

This is why search is eventually consistent: publication is durable before every replica has necessarily applied the event.

## Metadata Model

`KSMetadataManager` is the authority for:

- index definitions
- shard-to-replica assignments
- tablet state and readiness
- document replay checkpoints
- latest snapshot pointers
- metadata watches for route-cache refresh

The interface has both an in-memory implementation and an etcd implementation. The coordinator can start from a metadata snapshot and then apply watch events as the cluster changes.

## Snapshot and Restore Flow

Snapshots are used for backup, restore, and replica bootstrap. They are not in the hot query path.

1. A tablet exports a snapshot payload and manifest.
2. The snapshot package is compressed.
3. The store verifies checksum and identity before persisting.
4. The S3-compatible store writes data and manifest objects under deterministic object names.
5. Metadata stores a pointer to the latest known snapshot.
6. Restore downloads the manifest and data, verifies checksum and identity, restores the tablet payload, then replays events after the snapshot checkpoint.

If no snapshot exists, recovery can fall back to full event replay only when retained events are still available.

## Health and Routing

memberlist gives fast advisory health about search nodes. It is deliberately not the source of shard ownership. The coordinator combines health hints with metadata-backed tablet state and selects only ready replicas for search.

This avoids using gossip as a control plane while still surfacing useful operator status.
