# Kitsune 05 etcd Metadata Manager Design

Master spec: [Kitsune Distributed Search Roadmap Design](2026-05-23-kitsune-distributed-search-roadmap-design.md)  
Roadmap spec: [05 Etcd Metadata Manager](../../roadmaps/05-etcd-metadata-manager.md)  
Previous: [04 Multiple Indexes Static Shards](2026-05-23-kitsune-04-multiple-indexes-static-shards-design.md)  
Next: [06 NATS Events Replay](2026-05-23-kitsune-06-nats-events-replay-design.md)

## Goal

Move authoritative metadata into `KSMetadataManager`, backed by etcd first and abstracted for a possible Consul backend later.

## Architecture

`KSMetadataManager` is an interface owned by Kitsune. etcd is the first implementation. The coordinator maintains an in-memory route cache from metadata reads and watches. Search nodes use metadata to discover hosted tablet assignments and publish status/checkpoints.

## Components

- Metadata interface.
- etcd-backed implementation.
- Key namespace and value schemas.
- Transaction helpers for compare-and-swap updates.
- Watcher that resumes safely or reloads state.
- Routing cache integration.

## Data Flow

Index and shard metadata are written to etcd. Coordinators read the current metadata snapshot and watch for updates. Search nodes publish tablet status and checkpoints. Route cache updates are driven by etcd revisions and preserve ordering guarantees.

## Error Handling

Stale updates fail through transaction comparison. Lost watches resume from the last observed revision when possible; if history is compacted, the coordinator reloads a full snapshot. etcd unavailability surfaces as degraded coordinator/search-node state with clear errors.

## Testing

Implementation must use TDD for:

- Store and retrieve index definitions.
- Compare-and-swap stale update rejection.
- Watch update propagation to route cache.
- Watch interruption and safe reload behavior.
- Metadata key namespace validation.

## Plan Handoff

Future plan path: `docs/superpowers/plans/2026-05-23-kitsune-05-etcd-metadata-manager.md`.

The plan should start with interface-level fake tests, then add etcd integration tests.
