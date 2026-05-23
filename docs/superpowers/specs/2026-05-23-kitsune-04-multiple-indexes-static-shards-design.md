# Kitsune 04 Multiple Indexes Static Shards Design

Master spec: [Kitsune Distributed Search Roadmap Design](2026-05-23-kitsune-distributed-search-roadmap-design.md)  
Roadmap spec: [04 Multiple Indexes Static Shards](../../roadmaps/04-multiple-indexes-static-shards.md)  
Previous: [03 Coordinator REST Static Routing](2026-05-23-kitsune-03-coordinator-rest-static-routing-design.md)  
Next: [05 etcd Metadata Manager](2026-05-23-kitsune-05-etcd-metadata-manager-design.md)

## Goal

Support multiple logical indexes from the start while still using static shard assignment.

## Architecture

Index name becomes part of every durable identity: tablet ID, route key, storage path, event key, and API route. Static config remains the source of assignment truth in this milestone. Mapping definitions are per-index and immutable after creation.

## Components

- Index registry.
- Static assignment validator.
- Per-index mapping records.
- Per-index route lookup.
- Storage path builder that prevents cross-index collisions.

## Data Flow

An index is created with mapping, shard count, replication factor, and static assignments. Documents are written and searched by index name. The coordinator routes each index independently and never mixes tablets from another index.

## Error Handling

Duplicate index creation, invalid index names, missing static assignments, cross-index path collisions, invalid shard IDs, and mapping changes return explicit validation errors.

## Testing

Implementation must use TDD for:

- Two indexes with different mappings.
- Same document ID isolated across indexes.
- Search isolation by index.
- Static assignment validation.
- Storage path collision prevention.

## Plan Handoff

Future plan path: `docs/superpowers/plans/2026-05-23-kitsune-04-multiple-indexes-static-shards.md`.

The plan should refactor earlier single-index assumptions only after tests prove the multi-index behavior fails.
