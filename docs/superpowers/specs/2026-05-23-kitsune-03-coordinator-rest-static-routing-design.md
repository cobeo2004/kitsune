# Kitsune 03 Coordinator REST Static Routing Design

Master spec: [Kitsune Distributed Search Roadmap Design](2026-05-23-kitsune-distributed-search-roadmap-design.md)  
Roadmap spec: [03 Coordinator REST Static Routing](../../roadmaps/03-coordinator-rest-static-routing.md)  
Previous: [02 Search Node gRPC](2026-05-23-kitsune-02-search-node-grpc-design.md)  
Next: [04 Multiple Indexes Static Shards](2026-05-23-kitsune-04-multiple-indexes-static-shards-design.md)

## Goal

Build the first public coordinator surface: REST APIs that use static shard routing and internal gRPC search-node clients.

## Architecture

`KSCoordinator` owns client-facing REST APIs and routing. It does not open local Bleve indexes. Static config supplies index, shard, replica, and node endpoint information until etcd metadata is introduced later.

## Components

- REST router and handlers.
- Static route table.
- Internal shard client interface.
- Result merger and paginator.
- Request validation for index creation, document writes, and search.
- Config knobs including default 1 MiB document payload limit.

## Data Flow

REST clients create indexes, upsert documents, and search through the coordinator. In the pre-NATS milestone, writes may call internal node/tablet APIs directly as a transitional path. Searches fan out to statically configured shard replicas, collect hits, merge by score, and apply pagination.

## Error Handling

Missing index, invalid mapping, invalid shard count, missing healthy replica, oversized document payload, and shard RPC failure produce stable REST errors. The coordinator returns partial-failure behavior only if the implementation plan explicitly defines it; otherwise a required shard failure makes the search fail clearly.

## Testing

Implementation must use TDD for:

- REST index creation validation.
- Default 1 MiB payload rejection.
- Configurable payload limit.
- Search fan-out through fake shard clients.
- Merged result ordering and pagination.
- Clear missing-index and no-replica errors.

## Plan Handoff

Future plan path: `docs/superpowers/plans/2026-05-23-kitsune-03-coordinator-rest-static-routing.md`.

The plan should prefer fake shard clients for coordinator unit tests and add one small integration test with a real search node.
