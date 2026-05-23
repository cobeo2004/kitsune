# 03 Coordinator REST Static Routing

Roadmap index: [index.md](index.md)  
Previous: [02 Search Node gRPC](02-search-node-grpc.md)  
Next: [04 Multiple Indexes Static Shards](04-multiple-indexes-static-shards.md)  
Traceability: [PRD Traceability](prd-traceability.md)

## Outcome

Create `KSCoordinator` with REST-first client APIs and static shard routing to search nodes over internal gRPC.

## Scope

- Public REST endpoints for index management, document writes, search, and basic cluster status.
- Static routing config for index-to-shard-to-replica assignment.
- Coordinator fan-out to shard replicas over internal gRPC.
- Result merging at coordinator level.
- Default 1 MiB JSON document payload limit, configurable through knobs.

## Out of Scope

- Public gRPC API.
- etcd metadata.
- NATS event bus.
- Automatic shard placement.
- Replica health failover beyond static availability checks.

## Acceptance Criteria

- REST index creation validates index name, shard count, replication factor, and immutable mapping.
- REST document upsert accepts documents within the configured payload limit.
- Oversized documents fail with a clear client error.
- REST search fans out to the statically configured shard replicas.
- Coordinator merges shard results and applies limit/offset.
- Missing index and missing healthy replica errors are clear.
- Coordinator never opens local Bleve files directly.

## TDD Plan Shape

- RED: create index validates required fields.
- RED: upsert rejects payloads above 1 MiB by default.
- RED: search routes to configured shard RPC clients.
- RED: shard results are merged and paginated.
- RED: missing index returns a clear REST error.

## OMX Usage

Use a planner or architect review before implementation if REST route shape or response schemas are still disputed. Use TDD for each endpoint behavior.

## Verification

- REST handler tests.
- Coordinator routing unit tests with fake shard clients.
- Integration smoke test with one coordinator and one search node.
