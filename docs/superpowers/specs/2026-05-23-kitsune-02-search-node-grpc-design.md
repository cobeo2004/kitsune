# Kitsune 02 Search Node gRPC Design

Master spec: [Kitsune Distributed Search Roadmap Design](2026-05-23-kitsune-distributed-search-roadmap-design.md)  
Roadmap spec: [02 Search Node gRPC](../../roadmaps/02-search-node-grpc.md)  
Previous: [01 Bleve Tablet Core](2026-05-23-kitsune-01-bleve-tablet-core-design.md)  
Next: [03 Coordinator REST Static Routing](2026-05-23-kitsune-03-coordinator-rest-static-routing-design.md)

## Goal

Build `KSSearchNode`, a process boundary that owns tablets and exposes internal gRPC shard operations.

## Architecture

The search node contains a tablet registry and a gRPC server. It does not decide global routing or ownership; it serves operations for tablets it hosts. Internal gRPC is used here because the coordinator will call search nodes across process boundaries.

## Components

- Tablet registry keyed by index, shard, and replica.
- gRPC service definitions for shard search and status.
- gRPC server implementation that delegates to tablets.
- Status mapper from tablet state to internal API responses.
- Error mapper from Go errors to gRPC status codes.

## Data Flow

The node starts with configured tablet identities, opens tablets, then serves gRPC. A `SearchShard` request identifies the tablet and query. The node finds the tablet, checks readiness, calls tablet search, and returns hits. Status requests enumerate hosted tablets and readiness.

## Error Handling

Unknown tablets return `NotFound`. Not-ready tablets return an unavailable-style status. Invalid requests return invalid argument errors. Internal Bleve/tablet failures are returned as internal status errors with concise messages.

## Testing

Implementation must use TDD for:

- Starting with one hosted tablet.
- Returning status for hosted tablets.
- Searching through gRPC.
- Returning `NotFound` for missing tablets.
- Mapping tablet errors to gRPC statuses.

## Plan Handoff

Future plan path: `docs/superpowers/plans/2026-05-23-kitsune-02-search-node-grpc.md`.

The plan should include generated protobuf ownership and a reproducible generation command.
