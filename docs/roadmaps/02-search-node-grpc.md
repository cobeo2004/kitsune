# 02 Search Node gRPC

Roadmap index: [index.md](index.md)  
Previous: [01 Bleve Tablet Core](01-bleve-tablet-core.md)  
Next: [03 Coordinator REST Static Routing](03-coordinator-rest-static-routing.md)  
Traceability: [PRD Traceability](prd-traceability.md)

## Outcome

Create `KSSearchNode`, a process-level owner of tablets that exposes internal gRPC APIs for shard operations.

## Scope

- Host multiple tablets in one search node.
- Expose internal gRPC for shard search.
- Expose internal gRPC for document upsert/delete if needed before NATS is introduced.
- Report tablet status from the node.
- Keep gRPC internal-only at this milestone.

## Out of Scope

- Public gRPC clients.
- Coordinator REST.
- Dynamic shard assignment.
- NATS replay.
- memberlist gossip.

## Acceptance Criteria

- A search node can start with a configured tablet set.
- A gRPC client can search a named tablet.
- Unknown tablet requests return a clear gRPC `NotFound` status.
- Tablet errors map to appropriate gRPC status errors.
- The node can report hosted tablet IDs and readiness.

## TDD Plan Shape

- RED: starting a node with one tablet exposes it in status.
- RED: gRPC `SearchShard` returns local tablet results.
- RED: searching a missing tablet returns `NotFound`.
- RED: node startup fails clearly when tablet config is invalid.

## OMX Usage

Use an executor lane for protobuf/service implementation if the `.proto` surface and server package are handled separately. Use verifier for generated-code and test coverage review.

## Verification

- Unit tests for node tablet registry behavior.
- gRPC integration tests over an in-process or local listener.
- Generated protobuf files are reproducible from committed `.proto` definitions.
