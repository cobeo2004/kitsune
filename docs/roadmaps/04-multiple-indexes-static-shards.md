# 04 Multiple Indexes Static Shards

Roadmap index: [index.md](index.md)  
Previous: [03 Coordinator REST Static Routing](03-coordinator-rest-static-routing.md)  
Next: [05 Etcd Metadata Manager](05-etcd-metadata-manager.md)  
Traceability: [PRD Traceability](prd-traceability.md)

## Outcome

Generalize the static-routing milestone so multiple logical indexes work from the start.

## Scope

- Multiple named logical indexes.
- Static shard and replica assignment per index.
- Deterministic tablet identity and storage paths per index/shard/replica.
- Mapping version per index.
- Request validation that prevents cross-index ambiguity.

## Out of Scope

- Automatic shard placement.
- Shard rebalancing.
- Mapping migration.
- etcd as source of truth.

## Acceptance Criteria

- Two indexes can have different mappings and shard counts.
- Documents with the same ID in different indexes do not conflict.
- Search is isolated by index.
- Static config can assign shards for multiple indexes.
- Invalid static assignment is rejected at startup or index creation.

## TDD Plan Shape

- RED: documents with the same ID remain isolated across indexes.
- RED: searching index A does not query index B tablets.
- RED: invalid shard assignment fails validation.
- RED: mapping changes after index creation fail.

## OMX Usage

Solo execution is likely sufficient. Use verifier for config validation edge cases.

## Verification

- Multi-index integration tests.
- Config validation tests.
- Storage path collision tests.
