# 05 Etcd Metadata Manager

Roadmap index: [index.md](index.md)  
Previous: [04 Multiple Indexes Static Shards](04-multiple-indexes-static-shards.md)  
Next: [06 NATS Events Replay](06-nats-events-replay.md)  
Traceability: [PRD Traceability](prd-traceability.md)

## Outcome

Move authoritative cluster metadata into `KSMetadataManager`, backed by etcd first and shaped behind an interface that can support Consul later.

## Scope

- Store index definitions, shard assignments, tablet status, and checkpoints.
- Watch metadata changes and update coordinator routing cache.
- Use etcd transactions for compare-and-swap updates.
- Use leases where temporary ownership or locks are needed.
- Keep memberlist health advisory, not authoritative.

## Out of Scope

- Automatic placement.
- Consul backend.
- Dynamic rebalancing.
- NATS event data storage.

## Acceptance Criteria

- Coordinator reads index/shard/replica metadata from `KSMetadataManager`.
- Coordinator watches metadata and refreshes its routing cache.
- Concurrent metadata writes are protected by etcd transaction semantics.
- Metadata keys are namespaced and documented.
- Lost watch/reconnect behavior resumes from a safe revision or reloads state.

## TDD Plan Shape

- RED: metadata manager stores and retrieves an index definition.
- RED: compare-and-swap rejects stale updates.
- RED: watch callback updates a routing cache.
- RED: watch interruption triggers safe reload behavior.

## OMX Usage

Use architect review for key layout and consistency boundaries before implementation. Use verifier for watch/revision behavior.

## Verification

- Unit tests with an interface fake.
- Integration tests against etcd in Docker when practical.
- Failure-path tests for stale revisions and unavailable etcd.
