# 10 Memberlist Health Cluster Status

Roadmap index: [index.md](index.md)  
Previous: [09 Snapshots Restore](09-snapshots-restore.md)  
Next: [11 Docker Compose Local Ops](11-docker-compose-local-ops.md)  
Traceability: [PRD Traceability](prd-traceability.md)

## Outcome

Add advisory gossip membership and cluster status reporting.

## Scope

- Search nodes join memberlist on startup.
- Nodes gossip basic node status and reachable gRPC address.
- Nodes may gossip approximate load, disk availability, and hosted tablet IDs within memberlist metadata limits.
- Coordinator exposes cluster status, index status, shard status, tablet status, and checkpoint/status visibility.
- etcd remains authoritative for ownership and shard assignments.

## Out of Scope

- Gossip as source of truth.
- Automatic failover with primary election.
- Production-grade security hardening.

## Acceptance Criteria

- Search nodes can discover each other through memberlist.
- Membership changes are visible through cluster status.
- Gossip data is treated as advisory.
- Authoritative shard ownership still comes from metadata manager.
- Cluster status clearly reports node, tablet, shard, readiness, and checkpoint information.

## TDD Plan Shape

- RED: memberlist event updates advisory node view.
- RED: stale gossip cannot overwrite authoritative metadata.
- RED: cluster status includes tablets and checkpoints.
- RED: unhealthy node status causes coordinator to avoid replicas once selector integrates health.

## OMX Usage

Use verifier for authority-boundary review: gossip must not silently become metadata truth.

## Verification

- Unit tests for advisory health cache.
- Integration test with multiple memberlist nodes where practical.
- Cluster status endpoint tests.
