# Kitsune 10 memberlist Health Cluster Status Design

Master spec: [Kitsune Distributed Search Roadmap Design](2026-05-23-kitsune-distributed-search-roadmap-design.md)  
Roadmap spec: [10 Memberlist Health Cluster Status](../../roadmaps/10-memberlist-health-cluster-status.md)  
Previous: [09 Snapshots Restore](2026-05-23-kitsune-09-snapshots-restore-design.md)  
Next: [11 Docker Compose Local Ops](2026-05-23-kitsune-11-docker-compose-local-ops-design.md)

## Goal

Add advisory gossip membership and expose cluster status across nodes, indexes, shards, tablets, readiness, and checkpoints.

## Architecture

memberlist provides gossip-based membership and advisory health. It never becomes authoritative shard ownership. etcd metadata remains authoritative; gossip helps the coordinator and operators understand liveness, reachable gRPC addresses, and rough node state.

## Components

- Member manager.
- memberlist delegate for compact node metadata.
- Membership event handler.
- Advisory health cache.
- Cluster status aggregator.
- REST cluster status endpoint.

## Data Flow

Search nodes join the memberlist cluster on startup and gossip compact status. The coordinator observes membership events and combines advisory health with authoritative metadata and tablet checkpoints. Cluster status reports both authoritative assignment and advisory liveness.

## Error Handling

Gossip failures degrade health visibility but do not mutate shard ownership. Oversized member metadata is rejected or trimmed according to the implementation plan. Stale gossip is marked stale and not treated as current authority.

## Testing

Implementation must use TDD for:

- Membership event updates advisory node view.
- Gossip cannot overwrite authoritative metadata.
- Cluster status includes nodes, shards, tablets, readiness, and checkpoints.
- Unhealthy advisory state influences replica avoidance only through explicit selector integration.

## Plan Handoff

Future plan path: `docs/superpowers/plans/2026-05-23-kitsune-10-memberlist-health-cluster-status.md`.

The plan should include authority-boundary tests to prevent gossip from becoming source of truth.
