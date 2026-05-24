# Kitsune 11 Docker Compose Local Ops Design

Master spec: [Kitsune Distributed Search Roadmap Design](2026-05-23-kitsune-distributed-search-roadmap-design.md)  
Roadmap spec: [11 Docker Compose Local Ops](../../roadmaps/11-docker-compose-local-ops.md)  
Previous: [10 memberlist Health Cluster Status](2026-05-23-kitsune-10-memberlist-health-cluster-status-design.md)  
Next: none

## Goal

Provide a local developer cluster that demonstrates the full MVP path.

## Architecture

Docker Compose runs the coordinator, at least three search nodes, etcd, NATS JetStream, and S3-compatible object storage. Healthchecks control startup readiness where dependencies need to be healthy before dependent services start. Scripts or documented commands demonstrate the PRD success metrics end to end.

## Components

- Compose file.
- Coordinator service.
- Search node services.
- etcd service.
- NATS JetStream service.
- S3-compatible object storage service for local snapshots.
- Local config files and knobs.
- Smoke scripts or documented command sequence.

## Data Flow

The developer starts Compose. Services become healthy in dependency order. The developer creates an index, writes documents through REST and direct NATS events, searches through the coordinator, triggers snapshot/restore, stops one node, and verifies search continues when another replica is ready.

## Error Handling

Compose healthchecks expose dependency readiness. Smoke scripts fail fast with clear command and response output. Local port conflicts and missing Docker dependencies are documented as setup failures, not Kitsune runtime failures.

## Testing

Implementation must use TDD-style smoke development:

- A failing smoke script for missing services.
- A green local cluster startup path.
- Create-index and write/search smoke tests.
- Direct NATS publish smoke test.
- Snapshot/restore smoke test.
- One-node-stopped search smoke test.

## Plan Handoff

Future plan path: `docs/superpowers/plans/2026-05-23-kitsune-11-docker-compose-local-ops.md`.

The plan should use verification commands that a new developer can run without hidden local state.
