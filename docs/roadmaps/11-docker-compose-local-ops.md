# 11 Docker Compose Local Ops

Roadmap index: [index.md](index.md)  
Previous: [10 Memberlist Health Cluster Status](10-memberlist-health-cluster-status.md)  
Next: none  
Traceability: [PRD Traceability](prd-traceability.md)

## Outcome

Provide a local developer cluster that demonstrates the full MVP path.

## Scope

- Docker Compose environment with coordinator, at least three search nodes, etcd, NATS JetStream, and MinIO.
- Healthchecks and startup ordering for service dependencies.
- Example static or metadata-backed cluster configuration.
- Developer commands for create index, upsert, direct event publish, search, snapshot, restore, node stop, and cluster status.
- Documentation for knobs including document size limit, paths, ports, retention, and snapshot storage.

## Out of Scope

- Kubernetes deployment.
- Multi-region deployment.
- Production security hardening.
- Managed cloud deployment.

## Acceptance Criteria

- A developer can run a local three-node Kitsune cluster using Docker Compose.
- A developer can create an index with three shards and replication factor two.
- A developer can upsert documents through the coordinator REST API.
- A developer can publish document events directly to NATS JetStream.
- Search nodes consume events and update local Bleve indexes.
- Coordinator searches across shards and returns merged results.
- Coordinator avoids not-ready tablets.
- A tablet can snapshot to MinIO and restore plus replay.
- The system tolerates one stopped search node when another healthy replica exists.
- Cluster status shows nodes, tablets, shard assignments, and health.

## TDD Plan Shape

- RED: compose smoke script fails before services exist.
- RED: local cluster script creates index and indexes documents.
- RED: stopping one node still allows search when replicas exist.
- RED: restore workflow rebuilds a missing tablet.

## OMX Usage

Use a verifier lane for end-to-end smoke coverage. Use team mode only if compose, docs, and tests are split into parallel implementation lanes.

## Verification

- `docker compose up` smoke test.
- REST smoke script.
- Direct NATS publish smoke script.
- Snapshot/restore smoke script.
- Node failure smoke script.
