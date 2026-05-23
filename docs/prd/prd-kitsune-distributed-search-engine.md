# PRD: Kitsune Distributed Search Engine
> Infrastructure Image could be found at [distribuited_search_engine_architecture_diagram.png](./assets/distribuited_search_engine_architecture_diagram.png)
---

## 1. Introduction / Overview

Kitsune Distributed Search Engine is a Go-based distributed search system that uses **Bleve** as the local search engine inside each shard replica. Instead of implementing a custom tokenizer, inverted index, ranking system, prefix search, and typo tolerance from scratch, Kitsune will rely on Bleve for local full-text indexing and querying.

The system will distribute search across multiple nodes by splitting each logical index into shards. Each shard is hosted as one or more **KSTablet** replicas on search nodes. A **KSCoordinator** receives client search requests, reads the shard map from the metadata store, sends gRPC search requests to the correct shard replicas, merges results, and returns a single response to the client.

The redesigned architecture includes:

1. **Bleve** for local per-shard indexing and searching.
2. **KSCoordinator** for query routing, shard fan-out, and result merging.
3. **KSTablet** as the local unit of shard ownership and Bleve index management.
4. **etcd-first metadata manager** with an interface that can support Consul later.
5. **HashiCorp memberlist** for gossip-based membership and health hints.
6. **NATS JetStream** for document change event distribution.
7. **S3 or MinIO** for compressed shard snapshots, backup, restore, and replica bootstrap.
8. **gRPC** for coordinator-to-search-node communication.

The goal is to build a practical distributed search engine that is easier to implement than Elasticsearch-style systems, while still exposing realistic distributed-system concepts: sharding, replication, metadata, event replay, snapshots, health, and recovery.

---

## 2. Relationship to V1 and V2

### 2.1 V1 Relationship

V1 focused on building a custom local search engine with its own inverted index, tokenizer, ranking, prefix search, typo tolerance, and local persistence. In this new PRD, those custom local search internals are no longer the primary implementation path.

Kitsune should **not** implement its own full-text search core for the MVP. Bleve will provide local search capabilities.

### 2.2 V2 Relationship

V2 introduced a multi-node architecture with coordinator, search nodes, sharding, replication, NATS JetStream, gossip membership, metadata storage, and local persistence.

This PRD keeps the distributed architecture direction from V2 but replaces the local search implementation with Bleve and introduces clearer service boundaries:

1. `KSCoordinator` handles routing and result merging.
2. `KSSearchNode` hosts multiple `KSTablet` instances.
3. `KSTablet` wraps one local Bleve shard replica.
4. `KSMetadataManager` stores authoritative metadata in etcd first.
5. `KSMemberManager` uses memberlist for gossip membership.
6. `KSEventBus` uses NATS JetStream for document events.
7. `KSSnapshotStore` uses S3/MinIO for snapshots.

---

## 3. Goals

1. Build a Go-based distributed search engine using Bleve for local shard indexing and search.
2. Support multiple search nodes running as one logical search cluster.
3. Support logical indexes split into configurable shards.
4. Support multiple shard replicas across different search nodes.
5. Support a coordinator that routes search requests to shard replicas using gRPC.
6. Support result merging at the coordinator level.
7. Support document create, update, and delete events through NATS JetStream.
8. Support direct write APIs through the coordinator and direct event publishing into NATS JetStream.
9. Use etcd as the first metadata backend while designing `KSMetadataManager` as an interface that can support Consul later.
10. Use HashiCorp memberlist for node discovery and health gossip.
11. Use S3 or MinIO for shard snapshots, restore, backup, and replica bootstrap.
12. Keep hot search indexes local on search nodes for low-latency search.
13. Keep S3/MinIO out of the hot query path.
14. Provide clear APIs and operational flows that a junior developer can implement step by step.

---

## 4. User Stories

### User Story 1: Run a Local Distributed Search Cluster

As a developer, I want to run a local Kitsune cluster so that I can test distributed search behavior on my machine.

**Example:**

```txt
ks-coordinator
ks-node-a
ks-node-b
ks-node-c
etcd
nats-jetstream
minio
```

The coordinator should route search requests to search nodes based on shard metadata.

---

### User Story 2: Create a Search Index

As a backend developer, I want to create a logical search index with a configured shard count and replica count so that documents can be distributed across nodes.

**Example:**

```json
{
  "index": "products",
  "shardCount": 3,
  "replicationFactor": 2,
  "mapping": {
    "title": "text",
    "description": "text",
    "price": "number",
    "createdAt": "datetime"
  }
}
```

---

### User Story 3: Search Without Knowing Shard Locations

As an application user, I want to send a normal search query without knowing which node owns which shard.

**Example:**

```txt
GET /indexes/products/search?q=wireless keyboard
```

The coordinator should fan out the query to relevant shard replicas, merge results, and return one final ranked list.

---

### User Story 4: Index Documents Through the Coordinator

As a backend developer, I want to send document writes to the coordinator so that the coordinator can publish document change events to NATS JetStream.

**Example:**

```txt
POST /indexes/products/documents
```

The coordinator validates the request, calculates the shard ID, publishes the event, and returns an accepted response.

---

### User Story 5: Index Documents Through Direct Event Publishing

As a platform engineer, I want backend applications to publish document change events directly to NATS JetStream so that indexing can be decoupled from the coordinator API.

**Example:**

```txt
application service -> NATS JetStream -> shard replicas -> local Bleve indexes
```

This path is useful for services that already use event-driven architecture.

---

### User Story 6: Store Each Shard as a Local Bleve Index

As a search node, I want each tablet to manage its own local Bleve index directory so that shard replicas can be opened, closed, snapshotted, restored, and moved independently.

**Example local structure:**

```txt
/data/kitsune/
  indexes/
    products/
      shard-0/
        replica-node-a/
          bleve-index/
          checkpoint.json
      shard-3/
        replica-node-a/
          bleve-index/
          checkpoint.json
```

---

### User Story 7: Restore a Shard Replica from Snapshot

As an operator, I want a new search node to restore a shard replica from S3/MinIO so that the cluster can recover or add capacity without rebuilding everything from the beginning.

**Example flow:**

```txt
node assigned shard replica
  -> download latest snapshot
  -> verify manifest and checksum
  -> open Bleve index locally
  -> replay events after checkpoint
  -> mark tablet ready
```

---

### User Story 8: Observe Cluster Health

As an operator, I want to see which nodes, indexes, shards, and replicas are healthy so that I can diagnose cluster problems quickly.

**Example:**

```txt
GET /cluster/status
```

The response should show node health, tablet status, shard assignment, metadata connectivity, event bus connectivity, and snapshot store connectivity.

---

## 5. Functional Requirements

### 5.1 System Roles

1. The system must provide a **KSCoordinator** process.
2. The system must provide a **KSSearchNode** process.
3. The system must allow multiple search nodes to join the same logical cluster.
4. The system must allow a search node to host multiple tablets.
5. The system must define `KSTablet` as one local shard replica backed by one local Bleve index.
6. The system must allow the coordinator and search nodes to run as separate processes.
7. The system should support configuration through YAML or environment variables.

**Example process commands:**

```txt
kitsune coordinator --config ./configs/coordinator.yaml
kitsune node --config ./configs/node-a.yaml
```

---

### 5.2 KSCoordinator

8. The coordinator must expose a public REST API for index management, document writes, search, and cluster status.
9. The coordinator should expose a public gRPC API for clients that prefer gRPC.
10. The coordinator must watch index and shard metadata from `KSMetadataManager`.
11. The coordinator must maintain an in-memory routing cache of index-to-shard-to-replica assignments.
12. The coordinator must route search requests to search nodes using internal gRPC.
13. The coordinator must select one healthy replica per shard for search requests.
14. The coordinator must support fallback to another replica if the selected replica is unavailable.
15. The coordinator must merge shard-level search results into one final result list.
16. The coordinator must support pagination parameters such as `limit` and `offset`.
17. The coordinator must return clear error messages when an index does not exist or no healthy replica is available.
18. The coordinator must support document writes by publishing events to NATS JetStream.
19. The coordinator must not open or directly modify local Bleve index files.

---

### 5.3 KSSearchNode

20. A search node must expose an internal gRPC API for shard search operations.
21. A search node must expose an internal gRPC API for tablet lifecycle operations if required by the coordinator or operator tooling.
22. A search node must connect to `KSMetadataManager` on startup.
23. A search node must register itself in the metadata store with a lease.
24. A search node must join the memberlist gossip cluster on startup.
25. A search node must read its assigned tablets from the metadata store.
26. A search node must start one `KSTablet` instance for each assigned shard replica.
27. A search node must report tablet readiness to the metadata store.
28. A search node must consume document events from NATS JetStream for the shards it owns.
29. A search node must update local Bleve indexes when it consumes valid document events.
30. A search node must acknowledge document events only after the relevant tablet successfully applies the update.
31. A search node must upload and restore shard snapshots through `KSSnapshotStore`.
32. A search node must expose health information for operational debugging.

---

### 5.4 KSTablet

33. A tablet must represent exactly one shard replica.
34. A tablet must own exactly one local Bleve index directory.
35. A tablet must support opening an existing Bleve index from disk.
36. A tablet must support creating a new Bleve index if no local index exists.
37. A tablet must support document upsert operations.
38. A tablet must support document delete operations.
39. A tablet must support local search operations.
40. A tablet must store a checkpoint indicating the last applied document event.
41. A tablet must flush or safely close its Bleve index during shutdown.
42. A tablet must expose its current state: `initializing`, `restoring`, `replaying`, `ready`, `degraded`, or `failed`.
43. A tablet must prevent search requests from being served before it is ready.
44. A tablet must support creating a snapshot package from its local Bleve index directory.
45. A tablet must support restoring a local Bleve index directory from a verified snapshot.

---

### 5.5 Index and Shard Management

46. The system must allow creating a logical index.
47. The system must require `shardCount` when creating an index.
48. The system must require `replicationFactor` when creating an index.
49. The system must store index configuration in the metadata store.
50. The system must store shard-to-node assignments in the metadata store.
51. The system must calculate document shard assignment using deterministic hashing of the external document ID.
52. The system must keep shard count fixed after index creation for the MVP.
53. The system must assign shard replicas to different nodes where possible.
54. The system must prevent the same shard primary and secondary replica from being assigned to the same node when enough nodes exist.
55. The system must expose index metadata through an API endpoint.

**Example shard routing:**

```txt
shard_id = hash(document_id) % shard_count
```

---

### 5.6 Bleve Local Search

56. The system must use Bleve as the local search engine for each tablet.
57. The system must define a Bleve mapping when creating an index.
58. The system must support text fields.
59. The system must support numeric fields if Bleve mapping configuration enables them.
60. The system must support date/time fields if Bleve mapping configuration enables them.
61. The system must support keyword-style fields for filtering if Bleve mapping configuration enables them.
62. The system must support full-text match queries.
63. The system must support prefix queries where Bleve supports them.
64. The system must support fuzzy queries where Bleve supports them.
65. The system must support boolean queries where Bleve supports them.
66. The system should support highlighting if enabled in the search request.
67. The system should support facets if enabled in the search request and mapping.
68. The system must treat Bleve scoring as local shard scoring for the MVP.
69. The system must document that global score normalization is not included in the MVP.

---

### 5.7 Search API

70. The system must provide an endpoint to search a logical index.
71. The search request must include the index name.
72. The search request must support a query string.
73. The search request must support `limit`.
74. The search request must support `offset`.
75. The search request should support optional filters if Bleve mapping supports the relevant fields.
76. The search request should support optional fields to return.
77. The coordinator must fan out search requests to one healthy replica of each shard.
78. Each search node must return local top-k results from its tablet.
79. The coordinator must merge local top-k results from all shards.
80. The coordinator must return total hit information if available from shard responses.
81. The coordinator must include shard or node debugging information only when a debug flag is enabled.

**Example request:**

```json
{
  "q": "wireless keyboard",
  "limit": 20,
  "offset": 0,
  "highlight": true
}
```

---

### 5.8 Document Write API

82. The system must support document upsert through the coordinator.
83. The system must support document delete through the coordinator.
84. The coordinator must validate that the target index exists before publishing a document event.
85. The coordinator must calculate the target shard ID before publishing a document event.
86. The coordinator must publish document events to NATS JetStream.
87. The coordinator must return an accepted response after the event is successfully published.
88. The system must support direct document event publishing to NATS JetStream for trusted internal services.
89. The system must define a stable document event schema.
90. The system must include event ID, index name, document ID, operation type, document version, target shard ID, and event timestamp in each event.
91. The system must reject or ignore events that target unknown indexes.
92. The system must make document events idempotent where possible by using document version or event ID.

**Example document event:**

```json
{
  "eventId": "evt_000001",
  "index": "products",
  "documentId": "product_123",
  "operation": "upsert",
  "shardId": 0,
  "version": 12,
  "timestamp": "2026-05-23T10:00:00Z",
  "document": {
    "title": "Wireless Mechanical Keyboard",
    "description": "Compact keyboard with Bluetooth support",
    "price": 129.99
  }
}
```

---

### 5.9 NATS JetStream Event Bus

93. The system must use NATS JetStream as the document change event bus.
94. The system must define a stream for document changes.
95. The system must use subjects that allow shard-specific consumption.
96. The system must allow shard replicas to replay events after a snapshot checkpoint.
97. The system must ensure that a tablet acknowledges an event only after successful local indexing.
98. The system must expose lag information for each tablet consumer.
99. The system must handle duplicate events safely.
100. The system must handle event replay during recovery.

**Suggested subject pattern:**

```txt
kitsune.index.<index_name>.shard.<shard_id>.events
```

---

### 5.10 Metadata Manager

101. The system must define a `KSMetadataManager` interface.
102. The first implementation of `KSMetadataManager` must use etcd.
103. The interface design must allow a future Consul implementation.
104. The metadata store must contain index definitions.
105. The metadata store must contain Bleve mapping configuration or a reference to the mapping configuration.
106. The metadata store must contain shard assignments.
107. The metadata store must contain replica assignments.
108. The metadata store must contain tablet state.
109. The metadata store must contain node lease information.
110. The metadata store must contain latest snapshot pointers.
111. The metadata store must support watches so the coordinator can update its routing cache.
112. The metadata store must support locks or leases for operations such as snapshot creation, restore, and assignment changes.

**Example metadata keys:**

```txt
/kitsune/indexes/products/config
/kitsune/indexes/products/shards/0/replicas
/kitsune/indexes/products/shards/1/replicas
/kitsune/nodes/node-a/status
/kitsune/tablets/products/0/node-a/state
/kitsune/snapshots/products/0/latest
/kitsune/locks/products/shard-0/snapshot
```

---

### 5.11 Member Manager

113. The system must use HashiCorp memberlist for gossip-based membership.
114. Each search node must join the memberlist cluster on startup.
115. Search nodes must gossip basic node status.
116. Search nodes must gossip reachable gRPC address.
117. Search nodes should gossip approximate load and disk availability.
118. Search nodes should gossip hosted tablet IDs.
119. Gossip membership must be treated as advisory, not authoritative.
120. Official shard ownership must come from `KSMetadataManager`, not from gossip.
121. The coordinator does not need to join memberlist in the MVP.
122. Search nodes should report important membership changes to the metadata store when appropriate.

---

### 5.12 Snapshot Store

123. The system must define a `KSSnapshotStore` interface.
124. The first implementation must support S3-compatible storage, including MinIO.
125. The system must support uploading compressed shard snapshots.
126. The system must support downloading compressed shard snapshots.
127. Each snapshot must include a manifest file.
128. Each snapshot manifest must include index name, shard ID, replica source node, snapshot generation, Bleve mapping version, last applied event ID, creation time, and checksum.
129. The system must verify snapshot checksums before restoring.
130. The system must update the metadata store with the latest snapshot pointer after upload.
131. The system must use snapshots only for backup, restore, and replica bootstrap.
132. The system must not use S3/MinIO as the hot search path.

**Example snapshot layout:**

```txt
s3://kitsune-search/snapshots/products/shard-0/generation-000001/index.tar.zst
s3://kitsune-search/snapshots/products/shard-0/generation-000001/manifest.json
```

---

### 5.13 Replica Recovery

133. The system must support restoring a missing shard replica from the latest snapshot.
134. After restoring a snapshot, the tablet must replay document events after the snapshot checkpoint.
135. The tablet must mark itself as `ready` only after replay is complete.
136. If no snapshot exists, the tablet should support rebuilding from the event stream if retained events are available.
137. If neither snapshot nor event history is available, the tablet must enter a failed state and expose a clear error.
138. The system should document manual recovery steps for failed tablets.

---

### 5.14 Replication

139. The system must support a replication factor greater than one.
140. Each shard replica must consume the same document events for its shard.
141. Each replica must maintain its own local Bleve index.
142. The system must support searching from any healthy replica.
143. The coordinator must prefer ready replicas.
144. The coordinator must avoid replicas marked `failed`, `restoring`, or `replaying`.
145. The system must not replicate raw Bleve index files through Raft or etcd.
146. The system must use event replay and snapshots for replica synchronization.

---

### 5.15 Observability and Operations

147. The system must expose a cluster status endpoint.
148. The system must expose index status.
149. The system must expose shard and tablet status.
150. The system must expose node health.
151. The system must expose NATS consumer lag per tablet.
152. The system must expose snapshot generation and last snapshot time.
153. The system should expose Prometheus metrics.
154. The system should log key lifecycle events: node startup, metadata registration, tablet open, event apply, snapshot upload, snapshot restore, and search fan-out errors.
155. The system should include basic structured logging.

---

## 6. Non-Goals / Out of Scope

The following items are explicitly out of scope for this PRD:

1. Building a custom inverted index.
2. Building a custom tokenizer.
3. Building a custom ranking algorithm.
4. Building custom typo tolerance.
5. Replacing Bleve scoring with a custom global scoring system.
6. Dynamic shard rebalancing.
7. Fully automatic failover with primary election.
8. Multi-region deployment.
9. Strong global consistency across all replicas.
10. Distributed transactions.
11. Implementing Raft from scratch.
12. Replicating Bleve index files through Raft logs.
13. Using S3/MinIO as the hot search query path.
14. Vector search.
15. Hybrid vector and full-text search.
16. Advanced relevance tuning UI.
17. Multi-tenant billing, quotas, and organization management.
18. Production-grade security hardening.
19. Kubernetes operator implementation.
20. Cross-cluster replication.
21. Automatic schema migration for existing Bleve indexes.
22. Zero-downtime index mapping migration.
23. Perfect exactly-once indexing semantics.

These items may be considered in later versions after the core distributed Bleve-based architecture is stable.

---

## 7. Design Considerations

### 7.1 Architecture Diagram

The current architecture diagram should be treated as the visual reference for this PRD:

![Distributed Search Engine Architecture](../distribuited_search_engine_architecture_diagram.png)

If the image is unavailable in the implementation environment, the expected architecture is:

```txt
Client SDK / REST / gRPC
        ↓
Load Balancer
        ↓
KSCoordinator
        ↓ gRPC fan-out
KSSearch Cluster
  ├── Search Node A
  │   ├── KSTablet A0 -> Bleve local index
  │   └── KSTablet A3 -> Bleve local index
  ├── Search Node B
  │   ├── KSTablet B1 -> Bleve local index
  │   └── KSTablet B0 -> Bleve local index
  └── Search Node C
      ├── KSTablet C2 -> Bleve local index
      └── KSTablet C1 -> Bleve local index

External services:
  - KSMetadataManager: etcd first, Consul later
  - KSMemberManager: HashiCorp memberlist
  - KSEventBus: NATS JetStream
  - KSSnapshotStore: S3 / MinIO
```

### 7.2 Naming

Use the following names consistently:

| Name | Meaning |
|---|---|
| Kitsune | Product/system name |
| KSCoordinator | Query router and result merger |
| KSSearchNode | Node that hosts tablets |
| KSTablet | One local shard replica backed by Bleve |
| KSMetadataManager | Metadata, leases, locks, shard map |
| KSMemberManager | Gossip membership and health |
| KSEventBus | NATS JetStream document events |
| KSSnapshotStore | S3/MinIO snapshot storage |

### 7.3 Developer Experience

The implementation should include a Docker Compose setup for local development:

```txt
etcd
nats
minio
ks-coordinator
ks-node-a
ks-node-b
ks-node-c
```

A junior developer should be able to start the cluster with one command and run example search/indexing requests.

---

## 8. Technical Considerations

### 8.1 Recommended Go Modules

The implementation may use the following Go packages:

```txt
blevesearch/bleve      -> local search engine
nats-io/nats.go        -> NATS JetStream client
etcd-io/etcd/client/v3 -> etcd client
hashicorp/memberlist   -> gossip membership
grpc-go/grpc           -> internal gRPC communication
minio/minio-go         -> S3-compatible snapshot storage
```

### 8.2 Suggested Project Structure

```txt
kitsune/
  cmd/
    kitsune/
      main.go
  internal/
    coordinator/
    node/
    tablet/
    bleveindex/
    metadata/
      interface.go
      etcd/
      consul/        # future placeholder only
    member/
    eventbus/
    snapshot/
    routing/
    api/
    proto/
    config/
    observability/
  configs/
  deployments/
    docker-compose.yaml
  docs/
  tasks/
```

### 8.3 Internal gRPC Services

Suggested internal service definitions:

```txt
SearchNodeService
  - SearchShard
  - TabletStatus
  - NodeHealth

CoordinatorService
  - SearchIndex
  - UpsertDocument
  - DeleteDocument
  - CreateIndex
  - ClusterStatus
```

### 8.4 Metadata Backend Strategy

The PRD standardizes on:

```txt
interface first
etcd implementation first
Consul support later
```

This means junior developers should not implement Consul in the MVP, but the interface should not be tightly coupled to etcd-specific types.

### 8.5 Consistency Model

The MVP consistency model should be documented as:

```txt
Writes are accepted once published to NATS JetStream.
Search results are eventually consistent.
Replicas may temporarily differ while replaying events.
The coordinator should route only to ready replicas.
```

### 8.6 S3/MinIO Role

S3/MinIO should be used for:

```txt
backup
restore
replica bootstrap
shard migration support later
```

S3/MinIO should not be used for:

```txt
serving live search queries
per-document replication
storing metadata locks
```

---

## 9. Success Metrics

1. A developer can run a local three-node Kitsune cluster using Docker Compose.
2. A developer can create an index with three shards and replication factor two.
3. A developer can upsert documents through the coordinator API.
4. A developer can publish document events directly to NATS JetStream.
5. Search nodes consume document events and update the correct local Bleve shard indexes.
6. The coordinator can search across all shards and return merged results.
7. The coordinator can avoid routing search requests to tablets that are not ready.
8. Each search node can report tablet health and checkpoint information.
9. A tablet can create a compressed snapshot and upload it to S3/MinIO.
10. A new tablet replica can restore from the latest snapshot and replay remaining events.
11. The system can tolerate one search node being stopped if another healthy replica exists for the affected shards.
12. Cluster status clearly shows nodes, tablets, shard assignments, and health.
13. The architecture avoids custom full-text search internals and delegates local search behavior to Bleve.

---

## 10. Open Questions

1. Should index creation automatically assign shards to nodes, or should the first MVP use a static config file for shard assignment?
2. Should the coordinator expose both REST and gRPC in the first implementation, or REST first with gRPC internal only?
3. What is the minimum acceptable NATS JetStream retention period for event replay?
4. Should snapshot creation be time-based, event-count-based, or manually triggered in the first implementation?
5. Should direct NATS event publishing require a separate validation service, or should search nodes validate events themselves?
6. Should delete operations use hard deletes immediately or tombstones with later compaction?
7. How should Bleve mapping changes be handled for existing indexes in a later version?
8. What is the expected document size limit for MVP testing?
9. Should the MVP support multiple logical indexes immediately, or start with one index and generalize after the first working version?
10. Should node failure handling only avoid unhealthy replicas, or should it also trigger manual recovery workflows in the MVP?

---

## 11. Implementation Notes for Junior Developers

Start with the smallest working path:

1. Create one local `KSTablet` that wraps one Bleve index.
2. Add document upsert and search directly against that tablet.
3. Add a `KSSearchNode` that hosts the tablet and exposes gRPC `SearchShard`.
4. Add `KSCoordinator` that calls the search node through gRPC.
5. Add etcd metadata for index and shard assignment.
6. Add multiple search nodes and route by shard map.
7. Add NATS JetStream document events.
8. Add replica consumption.
9. Add S3/MinIO snapshot upload.
10. Add snapshot restore and event replay.
11. Add memberlist gossip health.
12. Add cluster status and metrics.

Do not start with automatic failover, dynamic rebalancing, or custom search internals. The first stable version should prove that a document can be indexed into local Bleve shard replicas, searched through the coordinator, snapshotted, restored, and observed through cluster status.
