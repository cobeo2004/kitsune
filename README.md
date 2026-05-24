# Kitsune <img src="docs/assets/kitsune-logo.png" style="width: 50px; height: 50px;"></img> 

Kitsune is a Go distributed search engine built around Bleve shard replicas. It is intentionally small enough to study, but shaped like a real distributed search system: REST writes and searches at the coordinator, gRPC fan-out to search nodes, etcd metadata, NATS JetStream replay, S3-compatible snapshots, and advisory gossip health.

![Kitsune distributed search architecture](docs/prd/assets/distribuited_search_engine_architecture_diagram.png)

## Documentation

The full documentation lives in the Astro Starlight-ready docs site under `kitsune-docs/src/content/docs`.

- Start here: `kitsune-docs/src/content/docs/index.md`
- Architecture: `kitsune-docs/src/content/docs/architecture.md`
- Usage: `kitsune-docs/src/content/docs/usage.md`
- Components: `kitsune-docs/src/content/docs/components.md`
- Technical decisions: `kitsune-docs/src/content/docs/technical-decisions.md`
- Roadmap: `kitsune-docs/src/content/docs/roadmap.md`

Run the docs site:

```powershell
cd kitsune-docs
npm install
npm run dev
```

## Current Implementation

The current codebase contains the first distributed-search slices:

- Bleve-backed tablet open, upsert, delete, search, metadata, and mapping-version checks.
- Search-node gRPC API and tablet registry.
- REST coordinator index creation, document write validation, search fan-out, result merge, and cluster status.
- Static multi-index shard routing with replica selection.
- etcd and in-memory metadata manager implementations with snapshots and watches.
- Document event validation, in-memory event bus, JetStream publication boundary, and replay applier.
- Tombstone-aware replay ordering and compaction-safe checkpoint evidence.
- Compressed snapshot packaging, checksum verification, S3-compatible storage, and trusted restore boundaries.
- HashiCorp memberlist advisory health cache surfaced through cluster status.
- Docker Compose wiring for a local coordinator, three search nodes, etcd, NATS, and local S3-compatible object storage.

Search is eventually consistent. A coordinator document write is accepted after the document event is published. The replay applier validates shard identity, applies events to tablets, acknowledges messages after successful apply, and persists checkpoints. Gossip health is advisory only; shard ownership and tablet readiness stay anchored in metadata.

## Quick Start

Build and test the repository:

```powershell
go test ./... -count=1
go vet ./...
```

Validate the local Compose model:

```powershell
docker compose -f deploy/local/compose.yaml config
```

Start a local cluster when Docker is available:

```powershell
docker compose -f deploy/local/compose.yaml up --build
```

Exercise the REST path:

```powershell
go run ./scripts/smoke/localcluster
```

Exercise direct NATS publication:

```powershell
go run ./scripts/smoke/directnats
```

## API Preview

Create an index:

```http
POST /v1/indexes
Content-Type: application/json

{
  "name": "books",
  "shardCount": 3,
  "replicationFactor": 2,
  "mappingVersion": 1,
  "mapping": { "defaultAnalyzer": "standard" }
}
```

Upsert a document:

```http
PUT /v1/indexes/books/documents/doc-1
Content-Type: application/json

{ "title": "Bleve distributed search" }
```

Search:

```http
GET /v1/indexes/books/search?q=Bleve&limit=10
```

Inspect cluster status:

```http
GET /v1/cluster/status
```

## Project Map

- `main.go`: `kitsune coordinator` and `kitsune search-node` process entrypoints.
- `api/searchnode/v1`: internal gRPC search-node service API.
- `internal/tablet`: local Bleve-backed shard replica.
- `internal/searchnode`: tablet host and gRPC server boundary.
- `internal/coordinator`: REST API, routing, search fan-out, merge, and status surface.
- `internal/metadata`: in-memory and etcd metadata managers.
- `internal/events`: document-event schema and NATS JetStream publisher.
- `internal/replay`: shard replay and checkpoint application.
- `internal/snapshot`: compressed snapshots, restore, filesystem store, and S3-compatible store.
- `internal/compaction`: tombstone compaction safety checks.
- `internal/member`: memberlist-backed advisory node health.
- `deploy/local`: local Compose topology and runtime YAML knobs.
- `scripts/smoke`: local cluster and direct NATS smoke programs.

## Roadmap

The implementation is intentionally milestone-driven. Start with:

- Product requirements: `docs/prd/prd-kitsune-distributed-search-engine.md`
- Roadmap index: `docs/roadmaps/index.md`
- Detailed specs: `docs/superpowers/specs`
- Execution plans: `docs/superpowers/plans`

The root docs under `docs/` are planning and requirements artifacts. The Starlight docs under `kitsune-docs/src/content/docs/` are the human-facing documentation set intended for publishing.
