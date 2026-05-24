---
title: Usage
description: How to build, test, run, call, and document Kitsune locally.
sidebar:
  label: Usage
  order: 3
---

# Usage

Kitsune currently exposes one binary with two process modes:

```powershell
kitsune coordinator --config ./deploy/local/config/coordinator.yaml
kitsune search-node --config ./deploy/local/config/search-node-a.yaml
```

The local Compose setup runs one coordinator, three search nodes, etcd, NATS JetStream, and a local S3-compatible object-storage service.

## Build and Test

Run the Go checks from the repository root:

```powershell
go test ./... -count=1
go vet ./...
```

Run the docs build from `kitsune-docs`:

```powershell
npm install
npm run build
```

## Local Cluster

Validate the Compose model:

```powershell
docker compose -f deploy/local/compose.yaml config
```

Start the cluster:

```powershell
docker compose -f deploy/local/compose.yaml up --build
```

The local topology exposes:

- coordinator REST API on `localhost:8080`
- etcd on `localhost:2379`
- NATS client port on `localhost:4222`
- NATS monitoring on `localhost:8222`
- local S3-compatible object storage on `localhost:9000`

Reset local state:

```powershell
docker compose -f deploy/local/compose.yaml down -v
```

## REST API

Create an index:

```http
POST /v1/indexes
Content-Type: application/json

{
  "name": "books",
  "shardCount": 3,
  "replicationFactor": 2,
  "mappingVersion": 1,
  "mapping": {
    "defaultAnalyzer": "standard"
  }
}
```

Upsert a document:

```http
PUT /v1/indexes/books/documents/doc-1
Content-Type: application/json

{
  "title": "Bleve distributed search"
}
```

Search:

```http
GET /v1/indexes/books/search?q=Bleve&limit=10&offset=0
```

Read cluster status:

```http
GET /v1/cluster/status
```

## Smoke Tests

Run the REST smoke after the cluster is up:

```powershell
go run ./scripts/smoke/localcluster
```

The smoke creates a `books` index, writes `doc-1`, polls until it appears in search results, and checks that cluster status includes nodes, tablets, and shard assignments.

Run the direct NATS smoke:

```powershell
go run ./scripts/smoke/directnats
```

The direct NATS smoke publishes the same validated document event envelope used by the coordinator event bus.

## Runtime Knobs

Local knobs live in `deploy/local/config/*.yaml`.

| Knob | Default |
| --- | --- |
| Coordinator HTTP address | `:8080` |
| Search-node gRPC addresses | `:9001`, `:9002`, `:9003` |
| Document payload limit | `1048576` bytes |
| NATS URL | `nats://nats:4222` |
| NATS stream | `KITSUNE_DOCUMENTS` |
| Event subject pattern | `kitsune.index.*.shard.*.events` |
| S3-compatible endpoint | `s3:9000` |
| S3-compatible bucket | `kitsune-snapshots` |
| S3-compatible region | `us-east-1` |

The default document payload limit is 1 MiB and is configurable through coordinator runtime config.

## Documentation Site

Run the Starlight docs locally:

```powershell
cd kitsune-docs
npm install
npm run dev
```

Build the static docs:

```powershell
cd kitsune-docs
npm run build
```
