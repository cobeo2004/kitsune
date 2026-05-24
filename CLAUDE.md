# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Frontend design
- For frontend design concepts, including layouts, themes, fonts, etc., always follow @DESIGN.md

## Project

**Kitsune** is a Go-based distributed search engine. Local per-shard indexing is delegated to **Bleve** — Kitsune does **not** implement its own inverted index, tokenizer, ranker, prefix search, or typo tolerance. The system layers sharding, replication, metadata, event replay, snapshots, and recovery on top of Bleve.

Authoritative specification: **@docs/prd/prd-kitsune-distributed-search-engine.md**

Architecture diagram: `docs/prd/assets/distribuited_search_engine_architecture_diagram.png`

## Current state

The repository is a fresh scaffold:

- `go.mod` — module `github.com/cobeo2004/kitsune`, Go 1.26.3
- `main.go` — Hello World placeholder
- `docs/` — PRD and architecture asset (see above)
- `docs/steps/` — reserved for implementation step notes (empty)

No `internal/`, `cmd/`, `configs/`, or `deployments/` packages exist yet. The target layout is defined in the PRD §8.2 — follow it when adding new packages.

## Commands

Current commands (project is pre-implementation):

```sh
go build ./...
go run .
go test ./...
go vet ./...
```

A Docker Compose dev environment is defined in `deploy/local/compose.yaml` with local operations documented in `docs/operations/local-cluster.md`.

## Component vocabulary

Use these names exactly when writing code, tests, configs, and docs (PRD §7.2):

| Name | Meaning |
|---|---|
| `KSCoordinator` | Query router and result merger |
| `KSSearchNode` | Node that hosts tablets |
| `KSTablet` | One local shard replica backed by Bleve |
| `KSMetadataManager` | Metadata, leases, locks, shard map (etcd first, Consul later) |
| `KSMemberManager` | HashiCorp memberlist gossip membership |
| `KSEventBus` | NATS JetStream document events |
| `KSSnapshotStore` | S3-compatible object storage snapshot storage |

## Architecture rules (non-negotiable)

These rules are load-bearing for the design and must not be relaxed without updating the PRD:

1. **Do not implement custom full-text search internals.** Bleve owns local indexing, scoring, and querying. See PRD §6 (non-goals 1–5) and §5.6.
2. **`KSCoordinator` must not open or modify local Bleve files.** It only routes, fans out gRPC search, merges results, and publishes write events to NATS. See PRD §5.2 (#19).
3. **`KSMetadataManager` is an interface; the first impl is etcd.** Do not couple call sites to etcd types — design so a Consul impl can drop in later. See PRD §5.10 and §8.4.
4. **Authoritative shard ownership lives in `KSMetadataManager`, not in gossip.** Memberlist is advisory only. See PRD §5.11.
5. **S3-compatible object storage is never on the hot query path.** Snapshots are for backup, restore, and replica bootstrap only. See PRD §5.12 (#131–132), §8.6.
6. **Replicas synchronize via event replay + snapshots, not by replicating raw Bleve files through Raft/etcd.** See PRD §5.14 (#145–146).
7. **Consistency model is eventual.** Writes accepted on NATS publish; coordinator routes only to `ready` replicas. See PRD §8.5.
8. **Document shard assignment is deterministic:** `shard_id = hash(document_id) % shard_count`. Shard count is fixed after index creation for the MVP. See PRD §5.5 (#51–52).

## Implementation order

Junior-developer path defined in PRD §11. Follow it — do not jump ahead to failover, rebalancing, or custom search internals before the base path works end-to-end:

1. One local `KSTablet` wrapping one Bleve index, with upsert + search.
2. `KSSearchNode` hosting the tablet, exposing gRPC `SearchShard`.
3. `KSCoordinator` calling the node via gRPC.
4. etcd metadata for index and shard assignment.
5. Multiple nodes, routing by shard map.
6. NATS JetStream document events + replica consumption.
7. S3-compatible object storage snapshot upload, restore, replay.
8. Memberlist gossip, cluster status, metrics.

## Out of scope (do not build)

See PRD §6 for the full list. Highlights: custom inverted index/tokenizer/ranker, dynamic rebalancing, automatic failover with primary election, multi-region, distributed transactions, custom Raft, vector search, K8s operator, exactly-once semantics.

## Sub-documentation

For anything not covered above, consult the PRD section indicated:

| Topic | PRD section |
|---|---|
| Goals, user stories | @docs/prd/prd-kitsune-distributed-search-engine.md (§3–4) |
| Functional requirements (all components) | @docs/prd/prd-kitsune-distributed-search-engine.md (§5) |
| Bleve mapping, search API, write API | @docs/prd/prd-kitsune-distributed-search-engine.md (§5.6–5.8) |
| NATS JetStream event schema and subjects | @docs/prd/prd-kitsune-distributed-search-engine.md (§5.8–5.9) |
| Metadata keys, snapshot layout | @docs/prd/prd-kitsune-distributed-search-engine.md (§5.10, §5.12) |
| Replica recovery and replication | @docs/prd/prd-kitsune-distributed-search-engine.md (§5.13–5.14) |
| Observability requirements | @docs/prd/prd-kitsune-distributed-search-engine.md (§5.15) |
| Project layout, recommended Go modules, gRPC services | @docs/prd/prd-kitsune-distributed-search-engine.md (§8.1–8.3) |
| Open questions awaiting decisions | @docs/prd/prd-kitsune-distributed-search-engine.md (§10) |
