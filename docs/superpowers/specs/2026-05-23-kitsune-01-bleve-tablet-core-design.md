# Kitsune 01 Bleve Tablet Core Design

Master spec: [Kitsune Distributed Search Roadmap Design](2026-05-23-kitsune-distributed-search-roadmap-design.md)  
Roadmap spec: [01 Bleve Tablet Core](../../roadmaps/01-bleve-tablet-core.md)  
Previous: none  
Next: [02 Search Node gRPC](2026-05-23-kitsune-02-search-node-grpc-design.md)

## Goal

Build `KSTablet`, the local per-shard search unit backed by one Bleve index.

## Architecture

`KSTablet` is the only package allowed to open or mutate local Bleve index files. It exposes a small Go API for lifecycle, upsert, delete, search, and status. Callers pass explicit context and typed request structs; the tablet returns typed results and wrapped errors.

## Components

- Tablet identity: index name, shard ID, replica ID, node ID, mapping version.
- Tablet store: deterministic filesystem path per tablet identity.
- Bleve adapter: create, open, close, index, delete, and search operations.
- Mapping guard: records mapping version and rejects mutation after creation.
- Tablet status: ready, failed, opening, and closing states.

## Data Flow

The caller opens a tablet with identity and mapping. The tablet creates or opens the Bleve index path, validates mapping compatibility, and marks itself ready. Upserts and deletes mutate the local Bleve index. Searches execute against local Bleve and return document hits and scores.

## Error Handling

Opening an invalid path, mapping mismatch, closed tablet usage, missing document deletion, and Bleve operation failures return explicit errors. The tablet does not log and return the same error; it wraps and returns errors for callers to handle.

## Testing

Implementation must use TDD for:

- Creating a missing tablet path.
- Reopening a persisted tablet.
- Upserting and searching one document.
- Replacing content for an existing document ID.
- Deleting a document from search results.
- Rejecting mapping changes after index creation.

## Plan Handoff

Future plan path: `docs/superpowers/plans/2026-05-23-kitsune-01-bleve-tablet-core.md`.

The plan should start with local unit tests and use temporary directories for persistent Bleve indexes.
