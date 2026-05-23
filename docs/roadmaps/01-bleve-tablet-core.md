# 01 Bleve Tablet Core

Roadmap index: [index.md](index.md)  
Previous: none  
Next: [02 Search Node gRPC](02-search-node-grpc.md)  
Traceability: [PRD Traceability](prd-traceability.md)

## Outcome

Create the local search unit: `KSTablet`, one local Bleve-backed shard replica.

## Scope

- Open or create one Bleve index for a tablet.
- Index documents into that tablet.
- Search documents in that tablet.
- Delete documents from local search state.
- Track tablet identity: index name, shard ID, replica ID, node ID, and mapping version.
- Treat Bleve mappings as immutable after index creation.

## Out of Scope

- Coordinator routing.
- gRPC.
- etcd metadata.
- NATS event replay.
- Snapshots.
- Replication.

## Acceptance Criteria

- A tablet can create a persistent Bleve index at a deterministic path.
- A tablet can reopen an existing index.
- Upsert by document ID replaces previous content.
- Search returns matching document IDs and scores from the local tablet.
- Delete removes the document from local search results.
- Mapping version is recorded and exposed.
- Attempts to change a mapping after index creation are rejected clearly.

## TDD Plan Shape

- RED: opening a missing tablet path creates a searchable index.
- RED: upserting a document makes it searchable.
- RED: upserting the same ID replaces previous content.
- RED: deleting a document removes it from search results.
- RED: reopening a tablet preserves indexed documents.
- RED: changing an existing mapping fails.

## OMX Usage

Solo execution is likely enough. Use a reviewer or verifier lane only if the tablet API grows beyond a small package.

## Verification

- Targeted Go tests for the tablet package.
- Race-free close/reopen behavior.
- No custom inverted index, tokenizer, or ranker code.
