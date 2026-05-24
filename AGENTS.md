# AGENTS.md

Operating notes for autonomous agents and subagents working in this repository. The authoritative product spec is **@docs/prd/prd-kitsune-distributed-search-engine.md** — treat it as the contract for every change.

For Claude Code session setup (commands, current state, component vocabulary, architecture rules), see **@CLAUDE.md**. This file focuses on agent-specific behavior.

## Identity

This project is **Kitsune**, a Go distributed search engine built on **Bleve** for local indexing. Component names are fixed (`KSCoordinator`, `KSSearchNode`, `KSTablet`, `KSMetadataManager`, `KSMemberManager`, `KSEventBus`, `KSSnapshotStore`) — see PRD §7.2 and CLAUDE.md. Use them verbatim in code, tests, configs, commits, and PR descriptions.

## Working agreement

1. **Read the PRD section before writing code in a new area.** Each functional requirement is numbered (§5.1–§5.15); cite the requirement number(s) in commit messages when implementing a specific behavior.
2. **Stay on the MVP path.** The implementation order in PRD §11 is canonical. Do not introduce failover, rebalancing, multi-region, or custom search internals — they are explicitly out of scope (PRD §6).
3. **Respect service boundaries.** `KSCoordinator` never touches Bleve files. `KSMemberManager` gossip is advisory only. `KSSnapshotStore` is never on the hot query path. See CLAUDE.md "Architecture rules" for the full list.
4. **Interfaces before implementations.** `KSMetadataManager` and `KSSnapshotStore` are defined as interfaces; the first impls are etcd and S3-compatible object storage. Do not leak backend-specific types into callers (PRD §5.10, §5.12, §8.4).
5. **Do not invent answers to open questions.** PRD §10 lists ten unresolved design questions (sharding strategy, REST vs gRPC scope, JetStream retention, snapshot trigger policy, delete semantics, etc.). If a task depends on one of these, surface the question to the user before coding around it.

## Subagent dispatch

When delegating work to subagents (Explore, general-purpose, Plan), pass:

- A pointer to the relevant PRD section(s) by number, e.g. "Implement §5.4 (#33–#45) for `KSTablet`."
- The component vocabulary the subagent must use (see CLAUDE.md "Component vocabulary").
- The architectural constraint the subagent must not violate (e.g. "`KSCoordinator` must not open Bleve files — only call `SearchShard` over gRPC").

Subagents do not have this conversation's context — restate constraints, do not assume.

## What is in the repo today

A fresh Go module (`go.mod`, `main.go` Hello World) and the PRD under `docs/`. No `internal/`, `cmd/`, `configs/`, or `deployments/` packages exist. The target layout is PRD §8.2 — create directories there when starting each step in the MVP path.

`docs/steps/` is reserved for per-step implementation notes (currently empty). If you produce step-by-step implementation breakdowns as part of planning, write them under `docs/steps/` rather than scattering them in the repo root.

## Reference index

| Need | Source |
|---|---|
| Project identity, commands, naming, architecture rules | @CLAUDE.md |
| Full product requirements (the contract) | @docs/prd/prd-kitsune-distributed-search-engine.md |
| Architecture diagram | `docs/prd/assets/distribuited_search_engine_architecture_diagram.png` |
| Implementation step notes | `docs/steps/` (write new step docs here) |
