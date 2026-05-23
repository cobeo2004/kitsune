# Kitsune 07 Replication Replica Selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Support multiple replicas per shard and select ready replicas for coordinator search.

**Architecture:** Add replica state and selection rules under `internal/coordinator`. Metadata supplies candidate replicas; the selector chooses one acceptable replica per shard and returns clear errors when none are available.

**Tech Stack:** Go 1.26.3, existing coordinator routing, existing metadata records, table-driven tests.

---

Design spec: [../specs/2026-05-23-kitsune-07-replication-replica-selection-design.md](../specs/2026-05-23-kitsune-07-replication-replica-selection-design.md)  
Roadmap spec: [../../roadmaps/07-replication-replica-selection.md](../../roadmaps/07-replication-replica-selection.md)

## File Structure

- Create: `internal/coordinator/replica.go` for replica state and candidate types.
- Create: `internal/coordinator/selector.go` for selection rules.
- Create: `internal/coordinator/selector_test.go` for selector matrix tests.
- Modify: `internal/coordinator/router.go` to use selected replicas.

### Task 1: Replica Selector Prefers Ready Replicas

**Files:**
- Create: `internal/coordinator/replica.go`
- Create: `internal/coordinator/selector.go`
- Test: `internal/coordinator/selector_test.go`

- [ ] **Step 1: Write the failing test**

```go
package coordinator

import "testing"

func TestSelectReplicaPrefersReady(t *testing.T) {
	t.Parallel()

	got, err := SelectReplica([]ReplicaCandidate{
		{IndexName: "books", ShardID: 0, ReplicaID: "r1", State: ReplicaReplaying},
		{IndexName: "books", ShardID: 0, ReplicaID: "r2", State: ReplicaReady},
	})
	if err != nil {
		t.Fatalf("select replica: %v", err)
	}
	if got.ReplicaID != "r2" {
		t.Fatalf("replica = %q, want r2", got.ReplicaID)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/coordinator -run TestSelectReplicaPrefersReady -count=1`

Expected: FAIL with undefined `SelectReplica`.

- [ ] **Step 3: Write minimal implementation**

```go
type ReplicaState string

const (
	ReplicaReady     ReplicaState = "ready"
	ReplicaFailed    ReplicaState = "failed"
	ReplicaRestoring ReplicaState = "restoring"
	ReplicaReplaying ReplicaState = "replaying"
)

type ReplicaCandidate struct {
	IndexName string
	ShardID   int
	ReplicaID string
	NodeID    string
	State     ReplicaState
	Client    ShardClient
}
```

```go
func SelectReplica(candidates []ReplicaCandidate) (ReplicaCandidate, error) {
	for _, candidate := range candidates {
		if candidate.State == ReplicaReady {
			return candidate, nil
		}
	}
	return ReplicaCandidate{}, fmt.Errorf("no healthy replica available")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/coordinator -run TestSelectReplicaPrefersReady -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/coordinator
git commit -m "Select ready replicas for shard search

Constraint: Coordinator avoids replicas that are not ready.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/coordinator -run TestSelectReplicaPrefersReady -count=1"
```

### Task 2: No Healthy Replica Error

**Files:**
- Modify: `internal/coordinator/selector.go`
- Test: `internal/coordinator/selector_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestSelectReplicaReturnsClearErrorWhenNoneReady(t *testing.T) {
	t.Parallel()

	_, err := SelectReplica([]ReplicaCandidate{
		{IndexName: "books", ShardID: 0, ReplicaID: "r1", State: ReplicaFailed},
		{IndexName: "books", ShardID: 0, ReplicaID: "r2", State: ReplicaRestoring},
	})
	if err == nil {
		t.Fatal("expected no healthy replica error")
	}
	if !strings.Contains(err.Error(), "books shard 0") {
		t.Fatalf("error = %q, want index and shard context", err.Error())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/coordinator -run TestSelectReplicaReturnsClearErrorWhenNoneReady -count=1`

Expected: FAIL because the error lacks index/shard context.

- [ ] **Step 3: Write minimal implementation**

```go
func SelectReplica(candidates []ReplicaCandidate) (ReplicaCandidate, error) {
	for _, candidate := range candidates {
		if candidate.State == ReplicaReady {
			return candidate, nil
		}
	}
	if len(candidates) == 0 {
		return ReplicaCandidate{}, fmt.Errorf("no healthy replica available")
	}
	first := candidates[0]
	return ReplicaCandidate{}, fmt.Errorf("no healthy replica available for %s shard %d", first.IndexName, first.ShardID)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/coordinator -run TestSelectReplicaReturnsClearErrorWhenNoneReady -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/coordinator
git commit -m "Explain missing healthy replica errors

Constraint: Search failures must identify unavailable shard replicas.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/coordinator -run TestSelectReplicaReturnsClearErrorWhenNoneReady -count=1"
```

### Task 3: Assignment Validation Avoids Same-Node Replicas

**Files:**
- Modify: `internal/coordinator/config.go`
- Test: `internal/coordinator/config_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestValidateStaticConfigRejectsSameNodeReplicasWhenAvoidable(t *testing.T) {
	t.Parallel()

	cfg := StaticConfig{
		Indexes: []IndexConfig{{Name: "books", ShardCount: 1, ReplicationFactor: 2}},
		Nodes:   []NodeConfig{{NodeID: "node-a"}, {NodeID: "node-b"}},
		Assignments: []ShardAssignment{
			{IndexName: "books", ShardID: 0, ReplicaID: "r1", NodeID: "node-a"},
			{IndexName: "books", ShardID: 0, ReplicaID: "r2", NodeID: "node-a"},
		},
	}
	if err := ValidateStaticConfig(cfg); err == nil {
		t.Fatal("expected same-node replicas to fail when another node exists")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/coordinator -run TestValidateStaticConfigRejectsSameNodeReplicasWhenAvoidable -count=1`

Expected: FAIL if same-node replicas are allowed.

- [ ] **Step 3: Write minimal implementation**

Add node config and per-shard node uniqueness check when `len(cfg.Nodes) >= replicationFactor`.

```go
type NodeConfig struct {
	NodeID string
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/coordinator -run TestValidateStaticConfigRejectsSameNodeReplicasWhenAvoidable -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/coordinator
git commit -m "Reject avoidable same-node shard replicas

Constraint: Replicas should tolerate a single stopped node when enough nodes exist.
Confidence: medium
Scope-risk: narrow
Tested: go test ./internal/coordinator -run TestValidateStaticConfigRejectsSameNodeReplicasWhenAvoidable -count=1"
```
