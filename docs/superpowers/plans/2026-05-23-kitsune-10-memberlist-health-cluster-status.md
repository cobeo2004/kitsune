# Kitsune 10 memberlist Health Cluster Status Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add advisory gossip membership through memberlist and expose cluster status with authoritative metadata plus advisory health.

**Architecture:** Add `internal/member` for gossip state and `internal/status` for cluster status aggregation. memberlist state is advisory and must not overwrite etcd metadata ownership.

**Tech Stack:** Go 1.26.3, `github.com/hashicorp/memberlist`, existing metadata and coordinator packages.

---

Design spec: [../specs/2026-05-23-kitsune-10-memberlist-health-cluster-status-design.md](../specs/2026-05-23-kitsune-10-memberlist-health-cluster-status-design.md)  
Roadmap spec: [../../roadmaps/10-memberlist-health-cluster-status.md](../../roadmaps/10-memberlist-health-cluster-status.md)

## File Structure

- Create: `internal/member/types.go` for node metadata.
- Create: `internal/member/cache.go` for advisory health cache.
- Create: `internal/member/memberlist.go` for memberlist manager.
- Create: `internal/status/cluster.go` for status aggregation.
- Create: `internal/status/cluster_test.go` for authority-boundary tests.

### Task 1: Advisory Health Cache

**Files:**
- Create: `internal/member/types.go`
- Create: `internal/member/cache.go`
- Test: `internal/member/cache_test.go`

- [ ] **Step 1: Write the failing test**

```go
package member

import "testing"

func TestCacheRecordsNodeHealth(t *testing.T) {
	t.Parallel()

	cache := NewCache()
	cache.Update(NodeView{NodeID: "node-a", GRPCAddress: "127.0.0.1:9001", Health: HealthAlive})
	got, ok := cache.Get("node-a")
	if !ok {
		t.Fatal("node-a missing")
	}
	if got.Health != HealthAlive {
		t.Fatalf("health = %s, want %s", got.Health, HealthAlive)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/member -run TestCacheRecordsNodeHealth -count=1`

Expected: FAIL with undefined `NewCache`.

- [ ] **Step 3: Write minimal implementation**

```go
package member

type Health string

const (
	HealthAlive   Health = "alive"
	HealthSuspect Health = "suspect"
	HealthDead    Health = "dead"
)

type NodeView struct {
	NodeID      string
	GRPCAddress string
	Health      Health
}
```

```go
package member

import "sync"

type Cache struct {
	mu    sync.RWMutex
	nodes map[string]NodeView
}

func NewCache() *Cache {
	return &Cache{nodes: make(map[string]NodeView)}
}

func (c *Cache) Update(view NodeView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[view.NodeID] = view
}

func (c *Cache) Get(nodeID string) (NodeView, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	view, ok := c.nodes[nodeID]
	return view, ok
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/member -run TestCacheRecordsNodeHealth -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/member
git commit -m "Track advisory node health

Constraint: Gossip health is advisory, not authoritative metadata.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/member -run TestCacheRecordsNodeHealth -count=1"
```

### Task 2: memberlist Metadata Boundary

**Files:**
- Create: `internal/member/memberlist.go`
- Test: `internal/member/cache_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestNodeMetaFitsMemberlistLimit(t *testing.T) {
	t.Parallel()

	data, err := EncodeNodeMeta(NodeView{NodeID: "node-a", GRPCAddress: "127.0.0.1:9001", Health: HealthAlive}, 512)
	if err != nil {
		t.Fatalf("encode node meta: %v", err)
	}
	if len(data) > 512 {
		t.Fatalf("metadata length = %d, want <= 512", len(data))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/member -run TestNodeMetaFitsMemberlistLimit -count=1`

Expected: FAIL with undefined `EncodeNodeMeta`.

- [ ] **Step 3: Write minimal implementation**

```go
func EncodeNodeMeta(view NodeView, limit int) ([]byte, error) {
	data, err := json.Marshal(view)
	if err != nil {
		return nil, fmt.Errorf("encode node metadata: %w", err)
	}
	if len(data) > limit {
		return nil, fmt.Errorf("node metadata exceeds memberlist limit: %d > %d", len(data), limit)
	}
	return data, nil
}
```

- [ ] **Step 4: Add memberlist dependency and manager skeleton**

Run: `go get github.com/hashicorp/memberlist`

Create a manager that accepts bind address, node name, delegate metadata, and join addresses.

- [ ] **Step 5: Run tests**

Run: `go test ./internal/member -count=1`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/member
git commit -m "Constrain memberlist node metadata

Constraint: memberlist metadata is compact advisory state.
Confidence: medium
Scope-risk: narrow
Tested: go test ./internal/member -count=1"
```

### Task 3: Cluster Status Aggregation

**Files:**
- Create: `internal/status/cluster.go`
- Test: `internal/status/cluster_test.go`

- [ ] **Step 1: Write the failing test**

```go
package status

import "testing"

func TestClusterStatusKeepsMetadataAuthoritative(t *testing.T) {
	t.Parallel()

	got := BuildClusterStatus(Input{
		Assignments: []AssignmentView{{IndexName: "books", ShardID: 0, NodeID: "node-a"}},
		Nodes:       []NodeHealthView{{NodeID: "node-a", Health: "dead"}},
	})
	if len(got.Assignments) != 1 || got.Assignments[0].NodeID != "node-a" {
		t.Fatalf("assignments = %#v", got.Assignments)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/status -run TestClusterStatusKeepsMetadataAuthoritative -count=1`

Expected: FAIL with undefined `BuildClusterStatus`.

- [ ] **Step 3: Write minimal implementation**

```go
package status

type AssignmentView struct {
	IndexName string `json:"indexName"`
	ShardID   int    `json:"shardId"`
	NodeID    string `json:"nodeId"`
}

type NodeHealthView struct {
	NodeID string `json:"nodeId"`
	Health string `json:"health"`
}

type Input struct {
	Assignments []AssignmentView
	Nodes       []NodeHealthView
}

type ClusterStatus struct {
	Assignments []AssignmentView `json:"assignments"`
	Nodes       []NodeHealthView `json:"nodes"`
}

func BuildClusterStatus(input Input) ClusterStatus {
	return ClusterStatus{Assignments: input.Assignments, Nodes: input.Nodes}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/status -run TestClusterStatusKeepsMetadataAuthoritative -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/status
git commit -m "Aggregate cluster status without changing ownership

Constraint: etcd metadata remains authoritative over gossip.
Confidence: high
Scope-risk: narrow
Tested: go test ./internal/status -run TestClusterStatusKeepsMetadataAuthoritative -count=1"
```
