# Kitsune 02 Search Node gRPC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `KSSearchNode`, a process-local tablet registry exposed through internal gRPC shard APIs.

**Architecture:** Add protobuf definitions under `api/searchnode/v1` and server logic under `internal/searchnode`. The server owns a registry of `internal/tablet.Tablet` instances and maps tablet errors to gRPC status errors.

**Tech Stack:** Go 1.26.3, `google.golang.org/grpc`, `google.golang.org/protobuf`, `protoc-gen-go`, `protoc-gen-go-grpc`, `internal/tablet`.

---

Design spec: [../specs/2026-05-23-kitsune-02-search-node-grpc-design.md](../specs/2026-05-23-kitsune-02-search-node-grpc-design.md)  
Roadmap spec: [../../roadmaps/02-search-node-grpc.md](../../roadmaps/02-search-node-grpc.md)

## File Structure

- Create: `api/searchnode/v1/searchnode.proto` for internal RPC contracts.
- Generate: `api/searchnode/v1/searchnode.pb.go` and `api/searchnode/v1/searchnode_grpc.pb.go`.
- Create: `internal/searchnode/node.go` for tablet registry and lifecycle.
- Create: `internal/searchnode/server.go` for gRPC service implementation.
- Create: `internal/searchnode/server_test.go` for in-process service tests.
- Modify: `go.mod` for gRPC and protobuf dependencies.

### Task 1: Define and Generate Internal gRPC API

**Files:**
- Create: `api/searchnode/v1/searchnode.proto`
- Generate: `api/searchnode/v1/searchnode.pb.go`
- Generate: `api/searchnode/v1/searchnode_grpc.pb.go`

- [ ] **Step 1: Write the proto file**

```proto
syntax = "proto3";

package kitsune.searchnode.v1;

option go_package = "github.com/cobeo2004/kitsune/api/searchnode/v1;searchnodev1";

service SearchNodeService {
  rpc SearchShard(SearchShardRequest) returns (SearchShardResponse);
  rpc TabletStatus(TabletStatusRequest) returns (TabletStatusResponse);
}

message TabletRef {
  string index_name = 1;
  int32 shard_id = 2;
  string replica_id = 3;
}

message SearchShardRequest {
  TabletRef tablet = 1;
  string query = 2;
  int32 limit = 3;
  int32 offset = 4;
}

message SearchHit {
  string document_id = 1;
  double score = 2;
}

message SearchShardResponse {
  uint64 total = 1;
  repeated SearchHit hits = 2;
}

message TabletStatusRequest {}

message TabletStatus {
  TabletRef tablet = 1;
  string node_id = 2;
  string state = 3;
  int64 last_checkpoint = 4;
}

message TabletStatusResponse {
  repeated TabletStatus tablets = 1;
}
```

- [ ] **Step 2: Run generation and verify dependencies**

Run:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go get google.golang.org/grpc google.golang.org/protobuf
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/searchnode/v1/searchnode.proto
```

Expected: generated `.pb.go` files exist and `go test ./...` reaches compile errors only for missing server packages.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum api/searchnode/v1
git commit -m "Define internal search node gRPC API

Constraint: Coordinator-to-search-node communication uses internal gRPC.
Confidence: high
Scope-risk: narrow
Tested: protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/searchnode/v1/searchnode.proto"
```

### Task 2: Tablet Registry

**Files:**
- Create: `internal/searchnode/node.go`
- Test: `internal/searchnode/server_test.go`

- [ ] **Step 1: Write the failing test**

```go
package searchnode

import (
	"testing"

	"github.com/cobeo2004/kitsune/internal/tablet"
)

func TestNodeReportsHostedTablet(t *testing.T) {
	t.Parallel()

	n := New(NodeConfig{NodeID: "node-a"})
	n.RegisterTablet("books", 0, "replica-a", fakeTabletStatus(tablet.StateReady))

	statuses := n.Statuses()
	if len(statuses) != 1 {
		t.Fatalf("len(statuses) = %d, want 1", len(statuses))
	}
	if statuses[0].Identity.IndexName != "books" {
		t.Fatalf("index = %q, want books", statuses[0].Identity.IndexName)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/searchnode -run TestNodeReportsHostedTablet -count=1`

Expected: FAIL with undefined package or undefined `New`.

- [ ] **Step 3: Write minimal implementation**

```go
package searchnode

import (
	"sync"

	"github.com/cobeo2004/kitsune/internal/tablet"
)

type Tablet interface {
	Status() tablet.Status
}

type NodeConfig struct {
	NodeID string
}

type Node struct {
	mu      sync.RWMutex
	nodeID  string
	tablets map[string]Tablet
}

func New(cfg NodeConfig) *Node {
	return &Node{nodeID: cfg.NodeID, tablets: make(map[string]Tablet)}
}

func key(index string, shardID int, replicaID string) string {
	return fmt.Sprintf("%s/%d/%s", index, shardID, replicaID)
}

func (n *Node) RegisterTablet(index string, shardID int, replicaID string, tb Tablet) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.tablets[key(index, shardID, replicaID)] = tb
}

func (n *Node) Statuses() []tablet.Status {
	n.mu.RLock()
	defer n.mu.RUnlock()
	statuses := make([]tablet.Status, 0, len(n.tablets))
	for _, tb := range n.tablets {
		statuses = append(statuses, tb.Status())
	}
	return statuses
}
```

Import `fmt` in `node.go` for the key formatter.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/searchnode -run TestNodeReportsHostedTablet -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/searchnode
git commit -m "Track tablets hosted by a search node

Constraint: Search nodes own tablets but not global routing.
Confidence: medium
Scope-risk: narrow
Tested: go test ./internal/searchnode -run TestNodeReportsHostedTablet -count=1"
```

### Task 3: gRPC Server Search and Status

**Files:**
- Create: `internal/searchnode/server.go`
- Modify: `internal/searchnode/node.go`
- Test: `internal/searchnode/server_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestSearchShardMissingTabletReturnsNotFound(t *testing.T) {
	t.Parallel()

	srv := NewServer(New(NodeConfig{NodeID: "node-a"}))
	_, err := srv.SearchShard(context.Background(), &searchnodev1.SearchShardRequest{
		Tablet: &searchnodev1.TabletRef{IndexName: "books", ShardId: 0, ReplicaId: "r1"},
		Query:  "bleve",
		Limit:  10,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %s, want %s; err=%v", status.Code(err), codes.NotFound, err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/searchnode -run TestSearchShardMissingTabletReturnsNotFound -count=1`

Expected: FAIL with undefined `NewServer`.

- [ ] **Step 3: Write minimal implementation**

```go
package searchnode

import (
	"context"

	searchnodev1 "github.com/cobeo2004/kitsune/api/searchnode/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	searchnodev1.UnimplementedSearchNodeServiceServer
	node *Node
}

func NewServer(node *Node) *Server {
	return &Server{node: node}
}

func (s *Server) SearchShard(ctx context.Context, req *searchnodev1.SearchShardRequest) (*searchnodev1.SearchShardResponse, error) {
	if req.GetTablet() == nil {
		return nil, status.Error(codes.InvalidArgument, "tablet is required")
	}
	return nil, status.Error(codes.NotFound, "tablet not found")
}

func (s *Server) TabletStatus(ctx context.Context, req *searchnodev1.TabletStatusRequest) (*searchnodev1.TabletStatusResponse, error) {
	statuses := s.node.Statuses()
	out := make([]*searchnodev1.TabletStatus, 0, len(statuses))
	for _, st := range statuses {
		out = append(out, &searchnodev1.TabletStatus{
			Tablet: &searchnodev1.TabletRef{IndexName: st.Identity.IndexName, ShardId: int32(st.Identity.ShardID), ReplicaId: st.Identity.ReplicaID},
			NodeId: st.Identity.NodeID,
			State:  string(st.State),
		})
	}
	return &searchnodev1.TabletStatusResponse{Tablets: out}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/searchnode -run TestSearchShardMissingTabletReturnsNotFound -count=1`

Expected: PASS.

- [ ] **Step 5: Extend tests for successful search**

Add a fake tablet implementing search and status, then verify `SearchShard` maps tablet hits into protobuf hits.

Run: `go test ./internal/searchnode -count=1`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add api/searchnode/v1 internal/searchnode
git commit -m "Serve hosted tablets through internal gRPC

Constraint: gRPC is internal in the MVP sequence.
Confidence: medium
Scope-risk: moderate
Tested: go test ./internal/searchnode -count=1"
```
