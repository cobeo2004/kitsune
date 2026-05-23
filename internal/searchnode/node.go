package searchnode

import (
	"context"
	"fmt"
	"sync"

	"github.com/cobeo2004/kitsune/internal/tablet"
)

// Tablet is the local KSTablet behavior required by KSSearchNode.
type Tablet interface {
	Search(context.Context, tablet.SearchRequest) (tablet.SearchResult, error)
	Status() tablet.Status
}

// NodeConfig configures a KSSearchNode registry.
type NodeConfig struct {
	NodeID string
}

// Node tracks tablets hosted by one KSSearchNode.
type Node struct {
	mu      sync.RWMutex
	nodeID  string
	tablets map[string]Tablet
}

// New creates a KSSearchNode tablet registry.
func New(cfg NodeConfig) *Node {
	return &Node{
		nodeID:  cfg.NodeID,
		tablets: make(map[string]Tablet),
	}
}

// RegisterTablet registers one local KSTablet shard replica.
func (n *Node) RegisterTablet(index string, shardID int, replicaID string, tb Tablet) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.tablets[key(index, shardID, replicaID)] = tb
}

// Statuses reports all tablets hosted by the node.
func (n *Node) Statuses() []tablet.Status {
	n.mu.RLock()
	tablets := make([]Tablet, 0, len(n.tablets))
	for _, tb := range n.tablets {
		tablets = append(tablets, tb)
	}
	n.mu.RUnlock()

	statuses := make([]tablet.Status, 0, len(tablets))
	for _, tb := range tablets {
		statuses = append(statuses, tb.Status())
	}
	return statuses
}

func (n *Node) tablet(index string, shardID int, replicaID string) (Tablet, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	tb, ok := n.tablets[key(index, shardID, replicaID)]
	return tb, ok
}

func key(index string, shardID int, replicaID string) string {
	return fmt.Sprintf("%s/%d/%s", index, shardID, replicaID)
}
