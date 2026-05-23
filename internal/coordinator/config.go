package coordinator

import (
	"fmt"
	"strings"
)

// IndexConfig describes one logical index in static KSCoordinator config.
type IndexConfig struct {
	Name              string         `yaml:"name"`
	ShardCount        int            `yaml:"shardCount"`
	ReplicationFactor int            `yaml:"replicationFactor"`
	MappingVersion    int            `yaml:"mappingVersion"`
	Mapping           map[string]any `yaml:"mapping"`
}

// ShardAssignment binds one shard replica to a KSSearchNode in static config.
type ShardAssignment struct {
	IndexName string `yaml:"indexName"`
	ShardID   int    `yaml:"shardID"`
	ReplicaID string `yaml:"replicaID"`
	NodeID    string `yaml:"nodeID"`
}

// NodeConfig describes one search node available to static assignments.
type NodeConfig struct {
	NodeID string `yaml:"nodeID"`
}

// StaticConfig describes logical indexes and static shard replica assignments.
type StaticConfig struct {
	Indexes     []IndexConfig     `yaml:"indexes"`
	Nodes       []NodeConfig      `yaml:"nodes"`
	Assignments []ShardAssignment `yaml:"assignments"`
}

// ValidateStaticConfig validates static index and shard replica assignments.
func ValidateStaticConfig(cfg StaticConfig) error {
	indexes := make(map[string]IndexConfig, len(cfg.Indexes))
	for _, idx := range cfg.Indexes {
		if !indexNamePattern.MatchString(idx.Name) {
			return fmt.Errorf("index name %q is invalid", idx.Name)
		}
		if idx.ShardCount <= 0 {
			return fmt.Errorf("index %q shard count must be positive", idx.Name)
		}
		if idx.ReplicationFactor <= 0 {
			return fmt.Errorf("index %q replication factor must be positive", idx.Name)
		}
		if idx.MappingVersion < 0 {
			return fmt.Errorf("index %q mapping version must be non-negative", idx.Name)
		}
		if _, exists := indexes[idx.Name]; exists {
			return fmt.Errorf("index %q is duplicated", idx.Name)
		}
		indexes[idx.Name] = idx
	}

	nodes := make(map[string]struct{}, len(cfg.Nodes))
	for _, node := range cfg.Nodes {
		if !isStaticIDSegment(node.NodeID) {
			return fmt.Errorf("node ID must be a single path segment")
		}
		if _, exists := nodes[node.NodeID]; exists {
			return fmt.Errorf("node %q is duplicated", node.NodeID)
		}
		nodes[node.NodeID] = struct{}{}
	}

	type shardKey struct {
		indexName string
		shardID   int
	}
	assignments := make(map[shardKey]map[string]struct{})
	assignmentNodes := make(map[shardKey]map[string]struct{})
	for _, assignment := range cfg.Assignments {
		idx, ok := indexes[assignment.IndexName]
		if !ok {
			return fmt.Errorf("assignment references unknown index %q", assignment.IndexName)
		}
		if assignment.ShardID < 0 || assignment.ShardID >= idx.ShardCount {
			return fmt.Errorf("index %q shard %d is out of range", assignment.IndexName, assignment.ShardID)
		}
		if assignment.ReplicaID == "" {
			return fmt.Errorf("index %q shard %d replica ID is required", assignment.IndexName, assignment.ShardID)
		}
		if !isStaticIDSegment(assignment.ReplicaID) {
			return fmt.Errorf("index %q shard %d replica ID must be a single path segment", assignment.IndexName, assignment.ShardID)
		}
		if assignment.NodeID == "" {
			return fmt.Errorf("index %q shard %d node ID is required", assignment.IndexName, assignment.ShardID)
		}
		if !isStaticIDSegment(assignment.NodeID) {
			return fmt.Errorf("index %q shard %d node ID must be a single path segment", assignment.IndexName, assignment.ShardID)
		}

		key := shardKey{indexName: assignment.IndexName, shardID: assignment.ShardID}
		replicas, ok := assignments[key]
		if !ok {
			replicas = make(map[string]struct{})
			assignments[key] = replicas
		}
		nodeSet, ok := assignmentNodes[key]
		if !ok {
			nodeSet = make(map[string]struct{})
			assignmentNodes[key] = nodeSet
		}
		if _, exists := replicas[assignment.ReplicaID]; exists {
			return fmt.Errorf("index %q shard %d replica %q is duplicated", assignment.IndexName, assignment.ShardID, assignment.ReplicaID)
		}
		replicas[assignment.ReplicaID] = struct{}{}
		nodeSet[assignment.NodeID] = struct{}{}
	}

	for _, idx := range cfg.Indexes {
		for shardID := 0; shardID < idx.ShardCount; shardID++ {
			replicas := assignments[shardKey{indexName: idx.Name, shardID: shardID}]
			if len(replicas) != idx.ReplicationFactor {
				return fmt.Errorf("index %q shard %d has %d replicas, want %d", idx.Name, shardID, len(replicas), idx.ReplicationFactor)
			}
			if len(nodes) >= idx.ReplicationFactor && len(assignmentNodes[shardKey{indexName: idx.Name, shardID: shardID}]) < idx.ReplicationFactor {
				return fmt.Errorf("index %q shard %d assigns avoidable same-node replicas", idx.Name, shardID)
			}
		}
	}

	return nil
}

func validateRoutesAgainstStaticConfig(routes StaticRoutes, cfg StaticConfig) error {
	if len(cfg.Indexes) == 0 && len(cfg.Assignments) == 0 {
		return nil
	}

	assignments := make(map[staticAssignmentKey]struct{}, len(cfg.Assignments))
	for _, assignment := range cfg.Assignments {
		assignments[routeStaticAssignmentKey(assignment.IndexName, assignment.ShardID, assignment.ReplicaID, assignment.NodeID)] = struct{}{}
	}

	for indexName, indexRoutes := range routes {
		for _, route := range indexRoutes {
			if route.Client == nil {
				return fmt.Errorf("route for index %q shard %d has no client", indexName, route.ShardID)
			}
			if !isStaticIDSegment(route.ReplicaID) {
				return fmt.Errorf("route for index %q shard %d replica ID must be a single path segment", indexName, route.ShardID)
			}
			if !isStaticIDSegment(route.NodeID) {
				return fmt.Errorf("route for index %q shard %d node ID must be a single path segment", indexName, route.ShardID)
			}
			key := routeStaticAssignmentKey(indexName, route.ShardID, route.ReplicaID, route.NodeID)
			if _, ok := assignments[key]; !ok {
				return fmt.Errorf("route for index %q shard %d replica %q node %q has no static assignment", indexName, route.ShardID, route.ReplicaID, route.NodeID)
			}
		}
	}

	return nil
}

type assignmentKey struct {
	indexName string
	shardID   int
	replicaID string
}

func routeAssignmentKey(indexName string, shardID int, replicaID string) assignmentKey {
	return assignmentKey{
		indexName: indexName,
		shardID:   shardID,
		replicaID: replicaID,
	}
}

type staticAssignmentKey struct {
	indexName string
	shardID   int
	replicaID string
	nodeID    string
}

func routeStaticAssignmentKey(indexName string, shardID int, replicaID, nodeID string) staticAssignmentKey {
	return staticAssignmentKey{
		indexName: indexName,
		shardID:   shardID,
		replicaID: replicaID,
		nodeID:    nodeID,
	}
}

func isStaticIDSegment(value string) bool {
	return value != "" && value != "." && value != ".." && !strings.ContainsAny(value, `/\`)
}
