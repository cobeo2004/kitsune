package status

// AssignmentView reports authoritative shard ownership from metadata.
type AssignmentView struct {
	IndexName string `json:"indexName"`
	ShardID   int    `json:"shardId"`
	ReplicaID string `json:"replicaId"`
	NodeID    string `json:"nodeId"`
}

// NodeHealthView reports advisory gossip health.
type NodeHealthView struct {
	NodeID      string `json:"nodeId"`
	GRPCAddress string `json:"grpcAddress,omitempty"`
	Health      string `json:"health"`
}

// TabletView reports tablet readiness from authoritative tablet status metadata.
type TabletView struct {
	IndexName      string `json:"indexName"`
	ShardID        int    `json:"shardId"`
	ReplicaID      string `json:"replicaId"`
	NodeID         string `json:"nodeId"`
	State          string `json:"state"`
	LastCheckpoint int64  `json:"lastCheckpoint"`
}

// CheckpointView reports the latest persisted replay checkpoint for a tablet.
type CheckpointView struct {
	IndexName string `json:"indexName"`
	ShardID   int    `json:"shardId"`
	ReplicaID string `json:"replicaId"`
	Sequence  int64  `json:"sequence"`
	EventID   string `json:"eventId"`
}

// Input is the data needed to aggregate cluster status.
type Input struct {
	Assignments []AssignmentView
	Nodes       []NodeHealthView
	Tablets     []TabletView
	Checkpoints []CheckpointView
}

// ClusterStatus reports authoritative metadata with advisory node health.
type ClusterStatus struct {
	Assignments []AssignmentView `json:"assignments"`
	Nodes       []NodeHealthView `json:"nodes"`
	Tablets     []TabletView     `json:"tablets"`
	Checkpoints []CheckpointView `json:"checkpoints"`
}

// BuildClusterStatus aggregates cluster status without changing ownership.
func BuildClusterStatus(input Input) ClusterStatus {
	return ClusterStatus{
		Assignments: append([]AssignmentView(nil), input.Assignments...),
		Nodes:       append([]NodeHealthView(nil), input.Nodes...),
		Tablets:     append([]TabletView(nil), input.Tablets...),
		Checkpoints: append([]CheckpointView(nil), input.Checkpoints...),
	}
}
