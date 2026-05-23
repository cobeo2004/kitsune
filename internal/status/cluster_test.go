package status

import "testing"

func TestClusterStatusKeepsMetadataAuthoritative(t *testing.T) {
	t.Parallel()

	got := BuildClusterStatus(Input{
		Assignments: []AssignmentView{{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"}},
		Nodes:       []NodeHealthView{{NodeID: "node-a", Health: "dead"}},
	})
	if len(got.Assignments) != 1 || got.Assignments[0].NodeID != "node-a" {
		t.Fatalf("assignments = %#v", got.Assignments)
	}
}

func TestClusterStatusIncludesTabletsAndCheckpoints(t *testing.T) {
	t.Parallel()

	got := BuildClusterStatus(Input{
		Assignments: []AssignmentView{{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"}},
		Tablets:     []TabletView{{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", State: "ready"}},
		Checkpoints: []CheckpointView{{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", Sequence: 42, EventID: "event-42"}},
		Nodes:       []NodeHealthView{{NodeID: "node-a", Health: "alive"}},
	})

	if len(got.Tablets) != 1 || got.Tablets[0].State != "ready" {
		t.Fatalf("tablets = %#v", got.Tablets)
	}
	if len(got.Checkpoints) != 1 || got.Checkpoints[0].Sequence != 42 {
		t.Fatalf("checkpoints = %#v", got.Checkpoints)
	}
}
