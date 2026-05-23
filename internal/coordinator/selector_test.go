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
