package coordinator

import (
	"strings"
	"testing"
)

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
