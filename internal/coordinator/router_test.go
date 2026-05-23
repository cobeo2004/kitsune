package coordinator

import (
	"testing"

	"github.com/cobeo2004/kitsune/internal/metadata"
)

func TestRoutesFromMetadataUsesTabletStatusForSelection(t *testing.T) {
	t.Parallel()

	failedClient := &fakeShardClient{}
	readyClient := &fakeShardClient{}
	routes := routesFromMetadata(
		[]metadata.ShardReplicaRecord{
			{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"},
			{IndexName: "books", ShardID: 0, ReplicaID: "replica-b", NodeID: "node-b"},
		},
		[]metadata.TabletStatusRecord{
			{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", State: string(ReplicaFailed)},
			{IndexName: "books", ShardID: 0, ReplicaID: "replica-b", NodeID: "node-b", State: string(ReplicaReady)},
		},
		StaticRoutes{
			"books": {
				{ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a", Client: failedClient},
				{ShardID: 0, ReplicaID: "replica-b", NodeID: "node-b", Client: readyClient},
			},
		},
	)

	selected, ok := routes.selectedRoutes("books", 1)
	if !ok {
		t.Fatal("routes were not selected")
	}
	if selected[0].ReplicaID != "replica-b" {
		t.Fatalf("replica = %q, want replica-b", selected[0].ReplicaID)
	}
}
