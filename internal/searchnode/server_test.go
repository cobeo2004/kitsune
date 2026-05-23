package searchnode

import (
	"context"
	"errors"
	"net"
	"testing"

	searchnodev1 "github.com/cobeo2004/kitsune/api/searchnode/v1"
	"github.com/cobeo2004/kitsune/internal/tablet"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

func TestSearchShardRequiresTablet(t *testing.T) {
	t.Parallel()

	srv := NewServer(New(NodeConfig{NodeID: "node-a"}))
	_, err := srv.SearchShard(context.Background(), &searchnodev1.SearchShardRequest{
		Query: "bleve",
		Limit: 10,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %s, want %s; err=%v", status.Code(err), codes.InvalidArgument, err)
	}
}

func TestSearchShardRequiresRequest(t *testing.T) {
	t.Parallel()

	srv := NewServer(New(NodeConfig{NodeID: "node-a"}))
	_, err := srv.SearchShard(context.Background(), nil)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %s, want %s; err=%v", status.Code(err), codes.InvalidArgument, err)
	}
}

func TestTabletStatusMapsHostedTablets(t *testing.T) {
	t.Parallel()

	n := New(NodeConfig{NodeID: "node-a"})
	n.RegisterTablet("books", 0, "replica-a", fakeTabletStatus(tablet.StateReady))

	srv := NewServer(n)
	resp, err := srv.TabletStatus(context.Background(), &searchnodev1.TabletStatusRequest{})
	if err != nil {
		t.Fatalf("tablet status: %v", err)
	}
	if len(resp.GetTablets()) != 1 {
		t.Fatalf("len(tablets) = %d, want 1", len(resp.GetTablets()))
	}

	got := resp.GetTablets()[0]
	if got.GetTablet().GetIndexName() != "books" {
		t.Fatalf("index = %q, want books", got.GetTablet().GetIndexName())
	}
	if got.GetTablet().GetShardId() != 0 {
		t.Fatalf("shard = %d, want 0", got.GetTablet().GetShardId())
	}
	if got.GetTablet().GetReplicaId() != "replica-a" {
		t.Fatalf("replica = %q, want replica-a", got.GetTablet().GetReplicaId())
	}
	if got.GetNodeId() != "node-a" {
		t.Fatalf("node = %q, want node-a", got.GetNodeId())
	}
	if got.GetState() != tablet.StateReady {
		t.Fatalf("state = %q, want %q", got.GetState(), tablet.StateReady)
	}
}

func TestSearchShardMapsTabletResults(t *testing.T) {
	t.Parallel()

	n := New(NodeConfig{NodeID: "node-a"})
	fake := &fakeTablet{
		status: fakeStatus(tablet.StateReady),
		result: tablet.SearchResult{
			Total: 2,
			Hits: []tablet.SearchHit{
				{DocumentID: "doc-1", Score: 1.25},
				{DocumentID: "doc-2", Score: 0.75},
			},
		},
	}
	n.RegisterTablet("books", 0, "replica-a", fake)

	srv := NewServer(n)
	resp, err := srv.SearchShard(context.Background(), &searchnodev1.SearchShardRequest{
		Tablet: &searchnodev1.TabletRef{IndexName: "books", ShardId: 0, ReplicaId: "replica-a"},
		Query:  "bleve",
		Limit:  10,
		Offset: 5,
	})
	if err != nil {
		t.Fatalf("search shard: %v", err)
	}
	if fake.req.Query != "bleve" {
		t.Fatalf("query = %q, want bleve", fake.req.Query)
	}
	if fake.req.Limit != 10 {
		t.Fatalf("limit = %d, want 10", fake.req.Limit)
	}
	if fake.req.Offset != 5 {
		t.Fatalf("offset = %d, want 5", fake.req.Offset)
	}
	if resp.GetTotal() != 2 {
		t.Fatalf("total = %d, want 2", resp.GetTotal())
	}
	if len(resp.GetHits()) != 2 {
		t.Fatalf("len(hits) = %d, want 2", len(resp.GetHits()))
	}
	if resp.GetHits()[0].GetDocumentId() != "doc-1" {
		t.Fatalf("first document ID = %q, want doc-1", resp.GetHits()[0].GetDocumentId())
	}
	if resp.GetHits()[0].GetScore() != 1.25 {
		t.Fatalf("first score = %f, want 1.25", resp.GetHits()[0].GetScore())
	}
}

func TestSearchShardClientCanSearchTablet(t *testing.T) {
	t.Parallel()

	n := New(NodeConfig{NodeID: "node-a"})
	fake := &fakeTablet{
		status: fakeStatus(tablet.StateReady),
		result: tablet.SearchResult{
			Total: 1,
			Hits:  []tablet.SearchHit{{DocumentID: "doc-1", Score: 2.5}},
		},
	}
	n.RegisterTablet("books", 0, "replica-a", fake)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	searchnodev1.RegisterSearchNodeServiceServer(grpcServer, NewServer(n))
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("serve grpc: %v", err)
		}
	}()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.Dial(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close grpc conn: %v", err)
		}
	})

	client := searchnodev1.NewSearchNodeServiceClient(conn)
	resp, err := client.SearchShard(context.Background(), &searchnodev1.SearchShardRequest{
		Tablet: &searchnodev1.TabletRef{IndexName: "books", ShardId: 0, ReplicaId: "replica-a"},
		Query:  "bleve",
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("search shard: %v", err)
	}
	if resp.GetTotal() != 1 {
		t.Fatalf("total = %d, want 1", resp.GetTotal())
	}
	if len(resp.GetHits()) != 1 || resp.GetHits()[0].GetDocumentId() != "doc-1" {
		t.Fatalf("hits = %#v, want doc-1", resp.GetHits())
	}
}

func TestSearchShardUnavailableWhenTabletIsNotReady(t *testing.T) {
	t.Parallel()

	n := New(NodeConfig{NodeID: "node-a"})
	n.RegisterTablet("books", 0, "replica-a", &fakeTablet{
		status: fakeStatus(tablet.StateFailed),
		err:    errors.New("tablet is failed"),
	})

	srv := NewServer(n)
	_, err := srv.SearchShard(context.Background(), &searchnodev1.SearchShardRequest{
		Tablet: &searchnodev1.TabletRef{IndexName: "books", ShardId: 0, ReplicaId: "replica-a"},
		Query:  "bleve",
		Limit:  10,
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %s, want %s; err=%v", status.Code(err), codes.Unavailable, err)
	}
}

type fakeTabletStatus string

func (f fakeTabletStatus) Status() tablet.Status {
	return fakeStatus(string(f))
}

func (f fakeTabletStatus) Search(context.Context, tablet.SearchRequest) (tablet.SearchResult, error) {
	return tablet.SearchResult{}, errors.New("search not configured")
}

type fakeTablet struct {
	status tablet.Status
	req    tablet.SearchRequest
	result tablet.SearchResult
	err    error
}

func (f *fakeTablet) Status() tablet.Status {
	return f.status
}

func (f *fakeTablet) Search(_ context.Context, req tablet.SearchRequest) (tablet.SearchResult, error) {
	f.req = req
	if f.err != nil {
		return tablet.SearchResult{}, f.err
	}
	return f.result, nil
}

func fakeStatus(state string) tablet.Status {
	return tablet.Status{
		Identity: tablet.Identity{
			IndexName: "books",
			ShardID:   0,
			ReplicaID: "replica-a",
			NodeID:    "node-a",
		},
		State: state,
	}
}
