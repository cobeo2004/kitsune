package searchnode

import (
	"context"
	"errors"

	searchnodev1 "github.com/cobeo2004/kitsune/api/searchnode/v1"
	"github.com/cobeo2004/kitsune/internal/tablet"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server exposes KSSearchNode tablet operations through internal gRPC.
type Server struct {
	searchnodev1.UnimplementedSearchNodeServiceServer
	node *Node
}

// NewServer creates a SearchNodeService server backed by node.
func NewServer(node *Node) *Server {
	return &Server{node: node}
}

// SearchShard searches one hosted tablet.
func (s *Server) SearchShard(ctx context.Context, req *searchnodev1.SearchShardRequest) (*searchnodev1.SearchShardResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	ref := req.GetTablet()
	if ref == nil {
		return nil, status.Error(codes.InvalidArgument, "tablet is required")
	}

	tb, ok := s.node.tablet(ref.GetIndexName(), int(ref.GetShardId()), ref.GetReplicaId())
	if !ok {
		return nil, status.Error(codes.NotFound, "tablet not found")
	}
	if st := tb.Status(); st.State != tablet.StateReady {
		return nil, status.Errorf(codes.Unavailable, "tablet is %s", st.State)
	}

	result, err := tb.Search(ctx, tablet.SearchRequest{
		Query:  req.GetQuery(),
		Limit:  int(req.GetLimit()),
		Offset: int(req.GetOffset()),
	})
	if err != nil {
		return nil, grpcError(err)
	}

	hits := make([]*searchnodev1.SearchHit, 0, len(result.Hits))
	for _, hit := range result.Hits {
		hits = append(hits, &searchnodev1.SearchHit{
			DocumentId: hit.DocumentID,
			Score:      hit.Score,
		})
	}

	return &searchnodev1.SearchShardResponse{
		Total: result.Total,
		Hits:  hits,
	}, nil
}

func grpcError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// TabletStatus reports hosted tablets.
func (s *Server) TabletStatus(_ context.Context, _ *searchnodev1.TabletStatusRequest) (*searchnodev1.TabletStatusResponse, error) {
	statuses := s.node.Statuses()
	out := make([]*searchnodev1.TabletStatus, 0, len(statuses))
	for _, st := range statuses {
		out = append(out, &searchnodev1.TabletStatus{
			Tablet: &searchnodev1.TabletRef{
				IndexName: st.Identity.IndexName,
				ShardId:   int32(st.Identity.ShardID),
				ReplicaId: st.Identity.ReplicaID,
			},
			NodeId: st.Identity.NodeID,
			State:  st.State,
		})
	}
	return &searchnodev1.TabletStatusResponse{Tablets: out}, nil
}
