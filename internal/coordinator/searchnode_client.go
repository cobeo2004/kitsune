package coordinator

import (
	"context"
	"fmt"

	searchnodev1 "github.com/cobeo2004/kitsune/api/searchnode/v1"
)

// SearchNodeShardClient adapts the generated search-node gRPC client to ShardClient.
type SearchNodeShardClient struct {
	client    searchnodev1.SearchNodeServiceClient
	indexName string
	shardID   int32
	replicaID string
}

// NewSearchNodeShardClient creates a shard client bound to one search-node tablet.
func NewSearchNodeShardClient(client searchnodev1.SearchNodeServiceClient, tablet *searchnodev1.TabletRef) *SearchNodeShardClient {
	var c SearchNodeShardClient
	c.client = client
	if tablet != nil {
		c.indexName = tablet.GetIndexName()
		c.shardID = tablet.GetShardId()
		c.replicaID = tablet.GetReplicaId()
	}
	return &c
}

func (c *SearchNodeShardClient) tabletRef() *searchnodev1.TabletRef {
	return &searchnodev1.TabletRef{
		IndexName: c.indexName,
		ShardId:   c.shardID,
		ReplicaId: c.replicaID,
	}
}

// Search queries the configured search-node tablet over gRPC.
func (c *SearchNodeShardClient) Search(ctx context.Context, query string, limit, offset int) (ShardSearchResult, error) {
	if c.client == nil {
		return ShardSearchResult{}, fmt.Errorf("search node client is required")
	}
	resp, err := c.client.SearchShard(ctx, &searchnodev1.SearchShardRequest{
		Tablet: c.tabletRef(),
		Query:  query,
		Limit:  int32(limit),
		Offset: int32(offset),
	})
	if err != nil {
		return ShardSearchResult{}, fmt.Errorf("search shard: %w", err)
	}

	hits := make([]SearchHit, 0, len(resp.GetHits()))
	for _, hit := range resp.GetHits() {
		hits = append(hits, SearchHit{
			DocumentID: hit.GetDocumentId(),
			Score:      hit.GetScore(),
		})
	}
	return ShardSearchResult{
		Total: resp.GetTotal(),
		Hits:  hits,
	}, nil
}
