package coordinator

// CreateIndexRequest is the REST payload for creating a logical index.
type CreateIndexRequest struct {
	Name              string         `json:"name"`
	ShardCount        int            `json:"shardCount"`
	ReplicationFactor int            `json:"replicationFactor"`
	MappingVersion    int            `json:"mappingVersion"`
	Mapping           map[string]any `json:"mapping"`
}

// IndexInfo describes a logical index known by the coordinator.
type IndexInfo struct {
	Name              string         `json:"name"`
	ShardCount        int            `json:"shardCount"`
	ReplicationFactor int            `json:"replicationFactor"`
	MappingVersion    int            `json:"mappingVersion"`
	Mapping           map[string]any `json:"mapping"`
}

// ClusterStatus reports basic coordinator status.
type ClusterStatus struct {
	State        string `json:"state"`
	IndexCount   int    `json:"indexCount"`
	RouteIndexes int    `json:"routeIndexes"`
}

// SearchHit is one document hit returned by KSCoordinator search.
type SearchHit struct {
	DocumentID string  `json:"documentId"`
	Score      float64 `json:"score"`
}

// ShardSearchResult is the local result returned by one routed shard search.
type ShardSearchResult struct {
	Total uint64
	Hits  []SearchHit
}

// SearchResponse is the merged search response returned by KSCoordinator.
type SearchResponse struct {
	Total uint64      `json:"total"`
	Hits  []SearchHit `json:"hits"`
}
