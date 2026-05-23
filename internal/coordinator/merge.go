package coordinator

import "sort"

// MergeResults orders shard hits by score and applies response pagination.
func MergeResults(results []ShardSearchResult, limit, offset int) SearchResponse {
	if limit <= 0 {
		limit = 10
	}

	var total uint64
	var hits []SearchHit
	for _, result := range results {
		total += result.Total
		hits = append(hits, result.Hits...)
	}

	sort.SliceStable(hits, func(i, j int) bool {
		return hits[i].Score > hits[j].Score
	})

	if offset > len(hits) {
		return SearchResponse{Total: total}
	}

	end := offset + limit
	if end > len(hits) {
		end = len(hits)
	}

	return SearchResponse{Total: total, Hits: hits[offset:end]}
}
