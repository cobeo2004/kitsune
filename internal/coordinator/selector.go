package coordinator

import "fmt"

// SelectReplica chooses one ready replica from candidates.
func SelectReplica(candidates []ReplicaCandidate) (ReplicaCandidate, error) {
	for _, candidate := range candidates {
		if candidate.State == ReplicaReady {
			return candidate, nil
		}
	}
	if len(candidates) > 0 {
		first := candidates[0]
		return ReplicaCandidate{}, fmt.Errorf("no healthy replica available for %s shard %d", first.IndexName, first.ShardID)
	}
	return ReplicaCandidate{}, fmt.Errorf("no healthy replica available")
}
