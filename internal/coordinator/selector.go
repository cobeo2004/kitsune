package coordinator

import "fmt"

// SelectReplica chooses one ready replica from candidates.
func SelectReplica(candidates []ReplicaCandidate) (ReplicaCandidate, error) {
	for _, candidate := range candidates {
		if candidate.State == ReplicaReady {
			return candidate, nil
		}
	}
	return ReplicaCandidate{}, fmt.Errorf("no healthy replica available")
}
