package coordinator

// ReplicaState is the coordinator-visible state of one shard replica.
type ReplicaState string

const (
	// ReplicaReady can serve search requests.
	ReplicaReady ReplicaState = "ready"
	// ReplicaFailed must not serve search requests.
	ReplicaFailed ReplicaState = "failed"
	// ReplicaRestoring is rebuilding from a snapshot and must not serve search.
	ReplicaRestoring ReplicaState = "restoring"
	// ReplicaReplaying is catching up from events and must not serve search.
	ReplicaReplaying ReplicaState = "replaying"
)

// ReplicaCandidate is one shard replica eligible for selection.
type ReplicaCandidate struct {
	IndexName string
	ShardID   int
	ReplicaID string
	NodeID    string
	State     ReplicaState
	Client    ShardClient
}
