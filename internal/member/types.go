package member

// Health is advisory gossip health for one node.
type Health string

const (
	// HealthAlive means memberlist currently considers the node alive.
	HealthAlive Health = "alive"
	// HealthSuspect means memberlist suspects the node is unhealthy.
	HealthSuspect Health = "suspect"
	// HealthDead means memberlist observed the node leave or fail.
	HealthDead Health = "dead"
)

// NodeView is compact advisory metadata gossiped through memberlist.
type NodeView struct {
	NodeID      string `json:"nodeId"`
	GRPCAddress string `json:"grpcAddress"`
	Health      Health `json:"health"`
}

// EventKind describes an advisory membership event.
type EventKind string

const (
	// EventJoin marks a node as alive.
	EventJoin EventKind = "join"
	// EventLeave marks a node as dead.
	EventLeave EventKind = "leave"
	// EventUpdate replaces a node's advisory metadata.
	EventUpdate EventKind = "update"
)

// Event is a normalized memberlist membership update.
type Event struct {
	Kind EventKind
	View NodeView
}
