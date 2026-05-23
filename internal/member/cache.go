package member

import "sync"

// Cache tracks advisory member health.
type Cache struct {
	mu    sync.RWMutex
	nodes map[string]NodeView
}

// NewCache creates an empty advisory health cache.
func NewCache() *Cache {
	return &Cache{nodes: make(map[string]NodeView)}
}

// Update records one node's current advisory view.
func (c *Cache) Update(view NodeView) {
	if view.NodeID == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[view.NodeID] = view
}

// Apply records a normalized membership event.
func (c *Cache) Apply(event Event) {
	view := event.View
	if view.NodeID == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	switch event.Kind {
	case EventLeave:
		current := c.nodes[view.NodeID]
		if view.GRPCAddress == "" {
			view.GRPCAddress = current.GRPCAddress
		}
		view.Health = HealthDead
	case EventJoin:
		if view.Health == "" {
			view.Health = HealthAlive
		}
	case EventUpdate:
		if view.Health == "" {
			view.Health = c.nodes[view.NodeID].Health
		}
	}
	c.nodes[view.NodeID] = view
}

// Get returns a node's advisory view.
func (c *Cache) Get(nodeID string) (NodeView, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	view, ok := c.nodes[nodeID]
	return view, ok
}

// List returns a stable snapshot of all advisory views.
func (c *Cache) List() []NodeView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	nodes := make([]NodeView, 0, len(c.nodes))
	for _, view := range c.nodes {
		nodes = append(nodes, view)
	}
	return nodes
}
