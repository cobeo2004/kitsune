package coordinator

import (
	"context"

	"github.com/cobeo2004/kitsune/internal/metadata"
)

// ShardClient searches one shard replica owned by a KSSearchNode.
type ShardClient interface {
	Search(ctx context.Context, query string, limit, offset int) (ShardSearchResult, error)
}

// Route is one static KSCoordinator route to a shard replica.
type Route struct {
	ShardID   int
	ReplicaID string
	NodeID    string
	State     ReplicaState
	Client    ShardClient
}

// StaticRoutes maps index names to configured shard routes.
type StaticRoutes map[string][]Route

func (r StaticRoutes) routesForIndex(index string) []Route {
	if len(r) == 0 {
		return nil
	}
	return r[index]
}

func (r StaticRoutes) selectedRoutes(index string, shardCount int) ([]Route, bool) {
	routes := r.routesForIndex(index)
	if shardCount <= 0 || len(routes) == 0 {
		return nil, false
	}

	byShard := make(map[int][]ReplicaCandidate, shardCount)
	for _, route := range routes {
		if route.ShardID < 0 || route.ShardID >= shardCount || route.Client == nil {
			continue
		}
		byShard[route.ShardID] = append(byShard[route.ShardID], route.replicaCandidate(index))
	}

	selected := make([]Route, 0, shardCount)
	for shardID := 0; shardID < shardCount; shardID++ {
		candidates, ok := byShard[shardID]
		if !ok {
			return nil, false
		}
		candidate, err := SelectReplica(candidates)
		if err != nil {
			return nil, false
		}
		selected = append(selected, routeFromCandidate(candidate))
	}
	return selected, true
}

func (r Route) replicaCandidate(indexName string) ReplicaCandidate {
	state := r.State
	if state == "" {
		state = ReplicaReady
	}
	return ReplicaCandidate{
		IndexName: indexName,
		ShardID:   r.ShardID,
		ReplicaID: r.ReplicaID,
		NodeID:    r.NodeID,
		State:     state,
		Client:    r.Client,
	}
}

func routeFromCandidate(candidate ReplicaCandidate) Route {
	return Route{
		ShardID:   candidate.ShardID,
		ReplicaID: candidate.ReplicaID,
		NodeID:    candidate.NodeID,
		State:     candidate.State,
		Client:    candidate.Client,
	}
}

func routesFromMetadata(replicas []metadata.ShardReplicaRecord, statuses []metadata.TabletStatusRecord, clients StaticRoutes) StaticRoutes {
	states := statesFromMetadata(statuses)
	routes := make(StaticRoutes)
	for _, replica := range replicas {
		route, ok := routeForReplica(clients, replica)
		if !ok {
			continue
		}
		if state, ok := states[routeAssignmentKey(replica.IndexName, replica.ShardID, replica.ReplicaID, replica.NodeID)]; ok {
			route.State = state
		}
		routes[replica.IndexName] = append(routes[replica.IndexName], route)
	}
	return routes
}

func statesFromMetadata(statuses []metadata.TabletStatusRecord) map[assignmentKey]ReplicaState {
	states := make(map[assignmentKey]ReplicaState, len(statuses))
	for _, status := range statuses {
		states[routeAssignmentKey(status.IndexName, status.ShardID, status.ReplicaID, status.NodeID)] = ReplicaState(status.State)
	}
	return states
}

func routeForReplica(clients StaticRoutes, replica metadata.ShardReplicaRecord) (Route, bool) {
	for _, route := range clients.routesForIndex(replica.IndexName) {
		if route.ShardID == replica.ShardID && route.ReplicaID == replica.ReplicaID && route.NodeID == replica.NodeID && route.Client != nil {
			return route, true
		}
	}
	return Route{}, false
}

func upsertRoute(routes StaticRoutes, indexName string, route Route) StaticRoutes {
	if routes == nil {
		routes = make(StaticRoutes)
	}
	indexRoutes := routes.routesForIndex(indexName)
	for i, existing := range indexRoutes {
		if sameRouteIdentity(existing, route) {
			indexRoutes[i] = route
			routes[indexName] = indexRoutes
			return routes
		}
	}
	routes[indexName] = append(indexRoutes, route)
	return routes
}

func removeRoute(routes StaticRoutes, replica metadata.ShardReplicaRecord) StaticRoutes {
	indexRoutes := routes.routesForIndex(replica.IndexName)
	filtered := indexRoutes[:0]
	for _, route := range indexRoutes {
		if route.ShardID == replica.ShardID && route.ReplicaID == replica.ReplicaID && route.NodeID == replica.NodeID {
			continue
		}
		filtered = append(filtered, route)
	}
	if len(filtered) == 0 {
		delete(routes, replica.IndexName)
		return routes
	}
	routes[replica.IndexName] = filtered
	return routes
}

func updateRouteState(routes StaticRoutes, status metadata.TabletStatusRecord) StaticRoutes {
	indexRoutes := routes.routesForIndex(status.IndexName)
	for i, route := range indexRoutes {
		if route.ShardID == status.ShardID && route.ReplicaID == status.ReplicaID && route.NodeID == status.NodeID {
			indexRoutes[i].State = ReplicaState(status.State)
			routes[status.IndexName] = indexRoutes
			return routes
		}
	}
	return routes
}

func sameRouteIdentity(a, b Route) bool {
	return a.ShardID == b.ShardID && a.ReplicaID == b.ReplicaID && a.NodeID == b.NodeID
}
