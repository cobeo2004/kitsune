package coordinator

import (
	"context"
	"fmt"

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

func cloneStaticRoutes(routes StaticRoutes) StaticRoutes {
	if routes == nil {
		return nil
	}
	cloned := make(StaticRoutes, len(routes))
	for index, indexRoutes := range routes {
		cloned[index] = append([]Route(nil), indexRoutes...)
	}
	return cloned
}

func (r StaticRoutes) routesForIndex(index string) []Route {
	if len(r) == 0 {
		return nil
	}
	return r[index]
}

func (r StaticRoutes) selectedRoutes(index string, shardCount int) ([]Route, error) {
	groups, err := r.readyRoutesByShard(index, shardCount)
	if err != nil {
		return nil, err
	}

	selected := make([]Route, 0, len(groups))
	for _, routes := range groups {
		selected = append(selected, routes[0])
	}
	return selected, nil
}

func (r StaticRoutes) readyRoutesByShard(index string, shardCount int) ([][]Route, error) {
	routes := r.routesForIndex(index)
	if shardCount <= 0 || len(routes) == 0 {
		return nil, fmt.Errorf("no healthy replica available for %s shard 0", index)
	}

	byShard := make(map[int][]ReplicaCandidate, shardCount)
	for _, route := range routes {
		if route.ShardID < 0 || route.ShardID >= shardCount || route.Client == nil {
			continue
		}
		byShard[route.ShardID] = append(byShard[route.ShardID], route.replicaCandidate(index))
	}

	groups := make([][]Route, 0, shardCount)
	for shardID := 0; shardID < shardCount; shardID++ {
		candidates, ok := byShard[shardID]
		if !ok {
			return nil, fmt.Errorf("no healthy replica available for %s shard %d", index, shardID)
		}

		ready := make([]Route, 0, len(candidates))
		for _, candidate := range candidates {
			if candidate.State == ReplicaReady {
				ready = append(ready, routeFromCandidate(candidate))
			}
		}
		if len(ready) == 0 {
			if _, err := SelectReplica(candidates); err != nil {
				return nil, err
			}
		}
		groups = append(groups, ready)
	}
	return groups, nil
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
		if status, ok := states[routeAssignmentKey(replica.IndexName, replica.ShardID, replica.ReplicaID)]; ok && status.NodeID == route.NodeID {
			route.State = ReplicaState(status.State)
		} else {
			route.State = ReplicaUnknown
		}
		routes[replica.IndexName] = append(routes[replica.IndexName], route)
	}
	return routes
}

func statesFromMetadata(statuses []metadata.TabletStatusRecord) map[assignmentKey]metadata.TabletStatusRecord {
	states := make(map[assignmentKey]metadata.TabletStatusRecord, len(statuses))
	for _, status := range statuses {
		states[routeAssignmentKey(status.IndexName, status.ShardID, status.ReplicaID)] = status
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
	filtered := indexRoutes[:0]
	replaced := false
	for _, existing := range indexRoutes {
		if sameRouteIdentity(existing, route) {
			if !replaced {
				filtered = append(filtered, route)
				replaced = true
			}
			continue
		}
		filtered = append(filtered, existing)
	}
	if !replaced {
		filtered = append(filtered, route)
	}
	routes[indexName] = filtered
	return routes
}

func removeRoute(routes StaticRoutes, replica metadata.ShardReplicaRecord) StaticRoutes {
	indexRoutes := routes.routesForIndex(replica.IndexName)
	filtered := indexRoutes[:0]
	for _, route := range indexRoutes {
		if route.ShardID == replica.ShardID && route.ReplicaID == replica.ReplicaID {
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
		if route.ShardID == status.ShardID && route.ReplicaID == status.ReplicaID {
			if route.NodeID == status.NodeID {
				indexRoutes[i].State = ReplicaState(status.State)
			} else {
				indexRoutes[i].State = ReplicaUnknown
			}
			routes[status.IndexName] = indexRoutes
			return routes
		}
	}
	return routes
}

func sameRouteIdentity(a, b Route) bool {
	return a.ShardID == b.ShardID && a.ReplicaID == b.ReplicaID
}
