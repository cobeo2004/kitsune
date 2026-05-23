package coordinator

import "testing"

func TestValidateStaticConfigRejectsMissingShardAssignment(t *testing.T) {
	t.Parallel()

	cfg := StaticConfig{
		Indexes: []IndexConfig{
			{Name: "books", ShardCount: 2, ReplicationFactor: 1},
		},
		Assignments: []ShardAssignment{
			{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"},
		},
	}

	err := ValidateStaticConfig(cfg)
	if err == nil {
		t.Fatal("expected missing shard 1 assignment to fail")
	}
}

func TestValidateStaticConfigAcceptsMultipleIndexes(t *testing.T) {
	t.Parallel()

	cfg := StaticConfig{
		Indexes: []IndexConfig{
			{Name: "books", ShardCount: 2, ReplicationFactor: 1},
			{Name: "movies", ShardCount: 1, ReplicationFactor: 2},
		},
		Assignments: []ShardAssignment{
			{IndexName: "books", ShardID: 0, ReplicaID: "books-0-a", NodeID: "node-a"},
			{IndexName: "books", ShardID: 1, ReplicaID: "books-1-a", NodeID: "node-a"},
			{IndexName: "movies", ShardID: 0, ReplicaID: "movies-0-a", NodeID: "node-a"},
			{IndexName: "movies", ShardID: 0, ReplicaID: "movies-0-b", NodeID: "node-b"},
		},
	}

	if err := ValidateStaticConfig(cfg); err != nil {
		t.Fatalf("validate static config: %v", err)
	}
}

func TestValidateStaticConfigRejectsDuplicateReplicaAssignment(t *testing.T) {
	t.Parallel()

	cfg := StaticConfig{
		Indexes: []IndexConfig{
			{Name: "books", ShardCount: 1, ReplicationFactor: 1},
		},
		Assignments: []ShardAssignment{
			{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"},
			{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-b"},
		},
	}

	if err := ValidateStaticConfig(cfg); err == nil {
		t.Fatal("expected duplicate replica assignment to fail")
	}
}

func TestValidateStaticConfigRejectsUnsafeAssignmentIDs(t *testing.T) {
	t.Parallel()

	tests := map[string]ShardAssignment{
		"replica path segment": {IndexName: "books", ShardID: 0, ReplicaID: "replica/a", NodeID: "node-a"},
		"node path segment":    {IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node/a"},
	}

	for name, assignment := range tests {
		assignment := assignment
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := StaticConfig{
				Indexes: []IndexConfig{
					{Name: "books", ShardCount: 1, ReplicationFactor: 1},
				},
				Assignments: []ShardAssignment{assignment},
			}

			if err := ValidateStaticConfig(cfg); err == nil {
				t.Fatal("expected unsafe assignment ID to fail")
			}
		})
	}
}

func TestValidateStaticRoutesRejectsDelimiterCollision(t *testing.T) {
	t.Parallel()

	cfg := StaticConfig{
		Indexes: []IndexConfig{
			{Name: "books", ShardCount: 1, ReplicationFactor: 1},
		},
		Assignments: []ShardAssignment{
			{IndexName: "books", ShardID: 0, ReplicaID: "replica/a", NodeID: "node"},
		},
	}
	routes := StaticRoutes{
		"books": {{ShardID: 0, ReplicaID: "replica", NodeID: "a/node", Client: &fakeShardClient{}}},
	}

	if err := validateRoutesAgainstStaticConfig(routes, cfg); err == nil {
		t.Fatal("expected delimiter-colliding route to fail")
	}
}

func TestNewServerLoadsStaticIndexMappingMetadata(t *testing.T) {
	t.Parallel()

	srv := NewServer(ServerConfig{StaticConfig: StaticConfig{
		Indexes: []IndexConfig{
			{
				Name:              "books",
				ShardCount:        1,
				ReplicationFactor: 1,
				MappingVersion:    7,
				Mapping:           map[string]any{"defaultAnalyzer": "standard"},
			},
		},
		Assignments: []ShardAssignment{
			{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"},
		},
	}})

	info, ok := srv.index("books")
	if !ok {
		t.Fatal("static index was not loaded")
	}
	if info.MappingVersion != 7 {
		t.Fatalf("mapping version = %d, want 7", info.MappingVersion)
	}
	if info.Mapping["defaultAnalyzer"] != "standard" {
		t.Fatalf("mapping = %#v, want defaultAnalyzer standard", info.Mapping)
	}
}

func TestNewServerRejectsInvalidStaticConfig(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Fatal("expected invalid static config to panic")
		}
	}()

	_ = NewServer(ServerConfig{StaticConfig: StaticConfig{
		Indexes: []IndexConfig{
			{Name: "books", ShardCount: 2, ReplicationFactor: 1},
		},
		Assignments: []ShardAssignment{
			{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"},
		},
	}})
}

func TestNewServerRejectsRouteOutsideStaticAssignments(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Fatal("expected route outside static assignment to panic")
		}
	}()

	_ = NewServer(ServerConfig{
		StaticConfig: StaticConfig{
			Indexes: []IndexConfig{
				{Name: "books", ShardCount: 1, ReplicationFactor: 1},
			},
			Assignments: []ShardAssignment{
				{IndexName: "books", ShardID: 0, ReplicaID: "replica-a", NodeID: "node-a"},
			},
		},
		Routes: StaticRoutes{
			"books": {{ShardID: 0, ReplicaID: "replica-b", NodeID: "node-b", Client: &fakeShardClient{}}},
		},
	})
}
