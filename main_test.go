package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cobeo2004/kitsune/internal/coordinator"
)

func TestRunRejectsUnknownCommand(t *testing.T) {
	t.Parallel()

	if err := run([]string{"unknown"}); err == nil {
		t.Fatal("expected unknown command to fail")
	}
}

func TestLoadCoordinatorConfig(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "coordinator.yaml")
	err := os.WriteFile(path, []byte("httpAddress: ':8080'\ndocumentMaxBytes: 1048576\n"), 0o600)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}

	var cfg coordinatorConfig
	if err := loadYAML(path, &cfg); err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.HTTPAddress != ":8080" || cfg.DocumentMaxByte != 1048576 {
		t.Fatalf("config = %#v", cfg)
	}
}

func TestLoadSearchNodeConfig(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "node.yaml")
	err := os.WriteFile(path, []byte("nodeID: node-a\ngrpcAddress: ':9001'\ndataDir: /data/kitsune\n"), 0o600)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}

	var cfg searchNodeConfig
	if err := loadYAML(path, &cfg); err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.NodeID != "node-a" || cfg.GRPCAddress != ":9001" || cfg.DataDir != "/data/kitsune" {
		t.Fatalf("config = %#v", cfg)
	}
}

func TestLoadYAMLRejectsUnknownFields(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "coordinator.yaml")
	err := os.WriteFile(path, []byte("httpAddress: ':8080'\nunknown: value\n"), 0o600)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}

	var cfg coordinatorConfig
	if err := loadYAML(path, &cfg); err == nil {
		t.Fatal("expected unknown field to fail")
	}
}

func TestLoadYAMLRejectsMultipleDocuments(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "coordinator.yaml")
	err := os.WriteFile(path, []byte("httpAddress: ':8080'\n---\nunknown: value\n"), 0o600)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}

	var cfg coordinatorConfig
	if err := loadYAML(path, &cfg); err == nil {
		t.Fatal("expected multiple documents to fail")
	}
}

func TestLocalDeployConfigsLoad(t *testing.T) {
	t.Parallel()

	var coordinatorCfg coordinatorConfig
	if err := loadYAML("deploy/local/config/coordinator.yaml", &coordinatorCfg); err != nil {
		t.Fatalf("load coordinator config: %v", err)
	}
	if len(coordinatorCfg.StaticConfig.Indexes) != 1 {
		t.Fatalf("indexes = %d, want 1", len(coordinatorCfg.StaticConfig.Indexes))
	}
	if len(coordinatorCfg.Routes) != 6 {
		t.Fatalf("routes = %d, want 6", len(coordinatorCfg.Routes))
	}
	if coordinatorCfg.MinIO.Endpoint != "minio:9000" || coordinatorCfg.MinIO.Bucket == "" {
		t.Fatalf("minio config = %#v, want endpoint and bucket", coordinatorCfg.MinIO)
	}
	if err := coordinator.ValidateStaticConfig(coordinatorCfg.StaticConfig); err != nil {
		t.Fatalf("validate static config: %v", err)
	}
	for _, path := range []string{
		"deploy/local/config/search-node-a.yaml",
		"deploy/local/config/search-node-b.yaml",
		"deploy/local/config/search-node-c.yaml",
	} {
		var nodeCfg searchNodeConfig
		if err := loadYAML(path, &nodeCfg); err != nil {
			t.Fatalf("load %s: %v", path, err)
		}
		if len(nodeCfg.Tablets) != 2 {
			t.Fatalf("%s tablets = %d, want 2", path, len(nodeCfg.Tablets))
		}
	}
}

func TestLocalDeployTopologyMatchesCompose(t *testing.T) {
	t.Parallel()

	var compose composeFile
	if err := loadYAML("deploy/local/compose.yaml", &compose); err != nil {
		t.Fatalf("load compose: %v", err)
	}

	wantServices := []string{"coordinator", "etcd", "minio", "nats", "search-node-a", "search-node-b", "search-node-c"}
	for _, service := range wantServices {
		if _, ok := compose.Services[service]; !ok {
			t.Fatalf("compose missing service %q", service)
		}
	}

	var coordinatorCfg coordinatorConfig
	coordinatorService := compose.Services["coordinator"]
	if commandConfigPath(coordinatorService.Command) != "/config/coordinator.yaml" {
		t.Fatalf("coordinator command = %v, want /config/coordinator.yaml", coordinatorService.Command)
	}
	if coordinatorService.Build["context"] != "../.." {
		t.Fatalf("coordinator build context = %q, want ../..", coordinatorService.Build["context"])
	}
	assertServiceDependsHealthy(t, coordinatorService, "etcd", "nats", "minio")
	if !slices.Contains(coordinatorService.Ports, "8080:8080") {
		t.Fatalf("coordinator ports = %v, want 8080:8080", coordinatorService.Ports)
	}
	if err := loadYAML(composeConfigPath(commandConfigPath(coordinatorService.Command)), &coordinatorCfg); err != nil {
		t.Fatalf("load coordinator config: %v", err)
	}

	nodeConfigs := map[string]searchNodeConfig{}
	for _, service := range []string{"search-node-a", "search-node-b", "search-node-c"} {
		serviceCfg := compose.Services[service]
		configPath := commandConfigPath(serviceCfg.Command)
		if configPath == "" {
			t.Fatalf("%s command = %v, want --config path", service, serviceCfg.Command)
		}
		if serviceCfg.Build["context"] != "../.." {
			t.Fatalf("%s build context = %q, want ../..", service, serviceCfg.Build["context"])
		}
		assertServiceDependsHealthy(t, serviceCfg, "etcd", "nats", "minio")
		var nodeCfg searchNodeConfig
		if err := loadYAML(composeConfigPath(configPath), &nodeCfg); err != nil {
			t.Fatalf("load %s config: %v", service, err)
		}
		if !slices.Contains(serviceCfg.Volumes, "./config:/config:ro") {
			t.Fatalf("%s volumes = %v, want config mount", service, serviceCfg.Volumes)
		}
		if nodeCfg.DataDir != "/data/kitsune" || !hasVolumeTarget(serviceCfg.Volumes, nodeCfg.DataDir) {
			t.Fatalf("%s dataDir=%q volumes=%v, want data volume mounted at dataDir", service, nodeCfg.DataDir, serviceCfg.Volumes)
		}
		nodeConfigs[nodeCfg.NodeID] = nodeCfg
	}

	for _, route := range coordinatorCfg.Routes {
		nodeCfg, ok := nodeConfigs[route.NodeID]
		if !ok {
			t.Fatalf("route references missing node %q", route.NodeID)
		}
		if route.GRPCAddress != serviceAddress(route.NodeID, nodeCfg.GRPCAddress) {
			t.Fatalf("route for %s/%d/%s address=%q, want service address for %s %q", route.IndexName, route.ShardID, route.ReplicaID, route.GRPCAddress, route.NodeID, nodeCfg.GRPCAddress)
		}
		if !nodeHasTablet(nodeCfg, route.IndexName, route.ShardID, route.ReplicaID) {
			t.Fatalf("route %s/%d/%s points at node %q without matching tablet", route.IndexName, route.ShardID, route.ReplicaID, route.NodeID)
		}
	}
}

type composeFile struct {
	Services map[string]composeService `yaml:"services"`
	Volumes  map[string]any            `yaml:"volumes"`
}

type composeService struct {
	Build       map[string]string            `yaml:"build"`
	Command     []string                     `yaml:"command"`
	DependsOn   map[string]map[string]string `yaml:"depends_on"`
	Environment map[string]string            `yaml:"environment"`
	Healthcheck map[string]any               `yaml:"healthcheck"`
	Image       string                       `yaml:"image"`
	Ports       []string                     `yaml:"ports"`
	Volumes     []string                     `yaml:"volumes"`
}

func commandConfigPath(command []string) string {
	for i, arg := range command {
		if arg == "--config" && i+1 < len(command) {
			return command[i+1]
		}
	}
	return ""
}

func assertServiceDependsHealthy(t *testing.T, serviceCfg composeService, dependencies ...string) {
	t.Helper()

	for _, dependency := range dependencies {
		condition := serviceCfg.DependsOn[dependency]["condition"]
		if condition != "service_healthy" {
			t.Fatalf("depends_on[%s] condition = %q, want service_healthy", dependency, condition)
		}
	}
}

func composeConfigPath(path string) string {
	return filepath.Join("deploy", "local", strings.TrimPrefix(path, "/"))
}

func hasVolumeTarget(volumes []string, target string) bool {
	for _, volume := range volumes {
		if strings.HasSuffix(volume, ":"+target) {
			return true
		}
	}
	return false
}

func nodeHasTablet(cfg searchNodeConfig, indexName string, shardID int, replicaID string) bool {
	for _, tablet := range cfg.Tablets {
		if tablet.IndexName == indexName && tablet.ShardID == shardID && tablet.ReplicaID == replicaID {
			return true
		}
	}
	return false
}

func serviceAddress(nodeID, grpcAddress string) string {
	service := strings.ReplaceAll(nodeID, "node-", "search-node-")
	return service + grpcAddress
}
