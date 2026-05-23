package main

import (
	"os"
	"path/filepath"
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
