package member

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestCacheRecordsNodeHealth(t *testing.T) {
	t.Parallel()

	cache := NewCache()
	cache.Update(NodeView{NodeID: "node-a", GRPCAddress: "127.0.0.1:9001", Health: HealthAlive})
	got, ok := cache.Get("node-a")
	if !ok {
		t.Fatal("node-a missing")
	}
	if got.Health != HealthAlive {
		t.Fatalf("health = %s, want %s", got.Health, HealthAlive)
	}
}

func TestNodeMetaFitsMemberlistLimit(t *testing.T) {
	t.Parallel()

	data, err := EncodeNodeMeta(NodeView{NodeID: "node-a", GRPCAddress: "127.0.0.1:9001", Health: HealthAlive}, 512)
	if err != nil {
		t.Fatalf("encode node meta: %v", err)
	}
	if len(data) > 512 {
		t.Fatalf("metadata length = %d, want <= 512", len(data))
	}
}

func TestDecodeNodeMetaUsesMemberNameAsIdentity(t *testing.T) {
	t.Parallel()

	view := DecodeNodeMeta([]byte(`{"nodeId":"spoofed","grpcAddress":"127.0.0.1:9001","health":"alive"}`), "node-a")
	if view.NodeID != "node-a" {
		t.Fatalf("node ID = %q, want memberlist node name", view.NodeID)
	}
}

func TestCacheAppliesMembershipEventsAsAdvisoryHealth(t *testing.T) {
	t.Parallel()

	cache := NewCache()
	cache.Apply(Event{Kind: EventJoin, View: NodeView{NodeID: "node-a", GRPCAddress: "127.0.0.1:9001", Health: HealthAlive}})
	cache.Apply(Event{Kind: EventLeave, View: NodeView{NodeID: "node-a"}})

	got, ok := cache.Get("node-a")
	if !ok {
		t.Fatal("node-a missing")
	}
	if got.Health != HealthDead {
		t.Fatalf("health = %s, want %s", got.Health, HealthDead)
	}
	if got.GRPCAddress != "127.0.0.1:9001" {
		t.Fatalf("grpc address = %q, want previous address retained", got.GRPCAddress)
	}
}

func TestManagersJoinAndRecordAdvisoryHealth(t *testing.T) {
	t.Parallel()

	portA := freeTCPPort(t)
	portB := freeTCPPort(t)
	cacheA := NewCache()
	managerA, err := NewManager(Config{
		NodeID:      "node-a",
		GRPCAddress: "127.0.0.1:9001",
		BindAddress: "127.0.0.1",
		BindPort:    portA,
		Cache:       cacheA,
		LogOutput:   io.Discard,
	})
	if err != nil {
		t.Fatalf("new manager a: %v", err)
	}
	if err := managerA.Start(Config{
		NodeID:      "node-a",
		GRPCAddress: "127.0.0.1:9001",
		BindAddress: "127.0.0.1",
		BindPort:    portA,
		LogOutput:   io.Discard,
	}); err != nil {
		t.Fatalf("start manager a: %v", err)
	}
	t.Cleanup(func() {
		if err := managerA.Shutdown(time.Second); err != nil {
			t.Errorf("shutdown manager a: %v", err)
		}
	})

	cacheB := NewCache()
	managerB, err := NewManager(Config{
		NodeID:      "node-b",
		GRPCAddress: "127.0.0.1:9002",
		BindAddress: "127.0.0.1",
		BindPort:    portB,
		Cache:       cacheB,
		LogOutput:   io.Discard,
	})
	if err != nil {
		t.Fatalf("new manager b: %v", err)
	}
	if err := managerB.Start(Config{
		NodeID:      "node-b",
		GRPCAddress: "127.0.0.1:9002",
		BindAddress: "127.0.0.1",
		BindPort:    portB,
		Join:        []string{fmt.Sprintf("127.0.0.1:%d", portA)},
		LogOutput:   io.Discard,
	}); err != nil {
		t.Fatalf("start manager b: %v", err)
	}
	t.Cleanup(func() {
		if err := managerB.Shutdown(time.Second); err != nil {
			t.Errorf("shutdown manager b: %v", err)
		}
	})

	eventually(t, func() bool {
		view, ok := cacheA.Get("node-b")
		return ok && view.Health == HealthAlive && view.GRPCAddress == "127.0.0.1:9002"
	})
}

func TestManagerStartRejectsDivergentIdentity(t *testing.T) {
	t.Parallel()

	manager, err := NewManager(Config{NodeID: "node-a", GRPCAddress: "127.0.0.1:9001"})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	err = manager.Start(Config{NodeID: "node-b", GRPCAddress: "127.0.0.1:9001", BindAddress: "127.0.0.1"})
	if err == nil {
		t.Fatal("expected divergent node ID to fail")
	}

	err = manager.Start(Config{NodeID: "node-a", GRPCAddress: "127.0.0.1:9002", BindAddress: "127.0.0.1"})
	if err == nil {
		t.Fatal("expected divergent grpc address to fail")
	}
}

func TestManagerUpdateNodeRejectsDivergentIdentity(t *testing.T) {
	t.Parallel()

	manager, err := NewManager(Config{NodeID: "node-a", GRPCAddress: "127.0.0.1:9001"})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	err = manager.UpdateNode(NodeView{NodeID: "node-b", GRPCAddress: "127.0.0.1:9001", Health: HealthAlive}, time.Second)
	if err == nil {
		t.Fatal("expected divergent node ID to fail")
	}
	err = manager.UpdateNode(NodeView{NodeID: "node-a", GRPCAddress: "127.0.0.1:9002", Health: HealthAlive}, time.Second)
	if err == nil {
		t.Fatal("expected divergent grpc address to fail")
	}
}

func TestManagerStartUsesConstructorConfig(t *testing.T) {
	t.Parallel()

	port := freeTCPPort(t)
	manager, err := NewManager(Config{
		NodeID:      "node-a",
		GRPCAddress: "127.0.0.1:9001",
		BindAddress: "127.0.0.1",
		BindPort:    port,
		LogOutput:   io.Discard,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := manager.Start(Config{}); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	t.Cleanup(func() {
		if err := manager.Shutdown(time.Second); err != nil {
			t.Errorf("shutdown manager: %v", err)
		}
	})
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for free port: %v", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close free port listener: %v", err)
		}
	}()
	return listener.Addr().(*net.TCPAddr).Port
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not satisfied before timeout")
}
