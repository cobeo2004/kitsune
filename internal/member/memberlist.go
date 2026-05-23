package member

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

// EncodeNodeMeta encodes compact memberlist node metadata.
func EncodeNodeMeta(view NodeView, limit int) ([]byte, error) {
	data, err := json.Marshal(view)
	if err != nil {
		return nil, fmt.Errorf("encode node metadata: %w", err)
	}
	if limit >= 0 && len(data) > limit {
		return nil, fmt.Errorf("node metadata exceeds memberlist limit: %d > %d", len(data), limit)
	}
	return data, nil
}

// DecodeNodeMeta decodes memberlist node metadata.
func DecodeNodeMeta(data []byte, fallbackNodeID string) NodeView {
	var view NodeView
	if err := json.Unmarshal(data, &view); err != nil {
		return NodeView{NodeID: fallbackNodeID}
	}
	view.NodeID = fallbackNodeID
	return view
}

type delegate struct {
	mu   sync.RWMutex
	view NodeView
}

func newDelegate(view NodeView) *delegate {
	return &delegate{view: view}
}

func (d *delegate) setView(view NodeView) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.view = view
}

func (d *delegate) NodeMeta(limit int) []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()
	data, err := EncodeNodeMeta(d.view, limit)
	if err != nil {
		return nil
	}
	return data
}

func (d *delegate) NotifyMsg([]byte) {}

func (d *delegate) GetBroadcasts(int, int) [][]byte {
	return nil
}

func (d *delegate) LocalState(bool) []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()
	data, _ := EncodeNodeMeta(d.view, -1)
	return data
}

func (d *delegate) MergeRemoteState([]byte, bool) {}

// Config configures a memberlist-backed advisory membership manager.
type Config struct {
	NodeID      string
	GRPCAddress string
	BindAddress string
	BindPort    int
	Join        []string
	Cache       *Cache
	LogOutput   io.Writer
}

// Manager owns one memberlist node and its advisory cache updates.
type Manager struct {
	cache    *Cache
	delegate *delegate
	cfg      Config
	list     *memberlist.Memberlist
}

// NewManager creates a stopped memberlist manager.
func NewManager(cfg Config) (*Manager, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("node ID is required")
	}
	if cfg.GRPCAddress == "" {
		return nil, fmt.Errorf("grpc address is required")
	}
	cache := cfg.Cache
	if cache == nil {
		cache = NewCache()
	}
	view := NodeView{NodeID: cfg.NodeID, GRPCAddress: cfg.GRPCAddress, Health: HealthAlive}
	return &Manager{
		cache:    cache,
		delegate: newDelegate(view),
		cfg:      cfg,
	}, nil
}

// Start creates the local memberlist node and joins configured peers.
func (m *Manager) Start(cfg Config) error {
	if m.list != nil {
		return nil
	}
	cfg = m.mergeConfig(cfg)
	if cfg.NodeID == "" {
		cfg.NodeID = m.cfg.NodeID
	}
	if cfg.GRPCAddress == "" {
		cfg.GRPCAddress = m.cfg.GRPCAddress
	}
	if cfg.NodeID != m.cfg.NodeID {
		return fmt.Errorf("node ID %q does not match manager node ID %q", cfg.NodeID, m.cfg.NodeID)
	}
	if cfg.GRPCAddress != m.cfg.GRPCAddress {
		return fmt.Errorf("grpc address %q does not match manager grpc address %q", cfg.GRPCAddress, m.cfg.GRPCAddress)
	}
	listCfg := memberlist.DefaultLANConfig()
	listCfg.Name = cfg.NodeID
	listCfg.BindAddr = cfg.BindAddress
	listCfg.BindPort = cfg.BindPort
	listCfg.Delegate = m.delegate
	listCfg.Events = eventDelegate{cache: m.cache}
	if cfg.LogOutput != nil {
		listCfg.LogOutput = cfg.LogOutput
	}

	list, err := memberlist.Create(listCfg)
	if err != nil {
		return fmt.Errorf("create memberlist: %w", err)
	}
	m.list = list
	m.cache.Update(NodeView{NodeID: cfg.NodeID, GRPCAddress: cfg.GRPCAddress, Health: HealthAlive})
	if len(cfg.Join) > 0 {
		if _, err := list.Join(cfg.Join); err != nil {
			_ = list.Shutdown()
			m.list = nil
			return fmt.Errorf("join memberlist: %w", err)
		}
	}
	return nil
}

// UpdateNode updates local metadata and broadcasts it to peers.
func (m *Manager) UpdateNode(view NodeView, timeout time.Duration) error {
	if view.NodeID == "" {
		view.NodeID = m.cfg.NodeID
	}
	if view.GRPCAddress == "" {
		view.GRPCAddress = m.cfg.GRPCAddress
	}
	if view.NodeID != m.cfg.NodeID {
		return fmt.Errorf("node ID %q does not match manager node ID %q", view.NodeID, m.cfg.NodeID)
	}
	if view.GRPCAddress != m.cfg.GRPCAddress {
		return fmt.Errorf("grpc address %q does not match manager grpc address %q", view.GRPCAddress, m.cfg.GRPCAddress)
	}
	m.delegate.setView(view)
	m.cache.Update(view)
	if m.list == nil {
		return nil
	}
	if err := m.list.UpdateNode(timeout); err != nil {
		return fmt.Errorf("update memberlist node: %w", err)
	}
	return nil
}

// Shutdown gracefully leaves and shuts down memberlist.
func (m *Manager) Shutdown(timeout time.Duration) error {
	if m.list == nil {
		return nil
	}
	if timeout > 0 {
		_ = m.list.Leave(timeout)
	}
	if err := m.list.Shutdown(); err != nil {
		return fmt.Errorf("shutdown memberlist: %w", err)
	}
	m.list = nil
	return nil
}

// Cache returns the advisory membership cache.
func (m *Manager) Cache() *Cache {
	return m.cache
}

func (m *Manager) mergeConfig(cfg Config) Config {
	if cfg.NodeID == "" {
		cfg.NodeID = m.cfg.NodeID
	}
	if cfg.GRPCAddress == "" {
		cfg.GRPCAddress = m.cfg.GRPCAddress
	}
	if cfg.BindAddress == "" {
		cfg.BindAddress = m.cfg.BindAddress
	}
	if cfg.BindPort == 0 {
		cfg.BindPort = m.cfg.BindPort
	}
	if len(cfg.Join) == 0 {
		cfg.Join = m.cfg.Join
	}
	if cfg.Cache == nil {
		cfg.Cache = m.cfg.Cache
	}
	if cfg.LogOutput == nil {
		cfg.LogOutput = m.cfg.LogOutput
	}
	return cfg
}

type eventDelegate struct {
	cache *Cache
}

func (d eventDelegate) NotifyJoin(node *memberlist.Node) {
	d.cache.Apply(Event{Kind: EventJoin, View: DecodeNodeMeta(node.Meta, node.Name)})
}

func (d eventDelegate) NotifyLeave(node *memberlist.Node) {
	d.cache.Apply(Event{Kind: EventLeave, View: DecodeNodeMeta(node.Meta, node.Name)})
}

func (d eventDelegate) NotifyUpdate(node *memberlist.Node) {
	d.cache.Apply(Event{Kind: EventUpdate, View: DecodeNodeMeta(node.Meta, node.Name)})
}
