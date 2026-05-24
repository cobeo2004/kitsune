package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	searchnodev1 "github.com/cobeo2004/kitsune/api/searchnode/v1"
	"github.com/cobeo2004/kitsune/internal/coordinator"
	"github.com/cobeo2004/kitsune/internal/events"
	"github.com/cobeo2004/kitsune/internal/member"
	"github.com/cobeo2004/kitsune/internal/metadata"
	"github.com/cobeo2004/kitsune/internal/replay"
	"github.com/cobeo2004/kitsune/internal/searchnode"
	"github.com/cobeo2004/kitsune/internal/tablet"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultShutdownTimeout = 5 * time.Second

type coordinatorConfig struct {
	HTTPAddress     string                   `yaml:"httpAddress"`
	EtcdEndpoints   []string                 `yaml:"etcdEndpoints"`
	NATSURL         string                   `yaml:"natsURL"`
	S3              s3Config                 `yaml:"s3"`
	DocumentMaxByte int64                    `yaml:"documentMaxBytes"`
	StaticConfig    coordinator.StaticConfig `yaml:"staticConfig"`
	Routes          []routeConfig            `yaml:"routes"`
}

type s3Config struct {
	Endpoint        string `yaml:"endpoint"`
	Bucket          string `yaml:"bucket"`
	AccessKeyID     string `yaml:"accessKey"`
	SecretAccessKey string `yaml:"secretKey"`
	SessionToken    string `yaml:"sessionToken"`
	Region          string `yaml:"region"`
	Secure          bool   `yaml:"secure"`
}

type routeConfig struct {
	IndexName   string `yaml:"indexName"`
	ShardID     int    `yaml:"shardID"`
	ReplicaID   string `yaml:"replicaID"`
	NodeID      string `yaml:"nodeID"`
	GRPCAddress string `yaml:"grpcAddress"`
}

type searchNodeConfig struct {
	NodeID         string         `yaml:"nodeID"`
	GRPCAddress    string         `yaml:"grpcAddress"`
	DataDir        string         `yaml:"dataDir"`
	EtcdEndpoints  []string       `yaml:"etcdEndpoints"`
	NATSURL        string         `yaml:"natsURL"`
	MemberlistBind string         `yaml:"memberlistBind"`
	MemberlistJoin []string       `yaml:"memberlistJoin"`
	Tablets        []tabletConfig `yaml:"tablets"`
}

type tabletConfig struct {
	IndexName      string         `yaml:"indexName"`
	ShardID        int            `yaml:"shardID"`
	ReplicaID      string         `yaml:"replicaID"`
	MappingVersion int            `yaml:"mappingVersion"`
	Mapping        map[string]any `yaml:"mapping"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: kitsune <coordinator|search-node> --config <path>")
	}
	switch args[0] {
	case "coordinator":
		return runCoordinator(args[1:])
	case "search-node":
		return runSearchNode(args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runCoordinator(args []string) error {
	fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to coordinator YAML config")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *configPath == "" {
		return errors.New("coordinator config path is required")
	}

	var cfg coordinatorConfig
	if err := loadYAML(*configPath, &cfg); err != nil {
		return err
	}
	if cfg.HTTPAddress == "" {
		cfg.HTTPAddress = ":8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverCfg := coordinator.ServerConfig{
		MaxDocumentBytes: cfg.DocumentMaxByte,
		StaticConfig:     cfg.StaticConfig,
	}
	routes, conns, err := staticRoutes(cfg.Routes)
	if err != nil {
		return err
	}
	defer closeClientConns(conns)
	serverCfg.Routes = routes
	serverCfg.MemberCache = memberCacheFromRoutes(cfg.Routes)
	if len(cfg.EtcdEndpoints) > 0 {
		client, err := clientv3.New(clientv3.Config{Endpoints: cfg.EtcdEndpoints, DialTimeout: 5 * time.Second})
		if err != nil {
			return fmt.Errorf("connect etcd: %w", err)
		}
		defer client.Close()
		manager := metadata.NewEtcdManager(client)
		if err := bootstrapMetadata(ctx, manager, cfg.StaticConfig); err != nil {
			return err
		}
		serverCfg.MetadataManager = manager
	}
	if cfg.NATSURL != "" {
		nc, err := nats.Connect(cfg.NATSURL)
		if err != nil {
			return fmt.Errorf("connect nats: %w", err)
		}
		defer nc.Close()
		js, err := jetstream.New(nc)
		if err != nil {
			return fmt.Errorf("create jetstream context: %w", err)
		}
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     "KITSUNE_DOCUMENTS",
			Subjects: []string{"kitsune.index.*.shard.*.events"},
		}); err != nil {
			return fmt.Errorf("ensure document stream: %w", err)
		}
		serverCfg.EventBus = events.NewNATSBus(js)
	}

	srv := coordinator.NewServer(serverCfg)
	if serverCfg.MetadataManager != nil {
		if err := srv.StartMetadataWatch(ctx); err != nil {
			return fmt.Errorf("start metadata watch: %w", err)
		}
	}
	httpServer := &http.Server{
		Addr:              cfg.HTTPAddress,
		Handler:           srv,
		ReadHeaderTimeout: 5 * time.Second,
	}
	errs := make(chan error, 1)
	go func() {
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errs <- err
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer cancel()
		return httpServer.Shutdown(shutdownCtx)
	case err := <-errs:
		return err
	}
}

func runSearchNode(args []string) error {
	fs := flag.NewFlagSet("search-node", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to search-node YAML config")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *configPath == "" {
		return errors.New("search-node config path is required")
	}

	var cfg searchNodeConfig
	if err := loadYAML(*configPath, &cfg); err != nil {
		return err
	}
	if cfg.NodeID == "" {
		return errors.New("nodeID is required")
	}
	if cfg.GRPCAddress == "" {
		return errors.New("grpcAddress is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var manager metadata.KSMetadataManager
	if len(cfg.EtcdEndpoints) > 0 {
		client, err := clientv3.New(clientv3.Config{Endpoints: cfg.EtcdEndpoints, DialTimeout: 5 * time.Second})
		if err != nil {
			return fmt.Errorf("connect etcd: %w", err)
		}
		defer client.Close()
		manager = metadata.NewEtcdManager(client)
	}

	node := searchnode.New(searchnode.NodeConfig{NodeID: cfg.NodeID})
	opened, err := openConfiguredTablets(ctx, cfg, node, manager)
	if err != nil {
		return err
	}
	defer closeTablets(opened)

	var consumers []jetstream.ConsumeContext
	var natsConn *nats.Conn
	if cfg.NATSURL != "" {
		nc, err := nats.Connect(cfg.NATSURL)
		if err != nil {
			return fmt.Errorf("connect nats: %w", err)
		}
		defer nc.Close()
		natsConn = nc
		js, err := jetstream.New(natsConn)
		if err != nil {
			return fmt.Errorf("create jetstream context: %w", err)
		}
		consumers, err = startTabletConsumers(ctx, cfg.NodeID, js, opened, manager)
		if err != nil {
			return err
		}
		defer stopConsumers(consumers)
	}

	memberManager, err := startMemberlist(cfg)
	if err != nil {
		return err
	}
	if memberManager != nil {
		defer func() { _ = memberManager.Shutdown(defaultShutdownTimeout) }()
	}

	listener, err := net.Listen("tcp", cfg.GRPCAddress)
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	searchnodev1.RegisterSearchNodeServiceServer(grpcServer, searchnode.NewServer(node))

	errs := make(chan error, 1)
	go func() {
		errs <- grpcServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		done := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
			return nil
		case <-time.After(defaultShutdownTimeout):
			grpcServer.Stop()
			return nil
		}
	case err := <-errs:
		return err
	}
}

func loadYAML(path string, out any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %q: %w", path, err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("decode config %q: %w", path, err)
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); err == nil {
		return fmt.Errorf("decode config %q: multiple YAML documents are not supported", path)
	} else if !errors.Is(err, io.EOF) {
		return fmt.Errorf("decode config %q: %w", path, err)
	}
	return nil
}

func staticRoutes(routes []routeConfig) (coordinator.StaticRoutes, []*grpc.ClientConn, error) {
	out := make(coordinator.StaticRoutes)
	conns := make([]*grpc.ClientConn, 0, len(routes))
	for _, route := range routes {
		conn, err := grpc.NewClient(route.GRPCAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			closeClientConns(conns)
			return nil, nil, fmt.Errorf("create grpc client for %s: %w", route.GRPCAddress, err)
		}
		conns = append(conns, conn)
		client := searchnodev1.NewSearchNodeServiceClient(conn)
		out[route.IndexName] = append(out[route.IndexName], coordinator.Route{
			ShardID:   route.ShardID,
			ReplicaID: route.ReplicaID,
			NodeID:    route.NodeID,
			Client: coordinator.NewSearchNodeShardClient(client, &searchnodev1.TabletRef{
				IndexName: route.IndexName,
				ShardId:   int32(route.ShardID),
				ReplicaId: route.ReplicaID,
			}),
		})
	}
	return out, conns, nil
}

func closeClientConns(conns []*grpc.ClientConn) {
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func memberCacheFromRoutes(routes []routeConfig) *member.Cache {
	if len(routes) == 0 {
		return nil
	}
	cache := member.NewCache()
	seen := make(map[string]struct{})
	for _, route := range routes {
		if _, ok := seen[route.NodeID]; ok {
			continue
		}
		seen[route.NodeID] = struct{}{}
		cache.Update(member.NodeView{NodeID: route.NodeID, GRPCAddress: route.GRPCAddress, Health: member.HealthAlive})
	}
	return cache
}

func bootstrapMetadata(ctx context.Context, manager metadata.KSMetadataManager, cfg coordinator.StaticConfig) error {
	for _, index := range cfg.Indexes {
		err := manager.PutIndex(ctx, metadata.IndexRecord{
			SchemaVersion:     1,
			Name:              index.Name,
			ShardCount:        index.ShardCount,
			ReplicationFactor: index.ReplicationFactor,
			MappingVersion:    index.MappingVersion,
			Mapping:           index.Mapping,
		}, 0)
		if err != nil && !errors.Is(err, metadata.ErrRevisionMismatch) {
			return fmt.Errorf("bootstrap index %q: %w", index.Name, err)
		}
	}
	for _, assignment := range cfg.Assignments {
		err := manager.PutShardReplica(ctx, metadata.ShardReplicaRecord{
			IndexName: assignment.IndexName,
			ShardID:   assignment.ShardID,
			ReplicaID: assignment.ReplicaID,
			NodeID:    assignment.NodeID,
		}, 0)
		if err != nil && !errors.Is(err, metadata.ErrRevisionMismatch) {
			return fmt.Errorf("bootstrap shard replica %s/%d/%s: %w", assignment.IndexName, assignment.ShardID, assignment.ReplicaID, err)
		}
	}
	return nil
}

func openConfiguredTablets(ctx context.Context, cfg searchNodeConfig, node *searchnode.Node, manager metadata.KSMetadataManager) ([]*tablet.Tablet, error) {
	opened := make([]*tablet.Tablet, 0, len(cfg.Tablets))
	for _, tabletCfg := range cfg.Tablets {
		tb, err := tablet.Open(ctx, tablet.Config{
			RootDir: cfg.DataDir,
			Identity: tablet.Identity{
				IndexName:      tabletCfg.IndexName,
				ShardID:        tabletCfg.ShardID,
				ReplicaID:      tabletCfg.ReplicaID,
				NodeID:         cfg.NodeID,
				MappingVersion: tabletCfg.MappingVersion,
			},
			Mapping: tablet.DefaultMapping(),
		})
		if err != nil {
			closeTablets(opened)
			return nil, fmt.Errorf("open tablet %s/%d/%s: %w", tabletCfg.IndexName, tabletCfg.ShardID, tabletCfg.ReplicaID, err)
		}
		opened = append(opened, tb)
		node.RegisterTablet(tabletCfg.IndexName, tabletCfg.ShardID, tabletCfg.ReplicaID, tb)
		if manager != nil {
			if err := putReadyTabletStatus(ctx, manager, tb.Status(), 0); err != nil && !errors.Is(err, metadata.ErrRevisionMismatch) {
				closeTablets(opened)
				return nil, err
			}
		}
	}
	return opened, nil
}

func closeTablets(tablets []*tablet.Tablet) {
	for _, tb := range tablets {
		_ = tb.Close()
	}
}

func putReadyTabletStatus(ctx context.Context, manager metadata.KSMetadataManager, status tablet.Status, checkpoint int64) error {
	return manager.PutTabletStatus(ctx, metadata.TabletStatusRecord{
		IndexName:      status.Identity.IndexName,
		ShardID:        status.Identity.ShardID,
		ReplicaID:      status.Identity.ReplicaID,
		NodeID:         status.Identity.NodeID,
		State:          status.State,
		LastCheckpoint: checkpoint,
	}, 0)
}

func startTabletConsumers(ctx context.Context, nodeID string, js jetstream.JetStream, tablets []*tablet.Tablet, manager metadata.KSMetadataManager) ([]jetstream.ConsumeContext, error) {
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "KITSUNE_DOCUMENTS",
		Subjects: []string{"kitsune.index.*.shard.*.events"},
	})
	if err != nil {
		return nil, fmt.Errorf("ensure document stream: %w", err)
	}

	consumers := make([]jetstream.ConsumeContext, 0, len(tablets))
	for _, tb := range tablets {
		status := tb.Status()
		identity := replay.Identity{
			IndexName:      status.Identity.IndexName,
			ShardID:        status.Identity.ShardID,
			ReplicaID:      status.Identity.ReplicaID,
			MappingVersion: status.Identity.MappingVersion,
		}
		applier := replay.NewShardApplier(identity, tb, metadataCheckpointStore{manager: manager})
		consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Durable:       fmt.Sprintf("%s-%s-%d-%s", nodeID, status.Identity.IndexName, status.Identity.ShardID, status.Identity.ReplicaID),
			AckPolicy:     jetstream.AckExplicitPolicy,
			FilterSubject: events.Subject(events.DocumentEvent{IndexName: status.Identity.IndexName, ShardID: status.Identity.ShardID}),
		})
		if err != nil {
			stopConsumers(consumers)
			return nil, fmt.Errorf("create consumer for %s/%d/%s: %w", status.Identity.IndexName, status.Identity.ShardID, status.Identity.ReplicaID, err)
		}
		consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
			if err := applier.ApplyMessage(ctx, jetstreamMessage{msg: msg}); err != nil {
				log.Printf("apply event for %s/%d/%s: %v", status.Identity.IndexName, status.Identity.ShardID, status.Identity.ReplicaID, err)
				_ = msg.Nak()
				return
			}
			if manager != nil {
				checkpoint := int64(0)
				if meta, err := msg.Metadata(); err == nil {
					checkpoint = int64(meta.Sequence.Stream)
				}
				if err := putReadyTabletStatus(ctx, manager, tb.Status(), checkpoint); err != nil {
					log.Printf("update tablet status for %s/%d/%s: %v", status.Identity.IndexName, status.Identity.ShardID, status.Identity.ReplicaID, err)
				}
			}
		})
		if err != nil {
			stopConsumers(consumers)
			return nil, fmt.Errorf("consume events for %s/%d/%s: %w", status.Identity.IndexName, status.Identity.ShardID, status.Identity.ReplicaID, err)
		}
		consumers = append(consumers, consumeCtx)
	}
	return consumers, nil
}

func stopConsumers(consumers []jetstream.ConsumeContext) {
	for _, consumer := range consumers {
		consumer.Stop()
	}
}

type metadataCheckpointStore struct {
	manager metadata.KSMetadataManager
}

func (s metadataCheckpointStore) GetCheckpoint(ctx context.Context, id replay.Identity) (replay.Checkpoint, error) {
	if s.manager == nil {
		return replay.Checkpoint{}, replay.ErrNoCheckpoint
	}
	checkpoint, err := s.manager.GetCheckpoint(ctx, id.IndexName, id.ShardID, id.ReplicaID)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return replay.Checkpoint{}, replay.ErrNoCheckpoint
		}
		return replay.Checkpoint{}, err
	}
	return replay.Checkpoint{
		Sequence: checkpoint.Sequence,
		EventID:  checkpoint.EventID,
		Revision: checkpoint.Revision,
	}, nil
}

func (s metadataCheckpointStore) PutCheckpoint(ctx context.Context, id replay.Identity, checkpoint replay.Checkpoint, expectedRevision int64) error {
	if s.manager == nil {
		return nil
	}
	return s.manager.PutCheckpoint(ctx, metadata.CheckpointRecord{
		IndexName: id.IndexName,
		ShardID:   id.ShardID,
		ReplicaID: id.ReplicaID,
		Sequence:  checkpoint.Sequence,
		EventID:   checkpoint.EventID,
	}, expectedRevision)
}

type jetstreamMessage struct {
	msg jetstream.Msg
}

func (m jetstreamMessage) Event() events.DocumentEvent {
	var evt events.DocumentEvent
	if err := json.Unmarshal(m.msg.Data(), &evt); err != nil {
		return events.DocumentEvent{}
	}
	return evt
}

func (m jetstreamMessage) Sequence() int64 {
	meta, err := m.msg.Metadata()
	if err != nil {
		return 0
	}
	return int64(meta.Sequence.Stream)
}

func (m jetstreamMessage) Ack(ctx context.Context) error {
	return m.msg.DoubleAck(ctx)
}

func startMemberlist(cfg searchNodeConfig) (*member.Manager, error) {
	if cfg.MemberlistBind == "" {
		return nil, nil
	}
	host, port, err := net.SplitHostPort(cfg.MemberlistBind)
	if err != nil {
		return nil, fmt.Errorf("parse memberlistBind: %w", err)
	}
	portValue, err := net.LookupPort("tcp", port)
	if err != nil {
		return nil, fmt.Errorf("parse memberlist port: %w", err)
	}
	manager, err := member.NewManager(member.Config{
		NodeID:      cfg.NodeID,
		GRPCAddress: cfg.GRPCAddress,
		BindAddress: host,
		BindPort:    portValue,
		Join:        cfg.MemberlistJoin,
		LogOutput:   os.Stdout,
	})
	if err != nil {
		return nil, err
	}
	if err := manager.Start(member.Config{}); err != nil {
		return nil, err
	}
	return manager, nil
}
