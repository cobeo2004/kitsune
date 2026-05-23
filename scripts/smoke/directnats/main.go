package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cobeo2004/kitsune/internal/events"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}
	nc, err := nats.Connect(url)
	must(err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	must(err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "KITSUNE_DOCUMENTS",
		Subjects: []string{"kitsune.index.*.shard.*.events"},
	})
	must(err)
	bus := events.NewNATSBus(js)
	evt := events.DocumentEvent{
		ID:              "smoke-direct-1",
		SchemaVersion:   events.CurrentSchemaVersion,
		Operation:       events.OperationUpsert,
		IndexName:       "books",
		ShardID:         0,
		DocumentID:      "direct-1",
		DocumentVersion: 1,
		MappingVersion:  1,
		Sequence:        1,
		Timestamp:       time.Now().UTC(),
		Fields:          map[string]any{"title": "Direct NATS publish"},
	}
	must(bus.Publish(ctx, evt))
	fmt.Println("published direct NATS document event")
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
