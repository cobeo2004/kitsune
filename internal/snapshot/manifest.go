package snapshot

import "fmt"

// Manifest describes one durable shard snapshot package.
type Manifest struct {
	IndexName          string `json:"indexName"`
	ShardID            int    `json:"shardId"`
	ReplicaSourceNode  string `json:"replicaSourceNode"`
	SnapshotGeneration int64  `json:"snapshotGeneration"`
	MappingVersion     int    `json:"mappingVersion"`
	LastEventID        string `json:"lastEventId,omitempty"`
	LastEventSequence  int64  `json:"lastEventSequence"`
	CreatedUnix        int64  `json:"createdUnix"`
	Compression        string `json:"compression,omitempty"`
	ChecksumSHA256     string `json:"checksumSha256"`
}

// Validate checks that the manifest is safe to persist or restore.
func (m Manifest) Validate() error {
	if m.IndexName == "" {
		return fmt.Errorf("index name is required")
	}
	if m.ShardID < 0 {
		return fmt.Errorf("shard ID must be non-negative")
	}
	if m.ReplicaSourceNode == "" {
		return fmt.Errorf("replica source node is required")
	}
	if m.SnapshotGeneration <= 0 {
		return fmt.Errorf("snapshot generation must be positive")
	}
	if m.MappingVersion < 0 {
		return fmt.Errorf("mapping version must be non-negative")
	}
	if m.LastEventID == "" {
		return fmt.Errorf("last event ID is required")
	}
	if m.LastEventSequence < 0 {
		return fmt.Errorf("last event sequence must be non-negative")
	}
	if m.CreatedUnix <= 0 {
		return fmt.Errorf("created time is required")
	}
	if m.Compression != "" && m.Compression != CompressionGzip {
		return fmt.Errorf("unsupported snapshot compression %q", m.Compression)
	}
	if m.ChecksumSHA256 == "" {
		return fmt.Errorf("checksum is required")
	}
	return nil
}
