package events

import "time"

// Operation names the document mutation represented by an event.
type Operation string

const (
	// OperationUpsert indexes or replaces one document.
	OperationUpsert Operation = "upsert"
	// OperationDelete removes one document.
	OperationDelete Operation = "delete"
)

// DocumentEvent is the durable event envelope for one document mutation.
type DocumentEvent struct {
	ID             string         `json:"id"`
	Operation      Operation      `json:"operation"`
	IndexName      string         `json:"indexName"`
	ShardID        int            `json:"shardId"`
	DocumentID     string         `json:"documentId"`
	DocumentVersion int64          `json:"documentVersion"`
	Fields         map[string]any `json:"fields,omitempty"`
	MappingVersion int            `json:"mappingVersion"`
	Sequence       int64          `json:"sequence"`
	Timestamp      time.Time      `json:"timestamp"`
}
