package events

import "fmt"

// Validate checks that evt is safe for publication or replay.
func Validate(evt DocumentEvent) error {
	if evt.ID == "" {
		return fmt.Errorf("event ID is required")
	}
	if evt.SchemaVersion != CurrentSchemaVersion {
		return fmt.Errorf("event schema version %d is unsupported", evt.SchemaVersion)
	}
	if evt.IndexName == "" {
		return fmt.Errorf("index name is required")
	}
	if evt.ShardID < 0 {
		return fmt.Errorf("shard ID must be non-negative")
	}
	if evt.DocumentID == "" {
		return fmt.Errorf("document ID is required")
	}
	if evt.MappingVersion < 0 {
		return fmt.Errorf("mapping version must be non-negative")
	}
	if evt.Sequence < 0 {
		return fmt.Errorf("sequence must be non-negative")
	}
	if evt.DocumentVersion <= 0 {
		return fmt.Errorf("document version must be positive")
	}

	switch evt.Operation {
	case OperationUpsert:
		if len(evt.Fields) == 0 {
			return fmt.Errorf("upsert fields are required")
		}
	case OperationDelete:
	default:
		return fmt.Errorf("unsupported operation %q", evt.Operation)
	}

	return nil
}
