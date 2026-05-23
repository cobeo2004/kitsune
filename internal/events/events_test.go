package events

import "testing"

func TestValidateRejectsMissingIndex(t *testing.T) {
	t.Parallel()

	err := Validate(DocumentEvent{
		ID:            "evt-1",
		SchemaVersion: CurrentSchemaVersion,
		Operation:     OperationUpsert,
		DocumentID:    "doc-1",
		ShardID:       0,
		Sequence:      1,
		Fields:        map[string]any{"title": "Bleve"},
	})
	if err == nil {
		t.Fatal("expected missing index to fail")
	}
}

func TestValidateRejectsUnsupportedSchemaVersion(t *testing.T) {
	t.Parallel()

	err := Validate(DocumentEvent{
		ID:            "evt-1",
		SchemaVersion: 99,
		Operation:     OperationUpsert,
		IndexName:     "books",
		ShardID:       0,
		DocumentID:    "doc-1",
		Sequence:      1,
		Fields:        map[string]any{"title": "Bleve"},
	})
	if err == nil {
		t.Fatal("expected unsupported schema version to fail")
	}
}
