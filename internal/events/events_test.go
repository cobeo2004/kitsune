package events

import "testing"

func TestValidateRejectsMissingIndex(t *testing.T) {
	t.Parallel()

	err := Validate(DocumentEvent{
		ID:         "evt-1",
		Operation:  OperationUpsert,
		DocumentID: "doc-1",
		ShardID:    0,
		Fields:     map[string]any{"title": "Bleve"},
	})
	if err == nil {
		t.Fatal("expected missing index to fail")
	}
}
