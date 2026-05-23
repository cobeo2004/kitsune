package replay

import "github.com/cobeo2004/kitsune/internal/events"

type documentState struct {
	lastDocumentVersion int64
}

type stateTracker struct {
	seen map[documentKey]documentState
}

type documentKey struct {
	indexName  string
	shardID    int
	documentID string
}

func newStateTracker() *stateTracker {
	return &stateTracker{seen: make(map[documentKey]documentState)}
}

func (s *stateTracker) stale(evt events.DocumentEvent) bool {
	current := s.seen[keyForEvent(evt)]
	return evt.DocumentVersion <= current.lastDocumentVersion
}

func (s *stateTracker) record(evt events.DocumentEvent) {
	s.seen[keyForEvent(evt)] = documentState{lastDocumentVersion: evt.DocumentVersion}
}

func keyForEvent(evt events.DocumentEvent) documentKey {
	return documentKey{
		indexName:  evt.IndexName,
		shardID:    evt.ShardID,
		documentID: evt.DocumentID,
	}
}
