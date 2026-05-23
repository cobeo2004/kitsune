package replay

import "github.com/cobeo2004/kitsune/internal/events"

type documentState struct {
	lastSequence int64
}

type stateTracker struct {
	seen map[string]documentState
}

func newStateTracker() *stateTracker {
	return &stateTracker{seen: make(map[string]documentState)}
}

func (s *stateTracker) stale(evt events.DocumentEvent) bool {
	if evt.Sequence <= 0 {
		return false
	}
	current := s.seen[evt.DocumentID]
	return evt.Sequence <= current.lastSequence
}

func (s *stateTracker) record(evt events.DocumentEvent) {
	if evt.Sequence <= 0 {
		return
	}
	s.seen[evt.DocumentID] = documentState{lastSequence: evt.Sequence}
}
