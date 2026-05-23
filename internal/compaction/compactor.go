package compaction

import (
	"fmt"
	"sort"

	"github.com/cobeo2004/kitsune/internal/events"
)

// SafetyInput describes the checkpoints required before event compaction.
type SafetyInput struct {
	MinimumRequiredSequence int64
	ReplicaCheckpoints      map[string]int64
}

// CanCompact verifies that no known replica still needs pre-compaction events.
func CanCompact(input SafetyInput) error {
	if input.MinimumRequiredSequence < 0 {
		return fmt.Errorf("minimum required sequence must be non-negative")
	}
	if len(input.ReplicaCheckpoints) == 0 {
		return fmt.Errorf("replica checkpoints are required")
	}
	for replicaID, checkpoint := range input.ReplicaCheckpoints {
		if checkpoint < input.MinimumRequiredSequence {
			return fmt.Errorf("replica %s checkpoint %d is behind compaction sequence %d", replicaID, checkpoint, input.MinimumRequiredSequence)
		}
	}
	return nil
}

// CompactEvents removes superseded pre-threshold events while preserving final document state.
func CompactEvents(input SafetyInput, in []events.DocumentEvent) ([]events.DocumentEvent, error) {
	if err := CanCompact(input); err != nil {
		return nil, err
	}

	latestCompacted := make(map[documentKey]events.DocumentEvent)
	out := make([]events.DocumentEvent, 0, len(in))
	for _, evt := range in {
		if evt.Sequence > input.MinimumRequiredSequence {
			out = append(out, evt)
			continue
		}

		key := documentKey{
			indexName:  evt.IndexName,
			shardID:    evt.ShardID,
			documentID: evt.DocumentID,
		}
		current, ok := latestCompacted[key]
		if !ok || evt.Sequence > current.Sequence {
			latestCompacted[key] = evt
		}
	}

	for _, evt := range latestCompacted {
		out = append(out, evt)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Sequence < out[j].Sequence
	})
	return out, nil
}

type documentKey struct {
	indexName  string
	shardID    int
	documentID string
}
