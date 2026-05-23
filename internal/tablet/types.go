package tablet

import (
	"errors"

	bleve "github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
)

const (
	// StateOpening reports that a tablet is opening.
	StateOpening = "opening"
	// StateReady reports that a tablet can accept reads and writes.
	StateReady = "ready"
	// StateFailed reports that a tablet hit a lifecycle failure.
	StateFailed = "failed"
	// StateClosed reports that a tablet has been closed.
	StateClosed = "closed"
)

// ErrDocumentNotFound reports that a document ID is not present in the tablet.
var ErrDocumentNotFound = errors.New("document not found")

// Identity identifies one KSTablet shard replica.
type Identity struct {
	IndexName      string
	ShardID        int
	ReplicaID      string
	NodeID         string
	MappingVersion int
}

// Status reports the current KSTablet state.
type Status struct {
	Identity Identity
	State    string
}

// UpsertRequest describes a document write to a KSTablet.
type UpsertRequest struct {
	DocumentID string
	Fields     map[string]any
}

// SearchRequest describes a local KSTablet search.
type SearchRequest struct {
	Query  string
	Limit  int
	Offset int
}

// SearchHit is one local Bleve result from a KSTablet.
type SearchHit struct {
	DocumentID string
	Score      float64
	Fields     map[string]any
}

// SearchResult is the local result set returned by a KSTablet.
type SearchResult struct {
	Total uint64
	Hits  []SearchHit
}

// Config configures a KSTablet instance.
type Config struct {
	RootDir  string
	Identity Identity
	Mapping  *mapping.IndexMappingImpl
}

// DefaultMapping returns the default Bleve mapping for a KSTablet.
func DefaultMapping() *mapping.IndexMappingImpl {
	return bleve.NewIndexMapping()
}
