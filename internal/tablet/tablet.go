package tablet

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	bleve "github.com/blevesearch/bleve/v2"
)

// Tablet is one local KSTablet shard replica backed by one Bleve index.
type Tablet struct {
	mu       sync.Mutex
	id       Identity
	state    string
	index    bleve.Index
	indexDir string
}

// Open opens a KSTablet and creates its local Bleve index.
func Open(ctx context.Context, cfg Config) (*Tablet, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("open tablet context: %w", err)
	}
	if cfg.RootDir == "" {
		return nil, errors.New("root dir is required")
	}
	if err := validateIdentity(cfg.Identity); err != nil {
		return nil, fmt.Errorf("tablet identity: %w", err)
	}
	if cfg.Mapping == nil {
		cfg.Mapping = DefaultMapping()
	}

	tabletDir := filepath.Join(cfg.RootDir, cfg.Identity.IndexName, fmt.Sprintf("shard-%d", cfg.Identity.ShardID), cfg.Identity.ReplicaID)
	if err := os.MkdirAll(tabletDir, 0o755); err != nil {
		return nil, fmt.Errorf("create tablet dir: %w", err)
	}

	meta, exists, err := readMetadata(tabletDir)
	if err != nil {
		return nil, fmt.Errorf("load tablet metadata: %w", err)
	}
	if exists && meta.MappingVersion != cfg.Identity.MappingVersion {
		return nil, fmt.Errorf("mapping version changed from %d to %d", meta.MappingVersion, cfg.Identity.MappingVersion)
	}

	indexDir := filepath.Join(tabletDir, "index.bleve")
	var index bleve.Index
	if _, err := os.Stat(indexDir); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("stat bleve index: %w", err)
		}

		index, err = bleve.New(indexDir, cfg.Mapping)
		if err != nil {
			return nil, fmt.Errorf("create bleve index: %w", err)
		}
		if err := writeMetadata(tabletDir, cfg.Identity); err != nil {
			closeErr := index.Close()
			if closeErr != nil {
				return nil, fmt.Errorf("write metadata after create bleve index: %w; close bleve index: %v", err, closeErr)
			}
			return nil, fmt.Errorf("write metadata after create bleve index: %w", err)
		}
	} else {
		index, err = bleve.Open(indexDir)
		if err != nil {
			return nil, fmt.Errorf("open bleve index: %w", err)
		}
	}

	return &Tablet{
		id:       cfg.Identity,
		state:    StateReady,
		index:    index,
		indexDir: indexDir,
	}, nil
}

// Status returns the current KSTablet status.
func (t *Tablet) Status() Status {
	t.mu.Lock()
	defer t.mu.Unlock()

	return Status{
		Identity: t.id,
		State:    t.state,
	}
}

// Upsert indexes or replaces one document in the KSTablet.
func (t *Tablet) Upsert(ctx context.Context, req UpsertRequest) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("upsert document context: %w", err)
	}
	if req.DocumentID == "" {
		return errors.New("document ID is required")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != StateReady {
		return fmt.Errorf("tablet is %s", t.state)
	}
	if err := t.index.Index(req.DocumentID, req.Fields); err != nil {
		return fmt.Errorf("index document %q: %w", req.DocumentID, err)
	}

	return nil
}

func validateIdentity(id Identity) error {
	if err := validatePathSegment("index name", id.IndexName); err != nil {
		return err
	}
	if id.ShardID < 0 {
		return errors.New("shard ID must be non-negative")
	}
	if err := validatePathSegment("replica ID", id.ReplicaID); err != nil {
		return err
	}
	if id.NodeID == "" {
		return errors.New("node ID is required")
	}
	if id.MappingVersion < 0 {
		return errors.New("mapping version must be non-negative")
	}
	return nil
}

func validatePathSegment(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", name)
	}
	if value == "." || value == ".." || strings.ContainsAny(value, `/\`) {
		return fmt.Errorf("%s must be a single path segment", name)
	}
	return nil
}

// Search runs a local Bleve query against the KSTablet.
func (t *Tablet) Search(ctx context.Context, req SearchRequest) (SearchResult, error) {
	var empty SearchResult

	if ctx == nil {
		return empty, errors.New("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return empty, fmt.Errorf("search tablet context: %w", err)
	}
	if req.Limit <= 0 {
		req.Limit = 10
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != StateReady {
		return empty, fmt.Errorf("tablet is %s", t.state)
	}

	query := bleve.NewQueryStringQuery(req.Query)
	searchReq := bleve.NewSearchRequestOptions(query, req.Limit, req.Offset, false)
	searchReq.Fields = []string{"*"}

	result, err := t.index.SearchInContext(ctx, searchReq)
	if err != nil {
		return empty, fmt.Errorf("search bleve index: %w", err)
	}

	hits := make([]SearchHit, 0, len(result.Hits))
	for _, hit := range result.Hits {
		hits = append(hits, SearchHit{
			DocumentID: hit.ID,
			Score:      hit.Score,
			Fields:     hit.Fields,
		})
	}

	return SearchResult{
		Total: result.Total,
		Hits:  hits,
	}, nil
}

// Delete removes one document from the KSTablet.
func (t *Tablet) Delete(ctx context.Context, documentID string) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("delete document context: %w", err)
	}
	if documentID == "" {
		return errors.New("document ID is required")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != StateReady {
		return fmt.Errorf("tablet is %s", t.state)
	}
	doc, err := t.index.Document(documentID)
	if err != nil {
		return fmt.Errorf("load document %q before delete: %w", documentID, err)
	}
	if doc == nil {
		return fmt.Errorf("%w: %s", ErrDocumentNotFound, documentID)
	}
	if err := t.index.Delete(documentID); err != nil {
		return fmt.Errorf("delete document %q: %w", documentID, err)
	}

	return nil
}

// Close closes the underlying Bleve index. It is safe to call more than once.
func (t *Tablet) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state == StateClosed {
		return nil
	}

	if t.index != nil {
		if err := t.index.Close(); err != nil {
			t.state = StateFailed
			return fmt.Errorf("close bleve index: %w", err)
		}
		t.index = nil
	}

	t.state = StateClosed
	return nil
}
