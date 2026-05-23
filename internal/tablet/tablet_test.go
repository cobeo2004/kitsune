package tablet

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestOpenCreatesReadyTablet(t *testing.T) {
	ctx := context.Background()

	tb := openTestTablet(t, ctx, t.TempDir(), testIdentity())

	status := tb.Status()
	if status.State != StateReady {
		t.Fatalf("state = %q, want %q", status.State, StateReady)
	}
	if status.Identity.IndexName != "books" {
		t.Fatalf("index name = %q, want books", status.Identity.IndexName)
	}
}

func TestUpsertMakesDocumentSearchable(t *testing.T) {
	ctx := context.Background()

	tb := openTestTablet(t, ctx, t.TempDir(), testIdentity())

	err := tb.Upsert(ctx, UpsertRequest{
		DocumentID: "doc-1",
		Fields: map[string]any{
			"title": "Bleve for local search",
			"body":  "Kitsune stores shard replicas in Bleve indexes.",
		},
	})
	if err != nil {
		t.Fatalf("upsert document: %v", err)
	}

	result, err := tb.Search(ctx, SearchRequest{
		Query: "Bleve",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("search tablet: %v", err)
	}
	if result.Total != 1 {
		t.Fatalf("total = %d, want 1", result.Total)
	}
	if len(result.Hits) != 1 {
		t.Fatalf("hits length = %d, want 1", len(result.Hits))
	}
	if result.Hits[0].DocumentID != "doc-1" {
		t.Fatalf("document ID = %q, want doc-1", result.Hits[0].DocumentID)
	}
}

func TestUpsertReplacesDocumentWithSameID(t *testing.T) {
	ctx := context.Background()

	tb := openTestTablet(t, ctx, t.TempDir(), testIdentity())

	if err := tb.Upsert(ctx, UpsertRequest{
		DocumentID: "doc-1",
		Fields: map[string]any{
			"title": "old title",
		},
	}); err != nil {
		t.Fatalf("upsert old document: %v", err)
	}
	if err := tb.Upsert(ctx, UpsertRequest{
		DocumentID: "doc-1",
		Fields: map[string]any{
			"title": "new title",
		},
	}); err != nil {
		t.Fatalf("upsert replacement document: %v", err)
	}

	oldResult, err := tb.Search(ctx, SearchRequest{Query: "old", Limit: 10})
	if err != nil {
		t.Fatalf("search old document: %v", err)
	}
	if oldResult.Total != 0 {
		t.Fatalf("old total = %d, want 0", oldResult.Total)
	}

	newResult, err := tb.Search(ctx, SearchRequest{Query: "new", Limit: 10})
	if err != nil {
		t.Fatalf("search new document: %v", err)
	}
	if newResult.Total != 1 {
		t.Fatalf("new total = %d, want 1", newResult.Total)
	}
	if len(newResult.Hits) != 1 || newResult.Hits[0].DocumentID != "doc-1" {
		t.Fatalf("hits = %#v, want doc-1", newResult.Hits)
	}
}

func TestDeleteRemovesDocumentFromSearch(t *testing.T) {
	ctx := context.Background()

	tb := openTestTablet(t, ctx, t.TempDir(), testIdentity())

	err := tb.Upsert(ctx, UpsertRequest{
		DocumentID: "doc-1",
		Fields: map[string]any{
			"title": "delete me",
		},
	})
	if err != nil {
		t.Fatalf("upsert document: %v", err)
	}

	if err := tb.Delete(ctx, "doc-1"); err != nil {
		t.Fatalf("delete document: %v", err)
	}

	result, err := tb.Search(ctx, SearchRequest{
		Query: "delete",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("search tablet: %v", err)
	}
	if result.Total != 0 {
		t.Fatalf("total = %d, want 0", result.Total)
	}
}

func TestDeleteMissingDocumentReturnsError(t *testing.T) {
	ctx := context.Background()

	tb := openTestTablet(t, ctx, t.TempDir(), testIdentity())

	err := tb.Delete(ctx, "missing-doc")
	if err == nil {
		t.Fatal("delete missing document succeeded")
	}
	if !errors.Is(err, ErrDocumentNotFound) {
		t.Fatalf("error = %v, want ErrDocumentNotFound", err)
	}
}

func TestOpenReopensPersistedTablet(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	id := testIdentity()

	tb := openTestTablet(t, ctx, rootDir, id)
	if err := tb.Upsert(ctx, UpsertRequest{
		DocumentID: "doc-1",
		Fields: map[string]any{
			"title": "persisted document",
		},
	}); err != nil {
		t.Fatalf("upsert document: %v", err)
	}
	if err := tb.Close(); err != nil {
		t.Fatalf("close tablet: %v", err)
	}

	reopened := openTestTablet(t, ctx, rootDir, id)

	result, err := reopened.Search(ctx, SearchRequest{Query: "persisted", Limit: 10})
	if err != nil {
		t.Fatalf("search reopened tablet: %v", err)
	}
	if result.Total != 1 {
		t.Fatalf("total = %d, want 1", result.Total)
	}
	if len(result.Hits) != 1 || result.Hits[0].DocumentID != "doc-1" {
		t.Fatalf("hits = %#v, want doc-1", result.Hits)
	}
}

func TestOpenRejectsMappingVersionChange(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	id := testIdentity()

	tb := openTestTablet(t, ctx, rootDir, id)
	if err := tb.Close(); err != nil {
		t.Fatalf("close tablet: %v", err)
	}

	id.MappingVersion = 2
	_, err := Open(ctx, Config{
		RootDir:  rootDir,
		Identity: id,
		Mapping:  DefaultMapping(),
	})
	if err == nil {
		t.Fatal("open tablet with changed mapping version succeeded")
	}
	if !strings.Contains(err.Error(), "mapping version") {
		t.Fatalf("error = %q, want mapping version error", err)
	}
}

func TestOpenRejectsIdentityPathTraversal(t *testing.T) {
	t.Parallel()

	tests := map[string]func(Identity) Identity{
		"index path segment": func(id Identity) Identity {
			id.IndexName = "books/../movies"
			return id
		},
		"negative shard": func(id Identity) Identity {
			id.ShardID = -1
			return id
		},
		"replica path segment": func(id Identity) Identity {
			id.ReplicaID = "../replica-a"
			return id
		},
	}

	for name, mutate := range tests {
		mutate := mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := Open(context.Background(), Config{
				RootDir:  t.TempDir(),
				Identity: mutate(testIdentity()),
				Mapping:  DefaultMapping(),
			})
			if err == nil {
				t.Fatal("open tablet with unsafe identity succeeded")
			}
			if !strings.Contains(err.Error(), "tablet identity") {
				t.Fatalf("error = %q, want tablet identity error", err)
			}
		})
	}
}

func TestSameDocumentIDIsIsolatedAcrossIndexes(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()

	booksID := testIdentity()
	booksID.IndexName = "books"
	books := openTestTablet(t, ctx, rootDir, booksID)

	moviesID := testIdentity()
	moviesID.IndexName = "movies"
	movies := openTestTablet(t, ctx, rootDir, moviesID)

	if err := books.Upsert(ctx, UpsertRequest{
		DocumentID: "same",
		Fields: map[string]any{
			"title": "Go Search",
		},
	}); err != nil {
		t.Fatalf("upsert books document: %v", err)
	}
	if err := movies.Upsert(ctx, UpsertRequest{
		DocumentID: "same",
		Fields: map[string]any{
			"title": "Action Film",
		},
	}); err != nil {
		t.Fatalf("upsert movies document: %v", err)
	}

	got, err := books.Search(ctx, SearchRequest{Query: "Film", Limit: 10})
	if err != nil {
		t.Fatalf("search books: %v", err)
	}
	if got.Total != 0 {
		t.Fatalf("books total = %d, want 0", got.Total)
	}
}

func TestSameDocumentIDIsIsolatedAcrossIndexesAfterReopen(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()

	booksID := testIdentity()
	booksID.IndexName = "books"
	books := openTestTablet(t, ctx, rootDir, booksID)

	moviesID := testIdentity()
	moviesID.IndexName = "movies"
	movies := openTestTablet(t, ctx, rootDir, moviesID)

	if err := books.Upsert(ctx, UpsertRequest{DocumentID: "same", Fields: map[string]any{"title": "Go Search"}}); err != nil {
		t.Fatalf("upsert books document: %v", err)
	}
	if err := movies.Upsert(ctx, UpsertRequest{DocumentID: "same", Fields: map[string]any{"title": "Action Film"}}); err != nil {
		t.Fatalf("upsert movies document: %v", err)
	}
	if err := books.Close(); err != nil {
		t.Fatalf("close books tablet: %v", err)
	}
	if err := movies.Close(); err != nil {
		t.Fatalf("close movies tablet: %v", err)
	}

	reopenedBooks := openTestTablet(t, ctx, rootDir, booksID)
	booksResult, err := reopenedBooks.Search(ctx, SearchRequest{Query: "Search", Limit: 10})
	if err != nil {
		t.Fatalf("search reopened books: %v", err)
	}
	if booksResult.Total != 1 {
		t.Fatalf("books total = %d, want 1", booksResult.Total)
	}

	filmResult, err := reopenedBooks.Search(ctx, SearchRequest{Query: "Film", Limit: 10})
	if err != nil {
		t.Fatalf("search reopened books for film: %v", err)
	}
	if filmResult.Total != 0 {
		t.Fatalf("books film total = %d, want 0", filmResult.Total)
	}
}

func openTestTablet(t *testing.T, ctx context.Context, rootDir string, id Identity) *Tablet {
	t.Helper()

	tb, err := Open(ctx, Config{
		RootDir:  rootDir,
		Identity: id,
		Mapping:  DefaultMapping(),
	})
	if err != nil {
		t.Fatalf("open tablet: %v", err)
	}
	t.Cleanup(func() {
		if err := tb.Close(); err != nil {
			t.Errorf("close tablet: %v", err)
		}
	})

	return tb
}

func testIdentity() Identity {
	return Identity{
		IndexName:      "books",
		ShardID:        0,
		ReplicaID:      "replica-a",
		NodeID:         "node-a",
		MappingVersion: 1,
	}
}
